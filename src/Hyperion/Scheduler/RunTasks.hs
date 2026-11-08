{-# LANGUAGE DeriveAnyClass        #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE NoFieldSelectors      #-}
{-# LANGUAGE OverloadedRecordDot   #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE StaticPointers        #-}

module Hyperion.Scheduler.RunTasks
(runTasks)
where

import Control.Concurrent.STM                       (TVar, atomically, check,
                                                     newTVarIO, readTVar,
                                                     writeTVar)
import Control.Concurrent.Utils                     (Lock, mkExclusiveLock,
                                                     withLock)
import Control.DeepSeq                              (force)
import Control.Distributed.Process                  (getSelfPid)
import Control.Distributed.Process.Async            (Async)
import Control.Distributed.Process.Async            qualified as Async
import Control.Exception                            (evaluate)
import Control.Monad                                (foldM, forM_, forever,
                                                     unless, when)
import Control.Monad.Catch                          (Handler (..), catches)
import Control.Monad.IO.Class                       (liftIO)
import Control.Monad.Reader                         (lift)
import Control.Monad.Writer                         (Writer, runWriter, tell)
import Data.List.Extra                              (nubOrd, partition)
import Data.List.NonEmpty                           (NonEmpty (..))
import Data.List.NonEmpty                           qualified as NonEmpty
import Data.Map.Strict                              (Map, (!?))
import Data.Map.Strict                              qualified as Map
import Data.Maybe                                   (catMaybes, fromMaybe,
                                                     isNothing, listToMaybe)
import Data.Set                                     (Set)
import Data.Set                                     qualified as Set
import Data.Time.Clock                              (addUTCTime, diffUTCTime,
                                                     getCurrentTime)
import Hyperion                                     (Job, Process, RemoteError)
import Hyperion.Log                                 qualified as Log
import Hyperion.Scheduler.ConcurrentQueue           (ConcurrentQueue,
                                                     flushQueue, newQueue,
                                                     readAndFlushQueue,
                                                     readQueue, writeListQueue,
                                                     writeQueue)
import Hyperion.Scheduler.Config                    (Config (..))
import Hyperion.Scheduler.FilePath                  (ClusterFilePath (..),
                                                     VirtualFilePath (..),
                                                     isNodeLocal,
                                                     toClusterFilePath)
import Hyperion.Scheduler.FileService               (FileService, Response (..),
                                                     decrementActiveFileUsages,
                                                     deleteFilesFromAllNodes,
                                                     deleteGlobalFiles,
                                                     fetchFilesToNode,
                                                     incrementActiveFileUsages,
                                                     registerFilesOnNode,
                                                     reserveFilesOnNode,
                                                     withFileService)
import Hyperion.Scheduler.RemoteUtil                (AsyncFailedException (..),
                                                     asyncLinkedLocalJob,
                                                     getJobNodes,
                                                     throwOnAsyncFailed)
import Hyperion.Scheduler.RunTasks.NodeStatus       (NodeStatus)
import Hyperion.Scheduler.RunTasks.NodeStatus       qualified as NodeStatus
import Hyperion.Scheduler.RunTasks.ProgressMap      (ProgressMap)
import Hyperion.Scheduler.RunTasks.ProgressMap      qualified as ProgressMap
import Hyperion.Scheduler.RunTasks.RemoteRunTask    (RemoteRunTaskResult (..),
                                                     remoteRunTask)
import Hyperion.Scheduler.RunTasks.Report           (reportAfterRun,
                                                     reportBeforeRun)
import Hyperion.Scheduler.RunTasks.Shared           (Shared (..), newShared,
                                                     readShared)
import Hyperion.Scheduler.RunTasks.Shared           qualified as Shared
import Hyperion.Scheduler.RunTasks.TaskDistribution (CPUAllocation,
                                                     allocateCpusToTasks,
                                                     distributeTasksToNodesWithScores)
import Hyperion.Scheduler.RunTasks.TaskPriority     (TaskPriority,
                                                     mkTaskPriorityHelper,
                                                     taskPriority)
import Hyperion.Scheduler.RunTasks.TChangeNotifier  (TChangeNotifier,
                                                     newChangeNotifierIO,
                                                     notifyChangeM,
                                                     runWithRetry)
import Hyperion.Scheduler.StatKey                   (SizedTaskFile (..),
                                                     TaskFile (..))
import Hyperion.Scheduler.Stats                     (TaskRecord (..),
                                                     outputFileSizes,
                                                     taskEstimatesAt)
import Hyperion.Scheduler.Task                      (EstimatedTask,
                                                     EstimatedTaskMap,
                                                     IsTask (..), RunStage (..),
                                                     TaskEstimation (..),
                                                     TaskMap, TaskShape (..),
                                                     estimatedTasks,
                                                     originalTask,
                                                     taskEstimation,
                                                     taskInputPaths, taskInputs,
                                                     taskMemoryCapped,
                                                     taskOutputPaths,
                                                     taskOutputs, taskStatKey,
                                                     validateNodeLocalInputs)
import Hyperion.Scheduler.TaskGraph                 (TaskGraph)
import Hyperion.Scheduler.TaskGraph                 qualified as TaskGraph
import Hyperion.Scheduler.TPrioQueue                (TPrioQueue)
import Hyperion.Scheduler.TPrioQueue                qualified as TPrioQueue
import Hyperion.Scheduler.Types                     (Estimate (..),
                                                     FileSize (..),
                                                     MemorySize (..), Node (..),
                                                     NumCPUs,
                                                     schedulingEstimate)
import Hyperion.Scheduler.WorkerPool                (TWorker (workerId),
                                                     WorkerPool,
                                                     checkTaskLaunch,
                                                     toReusableWorker,
                                                     withWorkerPool,
                                                     withWorkersFromPool,
                                                     workerCpuIdToString)

-- Each task info has input and output files stored as VirtualFilePath's, i.e. global paths like /expanse/lustre/path/to/task.output
-- In fact, tasks will write their output either to that location (GlobalFilePath), or to a Local storage (NodeLocalFilePath), depending on Scheduler storage policy.
-- Scheduler will store this mapping in a FilePathResolveMap.
-- TODO: use e.g. use Control.Concurrent.Map from https://hackage.haskell.org/package/ctrie-0.2 instead of Shared IO (Map...)?
type FilePathResolveMap = Map VirtualFilePath ClusterFilePath
type FileSizeMap = Map VirtualFilePath FileSize

-- `StallingAt task` means that node is idle
-- but cannot dequeue the task from taskQueue
-- e.g. because local storage is running out of space.
data IsNodeStalling a = NotStalling | StallingAt a
  deriving (Eq, Ord)

-- If all nodes are stalling at the same task (the first task in the taskQueue), then we cannot progress.
-- NB: if some node reports `StallingAt somePreviousTask`, then it is not global stalling yet.
-- allNodesStalling returns False in this case.
allNodesStalling :: Ord a => [IsNodeStalling a] -> Bool
allNodesStalling statuses = case nubOrd statuses of
  [StallingAt _] -> True
  _              -> False

-- | Handle tasks for a single node. When the NodeStatus in
-- 'nodeStatusVar' changes, dequeue as many tasks as will fit given
-- memory and CPU constraints, and run them on the node, updating
-- NodeStatus to account for the new tasks.
--
-- Should we do this atomically?  Well let's imagine that the
-- NodeStatus changes while we are dequeuing tasks. Then we should
-- just dequeue more tasks. We update the NodeStatus when we start the
-- tasks. So, no. We do not need to do things atomically.
runNodeLoop
  :: forall a . IsTask a
  => Config
  -> Node
  -> FileService
  -> WorkerPool
  -> Shared IO NodeStatus
  -> TVar (IsNodeStalling (EstimatedTask a))
  -> TPrioQueue TaskPriority (EstimatedTask a)
  -> TChangeNotifier
  -> Lock
  -> Shared IO FileSizeMap
  -> Shared IO FilePathResolveMap
  -> ConcurrentQueue (EstimatedTask a)
  -> ConcurrentQueue (TaskRecord (EstimatedTask a))
  -> CPUAllocation (EstimatedTask a)
  -> Job ()
runNodeLoop
  config
  node
  fileService
  workerPool
  nodeStatusVar
  isNodeStallingVar
  taskQueue
  taskQueueNotifier
  taskQueueLock
  fileSizeMapVar
  pathResolveMapVar
  finishedTaskQueue
  taskRecordQueue
  initialAllocation = do

  forM_ (Map.keys initialAllocation) $ \task -> do
     (response, localFileSizes) <- lift $ reserveLocalTaskFiles task
     case response of
      ReserveSuccess -> pure ()
      _ -> Log.throwError $
        "Failed to reserve local disk space for a task from initial allocation: " <> show
          ( response
          , node
          , taskTag task
          , taskOutputPaths task
          , localFileSizes
          )

  runRemoteTasks initialAllocation
  forever getAndRunTasks

  where
    reserveLocalTaskFiles
      :: EstimatedTask a -> Process (Response, Map VirtualFilePath FileSize)
    reserveLocalTaskFiles t = do
      let localFiles = Set.filter (isNodeLocal config) $ Set.union (taskInputPaths t) (taskOutputPaths t)
      localFileSizes <- liftIO $ Shared.withRead fileSizeMapVar $ flip Map.restrictKeys localFiles
      response <- reserveFilesOnNode fileService localFileSizes node.address
      return (response, localFileSizes)

    -- Try to take the first element of the taskQueue.
    -- In case of failure (no tasks available, or node does not have enough free CPUs, memory or local disk space),
    -- wait and retry when notified by taskQueueNotifier
    -- (i.e. when either other tasks have finished, or other nodes have taken things from the taskQueue).
    -- Finally, return that first element.
    blockUntilNewTask :: Job (EstimatedTask a)
    blockUntilNewTask = runWithRetry taskQueueNotifier $ do
      status <- liftIO $ readShared nodeStatusVar
      let
        freeMem  = node.memory - status.memoryInUse
        freeCpus = node.cpus   - status.cpusInUse
      if freeCpus > 0 then do
        (maybeTask, mNextTaskInQueue) <- dequeueTask freeMem freeCpus

        -- Node is stalling if it is empty and cannot take the next task from taskQueue
        liftIO $ atomically $ writeTVar isNodeStallingVar $
          if status == NodeStatus.empty && isNothing maybeTask then
            case mNextTaskInQueue of
              Just t  -> StallingAt t
              Nothing -> NotStalling
          else
            NotStalling

        return maybeTask
      -- Optimize for the case (freeCpus == 0), avoid extra synchronization in dequeueTask
      else do
        liftIO $ atomically $ writeTVar isNodeStallingVar NotStalling
        return Nothing

    checkChangeActiveUsagesResponse :: Response -> Process ()
    checkChangeActiveUsagesResponse r = case r of
      ChangeActiveUsagesSuccess -> return ()
      _ -> Log.throwError $ "Failed to change active file usages: " ++ show r

    -- Remove a task from the taskQueue if it has memory less than freeMem
    -- and try to reserve space for input/output files on the local storage.
    -- In case of success, returns (read taskQueue, tryPeek taskQueue)
    -- In case of failure (taskQueue is empty or the tasks are too big),
    --   returns (Nothing, tryPeek taskQueue)
    -- NB: we return the second element (the first remaining task in taskQueue) here
    --     instead of calling `tryPeek taskQueue` later
    --     to ensure `atomic` behaviour guarded by taskQueueLock.
    --     This ensures that the correct task is passed to isNodeStallingVar.
    dequeueTask
      :: MemorySize -> NumCPUs
      -> Job (Maybe (EstimatedTask a), Maybe (EstimatedTask a))
    dequeueTask freeMem freeCpus = lift $ withLock taskQueueLock $ do
      maybeTask <- liftIO $ atomically $ TPrioQueue.tryPeek taskQueue
      case maybeTask of
        Just t -> do
          if (taskMemoryCapped node.memory t <= freeMem && taskMinThreads InProgressRun t <= freeCpus) then do
            (response, localFileSizes) <- reserveLocalTaskFiles t
            case response of
              ReserveSuccess -> do
                -- Mark input files as used, so they won't be deleted until the task finishes.
                -- NB: this should be done now (and not in remoteRunAndUpdateNodeStatus before/after fetching)
                -- to ensure that the Reserve request from the next `dequeueTask` will not try to delete these files.
                _ <- checkChangeActiveUsagesResponse <$> incrementActiveFileUsages fileService (taskInputPaths t) node.address
                (newTask, mNextTaskInQueue) <- liftIO $ atomically $ do
                  liftA2 (,) (TPrioQueue.read taskQueue) (TPrioQueue.tryPeek taskQueue)
                notifyChangeM taskQueueNotifier
                if (newTask /= t) then do
                  let taskInfo' task = (taskTag task, taskOutputPaths task)
                  Log.throwError $
                    "Non-atomic dequeueTask: someone else modified taskQueue without taking taskQueueLock! (peekTask,readTask): "
                    ++ show (taskInfo' t, taskInfo' newTask)
                else
                  return $ (Just newTask, mNextTaskInQueue)
              ReserveLimitExceeded _ _ -> do
                -- TODO: if all CPUs are free, shall we try to run the next task? Makes sense if it would help with cleanup.
                -- TODO: if all CPUs on all nodes are free, we should definitely do something. Either take another task or throw error and exit.
                Log.text $ "WARN: Cannot dequeue task, waiting until more local storage space becomes available: " <> Log.showText
                  -- TODO: we print only one of the files to make logs more compact
                  (response, node.address, taskTag t, listToMaybe $ Map.toAscList localFileSizes)
                pure (Nothing, maybeTask)
              _ -> Log.throwError $ "Failed to reserve space for task files: " ++ show (response, localFileSizes)
          else
            pure (Nothing, maybeTask)
        Nothing -> pure (Nothing, maybeTask)

    -- Grow newTasks by repeatedly dequeueing from the taskQueue until
    -- we have no more free cpus or memory. This operation is not
    -- atomic, so it could happen that the NodeStatus is updated by
    -- other threads while 'getMoreTasks' is running. This could
    -- potentially allow us to get even more tasks, so it is a good
    -- thing.
    getMoreTasks
      :: NonEmpty (EstimatedTask a) -> Job (NonEmpty (EstimatedTask a))
    getMoreTasks newTasks = do
      status <- liftIO $ readShared nodeStatusVar
      let
        freeMem  = node.memory - status.memoryInUse - sum (fmap (taskMemoryCapped node.memory) newTasks)
        freeCpus = node.cpus   - status.cpusInUse   - sum (fmap (taskMinThreads InProgressRun) newTasks)
      maybeTask <-
        if freeCpus > 0 then
          fst <$> dequeueTask freeMem freeCpus
        -- Optimize for the case (freeCpus == 0), avoid extra synchronization in dequeueTask
        else
          pure Nothing

      case maybeTask of
        Just t  -> getMoreTasks (NonEmpty.cons t newTasks)
        Nothing -> pure newTasks

    -- Block until there are available CPUs and the first task in the
    -- queue can fit in memory. When those conditions are met, dequeue
    -- all the tasks that can fit on the available resources.
    getNewTasks :: Job (NonEmpty (EstimatedTask a))
    getNewTasks = do
      newTask <- blockUntilNewTask
      getMoreTasks (NonEmpty.singleton newTask)

    runRemoteTasks :: CPUAllocation (EstimatedTask a) -> Job ()
    runRemoteTasks allocation = do
      let
        allocList = Map.toList allocation
        spawnTask alloc = do
          asyncTaskHandle <- asyncLinkedLocalJob $ remoteRunAndUpdateNodeStatus alloc
          lift $ throwOnAsyncFailed asyncTaskHandle

      -- Add all the tasks to NodeStatus
      liftIO $ Shared.withWrite_ nodeStatusVar $
        \s -> foldr NodeStatus.addTask s allocList
      mapM_ spawnTask allocList

    -- Run a RemoteTask and update NodeStatus when the task is
    -- finished.
    remoteRunAndUpdateNodeStatus :: (EstimatedTask a, NumCPUs) -> Job ()
    remoteRunAndUpdateNodeStatus t@(task, numCpus) = withWorkersFromPool workerPool node.address numCpus $ \workers -> do
      -- TODO:
      -- Resolve input/output file paths from taskInfo, using pathResolveMapVar. Put output files to Data.Bimap?
      let localVirtualPaths = Set.filter (isNodeLocal config) $ taskInputPaths task
      pathResolveMap <- liftIO $ Shared.withRead pathResolveMapVar $ flip Map.restrictKeys localVirtualPaths
      let
        resolvePath :: VirtualFilePath -> ClusterFilePath
        resolvePath vp@(VirtualFilePath p) = fromMaybe defaultPath maybeResolvedPath where
          maybeResolvedPath = pathResolveMap !? vp
          -- pathResolveMap should contain all files created in the current Scheduler run.
          -- If some input files (e.g. TTTT blocks) have been computed earlier, they are not in the map.
          -- In that case, we just use the provided path and check that it is a global path.
          defaultPath :: ClusterFilePath
            | isNodeLocal config vp = error $ "Node-local input file not found in FilePathResolveMap! " ++ show vp
            | otherwise      = GlobalFilePath p

        localInputPaths = Set.map resolvePath localVirtualPaths

        checkFetchResponse :: Response -> Process ()
        checkFetchResponse r = case r of
          (FetchSuccess _)   -> return ()
          (FetchError paths) -> Log.throwError $ "Failed to fetch files: " ++ show paths
          _                  -> Log.throwError $ "Illegal Fetch response: " ++ show r

      -- NB: Our current logic implies that underying FilePath does not change when we resolve VirtualFilePath.
      --     Changing paths for the task is not trivial, since we do not supply input/output paths as task arguments.
      --     If we want to change the paths used in the actual computation, we have to change e.g. boundFiles.jsonDir etc.
      --     inputPaths and outputPaths from TaskInfo are not related directly to the actual computation performed by a task
      -- TODO: what about redirecting logs?
      -- NB: We need to fetch only node-local files from other nodes.
      --     We filter files in fileManager.fetchFilesToNode (before sending actual request to NodeLocalFileManager process),
      --     so we don't have to do it here.

      -- TODO: each ReusableWorker writes to its own log file,
      -- but it is currently used only in NodeLocalFileManager and does not print anything.
      -- Shall we redirect all logs to a single file?
      _ <- when (not $ Set.null localInputPaths) $ do
        reusableWorkers <- mapM toReusableWorker workers
        fetchResponse <- lift $ fetchFilesToNode fileService localInputPaths node.address reusableWorkers
        lift $ checkFetchResponse fetchResponse

      -- If numCpus == 0, the task is not in fact running remotely (see CleanupTask and BoundTask).
      -- firstWorker = Nothing in that case.
      -- The names CanRunRemote and remoteRunTask are misleading in this case.
      -- TODO change API to make it clear and explicit?
      let firstWorker = listToMaybe workers

      -- TODO for debug
      selfPid <- lift getSelfPid
      Log.info "remoteRunTask (masterPid,workerId,tag,numCPUs,priority,outputPaths)"
        (selfPid, workerCpuIdToString <$> (.workerId) <$> firstWorker, taskTag task, numCpus, taskQueue.elemPriority task, taskOutputPaths task)

      start <- liftIO getCurrentTime
      res <- remoteRunTask firstWorker numCpus task
      end <- liftIO getCurrentTime
      let
        outputPathsMap = Map.fromSet (toClusterFilePath config node.address) $ taskOutputPaths task
        onDuplicate key _ _ = error $ "Output file path is used by more than one task: " ++ show key
      -- Add to Sheduler's FilePathResolveMap the files created by this task. This should happen before adding the task to finishedTaskQueue.
      liftIO $ Shared.withWrite_ pathResolveMapVar $ Map.unionWithKey onDuplicate outputPathsMap
      liftIO $ Shared.withWrite_ nodeStatusVar $ NodeStatus.removeTask t

      -- Map.union is left-biased and thus will override old file sizes (or estimates)
      liftIO $ Shared.withWrite_ fileSizeMapVar $ Map.union res.remoteTaskFileSizes
      registerRes <- lift $ registerFilesOnNode fileService res.remoteTaskFileSizes node.address
      case registerRes of
        RegisterSuccess -> pure ()
        _               -> Log.throwError $ "Failed to register output files: " ++ show (node.address, registerRes)

      _ <- lift $ checkChangeActiveUsagesResponse <$>
        decrementActiveFileUsages fileService (taskInputPaths task) node.address

      -- The summaries from the sizes the input files had when the task ran:
      -- by now every input exists and its size has been measured.
      inputSizes <- liftIO $ Shared.withRead fileSizeMapVar $
        flip Map.restrictKeys (taskInputPaths task)
      let measured = estimationWithInputSizes inputSizes task
      (inputSummary, producerSummary) <- liftIO $ evaluate $
        force (measured.inputSummary, measured.producerSummary)

      -- TODO: use a node-specific TChangeNotifier here?
      notifyChangeM taskQueueNotifier
      liftIO $ writeQueue taskRecordQueue MkTaskRecord
        { task        = task
        , taskStart   = start
        , taskRuntime = diffUTCTime end start
        , taskMemory  = res.remoteTaskMemory
        , taskNode    = node
        , taskNumCPUs = numCpus
        , taskFileSizes =
            outputFileSizes (taskOutputs task) res.remoteTaskFileSizes
        -- What the task was scheduled on, recorded next to what it used, so
        -- that estimates can be checked against reality after the run.
        , taskEstimates = taskEstimatesAt numCpus (taskEstimation task)
        , taskStatKey = taskStatKey task
        , taskInputSummary = inputSummary
        , taskProducerSummary = producerSummary
        }
      -- NB: this should be the last operation, since the nodeLoop process is killed
      -- after monitorProgressAndDeps reads the last task from finishedTaskQueue!
      liftIO $ writeQueue finishedTaskQueue task

    getAndRunTasks :: Job ()
    getAndRunTasks = do
      newTasks <- getNewTasks
      status <- liftIO $ readShared nodeStatusVar
      let
        freeCpus = node.cpus - status.cpusInUse
        allocation = allocateCpusToTasks InProgressRun freeCpus (NonEmpty.toList newTasks)
      runRemoteTasks allocation

cleanupLoop :: Config -> FileService -> TChangeNotifier -> CleanupQueue -> Job ()
cleanupLoop config fileService taskQueueNotifier cleanupQueue = lift go where
  go = do
    mPaths <- readAndFlushQueue cleanupQueue
    let
      paths = [p | Just p <- NonEmpty.toList mPaths]
      shouldExit = any isNothing mPaths
      (localPaths, globalPaths) = partition (isNodeLocal config) paths
      -- TODO merge and/or check responses
      checkResponse response = case response of
        FileDeleted -> pure ()
        FileDoesNotExist -> pure ()
        _ -> do Log.err $ "Unexpected response: " <> show (response, localPaths)

    localResponses <- deleteFilesFromAllNodes fileService localPaths
    mapM_ checkResponse localResponses

    globalResponses <- deleteGlobalFiles fileService globalPaths
    mapM_ checkResponse globalResponses

    -- Notify task queue readers that we cleaned up some disk space.
    -- This helps if they couldn't reserve disk space for a task.
    notifyChangeM taskQueueNotifier
    unless shouldExit go


-- | Continually read tasks from 'finishedTaskQueue' and update the
-- ProgressMap and TaskGraph, returning when all tasks are
-- completed. Report progress whenever an update comes in, provided it
-- has been at least 'reportInterval' since the last progress
-- report. For each task, when all the dependencies of a task have
-- finished, enqueue the task in the 'taskQueue'.
monitorProgressAndDeps
  :: IsTask a
  => Config
  -> TPrioQueue TaskPriority a
  -> TChangeNotifier
  -> Lock
  -> ConcurrentQueue a
  -> TaskGraph a
  -> CleanupDependenciesMap
  -> CleanupQueue
  -> Map Node (Shared IO NodeStatus, TVar (IsNodeStalling a))
  -> ProgressMap
  -> Job ()
monitorProgressAndDeps config taskQueue taskQueueNotifier taskQueueLock finishedTaskQueue taskGraph initCleanupDepCounts cleanupQueue nodeStatusMap initProgressMap = do
  Log.info "Building" (catMaybes (Map.keys initProgressMap))
  report initProgressMap
  start <- liftIO getCurrentTime

  monitorStallingHandle <- lift $ Async.asyncLinked $ Async.task $ do
    waitForGlobalStalling
    _ <- Log.throwError "All nodes are stalling: no tasks are running and the next task cannot be taken from the queue."
    return ()
  lift $ throwOnAsyncFailed monitorStallingHandle

  let initDepCounts = TaskGraph.dependencyCounts taskGraph
  -- TODO initialize initCleanupDepCounts here instead of passing it
  go start initDepCounts initCleanupDepCounts initProgressMap

  _ <- lift $ Async.cancelWait monitorStallingHandle

  finish <- liftIO getCurrentTime
  Log.info "Finished" ( catMaybes (Map.keys initProgressMap)
                      , diffUTCTime finish start
                      )
  where
    -- TODO: Order the tags appropriately
    -- TODO: Properly indent the progress bars
    report progressMap = do
      ProgressMap.displayLog progressMap
      nodeStatuses <- liftIO $ traverse (readShared . fst) nodeStatusMap
      Log.text $ "Nodes: \n" <> NodeStatus.display (Map.elems nodeStatuses)

    reportIfAfterInterval lastReportTime progressMap = do
      now <- liftIO getCurrentTime
      if now > addUTCTime config.reportInterval lastReportTime
        then report progressMap >> pure now
        else pure lastReportTime

    go lastReportTime depCounts cleanupDepCounts progressMap
      | ProgressMap.isFinished progressMap = do
        writeQueue cleanupQueue Nothing
        pure ()
      | otherwise = do
          finishedTask <- liftIO $ readQueue finishedTaskQueue
          let
            progressMap' = ProgressMap.update finishedTask progressMap
            (depCounts', newTasks) = TaskGraph.decrementReverseDependencies taskGraph depCounts finishedTask
            filesToCleanup = taskFilesToCleanup config finishedTask
            (cleanupDepCounts', pathsToCleanup) = runWriter $
              decrementCleanupCounts filesToCleanup cleanupDepCounts
          withLock taskQueueLock $ mapM_ (TPrioQueue.write taskQueue) newTasks
          writeListQueue cleanupQueue $ map Just pathsToCleanup
          notifyChangeM taskQueueNotifier
          lastReportTime' <- reportIfAfterInterval lastReportTime progressMap'
          go lastReportTime' depCounts' cleanupDepCounts' progressMap'

    -- Wait until all nodes are stalling.
    waitForGlobalStalling :: Process ()
    waitForGlobalStalling = do
      let isStallingVars = map snd $ Map.elems nodeStatusMap
      liftIO $ atomically $ do
        globalStalling <- allNodesStalling <$> mapM readTVar isStallingVars
        check globalStalling

type TaskRecords a = [TaskRecord a]

-- | Run the given tasks on the given list of nodes by initially
-- distributing largest memory tasks to nodes until we can't fit any
-- more. As tasks finish on a node, we start new tasks on that node,
-- given the newly available memory and cpus.
--
-- TODO: Check that no tasks require more memory than an entire node.
runTasks
  :: IsTask a
  => Config
  -> EstimatedTaskMap a
  -> Job (TaskRecords a)
runTasks config taskMap =
  map (fmap originalTask) <$> runTaskMap config (estimatedTasks taskMap)

runTaskMap
  :: IsTask a
  => Config
  -> TaskMap (EstimatedTask a)
  -> Job (TaskRecords (EstimatedTask a))
runTaskMap config taskMap = do
  let
    taskGraph = TaskGraph.fromEdges taskMap
    cleanupDependencies = buildCleanupDependenciesMap config taskMap
  either Log.throw pure $ validateNodeLocalInputs config taskMap
  checkTaskLaunch config.taskLaunch
  nodes <- getJobNodes config
  reportBeforeRun config nodes taskMap
  withFileService config nodes $ \fileService -> withWorkerPool config.taskLaunch nodes $ \workerPool -> do
      let
        getTaskPriority = taskPriority $ mkTaskPriorityHelper nodes taskGraph
        -- TODO: check disk space when computing initialAllocs
        -- TODO: seems that it is non-deterministic and often returns empty allocations, how???
        (initialAllocs, moreIndependentTasks) =
          --distributeTasksToNodes nodes (TaskGraph.independentKeys taskGraph)
          distributeTasksToNodesWithScores 0.75 config nodes (TaskGraph.independentKeys taskGraph) getTaskPriority

        toFileInfos t = Set.toList $ Set.union (taskInputs t) (taskOutputs t)
        toFileSizeMap t = Map.fromList
          [ (fileInfo.path, schedulingEstimate fileInfo.fileSize)
          | fileInfo <- toFileInfos t
          ]
        fileSizeEstimates = Map.unions $ map toFileSizeMap $ Set.toList $ TaskGraph.keys taskGraph

      -- TODO for debug
      Log.info "initialAllocs (node, numTasks)" $ Map.map Map.size initialAllocs
      -- NB: don't forget to call `notifyChangeM taskQueueNotifier` on any event
      --   that should trigger retrying in `blockUntilNewTask`!
      -- Currently, it's called when:
      -- 1. We add tasks to the queue (`TPrioQueue.write` in `monitorProgressAndDeps`)
      -- 2. We take a task from the queue (`TPrioQueue.read` in `dequeueTask`)
      -- 3. We finish task in `remoteRunAndUpdateNodeStatus`
      --   (i.e. available memory, CPUs and disk space are updated)
      -- TODO: the third case could be handled via separate notifiers for each node.
      taskQueueNotifier <- liftIO newChangeNotifierIO
      taskQueue <- TPrioQueue.new getTaskPriority
      -- A lock to ensure that `dequeueTask` is atomic.
      taskQueueLock <- liftIO mkExclusiveLock
      -- Will be updated with actual file sizes as they are created.
      fileSizeMapVar <- liftIO $ newShared fileSizeEstimates
      pathResolveMapVar <- liftIO $ newShared Map.empty
      finishedTaskQueue <- newQueue
      taskRecordQueue <- newQueue
      cleanupQueue <- newQueue
      -- NB: here we don't need taskQueueNotifier and taskQueueLock,
      -- since no one is accessing taskQueue yet.
      mapM_ (TPrioQueue.write taskQueue) moreIndependentTasks
      nodeLoopMap
        :: Map Node
             ( (Shared IO NodeStatus, TVar (IsNodeStalling (EstimatedTask a)))
             , Async () )
        <- flip Map.traverseWithKey initialAllocs $
        \node alloc -> do
          nodeStatusVar <- liftIO $ newShared NodeStatus.empty
          isNodeStallingVar <- liftIO $ newTVarIO NotStalling
          -- TODO: move workerPool to nodeStatusVar?
          loopHandle <- asyncLinkedLocalJob $ do
            -- NB: If we don't catch an exception here, then
            -- `withReusableWorkerPool.release` will be called before rethrowing (and logging) it.
            -- `release` will kill other workers, causing other exceptions (DiedDisconnect).
            -- Thus the original exception will be lost.
            -- To prevent that, we catch, log and rethrow it.
            -- NB: `cancelWait` send `kill`signal to `runNodeLoop`,
            -- thus catching @SomeException will lead to logging unnecessary error message:
            --   ERROR: killed-by=pid://exp-9-55.expanse.sdsc.edu:41403:0:26,reason=cancel
            -- Thus we choose catch only certain popular exception types.
            -- (Exception type for `kill` is not exposed, so we cannot ignore `kill` and catch everything else.)
            -- For example, AsyncFailedException is thrown when a remote task throws exception.
            -- TODO: what other exceptions should we catch?
            -- TODO: implement graceful exit for runNodeLoop.
            runNodeLoop
              config
              node
              fileService
              workerPool
              nodeStatusVar
              isNodeStallingVar
              taskQueue
              taskQueueNotifier
              taskQueueLock
              fileSizeMapVar
              pathResolveMapVar
              finishedTaskQueue
              taskRecordQueue
              alloc
              `catches`
                [ Handler (Log.throw @_ @IOError)
                , Handler (Log.throw  @_ @AsyncFailedException)
                , Handler (Log.throw  @_ @RemoteError)
                ]
          lift $ throwOnAsyncFailed loopHandle
          pure ((nodeStatusVar, isNodeStallingVar), loopHandle)
      let
        nodeLoopHandleMap = fmap snd nodeLoopMap
        nodeStatusMap  = fmap fst nodeLoopMap

      cleanupLoopHandle <- asyncLinkedLocalJob $ cleanupLoop config fileService taskQueueNotifier cleanupQueue
      lift $ throwOnAsyncFailed cleanupLoopHandle

      monitorProgressAndDeps
        config
        taskQueue
        taskQueueNotifier
        taskQueueLock
        finishedTaskQueue
        taskGraph
        cleanupDependencies
        cleanupQueue
        nodeStatusMap
        (ProgressMap.fromTasksTodo (TaskGraph.keys taskGraph))
      -- NB: we use cancelWait instead of e.g. cancelKill to ensure that nodeLoop task will finish with AsyncCancelled.
      -- Finishing with AsyncFailed or AsyncLinkFailed will trigger throwOnAsyncFailed.
      mapM_ (lift . Async.cancelWait) nodeLoopHandleMap
      _ <- lift $ Async.wait cleanupLoopHandle
      records <- flushQueue taskRecordQueue
      reportAfterRun records
      pure records

-- Files that can be removed (if there are no other dependencies)
-- when the task finished
-- NB: if an output file is never used by any other task, we should delete it too.
-- That's why we take both input and output paths.
-- TODO: remove also all global files with FileTreatment = RemoveFile?
-- TODO: make isNodeLocal configurable?
taskFilesToCleanup
  :: (IsTask a)
  => Config
  -> a
  -> Set VirtualFilePath
taskFilesToCleanup config task = Set.filter (isNodeLocal config) $ taskInputPaths task <> taskOutputPaths task

-- File path -> Number of tasks having this file as input or output
-- When this number goes to zero, we can delete this file
type CleanupDependenciesMap = Map VirtualFilePath Int

-- | The task's estimation with its input files at the given sizes. It reuses
-- the task's shape: only the input summaries and the models are computed
-- again.
estimationWithInputSizes
  :: IsTask a
  => Map VirtualFilePath FileSize -> EstimatedTask a -> TaskEstimation
estimationWithInputSizes sizes task = (taskShape task).estimate known
  where
    infos = Map.fromList [ (i.path, i) | i <- Set.toList (taskInputs task) ]
    -- 'fileSizeMapVar' starts with every task's files, so every input has a
    -- size.
    known file =
      case (Map.lookup file.path infos, Map.lookup file.path sizes) of
        (Just info, Just size) -> info { fileSize = EstimatedByTask size }
        _ -> error $
          "estimationWithInputSizes: no size for input " <> show file.path

-- Nothing means "no more cleanups expected, exit"
type CleanupQueue = ConcurrentQueue (Maybe VirtualFilePath)

buildCleanupDependenciesMap
  :: IsTask a
  => Config
  -> TaskMap a
  -> CleanupDependenciesMap
buildCleanupDependenciesMap config taskMap = cleanupMap where
  tasks = Map.keys taskMap
  partialCleanupMap task = Map.fromSet (const 1) $ taskFilesToCleanup config task
  cleanupMap = Map.unionsWith (+) $ map partialCleanupMap tasks

decrementCleanupCount
  :: VirtualFilePath
  -> CleanupDependenciesMap
  -> Writer [VirtualFilePath] CleanupDependenciesMap
decrementCleanupCount path depMap = Map.alterF go path depMap where
  go (Just i)
    | i > 1     = pure (Just (i-1))
    | otherwise = tell [path] >> pure Nothing
  go Nothing = error "decrementCleanupCount: path not present in map"

decrementCleanupCounts
  :: Set VirtualFilePath
  -> CleanupDependenciesMap
  -> Writer [VirtualFilePath] CleanupDependenciesMap
decrementCleanupCounts paths depMap =
  foldM (\m p -> decrementCleanupCount p m) depMap (Set.toList paths)
