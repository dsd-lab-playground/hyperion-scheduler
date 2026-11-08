{-# LANGUAGE DeriveAnyClass        #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE NoFieldSelectors      #-}
{-# LANGUAGE OverloadedRecordDot   #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE StaticPointers        #-}

-- | A long-lived process on each node that runs task closures on worker
-- processes it launches itself, one fresh worker per task.
--
-- Launching a worker from the master costs the master's node controller
-- about 20 messages per task, among them several process deaths and the
-- death of the worker's node, and each death scans all links and monitors
-- of the master's node. Here the master only sends a request to the
-- supervisor and waits for its reply; worker launches, monitors and deaths
-- stay on the supervisor's node. Each CPU slot keeps a launched worker
-- ready, whose task receiver waits for one closure on a channel. The ready
-- workers' memory (20-40 MB each on Expanse) is not part of any task's
-- estimate.
--
-- The supervisor runs on 2 capabilities: on Expanse, 1 slowed short tasks
-- (~45 per second per node) by 8%, and 4 did not help more. A worker runs on
-- one capability, whatever the CPUs of its task: tasks use more CPUs only in
-- external programs, and each capability makes the worker's GC wait for one
-- more core. A task that wants parallel Haskell can call
-- 'setNumCapabilities' itself.
--
-- Before it accepts a task, the supervisor prepares how it launches workers
-- ('Hyperion.Scheduler.TaskLaunch.prepareNodeLaunch'): it stages node-local
-- copies of the executables and renders the task environment, if the
-- 'TaskLaunch' asks for them. A failure there is the supervisor's start-up
-- reply, and the master stops before any task runs.
module Hyperion.Scheduler.NodeSupervisor
  ( NodeSupervisor
  , NodeSlots (..)
  , StartedSupervisor (..)
  , startNodeSupervisors
  , stopNodeSupervisor
  , runOnNodeSupervisor
  ) where

import Control.Concurrent            (forkIO, getNumCapabilities, myThreadId,
                                      rtsSupportsBoundThreads,
                                      setNumCapabilities, threadDelay, throwTo)
import Control.Concurrent.MVar       (MVar, newEmptyMVar, putMVar, takeMVar,
                                      tryPutMVar)
import Control.Concurrent.STM        (TVar, atomically, modifyTVar', newTVarIO,
                                      readTVar, readTVarIO)
import Control.Distributed.Process   (DiedReason, DidSpawn (..), MonitorRef, Process,
                                      ProcessId,
                                      ProcessMonitorNotification (..),
                                      ReceivePort, SendPort, SendPortId,
                                      getSelfNode, getSelfPid, kill, liftIO,
                                      link, match, matchChan, matchIf, monitor,
                                      newChan, processNodeId, receiveChan,
                                      receiveChanTimeout, receiveTimeout,
                                      receiveWait, send, sendChan, sendPortId,
                                      sendPortProcessId, spawnAsync, spawnLocal,
                                      unClosure, unlink, unmonitor)
import Control.Exception             (Exception, SomeException)
import Control.Monad                 (forM, forM_, forever, join, replicateM_,
                                      unless, void, when)
import Control.Monad.Catch           (bracket, onException, throwM, try)
import Control.Monad.Extra           (unlessM, whenM)
import Control.Monad.Trans           (lift)
import Data.Binary                   (Binary)
import Data.ByteString.Lazy          (ByteString)
import Data.IORef                    (IORef, modifyIORef', newIORef, readIORef,
                                      writeIORef)
import Data.Map.Strict               (Map)
import Data.Map.Strict               qualified as Map
import Data.Set                      (Set)
import Data.Set                      qualified as Set
import GHC.Clock                     (getMonotonicTime)
import GHC.Generics                  (Generic)
import Hyperion                      (Closure, Dict (..),
                                      HyperionStaticConfig (..), Job,
                                      Service (..), ServiceId, Static (..),
                                      WorkerAddr, WorkerLauncher (..), cAp,
                                      cPure, emptyOnServiceExit,
                                      emptyStoreCancelAction,
                                      getWorkerStaticConfig, remoteEvalOnWorker,
                                      withWorker)
import Hyperion.Command              (hyperionWorkerCommand)
import Hyperion.Log                  qualified as Log
import Hyperion.OsPath               (OsPath)
import Hyperion.OsString             (OsString, toString)
import Hyperion.Scheduler.RemoteUtil (spawnLocalJob)
import Hyperion.Scheduler.TaskLaunch (NodeLaunch (..), NodeTaskLaunch (..),
                                      Seconds (..), StagingError, StagingReport,
                                      StartupWaits (..), TaskLaunch (..),
                                      prepareNodeLaunch, startupTimeoutOf)
import Hyperion.Util                 (myExecutable, tryLogException)
import System.CPUTime                (getCPUTime)
import System.Exit                   (ExitCode (..))
import System.Process                (CreateProcess (..), createProcess,
                                      proc, terminateProcess, waitForProcess)

-- | The master's handle on a node's supervisor.
newtype NodeSupervisor = NodeSupervisor ProcessId
  deriving (Show)

type Reply = Either String ByteString

data TaskRequest = TaskRequest (Closure (Process ByteString)) (SendPort Reply)
  deriving (Generic, Binary)

requestId :: TaskRequest -> SendPortId
requestId (TaskRequest _ replyPort) = sendPortId replyPort

-- | Sent by the supervisor to the slot running the request.
data CancelRequest = CancelRequest SendPortId
  deriving (Generic, Binary)

data SupervisorMessage
  = RunTask Int TaskRequest
  | CancelTask SendPortId
  | StopSupervisor (SendPort ())
  deriving (Generic, Binary)

-- | The supervisor's start-up reply.
data StartupReply
  = Started ProcessId (Maybe StagingReport)  -- ^ with what it staged, if anything
  | CannotLaunchTasks StagingError
  deriving (Generic, Binary)

-- | A node to start a supervisor on, with the log file of each CPU slot's
-- workers.
data NodeSlots = NodeSlots
  { address  :: WorkerAddr
  , logPaths :: [OsPath]
  }

-- | A supervisor that has replied that it started.
data StartedSupervisor = StartedSupervisor
  { address    :: WorkerAddr
  , supervisor :: NodeSupervisor
  , staging    :: Maybe StagingReport
  }

-- | A supervisor that has been spawned and hasn't replied yet.
data StartingSupervisor = StartingSupervisor
  { address :: WorkerAddr
  , job     :: ProcessId
  , death   :: MonitorRef  -- ^ of the job, from when it was spawned
  , replies :: ReceivePort StartupReply
  }

-- | Why a node has no supervisor.
data StartupFailure
  = CouldNotPrepare WorkerAddr StagingError
  | SupervisorDied WorkerAddr DiedReason
  | NoReplyBeforeDeadline WorkerAddr
  deriving (Show)

-- | A reading of the monotonic clock.
newtype Deadline = Deadline Double
  deriving (Eq, Ord)

deadlineAfter :: Seconds -> IO Deadline
deadlineAfter (Seconds n) = Deadline . (+ fromIntegral n) <$> getMonotonicTime

-- | Start a supervisor on each node, all at once, and wait for all to reply.
-- If any fails, stops those that started, ends those that haven't replied, and
-- throws, naming every failure. Returns one for each node, in order.
startNodeSupervisors :: NodeTaskLaunch -> [NodeSlots] -> Job [StartedSupervisor]
startNodeSupervisors launch nodes = do
  starting <- forM nodes $ \node -> do
    (startupPort, startupReplies) <- lift newChan
    job <- spawnLocalJob $ remoteEvalOnWorker node.address $
      static supervisorMain `cAp` cPure launch `cAp` cPure node.logPaths
        `cAp` cPure startupPort
    death <- lift $ monitor job
    pure $ StartingSupervisor node.address job death startupReplies
  deadline <- liftIO $ traverse deadlineAfter (startupTimeoutOf launch.launch)
  let
    -- Ending the job that evaluates the supervisor shuts its worker down, even
    -- if it is still preparing and replies later to no one.
    endIfStarting s = kill s.job "the run's start failed"
  replies <- lift $
    awaitStartups launch.launch.startupWaits deadline starting
      `onException` mapM_ endIfStarting starting
  lift $ mapM_ (\s -> unmonitor s.death) starting
  let
    failures = [ f | Left f <- replies ]
    started = [ s | Right s <- replies ]
  unless (null failures) $ do
    lift $ forM_ (zip starting replies) $ \(s, reply) -> case reply of
      Left _     -> endIfStarting s
      Right node -> stopNodeSupervisor node.supervisor
    Log.throwError $ "Could not start node supervisors: " <> show failures
  forM_ started $ \s -> Log.info "Started NodeSupervisor" (s.address, s.supervisor, s.staging)
  pure started

-- | Every supervisor's reply, in order, as they come. After a failure, the
-- others have 'afterFailure' from then.
awaitStartups
  :: StartupWaits -> Maybe Deadline -> [StartingSupervisor]
  -> Process [Either StartupFailure StartedSupervisor]
awaitStartups waits firstDeadline starting = do
  replies <- go firstDeadline starting Map.empty
  pure [ Map.findWithDefault (Left (NoReplyBeforeDeadline s.address)) s.address replies | s <- starting ]
  where
    go _ [] replies = pure replies
    go deadline pending replies = receiveBefore deadline (concatMap matches pending) >>= \case
      Nothing -> pure replies
      Just (addr, reply) -> do
        deadline' <- case reply of
          Right _ -> pure deadline
          Left _ -> do
            afterFailure <- liftIO $ deadlineAfter waits.afterFailure
            pure $ Just $ maybe afterFailure (min afterFailure) deadline
        go deadline' (filter (\s -> s.address /= addr) pending) (Map.insert addr reply replies)
    matches s =
      [ matchChan s.replies (pure . (,) s.address . fromReply s)
      , matchIf (isDeathOf s.death) $ \(ProcessMonitorNotification _ _ reason) ->
          receiveChanTimeout (microseconds waits.replyAfterDeath) s.replies >>= \case
            Just reply -> pure (s.address, fromReply s reply)
            Nothing    -> pure (s.address, Left (SupervisorDied s.address reason))
      ]
    fromReply s = \case
      Started pid staged  -> Right $ StartedSupervisor s.address (NodeSupervisor pid) staged
      CannotLaunchTasks e -> Left $ CouldNotPrepare s.address e
    receiveBefore Nothing ms = Just <$> receiveWait ms
    receiveBefore (Just (Deadline d)) ms = do
      now <- liftIO getMonotonicTime
      receiveTimeout (max 0 (ceiling ((d - now) * 1e6))) ms

microseconds :: Seconds -> Int
microseconds (Seconds n) = n * 1_000_000

-- | Stop the supervisor, and wait up to 30 s for it to shut its workers down.
stopNodeSupervisor :: NodeSupervisor -> Process ()
stopNodeSupervisor (NodeSupervisor pid) = do
  (donePort, done) <- newChan
  send pid $ StopSupervisor donePort
  receiveChanTimeout 30_000_000 done >>= \case
    Just () -> pure ()
    Nothing -> Log.warn "NodeSupervisor did not stop in time" pid

-- | Run the closure on a fresh worker of the given CPU slot. Throws if the
-- closure throws or a process involved dies. If interrupted, the supervisor
-- stops the worker.
runOnNodeSupervisor
  :: NodeSupervisor -> Int -> Closure (Process ByteString)
  -> Process ByteString
runOnNodeSupervisor (NodeSupervisor pid) slot closure = do
  (replyPort, replies) <- newChan
  let request = TaskRequest closure replyPort
  reply <- bracket (monitor pid) unmonitor $ \ref -> do
    send pid $ RunTask slot request
    receiveWait
      [ matchChan replies pure
      , matchIf (isDeathOf ref) $ \(ProcessMonitorNotification _ _ reason) ->
          pure $ Left $ "NodeSupervisor died: " <> show reason
      ]
    `onException` send pid (CancelTask (requestId request))
  let failed err =
        Log.throwError $ "Task failed on slot " <> show slot <> ": " <> err
  either failed pure reply

-- * The supervisor

data SupervisorState = SupervisorState
  { -- | Requests being run, with their slot processes.
    running    :: TVar (Map SendPortId ProcessId)
    -- | Requests cancelled before a slot took them.
  , cancelled  :: TVar (Set SendPortId)
    -- | Waits of requests for a ready worker: (count, total s, max s).
  , readyWaits :: TVar (Int, Double, Double)
  }

-- | Run the supervisor, with one CPU slot per log path (the log file of the
-- slot's workers).
supervisorMain :: NodeTaskLaunch -> [OsPath] -> SendPort StartupReply -> Process ()
supervisorMain launch logPaths startupPort = do
  -- In local mode the supervisor shares the master's process.
  selfNode <- getSelfNode
  let masterNode = processNodeId $ sendPortProcessId $ sendPortId startupPort
  when (selfNode /= masterNode) $ liftIO $ setCapabilities 2
  prepared <- liftIO $ prepareNodeLaunch launch =<< myExecutable
  case prepared of
    Left err -> do
      Log.warn "NodeSupervisor could not prepare task launch" err
      sendChan startupPort (CannotLaunchTasks err)
    Right nodeLaunch -> do
      forM_ nodeLaunch.stagingReport $ Log.info "NodeSupervisor staged programs"
      supervise nodeLaunch logPaths startupPort

supervise :: NodeLaunch -> [OsPath] -> SendPort StartupReply -> Process ()
supervise nodeLaunch logPaths startupPort = do
  staticConfig <- getWorkerStaticConfig
  let
    -- Started like the workers that hyperion launches.
    workerCommand logPath service = WorkerCommand
      { command = staticConfig.workerCommand $
          hyperionWorkerCommand nodeLaunch.workerProgram service logPath
      , environment = nodeLaunch.workerEnvironment
      }
  startTime <- liftIO getMonotonicTime
  st <- liftIO $ SupervisorState
    <$> newTVarIO Map.empty <*> newTVarIO Set.empty <*> newTVarIO (0, 0, 0)
  slots <- forM logPaths $ \logPath -> do
    slotPid <- spawnLocal $ slotLoop st (workerCommand logPath)
    -- A slot that dies would leave its requests unanswered: die with it, so
    -- that the master notices.
    link slotPid
    pure slotPid
  let slotMap = Map.fromList (zip [0 ..] slots)
  self <- getSelfPid
  sendChan startupPort (Started self nodeLaunch.stagingReport)
  let
    loop = receiveWait [match handle]
    handle = \case
      RunTask slot request -> do
        case Map.lookup slot slotMap of
          Just slotPid -> send slotPid request
          Nothing      ->
            sendReply request (Left ("no such slot: " <> show slot))
        loop
      CancelTask rid -> do
        cancel st rid
        loop
      StopSupervisor donePort -> do
        Log.text "Stopping NodeSupervisor"
        liftIO $ do
          wall <- subtract startTime <$> getMonotonicTime
          cpu <- (/ 1e12) . fromIntegral <$> getCPUTime
          caps <- getNumCapabilities
          waits <- readTVarIO st.readyWaits
          Log.info "NodeSupervisor (wall s, CPU s, capabilities, \
            \ready waits (n, total s, max s))"
            (wall, cpu :: Double, caps, waits)
        forM_ slots unlink
        stopAll slots
        sendChan donePort ()
  loop

-- | Kill the processes and wait up to 10 s for them to exit.
stopAll :: [ProcessId] -> Process ()
stopAll pids = do
  refs <- forM pids $ \pid -> monitor pid <* kill pid "NodeSupervisor stopped"
  replicateM_ (length refs) $ receiveTimeout 10_000_000
    [match (\(ProcessMonitorNotification {}) -> pure ())]

cancel :: SupervisorState -> SendPortId -> Process ()
cancel st rid = do
  mSlotPid <- liftIO $ atomically $ do
    running <- readTVar st.running
    case Map.lookup rid running of
      Just slotPid -> pure (Just slotPid)
      Nothing -> do
        modifyTVar' st.cancelled (Set.insert rid)
        pure Nothing
  forM_ mSlotPid $ \slotPid -> send slotPid (CancelRequest rid)

sendReply :: TaskRequest -> Reply -> Process ()
sendReply (TaskRequest _ replyPort) = sendChan replyPort

-- | A worker ready for one task: the channel its task receiver waits on,
-- and how to shut the worker down afterwards.
data ReadyWorker = ReadyWorker (SendPort TaskRequest) (IO ())

-- | How to start a worker process: the command, and its environment
-- ('Nothing': the supervisor's).
data WorkerCommand = WorkerCommand
  { command     :: (OsString, [OsString])
  , environment :: Maybe [(String, String)]
  }

-- | The command that starts a worker for the service.
type WorkerCommandFor = Service -> WorkerCommand

-- | Run requests, one at a time, each on a fresh worker launched in advance.
slotLoop :: SupervisorState -> WorkerCommandFor -> Process ()
slotLoop st workerCommand = do
  self <- getSelfPid
  hosts <- liftIO $ newIORef Set.empty
  let
    newHost = do
      readyVar <- liftIO newEmptyMVar
      pid <- spawnLocal $ hostWorker workerCommand readyVar
      liftIO $ modifyIORef' hosts (Set.insert pid)
      pure (pid, readyVar)
    forgetHost pid = liftIO $ modifyIORef' hosts (Set.delete pid)
    -- Take the ready worker; if it failed to start, launch another.
    getWorker (pid, readyVar) attempts = do
      ready <- liftIO $ do
        t0 <- getMonotonicTime
        ready <- takeMVar readyVar
        t <- subtract t0 <$> getMonotonicTime
        atomically $ modifyTVar' st.readyWaits $
          \(n, total, mx) -> (n + 1, total + t, max mx t)
        pure ready
      case ready of
        Right worker -> pure (Right (pid, worker))
        Left err -> do
          forgetHost pid
          Log.warn "Worker failed to start" err
          if attempts > 1
            then newHost >>= \host -> getWorker host (attempts - 1 :: Int)
            else pure (Left err)
    takeRequest = receiveWait
      [ match $ \request -> do
          let rid = requestId request
          accepted <- liftIO $ atomically $ do
            wasCancelled <- Set.member rid <$> readTVar st.cancelled
            if wasCancelled
              then modifyTVar' st.cancelled (Set.delete rid) >> pure False
              else modifyTVar' st.running (Map.insert rid self) >> pure True
          if accepted then pure request else takeRequest
      -- For a request that has finished meanwhile.
      , match $ \(CancelRequest _) -> takeRequest
      ]
    finish request reply = do
      liftIO $ atomically $
        modifyTVar' st.running (Map.delete (requestId request))
      sendReply request reply
    -- Send the task to the worker and wait for its reply, its death, or a
    -- cancellation. Also returns the monitor if it is still active.
    runRequest request@(TaskRequest closure _) taskPort = do
      ref <- monitor $ sendPortProcessId $ sendPortId taskPort
      (replyPort, replies) <- newChan
      sendChan taskPort $ TaskRequest closure replyPort
      receiveWait
        [ matchChan replies $ \reply -> pure (reply, Just ref)
        , matchIf (isDeathOf ref) $ \(ProcessMonitorNotification _ _ reason) ->
            pure (Left ("worker died: " <> show reason), Nothing)
        , matchIf (\(CancelRequest rid) -> rid == requestId request) $ \_ ->
            pure (Left "task cancelled", Just ref)
        ]
    loop ready = do
      request <- takeRequest
      worker <- getWorker ready 3
      -- Launch the next worker now, so that the next task finds it ready.
      -- This cost running tasks nothing measurable on Expanse, while
      -- launching after the task made each task wait ~0.25 s for a worker.
      next <- newHost
      case worker of
        Left err -> finish request $ Left $ "cannot start a worker: " <> err
        Right (pid, ReadyWorker taskPort release) -> do
          (reply, activeMonitor) <- runRequest request taskPort
          finish request reply
          mapM_ unmonitorAndFlush activeMonitor
          liftIO release
          forgetHost pid
      loop next
    -- Shut down the ready and the running worker.
    stopHosts = liftIO (readIORef hosts) >>= stopAll . Set.toList
  (newHost >>= loop) `onException` stopHosts

-- | Launch a worker, start its task receiver, pass the worker to the slot,
-- and shut it down when the slot releases it.
hostWorker :: WorkerCommandFor -> MVar (Either String ReadyWorker) -> Process ()
hostWorker workerCommand readyVar = do
  starting <- liftIO $ newIORef Nothing
  released <- liftIO newEmptyMVar
  let
    useWorker workerNode _ = do
      (announcePort, announced) <- newChan
      spawnRef <- spawnAsync workerNode $
        static taskReceiver `cAp` cPure announcePort
      receiver <- receiveTimeout 300_000_000
        [ matchIf (\(DidSpawn ref _) -> ref == spawnRef) $
            \(DidSpawn _ pid) -> pure pid
        ]
        >>= maybe (Log.throwError "Task receiver was not spawned") pure
      -- Fail at once if the receiver dies before announcing itself, e.g. if
      -- its closure cannot be decoded.
      ref <- monitor receiver
      taskPort <- receiveTimeout 300_000_000
        [ matchChan announced pure
        , matchIf (isDeathOf ref) $ \(ProcessMonitorNotification _ _ reason) ->
            Log.throwError $ "Task receiver died: " <> show reason
        ]
        >>= maybe (Log.throwError "Task receiver did not start") pure
      unmonitorAndFlush ref
      liftIO $ do
        writeIORef starting Nothing
        putMVar readyVar $ Right $ ReadyWorker taskPort (putMVar released ())
        takeMVar released
  result <- try $ withWorker (slotLauncher workerCommand starting) useWorker
  case result of
    Right () -> pure ()
    Left (e :: SomeException) ->
      void $ liftIO $ tryPutMVar readyVar $ Left $ show e

-- | Runs on a fresh worker: announce a channel, run the one task sent on it,
-- and reply.
taskReceiver :: SendPort (SendPort TaskRequest) -> Process ()
taskReceiver announcePort = do
  liftIO $ setCapabilities 1
  (taskPort, tasks) <- newChan
  sendChan announcePort taskPort
  TaskRequest closure replyPort <- receiveChan tasks
  sendChan replyPort =<< tryLogException (join (unClosure closure))
  -- Do not exit until the worker shuts down: the slot monitors this process,
  -- and the notice of its exit could overtake the reply.
  liftIO $ forever $ threadDelay maxBound

-- | The non-threaded RTS has one capability and cannot change it.
setCapabilities :: Int -> IO ()
setCapabilities n = when rtsSupportsBoundThreads $ setNumCapabilities n

isDeathOf :: MonitorRef -> ProcessMonitorNotification -> Bool
isDeathOf ref (ProcessMonitorNotification ref' _ _) = ref' == ref

-- | Remove a monitor, and a notification that arrived before it was removed.
unmonitorAndFlush :: MonitorRef -> Process ()
unmonitorAndFlush ref = do
  unmonitor ref
  void $ receiveTimeout 0 [matchIf (isDeathOf ref) (const (pure ()))]

instance Static (Binary (SendPort (SendPort TaskRequest))) where
  closureDict = static Dict

-- | A worker process exited before it was ready.
data WorkerExited = WorkerExited ServiceId ExitCode
  deriving (Show, Exception)

-- | Launches workers on this node. The 'IORef' holds the worker that is not
-- ready yet: if its process exits, the launch fails at once instead of
-- waiting for a timeout. A worker that dies later is noticed by the slot's
-- monitor.
slotLauncher
  :: WorkerCommandFor -> IORef (Maybe ServiceId) -> WorkerLauncher ServiceId
slotLauncher workerCommand starting = WorkerLauncher
  { withLaunchedWorker = \_ service go -> do
      let WorkerCommand (cmd, args) workerEnv = workerCommand service
      hostThread <- liftIO myThreadId
      let isStarting = (== Just service.serviceId) <$> readIORef starting
      liftIO $ writeIORef starting (Just service.serviceId)
      (_, _, _, handle) <- liftIO $
        createProcess (proc (toString cmd) (map toString args)) { env = workerEnv }
      terminated <- liftIO $ newIORef False
      void $ liftIO $ forkIO $ waitForProcess handle >>= \case
        ExitSuccess -> pure ()
        exitCode -> unlessM (readIORef terminated) $ do
          Log.warn "Worker process failed" (service.serviceId, exitCode)
          whenM isStarting $
            throwTo hostThread (WorkerExited service.serviceId exitCode)
      -- A worker that never connected would retry for a while; stop it.
      go service.serviceId `onException` liftIO (whenM isStarting $ do
        writeIORef terminated True
        terminateProcess handle)
  , overrideToLogPath = Nothing
  , connectionTimeout = Just 300
  , onRemoteError     = \e _ -> throwM e
  , storeCancelAction = emptyStoreCancelAction
  , onServiceExit     = emptyOnServiceExit
  }

instance Static (Binary (SendPort StartupReply)) where
  closureDict = static Dict
