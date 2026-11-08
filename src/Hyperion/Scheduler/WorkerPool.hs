{-# LANGUAGE DeriveAnyClass        #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE NoFieldSelectors      #-}
{-# LANGUAGE OverloadedRecordDot   #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE StaticPointers        #-}

-- | WorkerPool creates one TWorker per CPU, for all nodes.
-- Each TWorker has its own log file, e.g. "exp-1-1.64.log"
-- For running computations on a TWorker, you have two options:
-- 1. Run closure on a new Hyperion worker (remoteRunOnNewWorker).
-- 2. Run closure on a ReusableWorker (remoteRunOnReusableWorker).
-- In both cases, output is redirected to TWorker.logPath
-- This allows us to reduce the total number of log files (one log per CPU).
--
-- important API:
-- - withWorkerPool :: [Node] -> (WorkerPool -> Job a) -> Job a
--   is a RAII function for creating a new WorkerPool.
--
-- - withWorkersFromPool:: WorkerPool -> WorkerAddr -> Int -> ([TWorker] -> Job a) -> Job a
--   is a RAII wrapper for marking a certain number of CPUs on a node as busy
--   and run computation on their TWorker's.
--
-- - remoteRunOnNewWorker :: TWorker -> Closure (Process a) -> Job a
--   Runs a closure on a new worker, logs are redirected to TWorker.logPath
--
-- - remoteRunOnReusableWorker :: TWorker -> Closure (Process a) -> Job a
--   Runs a closure on a ReusableWorker, logs are redirected to TWorker.logPath
--
-- - toReusableWorker :: TWorker -> Job ReusableWorker
--   lazy initialization of an underlying ReusableWorker
-- See RunTasks.hs for usage examples.
module Hyperion.Scheduler.WorkerPool where

import Control.Concurrent.MVar.Strict     (MVar, modifyMVar_, newMVar, putMVar,
                                           readMVar, takeMVar, tryPutMVar)
import Control.DeepSeq                    (NFData)
import Control.Monad                      (forM, replicateM, unless, when)
import Control.Monad.Catch                (MonadMask, bracket, bracketOnError)
import Control.Monad.Reader               (asks)
import Control.Monad.Trans                (MonadIO, lift, liftIO)
import Data.Binary
import Data.ByteString.Lazy               (ByteString)
import Data.Map.Strict                    (Map, (!))
import Data.Map.Strict                    qualified as Map
import Data.Set                           qualified as Set
import Data.Time.Clock                    (NominalDiffTime)
import Data.Typeable                      (Typeable)
import GHC.Clock                          (getMonotonicTimeNSec)
import Hyperion                           (Closure, Dict (..), Job, JobEnv (..),
                                           Process, ProgramInfo (..),
                                           Static (..), WorkerAddr, cAp,
                                           remoteEvalOnWorkerWithCustomLog)
import Hyperion.Log                       qualified as Log
import Hyperion.OsPath                    (OsPath, dropExtension, (<.>), (</>))
import Hyperion.OsString                  (OsString, fromString)
import Hyperion.Scheduler.ConcurrentQueue (ConcurrentQueue, flushQueue,
                                           newQueue, readQueue, writeQueue)
import Hyperion.Scheduler.FilePath        (getAddr)
import Hyperion.Scheduler.NodeSupervisor  (NodeSlots (..), NodeSupervisor,
                                           StartedSupervisor (..),
                                           runOnNodeSupervisor,
                                           startNodeSupervisors,
                                           stopNodeSupervisor)
import Hyperion.Scheduler.ReusableWorker  (ReusableWorker (..),
                                           deleteReusableWorker,
                                           runOnReusableWorker,
                                           spawnRemoteReusableWorker)
import Hyperion.Scheduler.TaskLaunch      (StagingReport (..),
                                           StagingSummary (..), TaskLaunch,
                                           TaskLaunchError (..),
                                           concreteTaskLaunch,
                                           isDefaultTaskLaunch,
                                           validateTaskLaunch)
import Hyperion.Scheduler.Types           (Node (..))
import Hyperion.Util                      (sanitizeFileString)
import System.Environment                 (lookupEnv)

data WorkerCpuId = WorkerCpuId
  { addr  :: WorkerAddr
  , cpuId :: Int
  }
  deriving (Eq, Ord, Show)

-- WorkerCpuId (RemoteAddr "exp-1-56" 64) -> "exp-1-56.64"
workerCpuIdToString :: WorkerCpuId -> OsString
workerCpuIdToString (WorkerCpuId workerAddr wId) = getAddr workerAddr <> "." <> fromString (show wId)

-- TODO rename
data TWorker = TWorker
  { workerId          :: WorkerCpuId
  , logPath           :: OsPath
  -- ReusableWorker is lazily initialized.
  , reusableWorkerVar :: MVar (Maybe ReusableWorker)
  -- The node's supervisor, which launches this CPU's workers, if any.
  , supervisor        :: Maybe NodeSupervisor
  }

newTWorker :: WorkerCpuId -> OsPath -> Maybe NodeSupervisor -> Job TWorker
newTWorker workerId logPath supervisor = do
  reusableWorkerVar <- liftIO $ newMVar Nothing
  return $ TWorker workerId logPath reusableWorkerVar supervisor

-- We need our own MVar functions to make it work for Job monad
modifyMVarMasked :: (MonadIO m, MonadMask m, NFData a) => MVar a -> (a -> m (a, b)) -> m b
modifyMVarMasked mVar go =
  bracketOnError acquire releaseOnError go' where
  acquire = liftIO $ takeMVar mVar
  releaseOnError = liftIO . tryPutMVar mVar
  go' x = do
    (x', res) <- go x
    liftIO $ putMVar mVar x'
    pure res

modifyMVarMasked_ :: (MonadIO m, MonadMask m, NFData a) => MVar a -> (a -> m a) -> m ()
modifyMVarMasked_ mVar go =
  modifyMVarMasked mVar $ \x -> do
    x' <- go x
    pure (x', ())

-- Lazy initialization for ReusableWorker
toReusableWorker :: TWorker -> Job ReusableWorker
toReusableWorker w =
  modifyMVarMasked w.reusableWorkerVar $ \mWorker -> do
    worker <- case mWorker of
      Just worker' -> pure worker'
      Nothing      -> spawnRemoteReusableWorker w.workerId.addr $ Just w.logPath
    pure (Just worker, worker)

deleteTWorker :: TWorker -> Job ()
deleteTWorker w =
  modifyMVarMasked_ w.reusableWorkerVar $ \mWorker -> do
    case mWorker of
      Just worker -> lift $ deleteReusableWorker worker
      Nothing     -> pure ()
    pure Nothing

-- | Run a closure on a new worker process, launched by the node's
-- supervisor if there is one, or else by the master.
remoteRunOnNewWorker
  :: (Typeable a, Static (Binary a))
  => TWorker -> Closure (Process a) -> Job a
remoteRunOnNewWorker w closure = case w.supervisor of
  Just supervisor -> lift $ decode <$>
    runOnNodeSupervisor supervisor w.workerId.cpuId
      (static encodeResult `cAp` closureDict `cAp` closure)
  Nothing ->
    remoteEvalOnWorkerWithCustomLog (const w.logPath) w.workerId.addr closure

encodeResult :: Dict (Binary a) -> Process a -> Process ByteString
encodeResult Dict = fmap encode


remoteRunOnReusableWorker
  :: (Typeable a, Static (Binary a))
  => TWorker -> Closure (Process a) -> Job a
remoteRunOnReusableWorker w closure = do
  rw <- toReusableWorker w
  lift $ runOnReusableWorker rw closure

-- NodeWorkerPool

type TimeNanoSec = Word64

-- For pretty-printing
data Stats = Stats
  { numWorkers               :: !Int
  , realTime                 :: NominalDiffTime
  , averageBusyWorkers       :: Double
  , averageWorkerUtilization :: Double
  }
  deriving (Show)


data NodeWorkerPool = NodeWorkerPool
  { workerMap   :: Map WorkerCpuId TWorker
  , supervisor  :: Maybe NodeSupervisor
  , idleWorkers :: ConcurrentQueue WorkerCpuId
  -- Total CPU time for all finished jobs
  , cpuTimeVar  :: MVar TimeNanoSec
  }

getLogPath :: OsPath -> WorkerCpuId -> OsPath
getLogPath logDir wId = logDir </> wId' <.> "log" where
  -- Sanitize workerCpuId string (e.g. "exp-1-56.64") to guarantee that it can be used in OsPath
  -- TODO: check that different nodes have different paths?
  wId' = sanitizeFileString $ workerCpuIdToString wId

nodeWorkerIds :: WorkerAddr -> Int -> [WorkerCpuId]
nodeWorkerIds addr maxWorkers = map (WorkerCpuId addr) [0 .. maxWorkers - 1]

newNodeWorkerPool :: WorkerAddr -> Int -> OsPath -> Maybe NodeSupervisor -> Job NodeWorkerPool
newNodeWorkerPool addr maxWorkers logDir supervisor = do
  let
    ids = nodeWorkerIds addr maxWorkers
  workerMap <- fmap Map.fromList $ forM ids $ \w -> do
    tWorker <- newTWorker w (getLogPath logDir w) supervisor
    return (w, tWorker)
  liftIO $ do
    idle <- newQueue
    mapM_ (writeQueue idle) ids
    cpuTimeVar <- newMVar 0
    return $ NodeWorkerPool workerMap supervisor idle cpuTimeVar

-- | Set to @0@ to launch task workers from the master instead of from a
-- 'NodeSupervisor' on each node.
nodeSupervisorsVar :: String
nodeSupervisorsVar = "HYPERION_SCHEDULER_NODE_SUPERVISORS"

deleteNodeWorkerPool :: NodeWorkerPool -> Job ()
deleteNodeWorkerPool pool = do
  idle <- fmap Set.fromList $ liftIO $ flushQueue pool.idleWorkers
  let
    allWorkers = Set.fromList $ Map.keys pool.workerMap
    busy = Set.difference allWorkers idle
  when (not $ Set.null busy) $ do
    Log.warn "NodeWorkerPool shutdown: some workers are still busy" $
      Set.map workerCpuIdToString busy
  mapM_ deleteTWorker $ Map.elems pool.workerMap
  mapM_ (lift . stopNodeSupervisor) pool.supervisor


-- WorkerPool

data WorkerPool = WorkerPool
  { nodeMap :: Map WorkerAddr NodeWorkerPool
  , logDir  :: OsPath
  }

newWorkerPool :: TaskLaunch -> [Node] -> Job WorkerPool
newWorkerPool launch nodes = do
  -- 'Log.getLogFile' is unset when logs go to stderr, e.g. under 'runJobLocal'.
  programLogDir <- asks ((.programLogDir) . jobProgramInfo)
  maybeLogFile <- Log.getLogFile
  let logDir = maybe (programLogDir </> "workers") dropExtension maybeLogFile
  supervisors <- startSupervisors launch logDir nodes
  nodeMap <- fmap Map.fromList $
    forM (zip nodes supervisors) $ \(node, supervisor) -> do
      pool <- newNodeWorkerPool node.address node.cpus logDir supervisor
      return (node.address, pool)
  return $ WorkerPool nodeMap logDir

-- | Stop the run if the task launch configuration can't work.
checkTaskLaunch :: TaskLaunch -> Job ()
checkTaskLaunch launch = do
  useSupervisor <- liftIO supervisorsEnabled
  let errors = validateTaskLaunch launch
        <> [ NeedsNodeSupervisors | not useSupervisor, not (isDefaultTaskLaunch launch) ]
  unless (null errors) $
    Log.throwError $ "Invalid task launch configuration: " <> show errors

supervisorsEnabled :: IO Bool
supervisorsEnabled = (/= Just "0") <$> lookupEnv nodeSupervisorsVar

-- | One supervisor per node, started all at once, or none if they are turned
-- off. Checks that every node staged the same programs: if not, stops them.
startSupervisors :: TaskLaunch -> OsPath -> [Node] -> Job [Maybe NodeSupervisor]
startSupervisors launch logDir nodes = do
  useSupervisor <- liftIO supervisorsEnabled
  if not useSupervisor
    then pure (map (const Nothing) nodes)
    else do
      nodeLaunch <- liftIO $ concreteTaskLaunch launch
      started <- startNodeSupervisors nodeLaunch
        [ NodeSlots node.address (map (getLogPath logDir) (nodeWorkerIds node.address node.cpus))
        | node <- nodes ]
      let
        reports = [ (s.address, report) | s <- started, Just report <- [s.staging] ]
        hashes = Set.fromList [ report.summary.manifestHash | (_, report) <- reports ]
      when (Set.size hashes > 1) $ do
        lift $ mapM_ (\s -> stopNodeSupervisor s.supervisor) started
        Log.throwError $ "Nodes staged different programs: " <> show reports
      case reports of
        (_, report) : _ -> Log.info "Every node staged the same programs" report.summary
        []              -> pure ()
      pure (map (Just . (.supervisor)) started)

withWorkerPool :: TaskLaunch -> [Node] -> (WorkerPool -> Job a) -> Job a
withWorkerPool launch nodes go = bracket acquire release go' where
  acquire = do
    startTime <- liftIO getMonotonicTimeNSec
    pool <- newWorkerPool launch nodes
    return (startTime, pool)

  release :: (TimeNanoSec, WorkerPool) -> Job ()
  release (startTime, pool) = do
    endTime <- liftIO getMonotonicTimeNSec
    let
      nodePools :: [NodeWorkerPool]
      nodePools = Map.elems pool.nodeMap
      getNodeCpuTime n = liftIO $ readMVar n.cpuTimeVar
    cpuTime <- sum <$> mapM getNodeCpuTime nodePools
    liftIO $ printStats cpuTime startTime endTime
    mapM_ deleteNodeWorkerPool nodePools

  go' (_, pool) = go pool

  printStats :: TimeNanoSec -> TimeNanoSec -> TimeNanoSec -> IO ()
  printStats cpuTime startTime endTime = do
    let
      numWorkers = sum $ map (.cpus) nodes
      realTime :: TimeNanoSec = endTime - startTime
      realTime' :: NominalDiffTime = (fromIntegral realTime) / 1e9
      averageBusyWorkers :: Double = (fromIntegral cpuTime) / (fromIntegral realTime)
      workerUtilization :: Double = averageBusyWorkers / (fromIntegral numWorkers)
    Log.info "Shutdown WorkerPool" $ Stats
      numWorkers realTime' averageBusyWorkers workerUtilization


-- RAII for taking one or more idle CPUs on a given node
withWorkersFromNodePool :: NodeWorkerPool -> Int -> ([TWorker] -> Job a) -> Job a
withWorkersFromNodePool pool numWorkers go
  | numWorkers < 0 = error $ "withWorkersFromNodePool invalid numWorkers=" <> show numWorkers
  | numWorkers == 0 = go []
  | otherwise = bracket acquire release go'
  where
    acquire = do
      workerIds <- replicateM numWorkers $ readQueue pool.idleWorkers
      let workers = Map.elems $ Map.restrictKeys pool.workerMap $ Set.fromList workerIds
      startTime <- liftIO getMonotonicTimeNSec
      return (startTime, workers)
    release (startTime, workers) = do
      endTime <- liftIO getMonotonicTimeNSec
      mapM_ (writeQueue pool.idleWorkers) $ map (.workerId) workers
      liftIO $ modifyMVar_ pool.cpuTimeVar $ \x -> pure $ x + (fromIntegral numWorkers * (endTime - startTime))
    go' (_, workers) = go workers

-- RAII for taking one or more idle CPUs on a given node
withWorkersFromPool:: WorkerPool -> WorkerAddr -> Int -> ([TWorker] -> Job a) -> Job a
withWorkersFromPool pool addr  = withWorkersFromNodePool $ pool.nodeMap ! addr
