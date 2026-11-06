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
import Control.Distributed.Process        (ProcessId)
import Control.Exception                  (SomeException)
import Control.Monad                      (forM, forM_, replicateM, when)
import Control.Monad.Catch                (MonadMask, bracket, bracketOnError,
                                           catch, finally)
import Control.Monad.Reader               (asks)
import Control.Monad.Trans                (MonadIO, lift, liftIO)
import Data.Binary
import Data.ByteString.Lazy               (ByteString)
import Data.Map.Strict                    (Map, (!))
import Data.Map.Strict                    qualified as Map
import Data.Maybe                         (catMaybes)
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
import Hyperion.Scheduler.NodeSupervisor  (NodeSupervisor,
                                           nodeSupervisorProcess,
                                           runOnNodeSupervisor,
                                           startNodeSupervisor,
                                           stopNodeSupervisor)
import Hyperion.Scheduler.RemoteUtil      (awaitProcessesEnd,
                                           getWorkerConnectSeconds,
                                           startAllOrNone)
import Hyperion.Scheduler.ReusableWorker  (ReusableWorker (..),
                                           deleteReusableWorker,
                                           runOnReusableWorker,
                                           spawnRemoteReusableWorker)
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
  , reusableWorkerVar :: MVar (Maybe (ReusableWorker, ProcessId))
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

-- Lazy initialization for ReusableWorker
toReusableWorker :: TWorker -> Job ReusableWorker
toReusableWorker w =
  modifyMVarMasked w.reusableWorkerVar $ \mWorker -> do
    worker <- case mWorker of
      Just worker' -> pure worker'
      Nothing      -> spawnRemoteReusableWorker w.workerId.addr $ Just w.logPath
    pure (Just worker, fst worker)

-- | Stop the worker's reusable worker, if any, and return the local process
-- that launched it, for 'awaitProcessesEnd'.
deleteTWorker :: TWorker -> Job (Maybe ProcessId)
deleteTWorker w =
  modifyMVarMasked w.reusableWorkerVar $ \mWorker -> case mWorker of
    Just (worker, launching) -> do
      lift $ deleteReusableWorker worker
      pure (Nothing, Just launching)
    Nothing -> pure (Nothing, Nothing)

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

newNodeWorkerPool :: WorkerAddr -> Int -> OsPath -> Job NodeWorkerPool
newNodeWorkerPool addr maxWorkers logDir = do
  let
    ids = map (WorkerCpuId addr) [0 .. maxWorkers - 1]
  useSupervisor <- liftIO $ (/= Just "0") <$> lookupEnv nodeSupervisorsVar
  supervisor <- if useSupervisor
    then do
      connectSeconds <- liftIO getWorkerConnectSeconds
      Just <$> startNodeSupervisor connectSeconds addr (map (getLogPath logDir) ids)
    else pure Nothing
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
  launching <- mapM deleteTWorker $ Map.elems pool.workerMap
  lift (awaitProcessesEnd $ catMaybes launching)
    `finally` mapM_ (lift . stopNodeSupervisor) pool.supervisor


-- WorkerPool

data WorkerPool = WorkerPool
  { nodeMap :: Map WorkerAddr NodeWorkerPool
  , logDir  :: OsPath
  }

newWorkerPool :: [Node] -> Job WorkerPool
newWorkerPool nodes = do
  -- 'Log.getLogFile' is unset when logs go to stderr, e.g. under 'runJobLocal'.
  programLogDir <- asks ((.programLogDir) . jobProgramInfo)
  maybeLogFile <- Log.getLogFile
  let logDir = maybe (programLogDir </> "workers") dropExtension maybeLogFile
  nodeMap <- fmap Map.fromList $
    startAllOrNone
      (\node -> do
        pool <- newNodeWorkerPool node.address node.cpus logDir
        return (node.address, pool))
      (deleteNodeWorkerPool . snd)
      nodes
  return $ WorkerPool nodeMap logDir

-- | The node supervisors' processes, named for error messages.
workerPoolProcesses :: WorkerPool -> [(String, ProcessId)]
workerPoolProcesses pool =
  [ ("NodeSupervisor on " <> show addr, nodeSupervisorProcess supervisor)
  | (addr, NodeWorkerPool { supervisor = Just supervisor }) <- Map.toList pool.nodeMap ]

withWorkerPool :: [Node] -> (WorkerPool -> Job a) -> Job a
withWorkerPool nodes go = bracket acquire release go' where
  acquire = do
    startTime <- liftIO getMonotonicTimeNSec
    pool <- newWorkerPool nodes
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
    -- In a failed run another task's failure can arrive here as an exception. It
    -- must not leave the other nodes' supervisors and workers running.
    forM_ nodePools $ \nodePool -> deleteNodeWorkerPool nodePool `catch`
      \(e :: SomeException) -> Log.warn "While stopping a node's workers" (show e)

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
