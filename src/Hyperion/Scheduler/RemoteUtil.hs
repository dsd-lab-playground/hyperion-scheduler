{-# LANGUAGE AllowAmbiguousTypes        #-}
{-# LANGUAGE DeriveAnyClass             #-}
{-# LANGUAGE DuplicateRecordFields      #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase                 #-}
{-# LANGUAGE OverloadedRecordDot        #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE RankNTypes                 #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE StaticPointers             #-}
{-# LANGUAGE TupleSections              #-}
{-# LANGUAGE TypeApplications           #-}
{-# LANGUAGE TypeFamilies               #-}

module Hyperion.Scheduler.RemoteUtil where


import Control.Concurrent                  (myThreadId)
import Control.Distributed.Process         (Closure, DiedReason, NodeId, Process,
                                            ProcessId,
                                            ProcessMonitorNotification (..),
                                            ReceivePort, SendPort, expect, kill,
                                            liftIO, matchChan, matchIf, monitor,
                                            newChan, receiveTimeout, receiveWait,
                                            spawnLocal, unmonitor)
import Control.Distributed.Process.Async   (Async, AsyncResult (..), async,
                                            asyncLinked, asyncWorker, task,
                                            wait)
import Control.Distributed.Process.Closure (SerializableDict (..))
import Control.Exception                   (Exception, throwTo)
import Control.Monad                       (forM, void)
import Control.Monad.Catch                 (bracket, onException)
import Control.Monad.Reader                (ask, asks, local, runReaderT)
import Control.Monad.Trans                 (lift)
import Data.Binary                         (Binary)
import Data.Kind                           (Type)
import Data.Map.Strict                     qualified as Map
import Data.Set                            qualified as Set
import Data.Time.Clock                     (NominalDiffTime)
import Data.Typeable                       (Typeable)
import GHC.Clock                           (getMonotonicTime)
import Hyperion                            (Dict (..), Job, JobEnv (..),
                                            Serializable, Static,
                                            WorkerAddr (..), WorkerLauncher (..),
                                            cAp, closureDict,
                                            remoteEvalOnWorker)
import Hyperion.CallClosure                (call')
import Hyperion.Log                        qualified as Log
import Hyperion.Scheduler.Config           (Config (..))
import Hyperion.Scheduler.Types            (Node (..))
import System.Environment                  (lookupEnv)
import Text.Read                           (readMaybe)

-- TODO orphan instance
instance Static (Binary (SendPort ProcessId)) where
  closureDict = static Dict

-- | Get all the nodes accessible to a Job
getJobNodes :: Config -> Job [Node]
getJobNodes config = do
  addrs <- asks (Map.keys . jobWorkerLauncherMap)
  cpusPerNode <- asks jobNodeCpus
  pure $ do
    addr <- addrs
    pure $ MkNode
      { memory           = config.nodeMemory
      , cpus             = cpusPerNode
      , localStoragePath = config.localStoragePath
      , localStorageSize = config.nodeLocalStorageSize
      , address          = addr
      }


-- * Workers that are lost

-- | Seconds that a worker the scheduler launches may take to connect; @0@ for
-- no limit. Read by the master. The limit ends at the connection: it says
-- nothing about how long a task or a service may run.
workerConnectTimeoutVar :: String
workerConnectTimeoutVar = "HYPERION_SCHEDULER_WORKER_CONNECT_TIMEOUT"

-- | 300 s. A worker is started on a node the job already holds, so it doesn't
-- wait in a queue, but on a slow filesystem it can take minutes to load (183 s
-- seen on Expanse), and a worker that gives up waits 50 s itself.
getWorkerConnectSeconds :: IO Int
getWorkerConnectSeconds = lookupEnv workerConnectTimeoutVar >>= \case
  Nothing -> pure 300
  Just s -> case readMaybe s of
    Just n | n >= 0 -> pure n
    _ -> Log.throwError $
      workerConnectTimeoutVar <> " must be a whole number of seconds, not " <> show s

-- | The limit for 'connectionTimeout', from 'getWorkerConnectSeconds'.
connectLimit :: Int -> Maybe NominalDiffTime
connectLimit 0 = Nothing
connectLimit n = Just (fromIntegral n)

-- | Every worker launched inside the given 'Job' may take this long to connect.
-- By default hyperion waits forever, as it should for a worker in a Slurm queue.
withWorkerConnectLimit :: Int -> Job a -> Job a
withWorkerConnectLimit seconds = local $ \env -> env
  { jobWorkerLauncherMap = (\l -> l { connectionTimeout = connectLimit seconds })
      <$> env.jobWorkerLauncherMap
  }

-- | Wait for a service's startup reply. The given local process is the one
-- launching the service: if it dies first, for example because the service's
-- worker never connected, say so instead of waiting forever. The reply itself
-- can be lost too, so the wait ends at twice the connect limit in any case.
awaitServiceStart
  :: String -> ProcessId -> ReceivePort a -> Process a
awaitServiceStart name launching startupReplies = do
  seconds <- liftIO getWorkerConnectSeconds
  let
    matches ref =
      [ matchChan startupReplies pure
      , matchIf (\(ProcessMonitorNotification ref' _ _) -> ref' == ref) $
          \(ProcessMonitorNotification _ _ reason) ->
            Log.throwError $ name <> " did not start: " <> show reason
      ]
  bracket (monitor launching) unmonitor $ \ref ->
    if seconds == 0
      then receiveWait (matches ref)
      else receiveTimeout (2 * seconds * 1000000) (matches ref) >>=
        maybe (Log.throwError $ name <> " did not start: no reply in "
                 <> show (2 * seconds) <> " s") pure

-- | Wait, up to 10 s in all, for local processes to end. A process launching a
-- service sends the service's worker its 'ShutDown' when the service has stopped:
-- if the job's process exits first, the worker waits for that message until the
-- allocation ends.
awaitProcessesEnd :: [ProcessId] -> Process ()
awaitProcessesEnd pids = do
  refs <- Set.fromList <$> mapM monitor pids
  deadline <- liftIO $ (+ 10) <$> getMonotonicTime
  let
    go remaining | Set.null remaining = pure ()
    go remaining = do
      now <- liftIO getMonotonicTime
      receiveTimeout (max 0 (round ((deadline - now) * 1e6)))
        [ matchIf (\(ProcessMonitorNotification ref _ _) -> Set.member ref remaining) $
            \(ProcessMonitorNotification ref _ _) -> pure ref
        ] >>= \case
          Just ref -> go (Set.delete ref remaining)
          Nothing -> do
            Log.warn "Processes still ending" (Set.size remaining)
            mapM_ unmonitor remaining
  go refs

-- | Start one service per item, in order. If one doesn't start, stop those
-- already started, so that their workers don't stay behind.
startAllOrNone :: (a -> Job b) -> (b -> Job ()) -> [a] -> Job [b]
startAllOrNone start stop = go []
  where
    go started []       = pure (reverse started)
    go started (x : xs) = do
      service <- start x `onException` mapM_ stop started
      go (service : started) xs

-- | A service of the run has died.
data ServiceLost = ServiceLost String DiedReason
  deriving (Show, Exception)

-- | Run the action, and throw 'ServiceLost' into it if one of the given named
-- processes dies. Requests to a service don't watch it, and some are made
-- holding the task queue's lock, so without this one lost service could stop
-- every node with no error. It costs one monitor per service and nothing per
-- task. The watch ends with the action, before the services are stopped.
withServiceWatch :: [(String, ProcessId)] -> Job a -> Job a
withServiceWatch services go = bracket start stop (const go)
  where
    start = lift $ do
      runThread <- liftIO myThreadId
      spawnLocal $ do
        refs <- forM services $ \(name, pid) -> (, name) <$> monitor pid
        ProcessMonitorNotification ref _ reason <- expect
        let name = Map.findWithDefault "A service" ref (Map.fromList refs)
        Log.err $ name <> " lost: " <> show reason
        liftIO $ throwTo runThread $ ServiceLost name reason
    stop watcher = lift $ do
      ref <- monitor watcher
      kill watcher "run finished"
      void $ receiveWait
        [matchIf (\(ProcessMonitorNotification ref' _ _) -> ref' == ref) (const (pure ()))]

-- | Run a Job computation in a separate thread.
spawnLocalJob :: Job () -> Job ProcessId
spawnLocalJob j = do
  jobEnv <- ask
  lift $ spawnLocal $ runReaderT j jobEnv

-- | (async . task) but for Job monad
asyncLocalJob :: (Binary a, Typeable a) => Job a -> Job (Async a)
asyncLocalJob j = do
  jobEnv <- ask
  lift $ async $ task $ runReaderT j jobEnv

-- | (asyncLinked . task) but for Job monad
-- NB: here "linked" means that the task links to the parent, and will abort if it dies.
-- Parent does not link to the task!
-- Use e.g. throwOnAsyncFailed (see below) to monitor job failures.
asyncLinkedLocalJob :: (Binary a, Typeable a) => Job a -> Job (Async a)
asyncLinkedLocalJob j = do
  jobEnv <- ask
  lift $ asyncLinked $ task $ runReaderT j jobEnv

data AsyncFailedException= AsyncFailedException !ProcessId !String
  deriving (Show, Exception)

throwOnAsyncFailed :: (Show a) => Async a -> Process ()
throwOnAsyncFailed asyncHandle = do
  tid <- liftIO myThreadId
  let asyncPid = asyncWorker asyncHandle
  _ <- spawnLocal $ do
    result <- wait asyncHandle
    case result of
      AsyncDone _    -> return ()
      AsyncCancelled -> return ()
      _              -> liftIO $ throwTo tid $ AsyncFailedException asyncPid $ show result
  return ()

toSerializableDict :: Typeable a => Closure (Dict (Serializable a) -> SerializableDict a)
toSerializableDict = static (\Dict -> SerializableDict)

closureSerializableDict :: (Typeable a, Static (Binary a)) => Closure (SerializableDict a)
closureSerializableDict = toSerializableDict `cAp` closureDict

callOnNode :: (Typeable a, Static (Binary a)) => NodeId -> Closure (Process a) -> Process a
callOnNode = call' closureSerializableDict

class (
    Binary (RequestType service),
    Binary (ResponseType service),
    Binary (StartupResponseType service),
    Typeable (RequestType service),
    Typeable (ResponseType service),
    Typeable (StartupResponseType service),
    Show (ResponseType service)
  ) => Service (service :: Type) where
    type RequestType service :: Type
    type ResponseType service :: Type
    type StartupResponseType service :: Type
    getServiceProcessId :: StartupResponseType service -> ProcessId

-- | Starts service on a remote worker.
-- Accepts worker address and a function to build a Closure with service loop.
-- Returns the service's process, and the local process launching it.
startRemoteService :: forall service. Service service
                  => String
                  -> WorkerAddr
                  -> (SendPort (StartupResponseType service) -> Closure (Process ()))
                  -> Job (ProcessId, ProcessId)
startRemoteService name address getServiceClosure = do
    (startupResponseSendPort, startupResponseReceivePort) <- lift newChan
    launching <- spawnLocalJob $
        remoteEvalOnWorker address $
        getServiceClosure startupResponseSendPort

    startupResponse <- lift $ awaitServiceStart
      (name <> " on " <> show address) launching startupResponseReceivePort
    return (getServiceProcessId @service startupResponse, launching)
