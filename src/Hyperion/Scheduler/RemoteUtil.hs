{-# LANGUAGE AllowAmbiguousTypes        #-}
{-# LANGUAGE DeriveAnyClass             #-}
{-# LANGUAGE DuplicateRecordFields      #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedRecordDot        #-}
{-# LANGUAGE RankNTypes                 #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE StaticPointers             #-}
{-# LANGUAGE TypeApplications           #-}
{-# LANGUAGE TypeFamilies               #-}

module Hyperion.Scheduler.RemoteUtil where


import Control.Concurrent                  (forkIO, myThreadId)
import Control.Concurrent.MVar             (newEmptyMVar, putMVar, readMVar,
                                            takeMVar, tryTakeMVar)
import Control.Distributed.Process         (Closure, NodeId, Process, ProcessId,
                                            ProcessMonitorNotification (..),
                                            SendPort, matchIf, monitor, newChan,
                                            receiveChan, receiveWait,
                                            spawnLocal)
import Control.Distributed.Process.Async   (Async, AsyncResult (..), async,
                                            asyncLinked, asyncWorker, task,
                                            wait)
import Control.Distributed.Process.Closure (SerializableDict (..))
import Control.Exception                   (BlockedIndefinitelyOnMVar (..),
                                            Exception, SomeException,
                                            fromException, handle, throwTo)
import Control.Monad.Catch                 (mask, onException, throwM, try)
import Control.Monad.IO.Class              (liftIO)
import Control.Monad.Reader                (ask, asks, runReaderT)
import Control.Monad.Trans                 (lift)
import Data.Binary                         (Binary)
import Data.Kind                           (Type)
import Data.Map.Strict                     qualified as Map
import Data.Time.Clock                     (UTCTime, diffUTCTime,
                                            getCurrentTime)
import Data.Typeable                       (Typeable)
import Hyperion                            (Dict (..), Job, JobEnv (..),
                                            Serializable, Static,
                                            WorkerAddr (..), cAp, closureDict,
                                            remoteEvalOnWorker)
import Hyperion.CallClosure                (call')
import Hyperion.Scheduler.Config           (Config (..))
import Hyperion.Scheduler.Types            (Node (..))
import System.Timeout                      (timeout)

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

-- | Thrown to the action of 'runUntil' to stop it.
data RunUntilStopped = RunUntilStopped
  deriving (Show, Exception)

-- | Run the action until the given time. If it is not done by then, it is
-- stopped, and this returns 'Nothing'. This returns, or throws, only after the
-- action has ended, unless an exception interrupts the wait: then the action
-- is still stopped, but may not have ended yet.
runUntil :: UTCTime -> Job a -> Job (Maybe a)
runUntil deadline action = do
  jobEnv <- ask
  lift $ mask $ \restore -> do
    threadIdVar <- liftIO newEmptyMVar
    resultVar <- liftIO newEmptyMVar
    pid <- spawnLocal $ do
      liftIO $ myThreadId >>= putMVar threadIdVar
      try @_ @SomeException (restore (runReaderT action jobEnv))
        >>= liftIO . putMVar resultVar
    ref <- monitor pid
    let
      stop = do
        -- From another thread, so that an interrupt of this stop does not
        -- cancel it: throwTo blocks until the action can receive the exception.
        -- The action may have died before it recorded its ThreadId.
        _ <- liftIO $ forkIO $ handle (\BlockedIndefinitelyOnMVar -> pure ()) $
          readMVar threadIdVar >>= flip throwTo RunUntilStopped
        receiveWait
          [matchIf (\(ProcessMonitorNotification ref' _ _) -> ref' == ref) (\_ -> pure ())]
    now <- liftIO getCurrentTime
    let micros = max 0 $ ceiling (1e6 * realToFrac (diffUTCTime deadline now) :: Double)
    inTime <- restore (liftIO $ timeout micros $ takeMVar resultVar) `onException` stop
    stop
    -- The action can finish, or fail, between the timeout and the stop.
    result <- maybe (liftIO $ tryTakeMVar resultVar) (pure . Just) inTime
    case result of
      Just (Right a) -> pure (Just a)
      Just (Left e)
        | Just RunUntilStopped <- fromException e -> pure Nothing
        | otherwise                               -> throwM e
      Nothing -> pure Nothing

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
-- Returns SendPort for submitting requests to the service
startRemoteService :: forall service. Service service
                  => WorkerAddr
                  -> (SendPort (StartupResponseType service) -> Closure (Process ()))
                  -> Job ProcessId
startRemoteService address getServiceClosure = do
    (startupResponseSendPort, startupResponseReceivePort) <- lift newChan
    _ <- spawnLocalJob $
        remoteEvalOnWorker address $
        getServiceClosure startupResponseSendPort

    startupResponse <- lift $ receiveChan startupResponseReceivePort
    return $ getServiceProcessId @service startupResponse
