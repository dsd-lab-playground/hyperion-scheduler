{-# LANGUAGE DeriveAnyClass        #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE NoFieldSelectors      #-}
{-# LANGUAGE OverloadedRecordDot   #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE StaticPointers        #-}

module Hyperion.Scheduler.ReusableWorker where

import Control.DeepSeq                            (NFData)
import Control.Distributed.Process                (NodeId, Process, ProcessId,
                                                   SendPort, getSelfPid, link,
                                                   newChan, processNodeId,
                                                   sendChan, spawnLocal, unlink)
import Control.Distributed.Process.Extras.Time    (Delay (..))
import Control.Distributed.Process.ManagedProcess
import Control.Monad.Catch                        (bracket)
import Control.Monad.Trans                        (lift)
import Data.Binary
import Data.Typeable                              (Typeable)
import GHC.Generics                               (Generic)
import Hyperion                                   (Closure, Dict (..), Job,
                                                   Static (..), WorkerAddr, cAp,
                                                   cPure, remoteEvalOnWorker,
                                                   remoteEvalOnWorkerWithCustomLog)
import Hyperion.Log                               qualified as Log
import Hyperion.OsPath                            (OsPath)
import Hyperion.Scheduler.RemoteUtil              (awaitServiceStart, callOnNode,
                                                   spawnLocalJob)

data ReusableWorker = ReusableWorker ProcessId
  deriving (Generic, Binary, Eq, Show, Ord, NFData)

instance Static (Binary ReusableWorker) where
  closureDict = static Dict

getNodeId :: ReusableWorker -> NodeId
getNodeId (ReusableWorker pid) = processNodeId pid

workerMain :: Process ()
workerMain = serve () (statelessInit Infinity) statelessProcess


withLocalReusableWorker :: (ReusableWorker -> Process a) -> Process a
withLocalReusableWorker = bracket acquire release where
  acquire = do
    workerPid <- spawnLocal workerMain
    Log.info "Spawned local ReusableWorker" workerPid
    return $ ReusableWorker workerPid
  release (ReusableWorker pid) = shutdown pid


-- | Also returns the local process launching the worker.
spawnRemoteReusableWorker :: WorkerAddr -> Maybe OsPath -> Job (ReusableWorker, ProcessId)
spawnRemoteReusableWorker workerAddr mLogPath = do
  (sendPort, receivePort) <- lift $ newChan
  let
    remoteEval = case mLogPath of
      Just logPath -> remoteEvalOnWorkerWithCustomLog (const logPath)
      Nothing      -> remoteEvalOnWorker
  launching <- spawnLocalJob $ remoteEval workerAddr $
    static startReusableWorker `cAp` cPure sendPort
  workerPid <- lift $ awaitServiceStart
    ("ReusableWorker on " <> show workerAddr) launching receivePort
  lift $ link workerPid
  Log.info "Spawned ReusableWorker" (workerAddr, workerPid)
  return (ReusableWorker workerPid, launching)

  where
  startReusableWorker :: SendPort ProcessId -> Process ()
  startReusableWorker sendPort = do
    Log.text "Start ReusableWorker"
    selfPid <- getSelfPid
    sendChan sendPort selfPid
    workerMain

deleteReusableWorker :: ReusableWorker -> Process ()
deleteReusableWorker w@(ReusableWorker pid) = do
  -- TODO for debug
  Log.info "deleteReusableWorker" w
  unlink pid
  shutdown pid

-- TODO actually use server to have more control of the tasks?
runOnReusableWorker
  :: (Typeable a, Static (Binary a))
  => ReusableWorker -> Closure (Process a) -> Process a
runOnReusableWorker w closure = do
  callOnNode (getNodeId w) closure
