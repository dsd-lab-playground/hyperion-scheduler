{-# LANGUAGE DeriveAnyClass        #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE NoFieldSelectors      #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE StaticPointers        #-}

module Hyperion.Scheduler.RunTasks.RemoteRunTask
  ( RemoteRunTaskResult(..)
  , emptyRemoteRunTaskResult
  , remoteRunTask
  )
where

import Control.Monad.IO.Class         (MonadIO, liftIO)
import Control.Monad.Reader           (local)
import Data.Binary                    (Binary)
import Data.Map.Strict                (Map)
import Data.Map.Strict                qualified as Map
import Data.Set                       (Set)
import Data.Set                       qualified as Set
import GHC.Generics                   (Generic)
import Hyperion                       (Dict (..), Job, Static (..), cAp, cPure,
                                       setTaskCpus)
import Hyperion.Log                   qualified as Log
import Hyperion.Scheduler.FilePath    (VirtualFilePath (VirtualFilePath))
import Hyperion.Scheduler.Task.IsTask (IsTask (..), taskOutputPaths)
import Hyperion.Scheduler.Types       (FileSize (..), MemorySize (..), NumCPUs)
import Hyperion.Scheduler.Util        (closeOpenFdsOnExec)
import Hyperion.Scheduler.WorkerPool  (TWorker, remoteRunOnNewWorker)
import Hyperion.Util                  (peakResidentSetSizeSelfOrChildren)
import System.Directory.OsPath        (getFileSize)

data RemoteRunTaskResult = MkRemoteRunTaskResult
  { remoteTaskMemory    :: Maybe MemorySize
  , remoteTaskFileSizes :: Map VirtualFilePath FileSize
  }
  deriving (Generic, Binary, Show)

instance Static (Binary Hyperion.Scheduler.RunTasks.RemoteRunTask.RemoteRunTaskResult) where
  closureDict = static Dict

emptyRemoteRunTaskResult :: RemoteRunTaskResult
emptyRemoteRunTaskResult = MkRemoteRunTaskResult { remoteTaskMemory = Nothing, remoteTaskFileSizes = Map.empty }

afterReturnMemoryM :: MonadIO m => m () -> m MemorySize
afterReturnMemoryM go = do
  go
  fromKilobytes <$> liftIO peakResidentSetSizeSelfOrChildren
  where
    fromKilobytes k = MemorySize $ 1024 * fromIntegral k

afterReturnRemoteRunTaskResultM
  :: MonadIO m
  => Set VirtualFilePath
  -> m ()
  -> m RemoteRunTaskResult
afterReturnRemoteRunTaskResultM files go = do
  -- A native the task starts would otherwise hold this worker's connections
  -- open, so that nothing would notice this worker's death until the native ended.
  liftIO closeOpenFdsOnExec
  mem <- afterReturnMemoryM go
  let
    getFileSize' (VirtualFilePath p) = liftIO $ getFileSize p
    pathAndSize p = do
      size <- getFileSize' p
      return (p, fromIntegral size)
  fileSizes <- Map.fromList <$> mapM pathAndSize (Set.toList files)
  return $ MkRemoteRunTaskResult { remoteTaskMemory = Just mem, remoteTaskFileSizes = fileSizes }

remoteRunTask :: IsTask a => Maybe TWorker -> NumCPUs -> a -> Job RemoteRunTaskResult
remoteRunTask mWorker numCpus task = case taskClosure task of
  Nothing -> pure emptyRemoteRunTaskResult
  Just getClosure -> case mWorker of
    -- 'Hyperion.Scheduler.Task.TaskMap.validateTaskMap' rejects a task that has
    -- work to run yet allows itself no CPUs, so this is reachable only for an
    -- 'IsTask' that bypassed validation.
    Nothing -> Log.throwError $
      "remoteRunTask has work to run but no worker to run it on, because the \
      \task was allocated no CPUs: " <> show (taskTag task, numCpus)
    Just w -> do
      -- TODO: is setTaskCpus really needed?
      local (setTaskCpus numCpus) $
        remoteRunOnNewWorker w $
        static afterReturnRemoteRunTaskResultM
        -- TODO: measure input file sizes too?
        `cAp` cPure (taskOutputPaths task)
        `cAp` getClosure numCpus
