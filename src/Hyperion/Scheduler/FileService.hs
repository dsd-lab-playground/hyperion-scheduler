{-# LANGUAGE DeriveAnyClass        #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE OverloadedRecordDot   #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE StaticPointers        #-}
{-# LANGUAGE TupleSections         #-}
{-# LANGUAGE TypeFamilies          #-}

-- | FileService
--
-- Functions:
-- withFileService :: Config -> [WorkerAddr] -> (FileService -> Job a) -> Job a
--
-- Usage:
-- withFileService config nodes go where
--   go fileService = do
--     fetchResponse <- fetchFilesToNode fileService sourceFiles destNode
--     deleteResponses <- deleteFileFromAllNodes fileService fileToDelete destNode

module Hyperion.Scheduler.FileService
  (FileService
  , Response(..)
  , withFileService
  , fileServiceProcesses
  , fetchFilesToNode
  , deleteGlobalFiles
  , deleteFilesFromNode
  , deleteFilesFromAllNodes
  , reserveFilesOnNode
  , registerFilesOnNode
  , incrementActiveFileUsages
  , decrementActiveFileUsages
  ) where


import Control.Distributed.Process                         (ProcessId)
import Control.Exception                                   (SomeException,
                                                            catch, tryJust)
import Control.Monad                                       (guard, unless)
import Control.Monad.IO.Class                              (liftIO)
import Data.List                                           (partition)
import Data.Map.Strict                                     (Map)
import Data.Map.Strict                                     qualified as Map
import Data.Set                                            (Set)
import Hyperion
import Hyperion.Log                                        qualified as Log
import Hyperion.Scheduler.Config                           (Config (..))
import Hyperion.Scheduler.FilePath                         (ClusterFilePath (..),
                                                            VirtualFilePath (..),
                                                            isNodeLocal)
import Hyperion.Scheduler.FileService.FileSender
import Hyperion.Scheduler.FileService.NodeLocalFileManager
import Hyperion.Scheduler.FileService.RetryTimeout         (RetryTimeoutPolicy (..))
import Hyperion.Scheduler.ReusableWorker                   (ReusableWorker)
import Hyperion.Scheduler.Types                            (FileSize, Node (..))
import Hyperion.Util                                       (minute)
import System.Directory.OsPath                             (removeFile)
import System.IO.Error                                     (isDoesNotExistError)


data FileService = MkFileService
  { config   :: Config
  , senders  :: FileSenders
  , managers :: NodeLocalFileManagers
  }

-- | The service's processes, named for error messages.
fileServiceProcesses :: FileService -> [(String, ProcessId)]
fileServiceProcesses service =
  fileSenderProcesses service.senders <> fileManagerProcesses service.managers

withFileService :: Config -> [Node] -> (FileService -> Job a) -> Job a
withFileService config nodes go = do
  let
    -- TODO pass configs as arguments
    senderConfig = MkFileSenderConfig
      { maxDownloadRequests = Just 16
      , timeout             = Nothing
      }
    mkManagerConfig node = MkNodeLocalFileManagerConfig
      { maxDownloadRequests = Nothing
      , maxDownloadRequestsPerNode = Just 2
      , retryPolicy = MkRetryTimeoutPolicy
        { maxRetryCount = 2
        -- 5 minute should be enough, unless we fetch too many files in parallel.
        -- For example, if numNodes = 16, maxDownloadRequestsPerNode = 2
        -- we'll have no more than 32 files fetching in parallel.
        -- For fetch speed ~200 MB/s (Expanse) and files sizes ~1GB (nmax=26),
        -- it will take ~160s to process these requests.
        , initialTimeout = Just $ 5 * minute
        }
      , localStoragePath = node.localStoragePath
      , localStorageSize = node.localStorageSize
      , reportInterval = config.reportInterval
      }
    nodesConfig = map (\node -> (node.address, mkManagerConfig node)) nodes
    go' fileSenders = withNodeLocalFileManagers fileSenders nodesConfig (go'' fileSenders)
    go'' fileSenders fileManagers = go $ MkFileService config fileSenders fileManagers

  withFileSenders senderConfig (map (.address) nodes) go'

getFileManager :: FileService -> WorkerAddr -> NodeLocalFileManager
getFileManager service node = case Map.lookup node managers of
  Just manager -> manager
  Nothing -> error $ "FileService: no file manager found for node: " <> show node
  where
    (NodeLocalFileManagers managers) = service.managers


fetchFilesToNode :: FileService -> Set ClusterFilePath -> WorkerAddr -> [ReusableWorker] -> Process Response
fetchFilesToNode service sourceFiles destNode workers = fetchFiles (getFileManager service destNode) sourceFiles workers

getNodes :: FileService -> [WorkerAddr]
getNodes service = Map.keys managers where
  (NodeLocalFileManagers managers) = service.managers

deleteGlobalFile :: FileService -> VirtualFilePath -> Process Response
deleteGlobalFile service file@(VirtualFilePath p) =
  if isNodeLocal service.config file then do
    Log.err $ "FileService: deleteGlobalFile: expected global file, but got node-local: " <> show file
    return FileDeleteError
  else
    liftIO $ doDelete `catch` \(e :: SomeException) -> do
      Log.err $ "FileService: deleteGlobalFile: " <> show (e, file)
      return FileDeleteError
  where
    doDelete = do
      res <- liftIO $ tryJust
        (guard . isDoesNotExistError)
        (removeFile p)
      return $ case res of
        Left _  -> FileDoesNotExist
        Right _ -> FileDeleted

deleteGlobalFiles :: FileService -> [VirtualFilePath] -> Process [Response]
deleteGlobalFiles  = mapM . deleteGlobalFile


deleteFilesFromNode :: FileService-> WorkerAddr -> [VirtualFilePath]  -> Process Response
deleteFilesFromNode service node files = deleteFiles (getFileManager service node) files

-- TODO rename
doOnAllNodes :: FileService -> (WorkerAddr -> Process a) -> Process (Map WorkerAddr a)
doOnAllNodes service go =
  Map.fromList <$> mapM go' (getNodes service)
  where
    go' addr = (addr, ) <$> go addr

-- TODO: This will send requests to all nodes, but ideally should only send to the nodes that have at least one file
deleteFilesFromAllNodes :: FileService -> [VirtualFilePath] -> Process (Map WorkerAddr Response)
deleteFilesFromAllNodes service files = doOnAllNodes service $ \node -> do
  unless (null globalFiles) $
    Log.err $ "FileService: deleteFilesFromAllNodes: expected node-local file, but got global: " <> show globalFiles
  deleteFilesFromNode service node localFiles
  where
    (localFiles, globalFiles) = partition (isNodeLocal service.config) files

reserveFilesOnNode :: FileService -> Map VirtualFilePath FileSize -> WorkerAddr -> Process Response
reserveFilesOnNode service fileSizes addr = reserveFiles (getFileManager service addr) fileSizes

registerFilesOnNode :: FileService -> Map VirtualFilePath FileSize -> WorkerAddr -> Process Response
registerFilesOnNode service fileSizes addr = registerFiles (getFileManager service addr) fileSizes

incrementActiveFileUsages :: FileService -> Set VirtualFilePath -> WorkerAddr -> Process Response
incrementActiveFileUsages service files addr = incrementActiveUsages (getFileManager service addr) files

decrementActiveFileUsages :: FileService -> Set VirtualFilePath -> WorkerAddr -> Process Response
decrementActiveFileUsages service files addr = decrementActiveUsages (getFileManager service addr) files
