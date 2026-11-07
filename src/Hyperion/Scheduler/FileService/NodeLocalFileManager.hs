{-# LANGUAGE AllowAmbiguousTypes        #-}
{-# LANGUAGE DeriveAnyClass             #-}
{-# LANGUAGE DerivingStrategies         #-}
{-# LANGUAGE DuplicateRecordFields      #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedRecordDot        #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE RankNTypes                 #-}
{-# LANGUAGE StaticPointers             #-}
{-# LANGUAGE TypeFamilies               #-}

-- | NodeLocalFileManager is running on a node.
-- It can do the following things:
-- - fetchFiles: fetch (Set NodeLocalFilePath) from other nodes to the local storage.
-- - deleteFile: delete NodeLocalFilePath from the local storage.
-- - reserveFiles: reserve space for files to be fetched or created.
-- - registerFiles: register existing file sizes (e.g. created locally by some task).
-- - incrementActiveUsages: Mark files as currently used (e.g. input files for a task that will be submitted).
-- - decrementActiveUsages: Decrement usage count (e.g. when a task finished).
-- Fetched files with ActiveUsages = 0 can be deleted to free up space.
--
-- Usage:
-- withNodeLocalFileManager node loop where
--   loop manager = do
--     fetchResponse <- manager.fetchFiles myClusterFilePaths workers
--     doSomeWork
--     deleteResponse <- manager.deleteFiles myFilesToDelete
--     return someResult
--
-- NB: We use FileService as a wrapper around NodeLocalFileManagers.

module Hyperion.Scheduler.FileService.NodeLocalFileManager
  ( NodeLocalFileManager (..)
  , NodeLocalFileManagers (..)
  , fileManagerProcesses
  , NodeLocalFileManagerConfig (..)
  , Response(..)
  , withNodeLocalFileManagers
  , fetchFiles
  , deleteFiles
  , reserveFiles
  , registerFiles
  , incrementActiveUsages
  , decrementActiveUsages
  ) where

import Control.Concurrent                          (threadDelay)
import Control.Concurrent.STM                      (STM, TQueue, TVar,
                                                    atomically, check,
                                                    modifyTVar', newTQueueIO,
                                                    newTVarIO, readTQueue,
                                                    readTVar, readTVarIO, retry,
                                                    writeTQueue, writeTVar)
import Control.Distributed.Process                 (ProcessId,
                                                    ProcessLinkException (..),
                                                    SendPort, getSelfNode,
                                                    getSelfPid, link, newChan,
                                                    processNodeId, receiveChan,
                                                    sendChan, spawnLocal,
                                                    unlink)
import Control.Distributed.Process.Async           (AsyncResult (..),
                                                    asyncLinked, cancelWait,
                                                    task)
import Control.Distributed.Process.Extras.Time     (Delay (..))
import Control.Distributed.Process.ManagedProcess  (StatelessHandler,
                                                    apiHandlers, cast,
                                                    continue_, handleCast_,
                                                    serve, shutdown,
                                                    shutdownHandler,
                                                    statelessInit,
                                                    statelessProcess)
import Control.Monad                               (filterM, forM, forever,
                                                    guard, when)
import Control.Monad.Catch                         (SomeException, bracket,
                                                    bracket_, catch, finally,
                                                    try, tryJust)
import Control.Monad.IO.Class                      (MonadIO, liftIO)
import Control.Monad.Trans                         (lift)
import Data.Binary                                 (Binary)
import Data.List                                   (intercalate, sortOn)
import Data.List.NonEmpty                          (NonEmpty (..))
import Data.List.NonEmpty                          qualified as NonEmpty
import Data.Map.Strict                             (Map, (!))
import Data.Map.Strict                             qualified as Map
import Data.Maybe                                  (fromMaybe)
import Data.Ord                                    (Down (..))
import Data.Set                                    (Set)
import Data.Set                                    qualified as Set
import Data.Text                                   (Text)
import Data.Text                                   qualified as Text
import Data.Time                                   (nominalDiffTimeToSeconds)
import Data.Time.Clock                             (NominalDiffTime)
import Data.Time.Format                            (defaultTimeLocale,
                                                    formatTime)
import Data.Time.LocalTime                         (ZonedTime, getZonedTime,
                                                    zonedTimeToUTC)
import Data.Typeable                               (Typeable)
import GHC.Clock                                   (getMonotonicTimeNSec)
import GHC.Generics                                (Generic)
import Hyperion                                    hiding (Service)
import Hyperion.Log                                qualified as Log
import Hyperion.OsPath                             (OsPath)
import Hyperion.OsString                           (toString)
import Hyperion.Scheduler.FilePath                 (ClusterFilePath (..),
                                                    VirtualFilePath (..),
                                                    isUnderLocalStorage,
                                                    toVirtualFilePath)
import Hyperion.Scheduler.FileService.FileSender   (FileSender,
                                                    FileSenders (..), TimeStamp)
import Hyperion.Scheduler.FileService.FileSender   qualified as FileSender
import Hyperion.Scheduler.FileService.RetryTimeout (RetryTimeoutPolicy (..))
import Hyperion.Scheduler.FileService.RetryTimeout qualified as RetryTimeout
import Hyperion.Scheduler.RemoteUtil               (RequestType, ResponseType,
                                                    Service (..),
                                                    StartupResponseType,
                                                    getServiceProcessId,
                                                    awaitProcessesEnd,
                                                    startAllOrNone,
                                                    startRemoteService)
import Hyperion.Scheduler.ReusableWorker           (ReusableWorker (..),
                                                    getNodeId,
                                                    runOnReusableWorker,
                                                    withLocalReusableWorker)
import Hyperion.Scheduler.TPrioQueue               (TPrioQueue (..))
import Hyperion.Scheduler.TPrioQueue               qualified as TPrioQueue
import Hyperion.Scheduler.Types                    (FileSize (..))
import Hyperion.Scheduler.Util                     (measureRealTime)
import Hyperion.Util                               (nominalDiffTimeToMicroseconds)
import System.Directory.OsPath                     qualified as System.Directory
import System.DiskSpace                            (DiskUsage (..),
                                                    getDiskUsage)
import System.IO.Error                             (isDoesNotExistError)
import Text.Printf                                 qualified as Printf

-- Public interface

data NodeLocalFileManager = NodeLocalFileManager
  { config :: NodeLocalFileManagerConfig
  , addr   :: WorkerAddr
  , pid    :: ProcessId
  }
  deriving(Generic, Binary)

data NodeLocalFileManagers = NodeLocalFileManagers (Map WorkerAddr NodeLocalFileManager)
  deriving(Generic, Binary)

-- | The managers' processes, named for error messages.
fileManagerProcesses :: NodeLocalFileManagers -> [(String, ProcessId)]
fileManagerProcesses (NodeLocalFileManagers managers) =
  [ ("NodeLocalFileManager on " <> show node, manager.pid)
  | (node, manager) <- Map.toList managers ]

data NodeLocalFileManagerConfig = MkNodeLocalFileManagerConfig
  { retryPolicy                :: RetryTimeoutPolicy
  -- Limit the number of active Download requests:
  , maxDownloadRequests        :: Maybe Int
  , maxDownloadRequestsPerNode :: Maybe Int
  , localStoragePath           :: OsPath
  , localStorageSize           :: FileSize
  , reportInterval             :: NominalDiffTime
  }
  deriving(Generic, Binary, Show)

instance Static (Binary NodeLocalFileManagerConfig) where
  closureDict = static Dict

isNodeLocal :: NodeLocalFileManagerConfig -> OsPath -> Bool
isNodeLocal config = isUnderLocalStorage config.localStoragePath

isNodeLocalV :: NodeLocalFileManagerConfig -> VirtualFilePath -> Bool
isNodeLocalV config = \(VirtualFilePath path) -> isLocal path
  where
    isLocal = isNodeLocal config

fetchFiles :: NodeLocalFileManager -> Set ClusterFilePath -> [ReusableWorker] -> Process Response
fetchFiles manager files workers = do
  let
    node = manager.addr
    needFetch (GlobalFilePath _)          = False
    needFetch (NodeLocalFilePath node' _) = node /= node'
    filteredFiles = Set.filter needFetch files
  if Set.null filteredFiles then
    return $ FetchSuccess 0
  else
    sendRequestAndGetResponse manager $ Fetch filteredFiles workers

deleteFiles :: NodeLocalFileManager -> [VirtualFilePath] -> Process Response
deleteFiles manager files = sendRequestAndGetResponse manager $ Delete files

-- Reserve space for files to be fetched or created.
reserveFiles :: NodeLocalFileManager -> Map VirtualFilePath FileSize -> Process Response
reserveFiles manager fileSizes = do
  let localFiles = Map.filterWithKey (\p _ -> isNodeLocalV manager.config p) fileSizes
  if Map.null localFiles then
    return ReserveSuccess
  else
    sendRequestAndGetResponse manager $ Reserve localFiles

-- Register existing file sizes (e.g. created locally by some task).
registerFiles :: NodeLocalFileManager -> Map VirtualFilePath FileSize -> Process Response
registerFiles manager fileSizes = do
  -- TODO: print error for global files?
  let localFiles = Map.filterWithKey (\p _ -> isNodeLocalV manager.config p) fileSizes
  if Map.null localFiles then
    return RegisterSuccess
  else
    sendRequestAndGetResponse manager $ Register localFiles

-- Mark files as currently used by some task (e.g. input files).
incrementActiveUsages :: NodeLocalFileManager -> Set VirtualFilePath -> Process Response
incrementActiveUsages manager files = do
  -- TODO: print error for global files?
  let localFiles = Set.filter (isNodeLocalV manager.config) files
  if Set.null localFiles then
    return ChangeActiveUsagesSuccess
  else
    sendRequestAndGetResponse manager $ IncrementActiveUsages localFiles

-- Decrement usage counter (e.g. for input files of a finished task).
decrementActiveUsages :: NodeLocalFileManager -> Set VirtualFilePath -> Process Response
decrementActiveUsages manager files = do
  -- TODO: print error for global files?
  let localFiles = Set.filter (isNodeLocalV manager.config) files
  if Set.null localFiles then
    return ChangeActiveUsagesSuccess
  else
    sendRequestAndGetResponse manager $ DecrementActiveUsages localFiles

-- | RAII wrapper for NodeLocalFileManagers.
-- Should be called on the master node. Starts NodeLocalFileManager on each worker node and shuts it down on exit.
withNodeLocalFileManagers
  :: FileSenders
  -> [(WorkerAddr, NodeLocalFileManagerConfig)]
  -> (NodeLocalFileManagers -> Job a)
  -> Job a
withNodeLocalFileManagers fileSenders nodesConfig go = bracket acquire release (go . fst) where
  acquire = do
    managers <- startAllOrNone
      (\(node, config) -> do
        (manager, launching) <- startFileManager config fileSenders node
        return ((node, manager), launching))
      (\((_, manager), launching) -> shutdown' manager >> lift (awaitProcessesEnd [launching]))
      nodesConfig
    return (NodeLocalFileManagers $ Map.fromList $ map fst managers, map snd managers)

  release (NodeLocalFileManagers managerMap, launching) = do
    mapM_ shutdown' $ Map.elems managerMap
    lift $ awaitProcessesEnd launching

  shutdown' manager = lift $ shutdown manager.pid


-- Implementation

-- FilerService requests and responses

-- | Request accepted by NodeLocalFileManager.
-- Client provides command (fetch/delete) and port used to send response.
data Request
  = Fetch (Set ClusterFilePath) [ReusableWorker] (SendPort Response)
  | Delete [VirtualFilePath] (SendPort Response)
  -- Reserve space for files of a certain size (to be fetched or created)
  | Reserve (Map VirtualFilePath FileSize) (SendPort Response)
  -- Register existing local files (created by some task)
  -- TODO: we can check file sizes by ourselves, but scheduler already has this information.
  | Register (Map VirtualFilePath FileSize) (SendPort Response)
  -- TODO: use generic (AddActiveUsages Int ...)?
  | IncrementActiveUsages (Set VirtualFilePath) (SendPort Response)
  | DecrementActiveUsages (Set VirtualFilePath) (SendPort Response)
  deriving (Generic, Binary, Show)

-- For pretty-printing ReserveLimitExceeded
data SpaceRequired a = SpaceRequired a
  deriving (Generic, Binary, Show, Eq)
data SpaceAvailable a = SpaceAvailable a
  deriving (Generic, Binary, Show, Eq)

data Response
  = FetchSuccess FileSize
  -- TODO: add error message
  | FetchError [ClusterFilePath]
  | FileDeleted
  | FileDoesNotExist
  -- TODO: add error message
  | FileDeleteError
  | ReserveSuccess
  -- Running out of local disk space
  | ReserveLimitExceeded (SpaceRequired FileSize) (SpaceAvailable FileSize)
  | ReserveError String
  | RegisterSuccess
  | RegisterError String
  | ChangeActiveUsagesSuccess
  | ChangeActiveUsagesError String
  deriving (Generic, Binary, Show, Eq)

-- NodeLocalFileManager implementation

-- Message sent by NodeLocalFileManager upon creation
type StartupResponse = ProcessId

instance Service NodeLocalFileManager  where
    type RequestType NodeLocalFileManager = Request
    type ResponseType NodeLocalFileManager = Response
    type StartupResponseType NodeLocalFileManager = StartupResponse
    getServiceProcessId = id

-- TODO orphan class instance, move to Hyperion.WorkerCpuPool
instance Static (Binary WorkerAddr) where
  closureDict = static Dict

-- Interaction with NodeLocalFileManager

sendRequestAndGetResponse :: NodeLocalFileManager -> (SendPort Response -> Request) -> Process Response
sendRequestAndGetResponse manager request = do
  (responseSendPort, responseReceivePort) <- newChan
  cast manager.pid $ request responseSendPort
  receiveChan responseReceivePort

startFileManager :: NodeLocalFileManagerConfig -> FileSenders -> WorkerAddr -> Job (NodeLocalFileManager, ProcessId)
startFileManager config fileSenders node = do
  let
    getClosure startupResponseSendPort =
      static mainLoop
      `cAp` cPure config
      `cAp` cPure fileSenders
      `cAp` cPure startupResponseSendPort
  Log.info "Start remote NodeLocalFileManager" node
  (pid, launching) <- startRemoteService @NodeLocalFileManager "NodeLocalFileManager" node getClosure
  return (NodeLocalFileManager config node pid, launching)

-- Statistics

data DiskUsageInfo = MkDiskUsageInfo
  { diskUsed  :: FileSize
  , diskTotal :: FileSize
  }
  deriving (Generic, Binary, Eq, Ord)

getDiskUsageInfo ::  OsPath -> IO DiskUsageInfo
getDiskUsageInfo path = do
  d <- getDiskUsage $ toString path
  return $ MkDiskUsageInfo
    { diskUsed = FileSize $ fromIntegral $ d.diskTotal - d.diskAvail
    , diskTotal = FileSize $ fromIntegral d.diskTotal
    }

instance Show DiskUsageInfo where
  show d =
    percentStr <> " (" <> show d.diskUsed <> " / " <> show d.diskTotal <> ")"
    where
      percentStr = Printf.printf "%.1f%%" percent
      used' = fromIntegral d.diskUsed :: Double
      total' = fromIntegral d.diskTotal :: Double
      percent = 100 * (used' / total')

data DiskUsageInfoWithTime = MkDiskUsageWithTime DiskUsageInfo ZonedTime
  deriving (Generic, Binary)

instance Eq DiskUsageInfoWithTime where
  (MkDiskUsageWithTime d t) == (MkDiskUsageWithTime d' t') =
    d == d' && zonedTimeToUTC t == zonedTimeToUTC t'
instance Ord DiskUsageInfoWithTime where
  compare (MkDiskUsageWithTime d t) (MkDiskUsageWithTime d' t') =
    compare (d, zonedTimeToUTC t) (d', zonedTimeToUTC t')

-- Same datetime formatting as in Hyperion.Log
instance Show DiskUsageInfoWithTime where
  show (MkDiskUsageWithTime d t) = show d ++ " " ++ formatTime defaultTimeLocale "[%a %D %X]" t

-- Use strict data types to prevent space leaks
data Stats = MkStats
  { filesFetched        :: !Int
  , bytesFetched        :: !FileSize
  , maxFileSize         :: !FileSize
  , fileFetchDuplicates :: !Int
  , filesRemoved        :: !Int
  , fetchErrors         :: !Int
  , removeErrors        :: !Int
    -- Real time spent on processing requests
  , realTime            :: !NominalDiffTime
  , realFetchTime       :: !NominalDiffTime
  , realDeleteTime      :: !NominalDiffTime
    -- Total time spent inside worker threads
  , parallelTime        :: !NominalDiffTime
  -- Max time per file (including waiting)
  , maxFetchFileTime    :: !NominalDiffTime
  , maxDeleteFileTime   :: !NominalDiffTime
  , maxTotalSize        :: !FileSize
  , initialDiskUsage    :: Maybe DiskUsageInfoWithTime
  , maxDiskUsage        :: Maybe DiskUsageInfoWithTime
  }
  deriving (Generic, Binary)

instance Semigroup Stats where
  a <> b = MkStats
    { filesFetched = add' filesFetched
    , bytesFetched = add' bytesFetched
    , maxFileSize = max' maxFileSize
    , fileFetchDuplicates = add' fileFetchDuplicates
    , filesRemoved = add' filesRemoved
    , fetchErrors  = add' fetchErrors
    , removeErrors = add' removeErrors
    , realTime = add' realTime
    , realFetchTime = add' realFetchTime
    , realDeleteTime = add' realDeleteTime
    , parallelTime = add' parallelTime
    , maxFetchFileTime = max' maxFetchFileTime
    , maxDeleteFileTime = max' maxDeleteFileTime
    , maxTotalSize = max' maxTotalSize
    , initialDiskUsage = earliest' initialDiskUsage
    , maxDiskUsage = max' maxDiskUsage
    }
    where
      add' :: Num a => (Stats -> a) -> a
      add' getProperty = getProperty a + getProperty b
      max' :: Ord a => (Stats -> a) -> a
      max' getProperty = getProperty a `max` getProperty b

      earliest' getProperty = getProperty a `earliest` getProperty b
      earliest Nothing d = d
      earliest d Nothing = d
      earliest dt@(Just (MkDiskUsageWithTime _ t)) dt'@(Just (MkDiskUsageWithTime _ t')) =
        if (zonedTimeToUTC t) <= (zonedTimeToUTC t') then dt
        else dt'

zeroStats :: Stats
zeroStats = MkStats 0 0 0 0 0 0 0 0 0 0 0 0 0 0 Nothing Nothing

instance Monoid Stats where
  mempty = zeroStats

statsToText :: Stats -> Text
statsToText stats = Text.pack $ intercalate endl ("Stats" : items) where
  endl = "\n  "
  showKeyVal :: Show a => String -> a -> String
  showKeyVal key value = key <> ": " <> show value
  showKeyGetVal :: Show a => String -> (Stats -> a) -> String
  showKeyGetVal key getValue = showKeyVal key (getValue stats)
  items =
    [ showKeyGetVal "Files fetched" filesFetched
    , showKeyGetVal "Bytes fetched" bytesFetched
    , showKeyGetVal "Max file size" maxFileSize
    , showKeyGetVal "File fetch duplicates" fileFetchDuplicates
    , showKeyGetVal "Files removed" filesRemoved
    , showKeyGetVal "Fetch errors" fetchErrors
    , showKeyGetVal "Delete errors" removeErrors
    , showKeyGetVal "Real time" realTime
    , showKeyGetVal "Real fetch time" realFetchTime
    , showKeyGetVal "Real delete time" realDeleteTime
    , showKeyGetVal "Parallel time" parallelTime
    , showKeyGetVal "Max FetchFile time" maxFetchFileTime
    , showKeyGetVal "Max DeleteFile time" maxDeleteFileTime
    , showKeyGetVal "Max total size (including reserved)" maxTotalSize
    , showKeyGetVal "Initial disk usage" initialDiskUsage
    , showKeyGetVal "Max disk usage" maxDiskUsage
    , showKeyVal "Effective fetch speed, MB/s" fetchSpeedMBs
    , showKeyVal "Effective delete speed, files/s" deleteSpeed
    ]
  realFetchSeconds = nominalDiffTimeToSeconds stats.realFetchTime
  -- TODO: we use 1 KB = 1000 B for file sizes, but here 1 KB = 1024 B.
  -- Shall we use 1000 here as well?
  megabytes = (fromIntegral stats.bytesFetched) / 1024 / 1024
  fetchSpeedMBs =
    if realFetchSeconds == 0 then 0
    else megabytes / realFetchSeconds
  realDeleteSeconds = nominalDiffTimeToSeconds stats.realDeleteTime
  deleteSpeed =
    if realDeleteSeconds == 0 then 0
    else (fromIntegral stats.filesRemoved) / realDeleteSeconds


data FileState
  -- Reserve space for a file to be fetched or created.
  = FileReserved
  -- File is created locally by some task.
  | FileCreated
  -- File is fetched from another node.
  | FileFetched
  -- Failed to fetch.
  | FileFetchError
  -- File is being fetched.
  | FileFetching
  -- File is being removed.
  | FileRemoving
  deriving (Eq, Ord, Show)

newtype ActiveUsages = ActiveUsages Int
  deriving stock   (Generic)
  deriving newtype (Binary, Eq, Ord, Show, Num, Enum, Real, Integral)

-- FileState, size (actual or estimated), active usages.
type FileInfo = (FileState, FileSize, ActiveUsages)

-- NB: do not alter fileStateMap directly, otherwise totalSize will be incorrect
data FileStates = MkFileStates
  { fileStateMap :: Map VirtualFilePath FileInfo
  -- Total size of all files. TODO: we can compute it on the fly, do we really need it? Depends on map size...
  , totalSize    :: !FileSize
  }

emptyFileStates :: FileStates
emptyFileStates = MkFileStates Map.empty 0

getTotalSize :: FileStates -> FileSize
getTotalSize = (.totalSize)

totalSizeByFileState :: FileStates -> Map FileState FileSize
totalSizeByFileState fileStates =
  Map.fromListWith (+) $
  map (\(state, size, _) -> (state, size)) $
  Map.elems fileStates.fileStateMap

lookupFileState :: VirtualFilePath -> FileStates -> Maybe FileInfo
lookupFileState path fileStates = Map.lookup path fileStates.fileStateMap

alterFileState :: (Maybe FileInfo -> Maybe FileInfo) -> VirtualFilePath -> FileStates -> FileStates
alterFileState f path fileStates = MkFileStates
  { fileStateMap = fileStateMap'
  , totalSize    = totalSize'
  }
  where
    fileStateMap' = Map.alter (const newState) path fileStates.fileStateMap
    totalSize' = fileStates.totalSize + (toSize newState) - (toSize oldState)

    oldState = Map.lookup path fileStates.fileStateMap
    newState = f oldState

    toSize :: Maybe FileInfo -> FileSize
    toSize = fromMaybe 0 . fmap toSize'

    toSize' (_, size, _) = size

insertFileState :: VirtualFilePath -> FileInfo -> FileStates -> FileStates
insertFileState path state = alterFileState (const $ Just state) path

deleteFileState :: VirtualFilePath -> FileStates -> FileStates
deleteFileState = alterFileState (const Nothing)

-- NB: it's different from ReusableWorker.TReusableWorkerPool = LazyReusableWorkerPool (long-living)
-- Here we create a temporary pool for a single fetchFiles task.
type TWorkerPool = TQueue ReusableWorker

createWorkerPool :: NonEmpty ReusableWorker -> IO (TWorkerPool)
createWorkerPool workers = do
  queue <- newTQueueIO
  mapM_ (atomically . writeTQueue queue) $ NonEmpty.toList workers
  return queue

withAnyWorker :: TWorkerPool -> (ReusableWorker -> Process a) -> Process a
withAnyWorker workerPool = bracket acquire release where
  acquire = do
    -- selfPid <- getSelfPid
    -- Log.info "withAnyWorker: acquiring from..." selfPid
    w@(ReusableWorker pid) <- liftIO $ atomically $ readTQueue workerPool
    -- Log.info "withAnyWorker: linking to" w
    link pid
    -- Log.info "withAnyWorker: acquired" (selfPid, w)
    return w
  release w@(ReusableWorker pid) = do
    -- selfPid <- getSelfPid
    -- Log.info "withAnyWorker: releasing from" (selfPid, w)
    unlink pid
    liftIO $ atomically $ writeTQueue workerPool w
    -- Log.info "withAnyWorker: released" (selfPid, w)

type DownloadTask = (TimeStamp, ClusterFilePath, TWorkerPool, TVar (Maybe DownloadFileResult))

-- Internal server state.
-- NB: we use VirtualFilePath instead of ClusterFilePath, because:
-- - NodeLocalFilePaths with different nodes and same OsPath correspond to the same file at the local storage.
-- - CleanupTask doesn't know which node the file belongs to, and uses VirtualFilePath.
data State = MkState
  { fileStatesVar :: TVar FileStates
  , statsVar      :: TVar Stats
  , downloadQueue :: TQueue DownloadTask
  }

initialState :: IO State
initialState = do
  fileStatesVar' <- liftIO $ newTVarIO emptyFileStates
  statsVar' <- liftIO $ newTVarIO zeroStats
  downloadQueue' <- liftIO newTQueueIO
  return $ MkState fileStatesVar' statsVar' downloadQueue'

-- Used internally, will be converted to Response = FetchSuccess | FetchError
data DownloadFileResult = DownloadSuccess FileSize | DownloadDuplicate FileSize | DownloadError
  deriving (Generic, Binary, Show, Eq)

instance Static (Binary (SendPort DownloadFileResult)) where
  closureDict = static Dict

instance Static (Binary (DownloadFileResult)) where
  closureDict = static Dict

-- Used internally, will be converted to Response =  FileDeleted | FileDoesNotExist | FileDeleteError
data RemoveFileResult = RemoveSuccess | RemoveDoesNotExist | RemoveError
  deriving (Generic, Binary, Show, Eq)

instance Static (Binary (SendPort RemoveFileResult)) where
  closureDict = static Dict

instance Static (Binary (RemoveFileResult)) where
  closureDict = static Dict

-- Helper functions for local file manipulation.
-- NB: You should filter files (e.g. remove all GlobalFilePath's) before calling these functions.

downloadFile :: TimeStamp -> FileSender -> OsPath -> Process DownloadFileResult
downloadFile timeStamp fileSender source = do
  -- Log.info "downloadFile" source
  let dest = source
  exists <- liftIO $ System.Directory.doesFileExist dest
  -- We keep track of fetched files in fileStatesVar, so we should never call downloadFile for an existing file.
  -- TODO: throw error?
  when exists $ Log.warn "File already exists and will be overwritten" dest
  maybeBytes <- FileSender.download timeStamp fileSender source dest
  return $ case maybeBytes of
    Just numBytes -> DownloadSuccess numBytes
    Nothing       -> DownloadError


removeFile :: NodeLocalFileManagerConfig -> VirtualFilePath -> Process RemoveFileResult
removeFile config (VirtualFilePath path) =
  if isNodeLocal config path then do
    -- Log.info "removeFile" vfp
    res <- liftIO $ tryJust
      (guard . isDoesNotExistError)
      (System.Directory.removeFile path)
    return $ case res of
      Left _  -> RemoveDoesNotExist
      Right _ -> RemoveSuccess
  else do
    Log.err $ "NodeLocalFileManager should not remove global files! Requested file removal: " <> show path
    return RemoveError

runWithRetryTimeout
  :: ( Binary a
     , Typeable a
     , Show a
     )
  => RetryTimeoutPolicy
  -> String
  -> Process a
  -> Process (Maybe a)
runWithRetryTimeout policy description action = do
  selfPid <- getSelfPid
  let
    beforeRetry asyncResult policy' =
      Log.warn "Failed to complete task, will retry" (description, selfPid, asyncResult, policy')

  asyncResult <- RetryTimeout.runWithRetryTimeout policy beforeRetry action
  case asyncResult of
    AsyncDone res -> return $ Just res
    _ -> do
      Log.err $ "Failed to complete task: " <> show (description, selfPid, asyncResult, policy)
      return Nothing

tryDownloadFile :: State -> TimeStamp -> TWorkerPool -> ClusterFilePath -> Process DownloadFileResult
tryDownloadFile state timeStamp workerPool clusterFilePath = bracket acquire release action where
  fileStatesVar = state.fileStatesVar
  virtualFilePath = toVirtualFilePath clusterFilePath
  -- RAII-style wrapping to make sure that the file does not hang in FileFetching state forever.
  -- For a new file, we set FileState = FileFetching in the beginning and FileExists/FileFetchError in the end (action or release).
  -- If a file has been acquired by another task, we use the result of that task.

  -- Return previous file state.
  -- If this is a new file, set state to FileFetching and return Nothing.
  -- If this is an expected (reserved) file, set state to FileFetching.
  -- NB: no other function should set state=FileFetching, otherwise our RAII won't work!
  acquire :: Process (Maybe FileInfo)
  acquire = liftIO $ atomically $ do
    fileStates <- readTVar fileStatesVar
    let prevState = lookupFileState virtualFilePath fileStates
    case prevState of
      Just (FileReserved, bytes, usages) -> do
        modifyTVar' fileStatesVar $ insertFileState virtualFilePath (FileFetching, bytes, usages)
        return prevState
      Nothing -> do
        modifyTVar' fileStatesVar $ insertFileState virtualFilePath (FileFetching, 0, 0)
        return prevState
      _ -> return prevState

  -- If the action has finished properly, state is set to FileExists or FileFetchError.
  -- FileFetching means that action has not finished properly, so we reset it to FileFetchError:
  release :: Maybe FileInfo -> Process ()
  release _ = liftIO $ atomically $ do
    fileStates <- readTVar fileStatesVar
    case lookupFileState virtualFilePath fileStates of
      Just (FileFetching, _, usages) -> do
        -- NB: insertFileState overwrites previous value
        modifyTVar' fileStatesVar $ insertFileState virtualFilePath (FileFetchError, 0, usages)
      _ -> return ()

  action :: Maybe FileInfo -> Process DownloadFileResult
  action previousFileState = do
    res <- case previousFileState of
      Just (FileCreated, bytes, _) -> return $ DownloadDuplicate bytes
      Just (FileFetched, bytes, _) -> return $ DownloadDuplicate bytes
      Just (FileFetchError, _, _)  -> return DownloadError
      -- Wait until other task finishes fetching
      Just (FileFetching, _, _) -> do
        Log.info "Waiting for other task to finish fetching" clusterFilePath
        otherState <- liftIO $ atomically $ do
          fileStates <- readTVar fileStatesVar
          let otherState' = lookupFileState virtualFilePath fileStates
          -- Wait
          case otherState' of
            Just (FileFetching, _, _) -> retry
            _                         -> return otherState'
        -- Process the other task's result
        action otherState
      Just (FileRemoving, _, _) -> do
        Log.err $ "Trying to fetch file that is being removed: " <> show clusterFilePath
        return DownloadError
      -- This is a new file, need to actually fetch it
      Just (FileReserved, _, _) -> doFetch
      Nothing                   -> doFetch

    return res

  doFetch :: Process DownloadFileResult
  doFetch = do
    resVar <- liftIO $ newTVarIO Nothing
    -- Add task to Download queue
    liftIO $ atomically $ writeTQueue state.downloadQueue (timeStamp, clusterFilePath, workerPool, resVar)
    -- Wait for Download result
    res <- liftIO $ atomically $ do
      maybeRes <- readTVar resVar
      case maybeRes of
        Just res -> return res
        -- Download not finished yet:
        Nothing  -> retry
    let
      (fileState, size) = case res of
        DownloadSuccess bytes   -> (FileFetched, bytes)
        DownloadDuplicate bytes -> (FileFetched, bytes)
        DownloadError           -> (FileFetchError, 0)
    -- NB: insertFileState updates existing value
    liftIO $ atomically $ do
      let
        alter Nothing = Just (fileState, size, 0)
        alter (Just (_, _, oldUsageCount)) = Just (fileState, size, oldUsageCount)
      modifyTVar' fileStatesVar $ alterFileState alter virtualFilePath
      fileStates <- readTVar fileStatesVar
      modifyTVar' state.statsVar (<> zeroStats { maxTotalSize = getTotalSize fileStates })
    return res

tryRemoveFile :: NodeLocalFileManagerConfig -> TVar FileStates -> VirtualFilePath -> Process RemoveFileResult
tryRemoveFile config fileStatesVar file = bracket acquire release action where
  -- RAII-style wrapping to make sure that the file does not hang in FileRemoving state forever.
  -- For a new file, we set FileState = FileRemoving in the beginning remove it from the fileStateMap in the end (action or release).
  -- If a file has been acquired by another task, we use the result of that task.

  -- Return previous file state.
  -- If this is a new file, set state to FileRemoving and return Nothing.
  -- NB: no other function should set state=FileRemoving, otherwise our RAII won't work!
  acquire :: Process (Maybe FileInfo)
  acquire = liftIO $ atomically $ do
    fileStates <- readTVar fileStatesVar
    case lookupFileState file fileStates of
      Just previousFileState -> return $ Just previousFileState
      Nothing -> do
        modifyTVar' fileStatesVar $ insertFileState file (FileRemoving, 0, 0)
        return Nothing

  -- TODO: shall we always delete the file from map? Or shall we remember delete errors?
  release :: Maybe FileInfo -> Process ()
  release _ = liftIO $ atomically $ modifyTVar' fileStatesVar $ deleteFileState file

  action :: Maybe FileInfo -> Process RemoveFileResult
  action previousFileState = do
    res <- case previousFileState of
      Just (FileFetching, _, _) -> do
        Log.err $ "Trying to delete file that is being fetched: " <> show file
        return RemoveError
      Just (FileReserved, _, _) -> do
        Log.err $ "Trying to delete file that is being reserved: " <> show file
        return RemoveError
      -- Wait until other task finishes removing
      Just (FileRemoving, _, _) -> do
        Log.info "Waiting for other task to finish removing" file
        otherState <- liftIO $ atomically $ do
          fileStates <- readTVar fileStatesVar
          let otherState' = lookupFileState file fileStates
          -- Wait
          case otherState' of
            Just (FileRemoving, _, _) -> retry
            _                         -> return otherState'
        -- TODO: just return RemoveDoesNotExist, assuming that other task removed the file successfully?
        -- Or store all removed files in fileStatesVar too?
        action otherState
      Just (_, _, usageCount) ->
        if usageCount > 0 then do
          Log.err $ "Trying to delete file that is being used: " <> show (usageCount, file)
          return RemoveError
        else
          doRemove
      Nothing  -> do
        res <- doRemove
        when (res /= RemoveDoesNotExist) $
          Log.warn "Tried to delete unknown file" (res, file)
        return res
    return res

  doRemove :: Process RemoveFileResult
  doRemove = do
    let description = "Delete " <> show file
    maybeRes <- runWithRetryTimeout config.retryPolicy description $ removeFile config file
    let res = fromMaybe RemoveError maybeRes
    case res of
      RemoveError -> pure ()
      _ -> liftIO $ atomically $ modifyTVar' fileStatesVar $ deleteFileState file
    return res


-- stats -> stats <> stats'
addToStatsVar :: MonadIO m => TVar Stats -> Stats -> m ()
addToStatsVar sVar s' = liftIO $ atomically $ modifyTVar' sVar (<> s')

deleteFileAndUpdateStats :: NodeLocalFileManagerConfig -> State -> VirtualFilePath -> Process Response
deleteFileAndUpdateStats config state file = do
  (time, res) <- measureRealTime $ tryRemoveFile config state.fileStatesVar file
  Log.text $ "Deleted: " <> Log.showText (res, time, file)
  addToStatsVar state.statsVar $ zeroStats { parallelTime = time, maxDeleteFileTime = time }
  case res of
    RemoveSuccess -> do
      addToStatsVar state.statsVar $ zeroStats { filesRemoved = 1 }
      return FileDeleted
    RemoveDoesNotExist -> return FileDoesNotExist
    RemoveError -> do
      addToStatsVar state.statsVar $ zeroStats { removeErrors = 1 }
      return FileDeleteError

deleteFilesAndUpdateStats :: NodeLocalFileManagerConfig -> State -> [VirtualFilePath] -> Process Response
deleteFilesAndUpdateStats config state files = do
  responses <- forM files $ deleteFileAndUpdateStats config state
  pure $ foldr combine FileDeleted responses
  where
    -- TODO need better response types and combine logic
    combine x y | x == y  = x
    combine FileDeleteError _ = FileDeleteError
    combine _ FileDeleteError = FileDeleteError
    combine FileDeleted _ = FileDeleted
    combine _ FileDeleted = FileDeleted
    combine x y = error $ "Unexpected responses" <> show (x,y)


fetchFileAndUpdateStats :: State -> TimeStamp -> TWorkerPool -> ClusterFilePath -> Process Response
fetchFileAndUpdateStats state timeStamp workerPool clusterFilePath = do
  (time, res) <- measureRealTime $ tryDownloadFile state timeStamp workerPool clusterFilePath
  Log.text $ "Fetched: " <> Log.showText (res, time, clusterFilePath)
  addToStatsVar state.statsVar $ zeroStats { parallelTime = time, maxFetchFileTime = time }
  case res of
    DownloadSuccess bytes -> do
      addToStatsVar state.statsVar $ zeroStats { filesFetched = 1, bytesFetched = bytes, maxFileSize = bytes }
      return $ FetchSuccess bytes
    DownloadDuplicate _ -> do
      addToStatsVar state.statsVar $ zeroStats { fileFetchDuplicates = 1 }
      return $ FetchSuccess 0
    DownloadError -> do
      addToStatsVar state.statsVar $ zeroStats { fetchErrors = 1 }
      return $ FetchError [clusterFilePath]


fetchFilesAndUpdateStats
  :: State
  -> NonEmpty ReusableWorker
  -> Set ClusterFilePath
  -> Process Response
fetchFilesAndUpdateStats state workers files = do
  timeStamp <- liftIO getMonotonicTimeNSec
  workerPool <- liftIO $ createWorkerPool workers
  let
    add :: Response -> Response -> Response
    add (FetchSuccess bytes) (FetchSuccess bytes') = FetchSuccess $ bytes + bytes'
    add (FetchSuccess _) (FetchError paths') = FetchError paths'
    add (FetchError paths) (FetchSuccess _) = FetchError paths
    add (FetchError paths) (FetchError paths') = FetchError $ paths ++ paths'
    add r r' = error $ "Illegal Fetch Reponses: " <> show (r, r')

  results <- mapConcurrently (fetchFileAndUpdateStats state timeStamp workerPool) $ Set.toList files
  return $ foldr add (FetchSuccess 0) results

-- Delete fetched but unused files to free at least `requiredSize` bytes.
-- Returns the number of bytes actually deleted
-- NB: this is non-atomic, so delete errors are possible if other requests are running in parallel.
-- TODO: shall we use mutexes to ensure atomic behaviour?
-- NB: we pass FileStates that can be different from state.fileStateVar.
--   This is needed by reserveFilesAndUpdateStats:
--   it marks files as reserved, but does not update state.fileStateVar if reserve attempt fails.
tryDeleteUnusedFiles
  :: NodeLocalFileManagerConfig
  -> State
  -> FileStates
  -> FileSize
  -> Process FileSize
tryDeleteUnusedFiles config state fileStates requiredSize = do
  let
    isUnusedFetchedFile (FileFetched, _, 0) = True
    isUnusedFetchedFile _                   = False

    -- Sort by size, the goal is to remove large files first.
    unusedFetchFiles :: [(VirtualFilePath, FileSize)]
    unusedFetchFiles =
      sortOn (Down . snd) $
      Map.toList $
      Map.map (\(_, sz, _) -> sz) $
      Map.filter isUnusedFetchedFile fileStates.fileStateMap

    go :: FileSize -> [(VirtualFilePath, FileSize)] -> Process FileSize
    go totalDeletedSize [] = return totalDeletedSize
    go totalDeletedSize ((path, size) : rest) = do
      if totalDeletedSize >= requiredSize then
        return totalDeletedSize
      else do
        Log.text $ "Removing unused file " <> Log.showText (path, size)
        res <- deleteFileAndUpdateStats config state path
        case res of
          FileDeleted -> go (totalDeletedSize + size) rest
          _           -> do
            -- Something could have happened to this file because of a concurrent reqeust
            -- (note that our function is non-atomic).
            -- We simply skip this file and try to delete the next one.
            Log.warn "Failed to remove unused file: " (res, path)
            go totalDeletedSize rest

  totalDeletedSize <- go 0 unusedFetchFiles
  when (totalDeletedSize > 0) $
    if totalDeletedSize < requiredSize then
      Log.text $
      "WARN: Removing unused files: required "
      <> Log.showText requiredSize
      <> ", but removed only "
      <> Log.showText totalDeletedSize
    else
      Log.text $
      "Removing unused files: required "
      <> Log.showText requiredSize
      <> ", successfully removed "
      <> Log.showText totalDeletedSize

  return totalDeletedSize

reserveFilesAndUpdateStats
  :: NodeLocalFileManagerConfig
  -> State
  -> Map VirtualFilePath FileSize
  -> Process Response
reserveFilesAndUpdateStats config state fileSizeMap = do
  let
    tryReserve :: VirtualFilePath -> FileSize -> Either String FileStates -> Either String FileStates
    tryReserve _ _ (Left err) = Left err
    tryReserve path size (Right fileStates) =
      case eitherNewState of
        Left err        -> Left err
        Right fileState -> Right $ insertFileState path fileState fileStates
      where
        eitherNewState = case lookupFileState path fileStates of
          Nothing -> Right (FileReserved, size, 0)
          Just oldState@(oldState', _, _) -> case oldState' of
            FileReserved -> Right oldState
            FileCreated  -> Right oldState
            FileFetched  -> Right oldState
            FileFetching -> Right oldState
            _            -> Left $ "Cannot reserve file: invalid state" ++ show (oldState', path)

    go = do
      let
        go' = liftIO $ atomically $ do
          fileStates <- readTVar state.fileStatesVar
          case Map.foldrWithKey' tryReserve (Right fileStates) fileSizeMap of
            Left err -> return $ (ReserveError err, fileStates)
            Right fileStates' -> do
              let
                totalSize  = getTotalSize fileStates
                totalSize' = getTotalSize fileStates'
                deltaSize = totalSize' - totalSize
                availSize = config.localStorageSize - totalSize
              if (deltaSize <= availSize) then do
                writeTVar state.fileStatesVar fileStates'
                modifyTVar' state.statsVar (<> zeroStats { maxTotalSize = totalSize' })
                return (ReserveSuccess, fileStates')
              else
                return $
                  ( ReserveLimitExceeded (SpaceRequired deltaSize) (SpaceAvailable availSize)
                  , fileStates'
                  )
      (res, fileStates') <- go'
      case res of
        -- If limit exceeded, delete unused fetched files (ignoring those that we want to reserve) and retry.
        -- Since this happens outside of STM transaction, generally file states can change.
        -- So we could retry multiple times and use some smart criteria to stop retrying.
        -- But we keep it simple and leave extra retries to client.
        -- In our use case, Reserve request is never submitted simultaneously
        -- with other Reserve of Fetch requests.
        -- The only thing that can happen concurrently is a Delete request (from an earlier CleanupTask).
        ReserveLimitExceeded (SpaceRequired req) (SpaceAvailable av) -> do
          -- NB: we pass fileStates' to mark files as reserved.
          -- Note that we update state.fileStatesVar only in case of success.
          _ <- tryDeleteUnusedFiles config state fileStates' (req - av)
          (res', _) <- go'
          return res'
        _ -> return res

  (time, res) <- measureRealTime go
  addToStatsVar state.statsVar $ zeroStats { parallelTime = time}
  case res of
    ReserveSuccess           -> pure ()
    ReserveLimitExceeded _ _ -> Log.text $ "WARN: Cannot reserve disk space: " <> Log.showText res
    ReserveError errorString -> Log.err errorString
    _                        -> Log.throwError $ "reserveFilesAndUpdateStats: unexpected Response: " ++ show res
  return res

-- TODO: check real file sizes?
registerFilesAndUpdateStats
  :: NodeLocalFileManagerConfig
  -> State
  -> Map VirtualFilePath FileSize
  -> Process Response
registerFilesAndUpdateStats config state fileSizeMap = do
  let doesNotExist (VirtualFilePath p) = liftIO $ not <$> System.Directory.doesFileExist p
  nonExistingFiles <- Set.fromList <$> filterM doesNotExist (Map.keys fileSizeMap)
  let
    go = liftIO $ atomically $ do
      let
        -- Either update file state, or add error to map
        tryRegister
          :: VirtualFilePath
          -> FileSize
          -> (FileStates, Map VirtualFilePath String)
          -> (FileStates, Map VirtualFilePath String)
        tryRegister path size (fileStates, errors) =
          case eitherNewState of
            Left err -> (fileStates, Map.insert path err errors)
            Right fileState -> (insertFileState path fileState fileStates, errors)
          where
            maybeOldState = lookupFileState path fileStates
            eitherNewState =
              if not (isNodeLocalV config path) then
                Left $ "Unexpected global file"
              else if path `Set.member` nonExistingFiles then
                Left $ "File does not exist"
              else case maybeOldState of
                -- TODO: this is not an error per se, but in practice we reserve all files in advance.
                -- So it's useful for debugging.
                -- Nothing -> Left $ "File has not been reserved in advance"
                Nothing -> Right $ (FileCreated, size, 0)
                Just (oldState, _, usages) -> case oldState of
                  FileReserved -> Right $ (FileCreated, size, usages)
                  -- TODO compare sizes?
                  FileCreated  -> Right $ (FileCreated, size, usages)
                  FileFetched  -> Right $ (FileFetched, size, usages)
                  _            -> Left $ "Current state: " <> show maybeOldState

      fileStates <- readTVar state.fileStatesVar
      let (fileStates', errors) = Map.foldrWithKey' tryRegister (fileStates, Map.empty) fileSizeMap
      -- Register good files regardless of errors. TODO: is it correct behaviour?
      writeTVar state.fileStatesVar fileStates'
      modifyTVar' state.statsVar (<> zeroStats { maxTotalSize = getTotalSize fileStates' })
      if Map.null errors then
        return RegisterSuccess
      else
        return $ RegisterError $ "Cannot register files: " <> show errors
  (time, res) <- measureRealTime go
  addToStatsVar state.statsVar $ zeroStats { parallelTime = time}
  case res of
    RegisterSuccess -> pure () -- Log.info "Registered" (res, time, fileSizeMap)
    RegisterError errorString -> Log.err errorString
    _ -> Log.throwError $ "registerFilesAndUpdateStats: unexpected Response: " ++ show res
  return res

addUsagesAndUpdateStats
  :: ActiveUsages
  -> State
  -> Set VirtualFilePath
  -> Process Response
addUsagesAndUpdateStats usageCount state files = do
  let
    tryAddUsages :: VirtualFilePath -> Either String FileStates -> Either String FileStates
    tryAddUsages _    (Left err) = Left err
    tryAddUsages file (Right fileStates) = case lookupFileState file fileStates of
      Nothing -> Left $ "Trying to change active usage count for unknown file: " <> show file
      Just (fileState, size, oldUsageCount) ->
        if newUsageCount >= 0 then
          Right $ insertFileState file (fileState, size, newUsageCount) fileStates
        else
          Left $ "Negative usage count not allowed (oldCount, newCount, path): "
            <> show (oldUsageCount, newUsageCount, file)
        where
          newUsageCount = oldUsageCount + usageCount

    go = liftIO $ atomically $ do
      fileStates <- readTVar state.fileStatesVar
      case Set.foldr' tryAddUsages (Right fileStates) files of
        Left err -> pure $ ChangeActiveUsagesError err
        Right fileStates' -> do
          writeTVar state.fileStatesVar fileStates'
          pure ChangeActiveUsagesSuccess
  (time, res) <- measureRealTime go
  addToStatsVar state.statsVar $ zeroStats { parallelTime = time}
  case res of
    ChangeActiveUsagesSuccess -> pure ()
    ChangeActiveUsagesError err -> Log.err err
    _ -> Log.throwError $ "addUsagesAndUpdateStats: unexpected Response: " ++ show res
  return res

-- Process Download requests queue for particular node.
-- We use TPrioQueue to prioritize requests with earlier timestamps;
-- This helps to finish the first Fetch request
-- and start the corresponding task as fast as possible.
downloadNodeLoop
  :: FileSender
  -> NodeLocalFileManagerConfig
  -> TVar Int
  -> TPrioQueue (Down TimeStamp) (TimeStamp, String, OsPath, TWorkerPool, TVar (Maybe DownloadFileResult))
  -> Process ()
downloadNodeLoop fileSender config activeDownloadRequestsVar queue = do
  activeNodeDownloadRequestsVar <- liftIO $ newTVarIO @Int 0

  let
    downloadOnWorker :: TimeStamp -> String -> OsPath -> ReusableWorker -> Process DownloadFileResult
    downloadOnWorker timeStamp description filePath w = do
      maybeRes <- runWithRetryTimeout config.retryPolicy description $ do
        selfNodeId <- getSelfNode
        if selfNodeId == getNodeId w then
          -- For localWorker, we don't need to create closure etc.
          downloadFile timeStamp fileSender filePath
        else
          runOnReusableWorker w $
          static downloadFile
          `cAp` cPure timeStamp
          `cAp` cPure fileSender
          `cAp` cPure filePath
      return $ fromMaybe DownloadError maybeRes

    doDownload :: TimeStamp -> String -> OsPath -> TWorkerPool -> TVar (Maybe DownloadFileResult) -> Process ()
    doDownload timeStamp description filePath workerPool resVar = do
      -- A dead ReusableWorker reaches us through the link in withAnyWorker. The waiter on resVar has no other way
      -- to learn of it, so answer with DownloadError: without this the fetch is never answered and the run hangs silently.
      res <- withAnyWorker workerPool (downloadOnWorker timeStamp description filePath)
        `catch` \(ProcessLinkException pid reason) -> do
          Log.err $ description <> " failed: linked process " <> show pid <> " on " <> show (processNodeId pid)
            <> " was lost (a ReusableWorker, unless the run is already shutting down): " <> show reason
          pure DownloadError
      liftIO $ atomically $ writeTVar resVar $ Just res

    checkIsLess countVar maybeMaxCount = case maybeMaxCount of
        Just maxCount -> do
          count <- readTVar countVar
          check $ count < maxCount
        Nothing -> pure ()

  forever $ do
    -- TODO: shall we pass workerPool or individual worker?
    (timeStamp, description, filePath, workerPool, resVar) <- liftIO $ atomically $ do
      -- Do not start if the current number of Download requests (total ot per node)
      -- exceeds the limits specified by config.
      -- This helps to avoid timeouts if FileSender is overloaded.
      checkIsLess activeNodeDownloadRequestsVar config.maxDownloadRequestsPerNode
      checkIsLess activeDownloadRequestsVar config.maxDownloadRequests
      -- Increment counters
      modifyTVar' activeDownloadRequestsVar succ
      modifyTVar' activeNodeDownloadRequestsVar succ
      TPrioQueue.read queue

    _ <- asyncLinked $ task $ do
      doDownload timeStamp description filePath workerPool resVar
        `finally` (liftIO $ atomically $ do
          -- Decrement counters
          modifyTVar' activeDownloadRequestsVar pred
          modifyTVar' activeNodeDownloadRequestsVar pred
        )
    return ()


-- Process Download requests queue.
-- Note that here we simply dispatch requests to node queues,
-- so we don't need TPrioQueue for the main queue.
downloadLoop
  :: NodeLocalFileManagerConfig
  -> FileSenders
  -> TQueue DownloadTask
  -> Process ()
downloadLoop config (FileSenders senderMap) queue = do
  activeDownloadRequestsVar <- liftIO $ newTVarIO @Int 0

  let nodes = Map.keys senderMap
  nodeLoops <- forM nodes $ \node -> do
    let fileSender = senderMap ! node
    -- Earlier timeStamps have higher priority
    nodeQueue <-TPrioQueue.new $ \(timeStamp, _, _, _, _) -> Down timeStamp
    hdl <- asyncLinked $ task $ downloadNodeLoop fileSender config activeDownloadRequestsVar nodeQueue
    return (node, (nodeQueue, hdl))
  let nodeLoopMap = Map.fromList nodeLoops


  -- Listen to the queue and dispath requests to nodes
  forever $ do
    (timeStamp, clusterFilePath, workerPool, resVar) <- liftIO $ atomically $ readTQueue queue
    case clusterFilePath of
      GlobalFilePath _ -> do
        Log.err $ "NodeLocalFileManager can fetch only NodeLocalFilePath, but received " <> show clusterFilePath
        liftIO $ atomically $ writeTVar resVar $ Just DownloadError
      NodeLocalFilePath node filePath -> do
        let
          description = "Fetch " <> show clusterFilePath
          (nodeQueue, _) = nodeLoopMap ! node
        TPrioQueue.write nodeQueue (timeStamp, description, filePath, workerPool, resVar)

-- | NodeLocalFileManager main loop, runs on worker node, listens to requests and processes them asynchronously
mainLoop :: NodeLocalFileManagerConfig -> FileSenders -> SendPort StartupResponse -> Process ()
mainLoop config fileSenders startupPort = do
  selfPid <- getSelfPid
  Log.info "Start NodeLocalFileManager" (selfPid, config)
  sendChan startupPort selfPid

  state <- liftIO initialState

  activeFetchRequestCountVar <- liftIO $ newTVarIO @Int 0
  activeDeleteRequestCountVar <- liftIO $ newTVarIO @Int 0
  activeReserveRequestCountVar <- liftIO $ newTVarIO @Int 0
  activeRegisterRequestCountVar <- liftIO $ newTVarIO @Int 0

  _ <- asyncLinked $ task $ downloadLoop config fileSenders state.downloadQueue

  let
    waitUntil :: (a -> Bool) -> STM a -> Process ()
    waitUntil cond getValue = liftIO $ atomically $ do
      value <- getValue
      check $ cond value

    getActiveRequestCount :: STM Int
    getActiveRequestCount = sum <$> mapM readTVar
      [ activeFetchRequestCountVar
      , activeDeleteRequestCountVar
      , activeReserveRequestCountVar
      , activeRegisterRequestCountVar
      ]

    waitForAllRequestsFinish :: Process ()
    waitForAllRequestsFinish = waitUntil (==0) getActiveRequestCount

    mkRealTimerLoop :: STM Int -> (NominalDiffTime -> Stats) -> Process ()
    mkRealTimerLoop getActiveCount timeToStats = do
      -- let loop = do
        -- Ignore idle time
        waitUntil (/=0) getActiveCount
        -- Measure activeCount time.
        -- On shutdown, the outer loops calls `exit realTimerPid`.
        -- Here we catch ProcessExitException and update measureRealTime one last time before exit.
        (time, res) <- measureRealTime $ try $ waitUntil (==0) getActiveCount
        addToStatsVar state.statsVar $ timeToStats time
        case res of
          Left (_ :: SomeException) -> return ()
          Right _                   -> mkRealTimerLoop getActiveCount timeToStats

    realTimerLoop = mkRealTimerLoop getActiveRequestCount $ \t -> zeroStats {realTime = t}
    realFetchTimerLoop = mkRealTimerLoop (readTVar activeFetchRequestCountVar) $ \t -> zeroStats {realFetchTime = t}
    realDeleteTimerLoop = mkRealTimerLoop (readTVar activeDeleteRequestCountVar) $ \t -> zeroStats {realDeleteTime = t}

    -- Print local disk usage and add it to stats
    diskUsageLoop :: Process ()
    diskUsageLoop = liftIO $ do
      let
        go isFirstTime = do
          time <- getZonedTime
          d <- getDiskUsageInfo config.localStoragePath
          let diskUsage = Just $ MkDiskUsageWithTime d time
          addToStatsVar state.statsVar $ zeroStats
            { maxDiskUsage = diskUsage
            , initialDiskUsage = if isFirstTime then diskUsage else Nothing
            }
          Log.text $ "Local disk usage: "  <> Log.showText d

          stats <- readTVarIO state.statsVar
          fileStates <- readTVarIO state.fileStatesVar
              -- Log.info doesn't look good (extra spacing etc.)
          Log.text $ "(totalSize, maxTotalSize, totalSizeByFileState): " <> Log.showText
            ( getTotalSize fileStates
            , stats.maxTotalSize
            , Map.toList $ totalSizeByFileState fileStates
            )

          -- For the first run, we compare `config.localStorageSize` with actual `diskAvail`.
          -- TODO: shall we also set config.localStorageSize e.g. to 90% of available space?
          when (isFirstTime && d.diskTotal - d.diskUsed < config.localStorageSize) $
            Log.warn "Available disk space is less than specified by config.localStorageSize" config.localStorageSize

          threadDelay $ nominalDiffTimeToMicroseconds config.reportInterval
          go False
      go True

  -- Start real timer loop. asyncLinked ensures that realTimerLoop process will be killed on shutdown.
  realTimerHandle <- asyncLinked $ task realTimerLoop
  realFetchTimerHandle <- asyncLinked $ task realFetchTimerLoop
  realDeleteTimerHandle <- asyncLinked $ task realDeleteTimerLoop

  _ <- asyncLinked $ task diskUsageLoop

  let

    printStats = do
      stats <- liftIO $ readTVarIO state.statsVar
      Log.text $ "NodeLocalFileManager: Statistics: " <> statsToText stats

    -- Temporarily increment activeCount request counter while running a task (Fetch or Delete request).
    withActiveCounter countTVar = bracket_ acquire release where
      acquire = liftIO $ atomically $ modifyTVar' countTVar succ
      release = liftIO $ atomically $ modifyTVar' countTVar pred

    -- Process in separate thread.
    -- TODO: limit maxThreads.
    -- TODO set timeout to kill hanging requests?
    processRequestAsync activeRequestCountVar responsePort getResponse = spawnLocal $
      withActiveCounter activeRequestCountVar $ do
        response <- getResponse
        Log.info "Sending Response" (responsePort, response)
        sendChan responsePort response


    processRequest :: ReusableWorker -> StatelessHandler () Request
    processRequest localWorker request s = do
      Log.info "Received Request" request
      _ <- case request of
        Fetch filePaths workers responsePort ->
          processRequestAsync activeFetchRequestCountVar responsePort $
          fetchFilesAndUpdateStats state (localWorker :| workers) filePaths
        Delete filePaths responsePort ->
          processRequestAsync activeDeleteRequestCountVar responsePort $
          deleteFilesAndUpdateStats config state filePaths
        Reserve fileSizeMap responsePort ->
          processRequestAsync activeReserveRequestCountVar responsePort $
          reserveFilesAndUpdateStats config state fileSizeMap
        Register fileSizeMap responsePort ->
          processRequestAsync activeRegisterRequestCountVar responsePort $
          registerFilesAndUpdateStats config state fileSizeMap
        IncrementActiveUsages files responsePort ->
          processRequestAsync activeRegisterRequestCountVar responsePort $
          addUsagesAndUpdateStats (ActiveUsages 1) state files
        DecrementActiveUsages files responsePort ->
          processRequestAsync activeRegisterRequestCountVar responsePort $
          addUsagesAndUpdateStats (ActiveUsages (-1)) state files

      continue_ s

    onShutdown _ reason = do
      Log.info "NodeLocalFileManager: preparing to exit" reason
      Log.text "Waiting for all NodeLocalFileManager tasks to finish..."
      waitForAllRequestsFinish
      -- Stop timers, it will update stats.realTime etc. upon termination.
      _ <- cancelWait realTimerHandle
      _ <- cancelWait realFetchTimerHandle
      _ <- cancelWait realDeleteTimerHandle
      printStats
      Log.text "NodeLocalFileManager: Shutdown"

  withLocalReusableWorker $ \localWorker ->
    serve () (statelessInit Infinity) $ statelessProcess
      { apiHandlers = [handleCast_ $ processRequest localWorker]
      , shutdownHandler = onShutdown
      }
