{-# LANGUAGE DeriveAnyClass      #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE StaticPointers      #-}
{-# LANGUAGE TypeFamilies        #-}

-- | FileSenders can copy ClusterFilePath from remote node to local OsPath.
-- We use it in NodeLocalFileManager.
--
-- Functions:
-- withFileSenders :: [WorkerAddr] -> (FileSenders -> Job a) -> Job a
-- download :: TimeStamp -> FileSender -> OsPath -> OsPath -> Process (Maybe FileSize)
--
-- Usage:
-- withFileSenders nodes go where
--   go fileSenders = do
--     -- source :: ClusterFilePath
--     -- dest :: OsPath
--     maybeFileSize <- copyRemoteToLocal fileSenders source dest
--     doSomeWork
--     return someResult

module Hyperion.Scheduler.FileService.FileSender
  ( FileSender
  , FileSenders (..)
  , fileSenderProcesses
  , FileSenderConfig (..)
  , TimeStamp
  , withFileSenders
  , download
  ) where

import Control.Concurrent.STM                      (TVar, atomically, check,
                                                    modifyTVar', newTVarIO,
                                                    readTVar, readTVarIO)
import Control.Distributed.Process                 (ProcessId, ReceivePort,
                                                    SendPort, getSelfPid, link,
                                                    newChan, receiveChan,
                                                    sendChan, unlink)
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
import Control.Monad                               (forever, when)
import Control.Monad.Catch                         (SomeException, bracket,
                                                    bracket_, catch, finally,
                                                    throwM, try)
import Control.Monad.IO.Class                      (liftIO)
import Control.Monad.Trans                         (lift)
import Data.Binary                                 (Binary, Word64)
import Data.ByteString                             qualified as B
import Data.List                                   (intercalate)
import Data.Map.Strict                             (Map)
import Data.Map.Strict                             qualified as Map
import Data.Ord                                    (Down (..))
import Data.Text                                   (Text)
import Data.Text                                   qualified as Text
import Data.Time.Clock                             (NominalDiffTime,
                                                    nominalDiffTimeToSeconds)
import Data.Typeable                               (Typeable)
import GHC.Generics                                (Generic)
import GHC.IO.Handle                               (Handle)
import Hyperion                                    hiding (Service)
import Hyperion.Log                                qualified as Log
import Hyperion.OsPath                             (OsPath, splitFileName,
                                                    takeDirectory)
import Hyperion.Scheduler.FileService.RetryTimeout (RetryTimeoutPolicy (..),
                                                    runWithRetryTimeout)
import Hyperion.Scheduler.RemoteUtil               (Service (..),
                                                    awaitProcessesEnd,
                                                    startAllOrNone,
                                                    startRemoteService)
import Hyperion.Scheduler.TPrioQueue               (TPrioQueue (..))
import Hyperion.Scheduler.TPrioQueue               qualified as TPrioQueue
import Hyperion.Scheduler.Types                    (Bytes, FileSize (..))
import Hyperion.Scheduler.Util                     (measureRealTime)
import System.Directory.OsPath                     (createDirectoryIfMissing,
                                                    doesFileExist, removeFile,
                                                    renamePath)
import System.File.OsPath                          (openBinaryFile,
                                                    openBinaryTempFile)
import System.IO                                   (IOMode (..), hClose)
import System.IO.Error                             (isDoesNotExistError)

-- Public interface

data FileSenders = FileSenders (Map WorkerAddr FileSender)
  deriving (Generic, Binary)

instance Static (Binary FileSenders) where
  closureDict = static Dict

data FileSenderConfig = MkFileSenderConfig
  { maxDownloadRequests :: Maybe Int
  , timeout             :: Maybe NominalDiffTime
  }
  deriving(Generic, Binary, Show)

instance Static (Binary FileSenderConfig) where
  closureDict = static Dict

-- RAII wrapper for starting FileSender on each worker
withFileSenders :: FileSenderConfig -> [WorkerAddr] -> (FileSenders -> Job a) -> Job a
withFileSenders config nodes go = bracket acquire release (go . fst) where

  acquire :: Job (FileSenders, [ProcessId])
  acquire = do
    senders <- startAllOrNone
      (\node -> do
        (sender, launching) <- startFileSender config node
        return ((node, sender), launching))
      (\((_, sender), launching) -> lift $ shutdown' sender >> awaitProcessesEnd [launching])
      nodes
    return (FileSenders $ Map.fromList $ map fst senders, map snd senders)

  release :: (FileSenders, [ProcessId]) -> Job ()
  release (FileSenders senderMap, launching) = do
    mapM_ (lift . shutdown') $ Map.elems senderMap
    lift $ awaitProcessesEnd launching

  shutdown' (FileSender _ pid) = shutdown pid

-- FileSender implementation

-- | Simple file sender
data FileSender = FileSender WorkerAddr ProcessId
  deriving (Generic, Binary, Show)

-- | The senders' processes, named for error messages.
fileSenderProcesses :: FileSenders -> [(String, ProcessId)]
fileSenderProcesses (FileSenders senders) =
  [ ("FileSender on " <> show node, pid) | (node, FileSender _ pid) <- Map.toList senders ]

-- | Copy file from remote sourcePath (on FileSender node) to local destPath (on this node).
-- Returns number of bytes if successful, Nothing otherwise.
-- Overwrites existing file.
-- NB: it does not check whether source == dest (same file path on the same disk).
download :: TimeStamp -> FileSender -> OsPath -> OsPath -> Process (Maybe FileSize)
download timeStamp sender source dest = do
  liftIO $ createDirectoryIfMissing True $ takeDirectory dest
  responseReceivePort <- sendRequest sender $ Download timeStamp source
  receiveFile responseReceivePort dest

sendRequest :: FileSender -> (SendPort Response -> Request) -> Process (ReceivePort Response)
sendRequest (FileSender _ pid) request = bracket_ acquire release go where
  -- Throw PortLinkException if FileSender not available.
  -- NB: if we remove `link` and use `usend` instead of `send`, then implicit reconnection is disabled,
  -- and `send` can hang - this happend for out nmax=6 test. Restarting download or calling `reconnect` did not help.
  -- `link` enables implicit reconnection, so that everything works fine unless connection is completely broken.
  -- NB: nested link/unlink expressions don't work (will unlink on innermost `unlink` call).
  -- This is OK since we send Download requests from different processes.
  acquire = link pid
  release = unlink pid
  go = do
    -- network-transport connections are lightweight and reuse the same TCP socket, it's OK to create one connection per request.
    (responseSendPort, responseReceivePort) <- newChan
    -- Each request send is independent, so we can use unreliable send and ignore previously lost messages.
    -- usend allows for implicit reconnects if the previous message has been lost.
    let requestWithPort = request responseSendPort
    -- Log.info "Sending request" (fileSender, requestWithPort)
    -- TODO check that it works, update comments above
    cast pid requestWithPort
    return responseReceivePort


instance Static (Binary FileSender) where
  closureDict = static Dict

-- FileSender implementation

instance Service FileSender  where
  type RequestType FileSender = Request
  type ResponseType FileSender = Response
  type StartupResponseType FileSender = StartupResponse
  getServiceProcessId = id

-- Returned by getMonotonicTimeNSec
type TimeStamp = Word64

-- | Request accepted by FileSender.
data Request = Download TimeStamp OsPath (SendPort Response)
  deriving (Generic, Binary, Show)

data Response = FileDoesNotExist | Chunk B.ByteString | EndOfFile | FileSenderError String
  deriving (Generic, Binary, Show)


-- Message sent by FileSender upon creation
type StartupResponse = ProcessId

startFileSender :: FileSenderConfig -> WorkerAddr -> Job (FileSender, ProcessId)
startFileSender config node = do
  let
    getClosure startupResponseSendPort =
      static fileSenderMainLoop
        `cAp` cPure config
        `cAp` cPure startupResponseSendPort

  Log.info "Start remote FileSender" node
  (pid, launching) <- startRemoteService @FileSender "FileSender" node getClosure
  return (FileSender node pid, launching)

-- Shortcut for atomic modifyTVar'
updateTVar :: TVar a -> (a -> a) -> Process ()
updateTVar tVar update = liftIO $ atomically $ modifyTVar' tVar update

addToStatsVar :: TVar Stats -> Stats -> Process ()
addToStatsVar statsVar stats = updateTVar statsVar (<> stats)

fileSenderMainLoop :: FileSenderConfig -> SendPort StartupResponse -> Process ()
fileSenderMainLoop config startupPort = do
  selfPid <- getSelfPid
  Log.info "Start FileSender" selfPid
  sendChan startupPort selfPid

  statsVar <- liftIO $ newTVarIO zeroStats
  -- Number of requests currently processing
  processRequestCountVar <- liftIO $ newTVarIO @Int 0

  -- TPrioQueue is a max-priority queue.
  -- We prioritze requests with earlier timestamps
  requestQueue <- TPrioQueue.new $ \(Download timeStamp _ _ ) -> Down timeStamp
  -- processQueueLoop will be killed automatically on exit, thanks to asyncLinked
  _ <- asyncLinked $ task $
    processQueueLoop config requestQueue processRequestCountVar statsVar

  let
    waitForAnyRequestStart = liftIO $ atomically $ do
      numProcessingRequests <- readTVar processRequestCountVar
      check $ numProcessingRequests /= 0

    waitForAllRequestsFinish = liftIO $ atomically $ do
      numProcessingRequests <- readTVar processRequestCountVar
      check $ numProcessingRequests == 0

    -- We define realTime as the time when at least one request is processing.
    realTimerLoop :: Process ()
    realTimerLoop = do
      -- Ignore idle time
      waitForAnyRequestStart
      -- Measure active time.
      -- On shutdown, the outer loops calls `exit realTimerPid`.
      -- Here we catch ProcessExitException and update measureRealTime one last time before exit.
      (time, res) <- measureRealTime $ try waitForAllRequestsFinish
      addToStatsVar statsVar zeroStats { realTime = time }
      case res of
        Left (_ :: SomeException) -> return ()
        Right _                   -> realTimerLoop

  -- Start real timer loop. asyncLinked ensures that realTimerLoop process will be killed on shutdown.
  realTimerHandle <- asyncLinked $ task realTimerLoop

  let
    printStats = do
      stats <- liftIO $ readTVarIO statsVar
      Log.text $ "FileSender: Statistics: " <> statsToText stats

    -- processRequest :: Request -> Process ()
    -- type ActionHandler s a = s -> a -> Action s
    processRequest :: StatelessHandler () Request
    processRequest request s = do
      Log.info "Received request" request
      TPrioQueue.write requestQueue request
      continue_ s

    onShutdown _ reason = do
      Log.info "FileSender: preparing to exit" reason
      Log.text "Waiting for all FileSender tasks to finish..."
      waitForAllRequestsFinish
      -- Stop timer, it will update stats.realTime upon termination.
      _ <- cancelWait realTimerHandle
      printStats
      Log.text "FileSender: Shutdown"

  serve () (statelessInit Infinity) $ statelessProcess
    { apiHandlers = [handleCast_ processRequest]
    , shutdownHandler = onShutdown
    }

processQueueLoop
  :: FileSenderConfig
  -> TPrioQueue (Down TimeStamp) Request
  -> TVar Int
  -> TVar Stats
  -> Process ()
processQueueLoop config queue processRequestCountVar statsVar = do
  let
    sentBytesToStats :: Maybe FileSize -> Stats
    sentBytesToStats maybeBytes = case maybeBytes of
      (Just numBytes) -> zeroStats
        { filesSent = 1
        , bytesSent = numBytes
        }
      Nothing -> zeroStats { numErrors = 1 }

    doDownloadWithStats filePath responsePort = do
      (time, sendResult) <- measureRealTime $ sendFileWithTimeout config.timeout responsePort filePath
      -- update stats
      let sendStats = sentBytesToStats sendResult <> zeroStats {parallelTime = time}
      addToStatsVar statsVar sendStats

  forever $ do
    (Download _ filePath responsePort) <- liftIO $ atomically $ do
      case config.maxDownloadRequests of
        Just maxCount -> do
          numProcessingRequests <- readTVar processRequestCountVar
          check $ numProcessingRequests < maxCount
        Nothing -> pure ()
      modifyTVar' processRequestCountVar succ
      TPrioQueue.read queue

    _ <- asyncLinked $ task $ do
      doDownloadWithStats filePath responsePort
        `finally` (liftIO $ atomically $ modifyTVar' processRequestCountVar pred)
    return ()


instance Static (Binary (SendPort Response)) where
  closureDict = static Dict

-- Use strict data types to prevent space leaks
data Stats = MkStats
  { filesSent    :: !Int
  , bytesSent    :: !FileSize
  , numErrors    :: !Int
  -- Real time spent on processing requests
  , realTime     :: !NominalDiffTime
  -- Total time spent inside worker threads
  , parallelTime :: !NominalDiffTime
  }

instance Semigroup Stats where
  a <> b = MkStats
    { filesSent = add' filesSent
    , bytesSent = add' bytesSent
    , numErrors = add' numErrors
    , realTime = add' realTime
    , parallelTime = add' parallelTime
    }
    where
      add' :: Num a => (Stats -> a) -> a
      add' getProperty = getProperty a + getProperty b

zeroStats :: Stats
zeroStats = MkStats 0 0 0 0 0

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
    [ showKeyGetVal "Files sent" filesSent
    , showKeyGetVal "Bytes sent" bytesSent
    , showKeyGetVal "Errors" numErrors
    , showKeyGetVal "Real time" realTime
    , showKeyGetVal "Parallel time" parallelTime
    , showKeyVal "Upload speed per file, MB/s" $ getMBs stats.parallelTime
    , showKeyVal "Total upload speed (average when not idle), MB/s" $ getMBs stats.realTime
    ]
  megabytes = (fromIntegral stats.bytesSent) / 1024 / 1024
  getMBs 0 = 0
  getMBs t = megabytes / (nominalDiffTimeToSeconds t)

-- Sending and receiving implemetation

-- Custom withBinaryFile implementation for Process monad instead of IO
withFile :: OsPath -> IOMode -> (Handle -> Process a) -> Process a
withFile path mode go = bracket acquire release go where
  acquire = liftIO $ openBinaryFile path mode
  release handle = liftIO $ hClose handle

-- withFile analogue that writes to temporary file and moves it to the destination path
withFileWriteViaTemp :: OsPath -> (Handle -> Process a) -> Process a
withFileWriteViaTemp path go = bracket acquire release go' where
  (tempDir, template) = splitFileName path
  acquire = liftIO $ openBinaryTempFile tempDir template
  release (tmpPath, handle) = liftIO $ do
    hClose handle
    removeIfExists tmpPath
  go' (tmpPath, handle) = do
    res <- go handle
    -- NB: we call renamePath only in case of success!
    liftIO $ hClose handle
    liftIO $ renamePath tmpPath path
    return res

removeIfExists :: OsPath -> IO ()
removeIfExists path =
  removeFile path `catch` \e ->
    if isDoesNotExistError e then
      return ()
    else
      throwM e

sendFile
  :: SendPort Response
  -> OsPath
  -> Process (Maybe FileSize)
sendFile sendPort path = do
  let
    sendChunks :: Bytes -> Handle -> Process FileSize
    sendChunks numBytes handle = do
      -- 1MB chunks.
      -- NB: larger chunks reduce messaging overhead but require more RAM.
      -- According to our tests on Expanse, the overhead is significant (up to 100%) for 4KB chunks,
      -- but is negligible for 40KB or higher.
      chunk <- liftIO $ B.hGetSome handle $ 1024*1024
      if B.null chunk then do
        sendChan sendPort EndOfFile
        return $ FileSize numBytes
      else do
        sendChan sendPort (Chunk chunk)
        let numBytes' = numBytes + B.length chunk
        sendChunks numBytes' handle

  exists <- liftIO $ doesFileExist path
  if exists then do
    (time, numBytes) <- measureRealTime $ withFile path ReadMode (sendChunks 0)
    -- Log.info doesn't look good (extra spacing etc.)
    Log.text $ "FileSender: Sent (bytes, time, path): " <> Log.showText (numBytes, time, path)
    return $ Just numBytes
  else do
    Log.err $ "FileSender: File does not exist: " <> show path
    sendChan sendPort FileDoesNotExist
    return Nothing

sendFileWithTimeout
  :: Maybe NominalDiffTime
  -> SendPort Response
  -> OsPath
  -> Process (Maybe FileSize)
sendFileWithTimeout mTimeout sendPort path = do
  let
    -- NB: we cannot retry sending, because it will send the same chunks twice!
    -- Retrying can be performed on client side only (sending new Download request etc.).
    policy = MkRetryTimeoutPolicy {maxRetryCount = 0, initialTimeout = mTimeout}
    beforeRetry _ _ = Log.throwError "No retries allowed!"

    runWithTimeout :: (Binary a, Typeable a) => Process a -> Process (AsyncResult a)
    runWithTimeout = runWithRetryTimeout policy beforeRetry

  asyncResult <- runWithTimeout $ sendFile sendPort path
  case asyncResult of
    AsyncDone (res :: Maybe FileSize) -> return res
    _ -> do
      Log.err $ "sendFileWithTimeout: " <> show (path, mTimeout, asyncResult)
      -- Use timeout once again to make sure than sendChan doesn't hang indefinitely
      _ <- runWithTimeout $ sendChan sendPort $ FileSenderError (show asyncResult)
      return Nothing

-- | Read chunks from port and write them to file
-- Return file size if success, Nothing otherwise
-- TODO: report number of bytes if fails
receiveFile
  :: ReceivePort Response
  -> OsPath
  -> Process (Maybe FileSize)
receiveFile port path = do
  let
    --withFileWriteViaTemp version that reuses existing handle if file is already open.
    withFileOrExistingHandle :: Maybe Handle -> (Handle -> Process a) -> Process a
    withFileOrExistingHandle Nothing f = do
      exists <- liftIO $ doesFileExist path
      when exists $ Log.warn "Destination path exists and will be overwritten: " path
      withFileWriteViaTemp path f
    withFileOrExistingHandle (Just handle) f = f handle

    getChunks :: Maybe Handle -> Maybe Bytes -> Process (Maybe FileSize)
    getChunks maybeHandle maybeSize = do
      -- -- Extra logging for debug purposes:
      -- case maybeHandle of
      --   Just _ -> return ()
      --   Nothing -> Log.info "FileSenders: getChunks: wait for the first chunk" path
      response <- receiveChan port
      case response of
        Chunk bytes -> withFileOrExistingHandle maybeHandle $ \handle -> do
          liftIO $ B.hPut handle bytes
          let
            oldSize = maybe 0 id maybeSize
            size = oldSize + B.length bytes
          getChunks (Just handle) (Just size)
        EndOfFile -> withFileOrExistingHandle maybeHandle $ \_ -> do
          let size = maybe 0 id maybeSize
          return $ Just $ FileSize size
        _ -> do
          Log.err $ "receiveFile: " <> show (response, path)
          return Nothing

  getChunks Nothing Nothing
