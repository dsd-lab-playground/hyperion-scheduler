{-# LANGUAGE DeriveAnyClass      #-}
{-# LANGUAGE DerivingStrategies  #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE NoFieldSelectors    #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings   #-}

-- | Node-local copies of the programs that tasks start, with the shared
-- libraries they load.
--
-- On a cluster, the hyperion executable, external programs and many of their
-- libraries live on network filesystems, and their @RPATH@s and the module
-- environment's @LD_LIBRARY_PATH@ point there. Every process start then makes
-- the loader look for each library in those folders. On Expanse, with CephFS
-- (@\/cm\/shared@) on the path, a start costs ~77 ms of kernel time when 32
-- processes start at once, against 0.7 ms with node-local folders only
-- (bootstrap-compute #343). Staging copies each program and every library it
-- resolves outside the system folders to node-local disk, once per node,
-- removes the copies' @RPATH@ and @RUNPATH@, and checks with the loader that
-- each staged program loads the libraries it would have loaded before.
--
-- A staged program is found by name: its folder goes first in the task
-- environment's @PATH@, its libraries' folder first in @LD_LIBRARY_PATH@.
module Hyperion.Scheduler.TaskLaunch.Staging
  ( -- * Configuration
    Staging (..)
  , defaultStaging
  , Program (..)
  , program
  , ReferencePath (..)
  , defaultSystemLibraryDirs
    -- * Staging on a node
  , StagedPrograms (..)
  , StagingSummary (..)
  , stagePrograms
  , checkStaged
    -- * Errors
  , StagingError (..)
  , ResolutionDifference (..)
    -- * Pure parts, exported for tests
  , ProgramName (..)
  , SoName (..)
  , Sha256 (..)
  , Resolution
  , parseLoaderTrace
  , Location (..)
  , classify
  , LibraryCopies
  , libraryCopies
  , differences
  , isUnder
  ) where

import Control.Exception      (IOException, SomeException, displayException,
                               try)
import Control.Monad          (forM, forM_, unless, void, when)
import Control.Monad.Except   (ExceptT (..), runExceptT, throwError,
                               withExceptT)
import Control.Monad.IO.Class (liftIO)
import Crypto.Hash            (Digest, SHA256, hashlazy)
import Data.Binary            (Binary)
import Data.ByteString.Char8  qualified as BS8
import Data.ByteString.Lazy   qualified as BL
import Data.List              (isPrefixOf, sort)
import Data.Map.Strict        (Map)
import Data.Map.Strict        qualified as Map
import Data.Maybe             (mapMaybe)
import Data.Set               qualified as Set
import Data.Text.Lazy         qualified as TL
import Data.Text.Lazy.Encoding qualified as TLE
import GHC.Clock              (getMonotonicTimeNSec)
import GHC.Generics           (Generic)
import Hyperion.OsPath        (OsPath, normalise, splitDirectories,
                               takeFileName, (</>))
import Hyperion.OsString      qualified as OsString
import Hyperion.Scheduler.TaskLaunch.Elf (ElfError (..), ElfInfo (..),
                                          readElfInfo, removeRunPaths)
import Hyperion.Scheduler.TaskLaunch.Environment (StagedDirectories (..))
import System.Directory.OsPath (copyFile, createDirectoryIfMissing,
                                doesDirectoryExist, doesFileExist,
                                getFileSize, getPermissions,
                                removePathForcibly, renameDirectory,
                                renameFile, setOwnerWritable, setPermissions)
import System.Exit            (ExitCode (..))
import System.File.OsPath     qualified as File
import System.Posix.Files     (deviceID, fileID, fileSize, getFileStatus,
                               modificationTimeHiRes)
import System.Posix.Process   (getProcessID)
import System.Process         (CreateProcess (..), proc,
                               readCreateProcessWithExitCode)

-- * Configuration

-- | A program that tasks start, besides the worker executable.
data Program = MkProgram
  { -- | On shared storage. Its file name is the name tasks use.
    executable        :: OsPath
    -- | Folders its launcher puts before @LD_LIBRARY_PATH@ (e.g. a task
    -- script's own @LD_LIBRARY_PATH=...:$LD_LIBRARY_PATH@). Used to resolve the
    -- program as it loads today, and in the check; not applied at run time.
    -- Libraries found there stay where they are: at run time the launcher's
    -- folders come first anyway.
  , libraryPathPrefix :: [OsPath]
  }
  deriving stock (Eq, Show, Generic)
  deriving anyclass (Binary)

-- | A program without a library path prefix.
program :: OsPath -> Program
program exe = MkProgram exe []

-- | The @LD_LIBRARY_PATH@ under which programs resolve their libraries today.
data ReferencePath
  = MasterLibraryPath      -- ^ the master's own, when the run starts
  | LibraryPath [OsPath]
  deriving stock (Eq, Show, Generic)
  deriving anyclass (Binary)

data Staging = MkStaging
  { -- | On node-local disk, e.g. under the job's local scratch. No default.
    -- Staged sets go in subfolders named by their manifest's hash.
    directory            :: OsPath
    -- | Besides the worker executable, which is always staged. Default none.
  , programs             :: [Program]
    -- | Default 'MasterLibraryPath'.
  , referenceLibraryPath :: ReferencePath
    -- | Libraries resolved under these are not copied. Default
    -- 'defaultSystemLibraryDirs'.
  , systemLibraryDirs    :: [OsPath]
  }
  deriving stock (Eq, Show, Generic)
  deriving anyclass (Binary)

defaultStaging :: OsPath -> Staging
defaultStaging dir = MkStaging
  { directory            = dir
  , programs             = []
  , referenceLibraryPath = MasterLibraryPath
  , systemLibraryDirs    = defaultSystemLibraryDirs
  }

-- | The loader's default folders, which are on the node's own disk.
defaultSystemLibraryDirs :: [OsPath]
defaultSystemLibraryDirs =
  [ "/lib64", "/usr/lib64", "/lib", "/usr/lib"
  , "/lib/x86_64-linux-gnu", "/usr/lib/x86_64-linux-gnu"
  , "/lib/aarch64-linux-gnu", "/usr/lib/aarch64-linux-gnu"
  ]

-- * Names and resolutions

-- | A program's file name.
newtype ProgramName = ProgramName OsPath
  deriving stock (Eq, Ord, Show, Generic)
  deriving anyclass (Binary)

-- | A library as the loader names it (@libgmp.so.10@).
newtype SoName = SoName String
  deriving stock (Eq, Ord, Show, Generic)
  deriving anyclass (Binary)

newtype Sha256 = Sha256 String
  deriving stock (Eq, Ord, Show, Generic)
  deriving anyclass (Binary)

-- | Where the loader finds each library a program needs.
type Resolution = Map SoName OsPath

-- | Parse the output of the loader with @LD_TRACE_LOADED_OBJECTS=1@: lines
-- @name => path (address)@. Lines without @=>@ (the vDSO, the loader
-- itself) are skipped. 'Left' with the libraries that were not found.
parseLoaderTrace :: String -> Either [SoName] Resolution
parseLoaderTrace out = case [ n | (n, Nothing) <- entries ] of
  []       -> Right $ Map.fromList [ (n, p) | (n, Just p) <- entries ]
  notFound -> Left notFound
  where
    entries = mapMaybe entry (lines out)
    entry line = case breakOn " => " (dropWhile (`elem` [' ', '\t']) line) of
      Just (name, rest)
        | "not found" `isPrefixOf` rest -> Just (SoName name, Nothing)
        | otherwise -> Just (SoName name, Just (OsString.fromString (stripAddress rest)))
      Nothing -> Nothing
    stripAddress s = case breakOn " (0x" s of
      Just (p, _) -> p
      Nothing     -> trimEnd s
    trimEnd = reverse . dropWhile (`elem` [' ', '\t', '\r']) . reverse

breakOn :: String -> String -> Maybe (String, String)
breakOn sep = go []
  where
    go acc s
      | sep `isPrefixOf` s = Just (reverse acc, drop (length sep) s)
      | otherwise = case s of
          c : rest -> go (c : acc) rest
          []       -> Nothing

-- | Whether a path is in a folder (by components, after 'normalise').
isUnder :: OsPath -> OsPath -> Bool
isUnder dir path = root `isPrefixOf` splitDirectories (normalise path)
  where root = splitDirectories (normalise dir)

-- | Where a staged program loads a library from.
data Location
  = StagedCopy     -- ^ the copy in the staged libraries' folder
  | SameFile OsPath -- ^ the same file as before: a system or prefix folder
  deriving stock (Eq, Ord, Show, Generic)
  deriving anyclass (Binary)

-- | Which libraries of a program's resolution are copied.
classify :: [OsPath] -> Program -> Resolution -> Map SoName Location
classify systemDirs prog = Map.map location
  where
    keepDirs = systemDirs <> prog.libraryPathPrefix
    location path
      | any (`isUnder` path) keepDirs = SameFile path
      | otherwise = StagedCopy

-- | The library files to copy, by name, with the programs that need each and
-- the sha256 of each source. 'Left' for names whose sources differ.
type LibraryCopies = Map SoName (OsPath, Sha256)

libraryCopies
  :: Map SoName [(ProgramName, OsPath, Sha256)]
  -> Either [(SoName, [(ProgramName, OsPath)])] LibraryCopies
libraryCopies wanted = case conflicts of
  [] -> Right $ Map.mapMaybe firstSource wanted
  cs -> Left cs
  where
    conflicts =
      [ (n, [ (prog, p) | (prog, p, _) <- uses ])
      | (n, uses) <- Map.toList wanted
      , Set.size (Set.fromList [ h | (_, _, h) <- uses ]) > 1
      ]
    firstSource = \case
      (_, p, h) : _ -> Just (p, h)
      []            -> Nothing

data ResolutionDifference
  = Missing SoName Location               -- ^ expected, not loaded
  | Unexpected SoName OsPath              -- ^ loaded, not expected
  | Moved SoName Location OsPath          -- ^ expected location, actual path
  deriving stock (Eq, Show, Generic)
  deriving anyclass (Binary)

-- | Differences between the expected locations and a staged program's
-- resolution, given the staged libraries' folder.
differences :: OsPath -> Map SoName Location -> Resolution -> [ResolutionDifference]
differences libDir expected actual = concat
  [ [ Missing n l | (n, l) <- Map.toList (Map.difference expected actual) ]
  , [ Unexpected n p | (n, p) <- Map.toList (Map.difference actual expected) ]
  , [ Moved n l p
    | (n, (l, p)) <- Map.toList (Map.intersectionWith (,) expected actual)
    , normalise p /= normalise (path n l)
    ]
  ]
  where
    path (SoName n) = \case
      StagedCopy -> libDir </> OsString.fromString n
      SameFile p -> p

-- * Errors

data StagingError
  = DuplicateProgramName ProgramName
  | NotFound OsPath
  | ElfFailed OsPath ElfError
  | LoaderFailed ProgramName Int String  -- ^ exit status and stderr
  | LibrariesNotFound ProgramName [SoName]
  | LibraryConflicts [(SoName, [(ProgramName, OsPath)])]
  -- | A staged program would load libraries from elsewhere than expected.
  | ResolutionChanged ProgramName [ResolutionDifference]
  -- | A staged file is not what was written (a corrupt or foreign folder).
  | StagedFileChanged OsPath
  | StagingIOError String
  deriving stock (Show, Generic)
  deriving anyclass (Binary)

-- * Staging

-- | What the master logs and compares across nodes.
data StagingSummary = StagingSummary
  { -- | Of the manifest: every program's and copied library's source path and
    -- sha256, and the libraries that stay. Equal on all nodes iff they stage
    -- the same files.
    manifestHash    :: Sha256
  , root            :: OsPath
  , programNames    :: [ProgramName]
  , copiedLibraries :: Int
  }
  deriving stock (Eq, Show, Generic)
  deriving anyclass (Binary)

data StagedPrograms = StagedPrograms
  { summary          :: StagingSummary
  , directories      :: StagedDirectories
    -- | The staged worker executable.
  , workerExecutable :: OsPath
    -- | Each program's library path prefix and expected library locations.
  , expected         :: Map ProgramName ([OsPath], Map SoName Location)
  }

-- | A program as resolved: its library locations, and (once hashed) its
-- source's sha256.
data Planned = Planned
  { name       :: ProgramName
  , prog       :: Program
  , resolution :: Resolution
  , locations  :: Map SoName Location
  }

-- | Stage the worker executable and the configured programs in
-- @directory/\<hash\>@, or reuse a folder staged there before with the same
-- manifest. The reference library path must be concrete ('LibraryPath').
-- Doesn't run the check: see 'checkStaged', which needs the task
-- environment's library path.
--
-- Supervisors start again for every 'Hyperion.Scheduler.RunTasks.runTasks',
-- so staging is cheap when nothing changed: a file @by-stat-\<key\>@ names the
-- folder staged from sources with the same paths, sizes, modification times
-- and inodes, and then no source is read; the staged files' sizes are
-- checked. Otherwise every source is hashed, and the staged files' sha256s
-- are checked.
stagePrograms :: Staging -> [OsPath] -> OsPath -> IO (Either StagingError StagedPrograms)
stagePrograms staging referencePath workerExe = runExceptT $ do
  let
    worker = program workerExe
    allPrograms = worker : staging.programs
    names = map (ProgramName . takeFileName . (.executable)) allPrograms
  forM_ (duplicates names) $ throwError . DuplicateProgramName
  planned <- forM (zip names allPrograms) $ \(n, p) -> do
    exists <- liftIO $ doesFileExist p.executable
    unless exists $ throwError (NotFound p.executable)
    res <- resolve n p.executable (p.libraryPathPrefix <> referencePath)
    pure Planned
      { name = n, prog = p, resolution = res
      , locations = classify staging.systemLibraryDirs p res
      }
  key <- statKey planned
  let Sha256 keyHex = key
      keyPath = staging.directory </> OsString.fromString ("by-stat-" <> take 32 keyHex)
  cached <- liftIO $ readCachedRoot staging.directory keyPath
  reused <- case cached of
    Just root -> do
      sizesOk <- stagedSizesMatch root
      if sizesOk then Just <$> fromStagedRoot planned workerExe root else pure Nothing
    Nothing -> pure Nothing
  case reused of
    Just staged -> pure staged
    Nothing -> do
      staged <- stageFromSources staging planned workerExe
      ioError' $ writeCachedRoot keyPath staged.summary.root
      pure staged

-- | Hash every source, write the manifest, and build the folder unless it
-- exists; check the staged files' sha256s either way.
stageFromSources :: Staging -> [Planned] -> OsPath -> ExceptT StagingError IO StagedPrograms
stageFromSources staging planned workerExe = do
  sourceHashes <- forM planned $ \pl -> (,) pl.name <$> sha256 pl.prog.executable
  wanted <- fmap concat $ forM planned $ \pl ->
    forM (Map.toList (stagedCopies pl)) $ \(so, src) -> do
      h <- sha256 src
      pure (Map.singleton so [(pl.name, src, h)])
  copies <- either (throwError . LibraryConflicts) pure $
    libraryCopies (Map.unionsWith (<>) wanted)
  let
    manifest = renderManifest planned (Map.fromList sourceHashes) copies
    root = staging.directory </> OsString.fromString (take 32 hex)
      where Sha256 hex = sha256String manifest
  done <- liftIO $ doesDirectoryExist root
  unless done $ build staging.directory root manifest
    [ (src, root </> "bin" </> takeFileName src) | pl <- planned, let src = pl.prog.executable ]
    [ (src, root </> "lib" </> OsString.fromString so) | (SoName so, (src, _)) <- Map.toList copies ]
  verifyStagedFiles root
  fromStagedRoot planned workerExe root

-- | The staged set in @root@, from its manifest.
fromStagedRoot :: [Planned] -> OsPath -> OsPath -> ExceptT StagingError IO StagedPrograms
fromStagedRoot planned workerExe root = do
  manifest <- ioError' $ fromUtf8 . BL.fromStrict <$> File.readFile' (root </> manifestName)
  let
    binDir = root </> "bin"
    libDir = root </> "lib"
  pure StagedPrograms
    { summary = StagingSummary
        { manifestHash = sha256String manifest
        , root = root
        , programNames = map (.name) planned
        , copiedLibraries = length [ () | l <- lines manifest, "library " `isPrefixOf` l ]
        }
    , directories = StagedDirectories binDir libDir
    , workerExecutable = binDir </> takeFileName workerExe
    , expected = Map.fromList
        [ (pl.name, (pl.prog.libraryPathPrefix, pl.locations)) | pl <- planned ]
    }

stagedCopies :: Planned -> Resolution
stagedCopies pl = Map.restrictKeys pl.resolution $
  Map.keysSet (Map.filter (== StagedCopy) pl.locations)

-- | A key of the sources' state: each program's and copied library's path,
-- size, modification time, device and inode, and where each library is
-- loaded from. Reads no file contents.
statKey :: [Planned] -> ExceptT StagingError IO Sha256
statKey planned = do
  let
    sources = Set.toList $ Set.fromList $
      [ pl.prog.executable | pl <- planned ] <> concatMap (Map.elems . stagedCopies) planned
  stats <- forM sources $ \src -> ioError' $ do
    st <- getFileStatus (OsString.toString src)
    pure $ unwords
      [ "source", OsString.toString src, show (fileSize st)
      , show (modificationTimeHiRes st), show (deviceID st), show (fileID st) ]
  let locations =
        [ unwords ["location", n, so, show l]
        | pl <- planned, let ProgramName pn = pl.name, let n = OsString.toString pn
        , (SoName so, l) <- Map.toList pl.locations ]
  pure $ sha256String $ unlines $ "hyperion-scheduler stat key v1" : sort (stats <> locations)

-- | The staged folder a key file names, if both exist.
readCachedRoot :: OsPath -> OsPath -> IO (Maybe OsPath)
readCachedRoot dir keyPath = do
  r <- try (File.readFile' keyPath)
  case r of
    Left (_ :: IOException) -> pure Nothing
    Right bytes -> do
      let rootName = takeWhile (/= '\n') (fromUtf8 (BL.fromStrict bytes))
          root = dir </> OsString.fromString rootName
      ok <- doesFileExist (root </> manifestName)
      pure $ if length rootName == 32 && ok then Just root else Nothing

-- | Write a key file naming the staged folder, atomically.
writeCachedRoot :: OsPath -> OsPath -> IO ()
writeCachedRoot keyPath root = do
  pid <- getProcessID
  let tmp = keyPath <> OsString.fromString (".tmp-" <> show pid)
  File.writeFile tmp (utf8 (OsString.toString (takeFileName root) <> "\n"))
  renameFile tmp keyPath

-- | Run the loader on each staged program, with its prefix and the given
-- library path (the task environment's @LD_LIBRARY_PATH@, which starts with
-- the staged libraries' folder), and compare with the expected locations.
checkStaged :: StagedPrograms -> [OsPath] -> IO (Either StagingError ())
checkStaged staged libraryPath = runExceptT $
  forM_ (Map.toList staged.expected) $ \(n@(ProgramName file), (prefix, expected)) -> do
    actual <- resolve n (staged.directories.binDirectory </> file) (prefix <> libraryPath)
    case differences staged.directories.libraryDirectory expected actual of
      []    -> pure ()
      diffs -> throwError (ResolutionChanged n diffs)

duplicates :: Ord a => [a] -> [a]
duplicates xs = Map.keys $ Map.filter (> (1 :: Int)) $ Map.fromListWith (+) [ (x, 1) | x <- xs ]

-- | The program's libraries as the loader resolves them with the given
-- @LD_LIBRARY_PATH@, without running the program: the loader named in its
-- @PT_INTERP@ is run with @LD_TRACE_LOADED_OBJECTS=1@ and an otherwise empty
-- environment. A program without @PT_INTERP@ is static: no libraries.
resolve :: ProgramName -> OsPath -> [OsPath] -> ExceptT StagingError IO Resolution
resolve n exe libraryPath = do
  info <- elfInfo exe
  case info >>= (.interpreter) of
    Nothing -> pure Map.empty
    Just interp -> do
      let
        env = ("LD_TRACE_LOADED_OBJECTS", "1") :
          [ ("LD_LIBRARY_PATH", renderPath libraryPath) | not (null libraryPath) ]
        cp = (proc (BS8.unpack interp) [OsString.toString exe]) { env = Just env }
      (code, out, err) <- ioError' $ readCreateProcessWithExitCode cp ""
      case code of
        ExitSuccess   -> either (throwError . LibrariesNotFound n) pure (parseLoaderTrace out)
        ExitFailure c -> throwError (LoaderFailed n c err)
  where
    renderPath = foldr1 (\a b -> a <> ":" <> b) . map OsString.toString

-- | The ELF information of a file; 'Nothing' if it isn't ELF (a script,
-- which is copied but has no libraries).
elfInfo :: OsPath -> ExceptT StagingError IO (Maybe ElfInfo)
elfInfo path = liftIO (readElfInfo path) >>= \case
  Right info  -> pure (Just info)
  Left NotElf -> pure Nothing
  Left err    -> throwError (ElfFailed path err)

sha256 :: OsPath -> ExceptT StagingError IO Sha256
sha256 path = ioError' $ do
  bytes <- File.readFile path
  let digest = hashlazy bytes :: Digest SHA256
  pure $! Sha256 (show digest)

sha256String :: String -> Sha256
sha256String s = Sha256 (show (hashlazy (utf8 s) :: Digest SHA256))

utf8 :: String -> BL.ByteString
utf8 = TLE.encodeUtf8 . TL.pack

fromUtf8 :: BL.ByteString -> String
fromUtf8 = TL.unpack . TLE.decodeUtf8

-- | One line per program and library, sorted: the identity of a staged set.
renderManifest :: [Planned] -> Map ProgramName Sha256 -> LibraryCopies -> String
renderManifest planned hashes copies = unlines $ "hyperion-scheduler staged programs v1" : sort (concat
  [ [ unwords ["program", showPath n, h, OsString.toString pl.prog.executable]
    | pl <- planned, let ProgramName n = pl.name
    , Just (Sha256 h) <- [Map.lookup pl.name hashes] ]
  , [ unwords ["library", so, h, OsString.toString src]
    | (SoName so, (src, Sha256 h)) <- Map.toList copies ]
  , [ unwords ["keeps", showPath n, so, OsString.toString p]
    | pl <- planned, let ProgramName n = pl.name
    , (SoName so, SameFile p) <- Map.toList pl.locations ]
  , [ unwords ["prefix", showPath n, OsString.toString d]
    | pl <- planned, let ProgramName n = pl.name, d <- pl.prog.libraryPathPrefix ]
  ])
  where showPath = OsString.toString

stagedListName, manifestName :: OsPath
stagedListName = "STAGED"
manifestName = "MANIFEST"

-- | Copy the files into a temporary folder next to @root@, remove their
-- @RPATH@s, write the manifest and the staged files' hashes, and rename the
-- folder to @root@. If another process staged @root@ meanwhile, use its.
build :: OsPath -> OsPath -> String -> [(OsPath, OsPath)] -> [(OsPath, OsPath)] -> ExceptT StagingError IO ()
build parent root manifest bins libs = do
  pid <- liftIO getProcessID
  t <- liftIO getMonotonicTimeNSec
  let tmp = parent </> OsString.fromString (".staging-" <> show pid <> "-" <> show t)
      relocate p = tmp </> relativeTo root p
  ioError' $ createDirectoryIfMissing True (tmp </> "bin")
  ioError' $ createDirectoryIfMissing True (tmp </> "lib")
  staged <- forM (bins <> libs) $ \(src, dst) -> do
    let dst' = relocate dst
    ioError' $ do
      copyFile src dst'
      perms <- getPermissions dst'
      setPermissions dst' (setOwnerWritable True perms)
    isElf <- (/= Nothing) <$> elfInfo dst'
    when isElf $ void $ withExceptT (ElfFailed src) $ ExceptT $ removeRunPaths dst'
    h <- sha256 dst'
    size <- ioError' $ getFileSize dst'
    pure (relativeTo root dst, h, size)
  ioError' $ do
    File.writeFile (tmp </> manifestName) (utf8 manifest)
    File.writeFile (tmp </> stagedListName) $ utf8 $ unlines
      [ h <> " " <> show size <> " " <> OsString.toString rel | (rel, Sha256 h, size) <- staged ]
  renamed <- liftIO $ try (renameDirectory tmp root)
  case renamed of
    Right () -> pure ()
    Left (e :: IOException) -> do
      won <- liftIO $ doesDirectoryExist root
      liftIO $ removePathForcibly tmp
      unless won $ throwError (StagingIOError (displayException e))

-- | The path of @p@ relative to @root@, which contains it.
relativeTo :: OsPath -> OsPath -> OsPath
relativeTo root p = foldr1 (</>) (drop (length (splitDirectories root)) (splitDirectories p))

-- | The list of staged files written when staging: path, sha256, size.
stagedList :: OsPath -> ExceptT StagingError IO [(OsPath, Sha256, Integer)]
stagedList root = do
  listing <- ioError' $ BL.fromStrict <$> File.readFile' (root </> stagedListName)
  forM (lines (fromUtf8 listing)) $ \line -> case words' line of
    Just (h, size, rel) | length h == 64 -> pure (root </> OsString.fromString rel, Sha256 h, size)
    _ -> throwError (StagedFileChanged (root </> stagedListName))
  where
    words' line = case break (== ' ') line of
      (h, ' ' : rest) -> case break (== ' ') rest of
        (n, ' ' : rel) | [(size, "")] <- reads n -> Just (h, size, rel)
        _ -> Nothing
      _ -> Nothing

-- | Check every staged file's sha256 against the list written when staging.
verifyStagedFiles :: OsPath -> ExceptT StagingError IO ()
verifyStagedFiles root = do
  files <- stagedList root
  forM_ files $ \(path, h, _) -> do
    actual <- sha256 path
    unless (actual == h) $ throwError (StagedFileChanged path)

-- | Whether every staged file still has the size written when staging.
stagedSizesMatch :: OsPath -> ExceptT StagingError IO Bool
stagedSizesMatch root = do
  files <- stagedList root
  and <$> forM files (\(path, _, size) -> ioError' $ (== size) <$> getFileSize path)

ioError' :: IO a -> ExceptT StagingError IO a
ioError' act = ExceptT $ try act >>= \case
  Left (e :: SomeException) -> pure (Left (StagingIOError (displayException e)))
  Right x -> pure (Right x)
