{-# LANGUAGE DeriveAnyClass        #-}
{-# LANGUAGE DerivingStrategies    #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE NoFieldSelectors      #-}
{-# LANGUAGE OverloadedRecordDot   #-}
{-# LANGUAGE OverloadedStrings     #-}

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
--
-- What the check can't see:
--
-- * Libraries a program opens with @dlopen@. Removing an @RPATH@ also removes
--   it from @dlopen@'s search, so a plugin next to a library may no longer be
--   found. A program that does this should not be staged, or its plugins'
--   folder should be on the task environment's 'libraryPath'.
-- * Programs that aren't listed. They run with the staged libraries' folder
--   first in @LD_LIBRARY_PATH@, so one with a @RUNPATH@ (which comes after
--   @LD_LIBRARY_PATH@, unlike an @RPATH@) loads a staged library of the same
--   name instead of its own. List every program that tasks start.
-- * Programs that find files next to themselves (through @$0@ or
--   @\/proc\/self\/exe@): only what is listed is next to a staged copy.
module Hyperion.Scheduler.TaskLaunch.Staging
  ( -- * Configuration
    Staging (..)
  , defaultStaging
  , Seconds (..)
  , defaultStartupTimeout
  , Program (..)
  , program
  , ReferencePath (..)
  , defaultSystemLibraryDirs
    -- * Staging on a node
  , StagedPrograms (..)
  , StagingSummary (..)
  , StagingOutcome (..)
  , ExpectedResolution (..)
  , stagePrograms
  , checkStaged
  , checkProgramLoads
    -- * Errors
  , StagingError (..)
  , ResolutionDifference (..)
  , LibraryConflict (..)
    -- * Pure parts, exported for tests
  , ProgramName (..)
  , SoName (..)
  , Sha256
  , sha256Bytes
  , Resolution
  , parseLoaderTrace
  , resolve
  , Location (..)
  , classify
  , LibraryUse (..)
  , LibrarySource (..)
  , libraryCopies
  , differences
  , isUnder
  ) where

import Control.Exception        (IOException, displayException, onException,
                                 try)
import Control.Monad            (forM, forM_, unless, void, when)
import Control.Monad.Except     (ExceptT (..), runExceptT, throwError,
                                 withExceptT)
import Control.Monad.IO.Class   (liftIO)
import Crypto.Hash              (Context, Digest, SHA256, digestFromByteString,
                                 hashFinalize, hashInit, hashUpdate, hashlazy)
import Data.Binary              (Binary (..), decodeOrFail, encode)
import Data.ByteArray           (convert)
import Data.ByteString          (ByteString)
import Data.ByteString          qualified as BS
import Data.ByteString.Char8    qualified as BS8
import Data.ByteString.Lazy     qualified as BL
import Data.Char                (isHexDigit)
import Data.Either              (isLeft)
import Data.List                (isPrefixOf, sort)
import Data.List.Extra          (stripInfix, stripInfixEnd, trimStart)
import Data.List.NonEmpty       (NonEmpty (..))
import Data.List.NonEmpty       qualified as NonEmpty
import Data.Map.Strict          (Map)
import Data.Map.Strict          qualified as Map
import Data.Maybe               (isJust, mapMaybe)
import Data.Set                 qualified as Set
import Data.Text.Lazy           qualified as TL
import Data.Text.Lazy.Encoding  qualified as TLE
import GHC.Clock                (getMonotonicTimeNSec)
import GHC.Generics             (Generic)
import Hyperion.Log             qualified as Log
import Hyperion.OsPath          (OsPath, normalise, splitDirectories,
                                 takeFileName, (</>))
import Hyperion.OsString        qualified as OsString
import Hyperion.Scheduler.TaskLaunch.Elf (ElfError (..), ElfInfo (..),
                                          readElfInfo, removeRunPaths)
import Hyperion.Scheduler.TaskLaunch.Environment (StagedDirectories (..),
                                                  envKey, libraryPathVar,
                                                  renderSearchPath)
import Hyperion.Util.EncodeFile (writeFileAtomic)
import System.Directory.OsPath  (canonicalizePath, copyPermissions,
                                 createDirectoryIfMissing, doesDirectoryExist,
                                 doesFileExist, getFileSize, getPermissions,
                                 removePathForcibly, renameDirectory,
                                 setOwnerWritable, setPermissions)
import System.Exit              (ExitCode (..))
import System.File.OsPath       qualified as File
import System.IO                (Handle, IOMode (..))
import System.Posix.Files       (deviceID, fileID, fileSize, getFileStatus,
                                 modificationTimeHiRes, statusChangeTimeHiRes)
import System.Posix.Process     (getProcessID)
import System.Process           (CreateProcess (..), proc,
                                 readCreateProcessWithExitCode)

-- * Configuration

-- | A program that tasks start, besides the worker executable.
data Program = MkProgram
  { -- | On shared storage. Its file name is the name tasks use.
    executable          :: OsPath
    -- | Folders that whatever starts this program (a task script's
    -- @LD_LIBRARY_PATH=...:$LD_LIBRARY_PATH@, say) puts before the task
    -- environment's @LD_LIBRARY_PATH@. The scheduler doesn't apply them: they
    -- are used to resolve the program as it loads today, and in the check,
    -- which therefore only holds for a start with these folders first.
    -- Started by its bare name without them, the program would load the staged
    -- library of the same name, if there is one, and nothing reports that.
    -- Libraries found there aren't copied. Default none.
  , launcherLibraryPath :: [OsPath]
  }
  deriving stock (Eq, Show, Generic)
  deriving anyclass (Binary)

-- | A program that its tasks start without changing @LD_LIBRARY_PATH@.
program :: OsPath -> Program
program exe = MkProgram exe []

-- | The @LD_LIBRARY_PATH@ under which programs resolve their libraries today.
data ReferencePath
  = MasterLibraryPath      -- ^ the master's own, when the run starts
  | LibraryPath [OsPath]
  deriving stock (Eq, Show, Generic)
  deriving anyclass (Binary)

-- | A duration in whole seconds.
newtype Seconds = Seconds Int
  deriving stock (Eq, Ord, Show, Generic)
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
    -- | How long the master waits for all nodes to stage and check, before it
    -- stops the run. Default 'defaultStartupTimeout'.
  , startupTimeout       :: Seconds
  }
  deriving stock (Eq, Show, Generic)
  deriving anyclass (Binary)

defaultStaging :: OsPath -> Staging
defaultStaging dir = MkStaging
  { directory            = dir
  , programs             = []
  , referenceLibraryPath = MasterLibraryPath
  , systemLibraryDirs    = defaultSystemLibraryDirs
  , startupTimeout       = defaultStartupTimeout
  }

-- | 10 minutes. Staging about 250 MB took 3-4 s per node on Expanse, so this
-- only ends a run whose filesystem is stuck, which would otherwise hold the
-- job to its time limit.
defaultStartupTimeout :: Seconds
defaultStartupTimeout = Seconds 600

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

-- | A library as the loader names it: @libgmp.so.10@, or its whole path if it
-- was linked by path.
newtype SoName = SoName String
  deriving stock (Eq, Ord, Show, Generic)
  deriving anyclass (Binary)

newtype Sha256 = Sha256 (Digest SHA256)
  deriving stock (Eq, Ord)

-- | In hexadecimal, as in the manifest.
instance Show Sha256 where
  show (Sha256 digest) = show digest

instance Binary Sha256 where
  put (Sha256 digest) = put (convert digest :: ByteString)
  get = get >>= \bytes -> case digestFromByteString (bytes :: ByteString) of
    Just digest -> pure (Sha256 digest)
    Nothing     -> fail "Sha256: wrong number of bytes"

sha256Bytes :: BL.ByteString -> Sha256
sha256Bytes = Sha256 . hashlazy

-- | Where the loader finds each library a program needs.
type Resolution = Map SoName OsPath

-- | Parse the output of the loader with @--list@: lines @name => path
-- (address)@, or @path (address)@ for a library linked by path and for the
-- loader itself. Lines that name no file (the vDSO, in both of the forms glibc
-- has used) are skipped. 'Left' with the libraries found through a relative
-- path, which would depend on the working folder. A library the loader can't
-- find doesn't appear here: with @--list@ that is the loader's failure.
parseLoaderTrace :: String -> Either [SoName] Resolution
parseLoaderTrace out = case [ n | Left n <- entries ] of
  []       -> Right $ Map.fromList [ e | Right e <- entries ]
  relative -> Left relative
  where
    entries = mapMaybe (entry . trimStart) (lines out)
    entry line = case stripInfix " => " line of
      Just (name, rest) -> file name (withoutAddress rest)
      Nothing
        | '/' `elem` withoutAddress line -> file (withoutAddress line) (withoutAddress line)
        | otherwise -> Nothing
    file name path = case path of
      ""      -> Nothing
      '/' : _ -> Just $ Right (SoName name, OsString.fromString path)
      _       -> Just $ Left (SoName name)
    withoutAddress s = maybe s fst (stripInfixEnd " (0x" s)

-- | Whether a path is in a folder (by components, after 'normalise').
isUnder :: OsPath -> OsPath -> Bool
isUnder dir path = root `isPrefixOf` splitDirectories (normalise path)
  where root = splitDirectories (normalise dir)

-- | Where a staged program loads a library from.
data Location
  = StagedCopy     -- ^ the copy in the staged libraries' folder
  | SameFile OsPath -- ^ the same file as before: a system or launcher's folder
  deriving stock (Eq, Ord, Show, Generic)
  deriving anyclass (Binary)

-- | Which libraries of a program's resolution are copied. Not a library linked
-- by path: the loader goes to that path whatever the search path says.
classify :: [OsPath] -> Program -> Resolution -> Map SoName Location
classify systemDirs prog = Map.mapWithKey location
  where
    keepDirs = systemDirs <> prog.launcherLibraryPath
    location (SoName so) path
      | '/' `elem` so = SameFile path
      | any (`isUnder` path) keepDirs = SameFile path
      | otherwise = StagedCopy

-- | A program's use of a library file that is to be copied.
data LibraryUse = LibraryUse
  { user   :: ProgramName
  , source :: OsPath
  , hash   :: Sha256
  }
  deriving stock (Eq, Show, Generic)
  deriving anyclass (Binary)

-- | A library file to copy.
data LibrarySource = LibrarySource
  { source :: OsPath
  , hash   :: Sha256
  }
  deriving stock (Eq, Show)

-- | Two programs need different files under one library name.
data LibraryConflict = LibraryConflict
  { library :: SoName
  , uses    :: [LibraryUse]
  }
  deriving stock (Eq, Show, Generic)
  deriving anyclass (Binary)

-- | The library files to copy, by name. All of a library's sources must have
-- the same contents, since one folder holds the copies. The remedy for a
-- conflict is to keep one program's library in place, with
-- 'launcherLibraryPath'.
libraryCopies :: Map SoName (NonEmpty LibraryUse) -> Either [LibraryConflict] (Map SoName LibrarySource)
libraryCopies wanted = case conflicts of
  [] -> Right $ Map.map firstSource wanted
  cs -> Left cs
  where
    conflicts =
      [ LibraryConflict n (NonEmpty.toList uses)
      | (n, uses) <- Map.toList wanted
      , Set.size (Set.fromList [ u.hash | u <- NonEmpty.toList uses ]) > 1
      ]
    firstSource (u :| _) = LibrarySource u.source u.hash

data ResolutionDifference
  = Missing SoName Location               -- ^ expected, not loaded
  | Unexpected SoName OsPath              -- ^ loaded, not expected
  | Moved SoName Location OsPath          -- ^ expected location, actual path
  deriving stock (Eq, Show, Generic)
  deriving anyclass (Binary)

-- | Differences between the expected locations and a staged program's
-- resolution, given the staged libraries' folder. Compares paths as given:
-- 'checkStaged' passes them canonical, so that two names of one file are equal.
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
  -- | The loader's exit status and stderr: for one, a library it can't find.
  | LoaderFailed ProgramName Int String
  -- | Found through a relative path: see 'parseLoaderTrace'.
  | LibrariesThroughRelativePaths ProgramName [SoName]
  | LibraryConflicts [LibraryConflict]
  -- | A staged program would load libraries from elsewhere than expected.
  | ResolutionChanged ProgramName [ResolutionDifference]
  -- | A staged file is not what was written (a corrupt or foreign folder).
  | StagedFileChanged OsPath
  -- | A source's contents changed between hashing it and copying it.
  | SourceChanged OsPath
  | StagingIOError OsPath String  -- ^ the path concerned, and the system's message
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

-- | How a node came to have its staged set.
data StagingOutcome
  = Built         -- ^ copied, edited and written now
  | ReusedByHash  -- ^ found in place after hashing every source and staged file
  | ReusedByStat  -- ^ found in place from the sources' state, reading no source
  deriving stock (Eq, Show, Generic)
  deriving anyclass (Binary)

-- | What a staged program should load.
data ExpectedResolution = ExpectedResolution
  { launcherLibraryPath :: [OsPath]
  , locations           :: Map SoName Location
  }
  deriving stock (Eq, Show)

data StagedPrograms = StagedPrograms
  { summary          :: StagingSummary
  , outcome          :: StagingOutcome
  , directories      :: StagedDirectories
    -- | The staged worker executable.
  , workerExecutable :: OsPath
  , expected         :: Map ProgramName ExpectedResolution
  }

-- | A program as resolved.
data Planned = Planned
  { name       :: ProgramName
  , prog       :: Program
  , resolution :: Resolution
  , locations  :: Map SoName Location
  }

-- | Stage the worker executable and the configured programs in
-- @directory/\<hash\>@, or reuse a folder staged there before with the same
-- manifest. Takes the reference library path. Doesn't run the check: see
-- 'checkStaged', which needs the task environment's library path.
--
-- Supervisors start again for every 'Hyperion.Scheduler.RunTasks.runTasks',
-- so staging is cheap when nothing changed: a file @by-stat-\<key\>@ names the
-- folder staged from sources with the same state ('sourceStateKey'), and then
-- no source is read; the manifest's name and the staged files' sizes are
-- checked, not their contents. Otherwise every source is hashed, and the
-- staged files' sha256s are checked.
stagePrograms :: Staging -> [OsPath] -> OsPath -> IO (Either StagingError StagedPrograms)
stagePrograms staging referencePath workerExe = runExceptT $ do
  let
    allPrograms = program workerExe : staging.programs
    names = map (ProgramName . takeFileName . (.executable)) allPrograms
  forM_ (duplicates names) $ throwError . DuplicateProgramName
  planned <- forM (zip names allPrograms) $ \(n, p) -> do
    exists <- ioErrorAt p.executable $ doesFileExist p.executable
    unless exists $ throwError (NotFound p.executable)
    -- Through the real file: the loader expands $ORIGIN from the path it is
    -- given, and an exec expands it from the file.
    realFile <- ioErrorAt p.executable $ canonicalizePath p.executable
    res <- resolve n realFile (p.launcherLibraryPath <> referencePath)
    pure Planned
      { name = n, prog = p, resolution = res
      , locations = classify staging.systemLibraryDirs p res
      }
  key <- sourceStateKey planned
  let keyPath = staging.directory </> keyFileName key
  cached <- liftIO $ readCachedRoot staging.directory keyPath
  reusable <- maybe (pure False) reusableFromState cached
  case cached of
    Just root | reusable -> fromStagedRoot ReusedByStat planned workerExe root
    _ -> do
      staged <- stageFromSources staging planned workerExe
      liftIO $ writeCachedRoot keyPath staged.summary.root
      pure staged

-- | Hash every source, write the manifest, and build the folder unless it
-- exists; if it exists, check it against the manifest and its staged files'
-- sha256s.
stageFromSources :: Staging -> [Planned] -> OsPath -> ExceptT StagingError IO StagedPrograms
stageFromSources staging planned workerExe = do
  hashes <- fmap Map.fromList $ forM (sourcesOf planned) $ \src -> (,) src <$> sha256File src
  let
    uses = Map.fromListWith (flip (<>))
      [ (so, LibraryUse pl.name src h :| [])
      | pl <- planned
      , (so, src) <- Map.toList (stagedCopies pl)
      , Just h <- [Map.lookup src hashes]
      ]
  copies <- either (throwError . LibraryConflicts) pure (libraryCopies uses)
  let
    manifest = renderManifest planned hashes copies
    root = staging.directory </> stagedFolderName (sha256Bytes manifest)
    sources =
      [ ToStage src h ("bin" </> takeFileName src)
      | pl <- planned, let src = pl.prog.executable, Just h <- [Map.lookup src hashes] ]
      <>
      [ ToStage lib.source lib.hash ("lib" </> OsString.fromString so)
      | (SoName so, lib) <- Map.toList copies ]
  exists <- ioErrorAt root $ doesDirectoryExist root
  outcome <- if exists
    then ReusedByHash <$ verifyStagedRoot root manifest
    else build staging.directory root manifest (Map.size copies) sources
  fromStagedRoot outcome planned workerExe root

-- | The staged set in @root@.
fromStagedRoot :: StagingOutcome -> [Planned] -> OsPath -> OsPath -> ExceptT StagingError IO StagedPrograms
fromStagedRoot outcome planned workerExe root = do
  manifest <- readManifest root
  staged <- readStagedSet root
  let
    binDir = root </> "bin"
    libDir = root </> "lib"
  pure StagedPrograms
    { summary = StagingSummary
        { manifestHash = sha256Bytes manifest
        , root = root
        , programNames = map (.name) planned
        , copiedLibraries = staged.copiedLibraries
        }
    , outcome = outcome
    , directories = StagedDirectories binDir libDir
    , workerExecutable = binDir </> takeFileName workerExe
    , expected = Map.fromList
        [ (pl.name, ExpectedResolution pl.prog.launcherLibraryPath pl.locations)
        | pl <- planned ]
    }

stagedCopies :: Planned -> Resolution
stagedCopies pl = Map.restrictKeys pl.resolution $
  Map.keysSet (Map.filter (== StagedCopy) pl.locations)

-- | Every program and copied library, each once.
sourcesOf :: [Planned] -> [OsPath]
sourcesOf planned = Set.toList $ Set.fromList $
  [ pl.prog.executable | pl <- planned ] <> concatMap (Map.elems . stagedCopies) planned

-- | A key of the sources' state, which reads no file contents: each program's
-- and copied library's path, size, modification time, status change time,
-- device and inode, each program's launcher's library path, and where each
-- library is loaded from. The status change time catches what restores the
-- modification time after an edit in place (@rsync --inplace -t@, @touch -r@).
sourceStateKey :: [Planned] -> ExceptT StagingError IO Sha256
sourceStateKey planned = do
  stats <- forM (sourcesOf planned) $ \src -> ioErrorAt src $ do
    st <- getFileStatus (OsString.toString src)
    pure $ unwords
      [ "source", OsString.toString src, show (fileSize st), show (modificationTimeHiRes st)
      , show (statusChangeTimeHiRes st), show (deviceID st), show (fileID st) ]
  let
    programLines = concat
      [ [ unwords ["location", n, so, show l] | (SoName so, l) <- Map.toList pl.locations ]
        <> [ unwords ["launcher", n, OsString.toString d] | d <- pl.prog.launcherLibraryPath ]
      | pl <- planned, let ProgramName pn = pl.name, let n = OsString.toString pn ]
  pure $ sha256Bytes $ utf8 $ unlines $ "hyperion-scheduler source state v2" : sort (stats <> programLines)

-- | Hexadecimal digits of a hash in the name of a staged folder or key file:
-- 128 bits, enough to tell apart the few sets one job stages.
hashDigitsInNames :: Int
hashDigitsInNames = 32

stagedFolderName :: Sha256 -> OsPath
stagedFolderName = OsString.fromString . take hashDigitsInNames . show

keyFileName :: Sha256 -> OsPath
keyFileName key = OsString.fromString ("by-stat-" <> take hashDigitsInNames (show key))

-- | The staged folder a key file names, if the file holds a folder's name.
readCachedRoot :: OsPath -> OsPath -> IO (Maybe OsPath)
readCachedRoot dir keyPath = try (File.readFile' keyPath) >>= \case
  Left (_ :: IOException) -> pure Nothing
  Right bytes -> do
    let rootName = BS8.unpack (BS8.takeWhile (/= '\n') bytes)
        wellFormed = length rootName == hashDigitsInNames && all isHexDigit rootName
        root = dir </> OsString.fromString rootName
    exists <- if wellFormed then doesDirectoryExist root else pure False
    pure $ if exists then Just root else Nothing

-- | Write a key file naming the staged folder, atomically. It is only a cache:
-- if this fails, the next start hashes the sources again.
writeCachedRoot :: OsPath -> OsPath -> IO ()
writeCachedRoot keyPath root = do
  let rootName = OsString.toString (takeFileName root)
  written <- try $ writeFileAtomic (\path -> writeFile path (rootName <> "\n")) keyPath
  case written of
    Right () -> pure ()
    Left (e :: IOException) -> Log.warn "Could not write the staging key file" (keyPath, displayException e)

-- | Whether a folder that a key file names can be used without reading any
-- source: its manifest is the one its name says, and every staged file still
-- has the size written when staging. A same-size change of a staged file isn't
-- seen here; a change of a source is, through the key.
reusableFromState :: OsPath -> ExceptT StagingError IO Bool
reusableFromState root = do
  hasManifest <- ioErrorAt root $ doesFileExist (root </> manifestName)
  if not hasManifest then pure False else do
    manifest <- readManifest root
    unless (stagedFolderName (sha256Bytes manifest) == takeFileName root) $
      throwError (StagedFileChanged (root </> manifestName))
    staged <- readStagedSet root
    and <$> forM staged.files (\file -> liftIO $ hasSize (root </> file.relativePath) file.size)
  where
    hasSize path size = try (getFileSize path) >>= \case
      Left (_ :: IOException) -> pure False
      Right actual            -> pure (actual == size)

-- | Run the loader on each staged program, with its launcher's folders and the
-- given library path (the task environment's @LD_LIBRARY_PATH@, which starts
-- with the staged libraries' folder), and compare with the expected locations.
checkStaged :: StagedPrograms -> [OsPath] -> IO (Either StagingError ())
checkStaged staged libraryPath = runExceptT $ do
  let libDir = staged.directories.libraryDirectory
  canonicalLibDir <- canonical libDir
  forM_ (Map.toList staged.expected) $ \(n@(ProgramName file), wanted) -> do
    let binary = staged.directories.binDirectory </> file
    actual <- resolve n binary (wanted.launcherLibraryPath <> libraryPath) >>= traverse canonical
    locations <- traverse canonicalLocation wanted.locations
    case differences canonicalLibDir locations actual of
      []    -> pure ()
      diffs -> throwError (ResolutionChanged n diffs)
  where
    canonical path = ioErrorAt path $ canonicalizePath path
    canonicalLocation = \case
      StagedCopy -> pure StagedCopy
      SameFile p -> SameFile <$> canonical p

-- | That a program finds all its libraries with the given library path,
-- wherever they are. For an explicit task environment without staging, which
-- can hide libraries that a worker found through the inherited path.
checkProgramLoads :: OsPath -> [OsPath] -> IO (Either StagingError ())
checkProgramLoads exe libraryPath = runExceptT $
  void $ resolve (ProgramName (takeFileName exe)) exe libraryPath

duplicates :: Ord a => [a] -> [a]
duplicates xs = Map.keys $ Map.filter (> (1 :: Int)) $ Map.fromListWith (+) [ (x, 1) | x <- xs ]

-- | The program's libraries as the loader resolves them with the given
-- @LD_LIBRARY_PATH@, without running the program: the loader named in its
-- @PT_INTERP@ is run with @--list@ and an otherwise empty environment (a
-- loader without that option refuses it). A program without @PT_INTERP@ is
-- static: no libraries.
resolve :: ProgramName -> OsPath -> [OsPath] -> ExceptT StagingError IO Resolution
resolve n exe libraryPath = do
  info <- elfInfo exe
  case info >>= (.interpreter) of
    Nothing -> pure Map.empty
    Just interp -> do
      let
        env = [ (envKey libraryPathVar, renderSearchPath libraryPath) | not (null libraryPath) ]
        cp = (proc (BS8.unpack interp) ["--list", OsString.toString exe]) { env = Just env }
      (code, out, err) <- ioErrorAt exe $ readCreateProcessWithExitCode cp ""
      case code of
        ExitSuccess   -> either (throwError . LibrariesThroughRelativePaths n) pure (parseLoaderTrace out)
        ExitFailure c -> throwError (LoaderFailed n c err)

-- | The ELF information of a file; 'Nothing' if it isn't ELF (a script,
-- which is copied but has no libraries).
elfInfo :: OsPath -> ExceptT StagingError IO (Maybe ElfInfo)
elfInfo path = liftIO (readElfInfo path) >>= \case
  Right info  -> pure (Just info)
  Left NotElf -> pure Nothing
  Left err    -> throwError (ElfFailed path err)

sha256File :: OsPath -> ExceptT StagingError IO Sha256
sha256File path = ioErrorAt path $ do
  bytes <- File.readFile path
  pure $! sha256Bytes bytes

utf8 :: String -> BL.ByteString
utf8 = TLE.encodeUtf8 . TL.pack

-- | One line per program and library, sorted: the identity of a staged set,
-- and a record of it for people. Its hash names the set's folder.
renderManifest :: [Planned] -> Map OsPath Sha256 -> Map SoName LibrarySource -> BL.ByteString
renderManifest planned hashes copies = utf8 $ unlines $ "hyperion-scheduler staged programs v2" : sort (concat
  [ [ unwords ["program", showPath n, show h, showPath pl.prog.executable]
    | pl <- planned, let ProgramName n = pl.name
    , Just h <- [Map.lookup pl.prog.executable hashes] ]
  , [ unwords ["library", so, show lib.hash, showPath lib.source]
    | (SoName so, lib) <- Map.toList copies ]
  , [ unwords ["keeps", showPath n, so, showPath p]
    | pl <- planned, let ProgramName n = pl.name
    , (SoName so, SameFile p) <- Map.toList pl.locations ]
  , [ unwords ["launcher", showPath n, showPath d]
    | pl <- planned, let ProgramName n = pl.name, d <- pl.prog.launcherLibraryPath ]
  ])
  where showPath = OsString.toString

-- | A file to put in a staged folder.
data ToStage = ToStage
  { source   :: OsPath
  , hash     :: Sha256      -- ^ of the source, as the manifest says
  , relative :: OsPath      -- ^ in the staged folder
  }

-- | A file in a staged folder.
data StagedFile = StagedFile
  { relativePath :: OsPath
  , hash         :: Sha256
  , size         :: Integer
  }
  deriving stock (Generic)
  deriving anyclass (Binary)

-- | What a staged folder holds, written when staging.
data StagedSet = StagedSet
  { files           :: [StagedFile]
  , copiedLibraries :: Int
  }
  deriving stock (Generic)
  deriving anyclass (Binary)

stagedSetName, manifestName :: OsPath
stagedSetName = "STAGED"
manifestName = "MANIFEST"

readManifest :: OsPath -> ExceptT StagingError IO BL.ByteString
readManifest root = ioErrorAt path $ BL.fromStrict <$> File.readFile' path
  where path = root </> manifestName

readStagedSet :: OsPath -> ExceptT StagingError IO StagedSet
readStagedSet root = do
  let path = root </> stagedSetName
  bytes <- ioErrorAt path $ BL.fromStrict <$> File.readFile' path
  case decodeOrFail bytes of
    Right (rest, _, staged) | BL.null rest -> pure staged
    _ -> throwError (StagedFileChanged path)

-- | Check a folder against the manifest that names it, and every staged file
-- against the sha256 written when staging.
verifyStagedRoot :: OsPath -> BL.ByteString -> ExceptT StagingError IO ()
verifyStagedRoot root manifest = do
  onDisk <- readManifest root
  unless (onDisk == manifest) $ throwError (StagedFileChanged (root </> manifestName))
  staged <- readStagedSet root
  forM_ staged.files $ \file -> do
    let path = root </> file.relativePath
    actual <- sha256File path
    unless (actual == file.hash) $ throwError (StagedFileChanged path)

-- | Copy the files into a temporary folder next to @root@, remove their
-- @RPATH@s, write the manifest and what was staged, and rename the folder to
-- @root@. If another process staged @root@ meanwhile, check and use its. The
-- temporary folder doesn't outlive a failure.
build :: OsPath -> OsPath -> BL.ByteString -> Int -> [ToStage] -> ExceptT StagingError IO StagingOutcome
build parent root manifest copied sources = do
  tmp <- liftIO $ do
    pid <- getProcessID
    t <- getMonotonicTimeNSec
    pure $ parent </> OsString.fromString (".staging-" <> show pid <> "-" <> show t)
  let
    fill = do
      ioErrorAt tmp $ do
        createDirectoryIfMissing True (tmp </> "bin")
        createDirectoryIfMissing True (tmp </> "lib")
      staged <- forM sources (stageFile tmp)
      ioErrorAt tmp $ do
        File.writeFile (tmp </> manifestName) manifest
        File.writeFile (tmp </> stagedSetName) (encode (StagedSet staged copied))
      renamed <- liftIO $ try (renameDirectory tmp root)
      case renamed of
        Right () -> pure Built
        Left (e :: IOException) -> do
          won <- ioErrorAt root $ doesDirectoryExist root
          unless won $ throwError (StagingIOError root (displayException e))
          ioErrorAt tmp $ removePathForcibly tmp
          ReusedByHash <$ verifyStagedRoot root manifest
    -- Failing to clean up must not hide why staging failed.
    cleanUp = void (try (removePathForcibly tmp) :: IO (Either IOException ()))
    removeOnFailure act = do
      result <- act `onException` cleanUp
      when (isLeft result) cleanUp
      pure result
  ExceptT $ removeOnFailure (runExceptT fill)

-- | Copy a source, checking that the bytes copied are those the manifest
-- says, and remove the copy's @RPATH@ if it's ELF.
stageFile :: OsPath -> ToStage -> ExceptT StagingError IO StagedFile
stageFile tmp file = do
  let dst = tmp </> file.relative
  copiedHash <- ioErrorAt file.source $ copyHashing file.source dst
  unless (copiedHash == file.hash) $ throwError (SourceChanged file.source)
  isElf <- isJust <$> elfInfo dst
  when isElf $ void $ withExceptT (ElfFailed file.source) $ ExceptT $ removeRunPaths dst
  h <- sha256File dst
  size <- ioErrorAt dst $ getFileSize dst
  pure StagedFile { relativePath = file.relative, hash = h, size = size }

-- | Bytes read at a time when copying.
copyChunkBytes :: Int
copyChunkBytes = 1024 * 1024

-- | Copy a file with its permissions, writable by its owner, in one read, and
-- return the sha256 of the bytes read.
copyHashing :: OsPath -> OsPath -> IO Sha256
copyHashing src dst = do
  digest <- File.withBinaryFile src ReadMode $ \from ->
    File.withBinaryFile dst WriteMode $ \to -> copyChunks from to hashInit
  copyPermissions src dst
  perms <- getPermissions dst
  setPermissions dst (setOwnerWritable True perms)
  pure (Sha256 digest)
  where
    copyChunks :: Handle -> Handle -> Context SHA256 -> IO (Digest SHA256)
    copyChunks from to !context = do
      chunk <- BS.hGetSome from copyChunkBytes
      if BS.null chunk
        then pure $! hashFinalize context
        else BS.hPut to chunk >> copyChunks from to (hashUpdate context chunk)

-- | An operation on a path; the system's errors become values that name it.
ioErrorAt :: OsPath -> IO a -> ExceptT StagingError IO a
ioErrorAt path act = ExceptT $ try act >>= \case
  Left (e :: IOException) -> pure (Left (StagingIOError path (displayException e)))
  Right x -> pure (Right x)
