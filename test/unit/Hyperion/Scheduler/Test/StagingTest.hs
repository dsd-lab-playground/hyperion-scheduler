{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE OverloadedRecordDot   #-}
{-# LANGUAGE OverloadedStrings     #-}

-- | Staging: the pure parts (the loader's output, which libraries are
-- copied, conflicts, differences), then staging this test's own executable
-- with one of its libraries moved to a temporary "shared" folder, the check
-- against the loader, the three ways of coming to a staged set, and what must
-- fail. Uses a temporary directory, and needs an executable that loads one of
-- 'movable' (it says so if it doesn't, and exits with an error).
module Hyperion.Scheduler.Test.StagingTest (runTest) where

import Control.Concurrent                        (forkIO, newEmptyMVar, putMVar,
                                                  takeMVar, threadDelay)
import Control.Exception                         (AssertionFailed (..), throwIO)
import Control.Monad                             (forM, forM_, unless)
import Control.Monad.Except                      (runExceptT)
import Data.ByteString                           qualified as BS
import Data.ByteString.Char8                     qualified as BS8
import Data.Either                               (isRight)
import Data.List                                 (isInfixOf, isPrefixOf)
import Data.List.NonEmpty                        (NonEmpty (..))
import Data.Map.Strict                           qualified as Map
import Data.Time.Clock                           (getCurrentTime)
import Hyperion.OsPath                           (OsPath, takeFileName, (</>))
import Hyperion.OsString                         (fromString, toString)
import Hyperion.Scheduler.Test.ElfTest           (dynamicBeyondEnd)
import Hyperion.Scheduler.TaskLaunch
import Hyperion.Util                             (myExecutable)
import System.Directory                          (copyFile, createDirectory,
                                                  createDirectoryLink,
                                                  getModificationTime,
                                                  getPermissions,
                                                  getTemporaryDirectory,
                                                  listDirectory,
                                                  removeDirectoryRecursive,
                                                  removeFile, setModificationTime,
                                                  setOwnerExecutable,
                                                  setPermissions)
import System.Environment                        (setEnv, unsetEnv)
import System.Posix.Process                      (getProcessID)

expect :: String -> Bool -> IO ()
expect label cond = do
  unless cond $ throwIO $ AssertionFailed $ "FAILED: " <> label
  putStrLn $ "ok: " <> label

failWith :: String -> IO a
failWith = throwIO . AssertionFailed . ("FAILED: " <>)

trace :: String
trace = unlines
  [ "\tlinux-vdso.so.1 (0x00007ffd2b9f1000)"
  , "\tlibgmp.so.10 => /cm/shared/apps/gmp/lib/libgmp.so.10 (0x00007f1c2a000000)"
  , "\tlibm.so.6 => /lib64/libm.so.6 (0x00007f1c29c00000)"
  , "\t/lib64/ld-linux-x86-64.so.2 (0x00007f1c2a400000)"
  ]

hashOf :: BS.ByteString -> Sha256
hashOf = sha256Bytes . BS.fromStrict

pureTests :: IO ()
pureTests = do
  let
    loader = "/lib64/ld-linux-x86-64.so.2"
    resolution = Map.fromList
      [ (SoName "libgmp.so.10", "/cm/shared/apps/gmp/lib/libgmp.so.10")
      , (SoName "libm.so.6", "/lib64/libm.so.6")
      , (SoName loader, fromString loader) ]
    native = MkProgram "/lustre/bin/blocks_3d" ["/lustre/gmp630/lib"]
    a = ProgramName "a"
    b = ProgramName "b"
    h = hashOf "h"
    g = hashOf "g"
  expect "the loader's output parses, without the vDSO and with the loader by its path" $
    parseLoaderTrace trace == Right resolution
  expect "a library found through a relative path is reported, not dropped" $
    parseLoaderTrace "\tlibfoo.so.1 => lib/libfoo.so.1 (0x1)\n\tlibm.so.6 => /lib64/libm.so.6 (0x1)\n"
      == Left [SoName "libfoo.so.1"]
  expect "so is one linked by a relative path" $
    parseLoaderTrace "\tlib/libfoo.so.1 (0x1)\n" == Left [SoName "lib/libfoo.so.1"]
  expect "a line that the loader's other mode prints is reported rather than taken for a path" $
    parseLoaderTrace "\tlibfoo.so.1 => not found\n" == Left [SoName "libfoo.so.1"]
  expect "a library linked by path, which has no arrow, is kept" $
    parseLoaderTrace "\t/net/lib/libx.so (0x00007f1c2a000000)\n"
      == Right (Map.singleton (SoName "/net/lib/libx.so") "/net/lib/libx.so")
  expect "old glibc's vDSO line names no file" $
    parseLoaderTrace "\tlinux-vdso.so.1 =>  (0x00007ffd2b9f1000)\n\tlibm.so.6 => /lib64/libm.so.6 (0x1)\n"
      == Right (Map.singleton (SoName "libm.so.6") "/lib64/libm.so.6")
  expect "a path with spaces and a parenthesis parses" $
    parseLoaderTrace "\tlibx.so => /a b (0x1)/libx.so (0x00007f1c2a000000)\n"
      == Right (Map.singleton (SoName "libx.so") "/a b (0x1)/libx.so")
  expect "system libraries and libraries linked by path stay, others are copied" $
    classify defaultSystemLibraryDirs (program "/x") resolution == Map.fromList
      [ (SoName "libgmp.so.10", StagedCopy)
      , (SoName "libm.so.6", SameFile "/lib64/libm.so.6")
      , (SoName loader, SameFile (fromString loader)) ]
  expect "a library linked by path outside the system folders stays" $
    classify defaultSystemLibraryDirs (program "/x")
      (Map.singleton (SoName "/net/lib/libx.so") "/net/lib/libx.so")
      == Map.singleton (SoName "/net/lib/libx.so") (SameFile "/net/lib/libx.so")
  expect "libraries in a program's launcher's folders stay" $
    classify defaultSystemLibraryDirs native
      (Map.singleton (SoName "libgmp.so.10") "/lustre/gmp630/lib/libgmp.so.10")
      == Map.singleton (SoName "libgmp.so.10") (SameFile "/lustre/gmp630/lib/libgmp.so.10")
  expect "isUnder goes by path components" $
    isUnder "/lib" "/lib/x.so" && not (isUnder "/lib" "/lib64/x.so")
  let
    useA = LibraryUse a "/p/l" h
    useB = LibraryUse b "/q/l" h
    useB' = LibraryUse b "/q/l" g
  expect "the same library for two programs is one copy" $
    libraryCopies (Map.singleton (SoName "l") (useA :| [useB]))
      == Right (Map.singleton (SoName "l") (LibrarySource "/p/l" h))
  expect "two different files under one name are a conflict" $
    libraryCopies (Map.singleton (SoName "l") (useA :| [useB']))
      == Left [LibraryConflict (SoName "l") [useA, useB']]
  let expected = Map.fromList [(SoName "l", StagedCopy), (SoName "m", SameFile "/lib64/m")]
  expect "no differences when the copy and the system file are loaded" $
    null (differences "/s/lib" expected (Map.fromList [(SoName "l", "/s/lib/l"), (SoName "m", "/lib64/m")]))
  expect "a library loaded from elsewhere is a difference" $
    differences "/s/lib" expected (Map.fromList [(SoName "l", "/cm/l"), (SoName "m", "/lib64/m")])
      == [Moved (SoName "l") StagedCopy "/cm/l"]
  expect "missing and unexpected libraries are differences" $
    differences "/s/lib" expected (Map.fromList [(SoName "l", "/s/lib/l"), (SoName "n", "/lib64/n")])
      == [Missing (SoName "m") (SameFile "/lib64/m"), Unexpected (SoName "n") "/lib64/n"]

configurationTests :: IO ()
configurationTests = do
  let
    staged s = defaultTaskLaunch { executables = NodeLocalExecutables s }
    base = defaultStaging "/scratch/programs"
    withEnvironment env = defaultTaskLaunch { environment = SetEnvironment env }
  expect "the default task launch is valid" $ null (validateTaskLaunch defaultTaskLaunch)
  expect "staging in a relative folder is invalid" $
    validateTaskLaunch (staged (defaultStaging "programs")) == [InvalidStagingDirectory "programs"]
  expect "a staging folder with ':' would split PATH, and is invalid" $
    validateTaskLaunch (staged (defaultStaging "/scratch/job:1")) == [InvalidStagingDirectory "/scratch/job:1"]
  expect "a negative wait is invalid" $
    validateTaskLaunch defaultTaskLaunch { startupWaits = MkStartupWaits (Seconds (-1)) (Seconds 5) }
      == [InvalidStartupWait (Seconds (-1))]
  expect "a relative program is invalid" $
    validateTaskLaunch (staged base { programs = [program "bin/x"] }) == [RelativeProgram "bin/x"]
  expect "a launcher's folder with ':' would change the loader's path, and is invalid" $
    validateTaskLaunch (staged base { programs = [MkProgram "/x" ["/a:/b"]] }) == [InvalidLibraryFolder "/a:/b"]
  expect "a reference library path is checked too" $
    validateTaskLaunch (staged base { referenceLibraryPath = LibraryPath ["rel"] }) == [InvalidLibraryFolder "rel"]
  expect "a timeout below a second is invalid" $
    validateTaskLaunch (staged base { startupTimeout = Seconds 0 }) == [InvalidStartupTimeout (Seconds 0)]
  expect "an environment's errors are reported" $
    validateTaskLaunch (withEnvironment defaultTaskEnvironment { variables = Map.singleton "PATH" "/x" })
      == [InvalidEnvironment [ReservedName "PATH"]]
  expect "the staged directory needs staging, whichever module checks it" $
    validateTaskLaunch (withEnvironment defaultTaskEnvironment { variables = Map.singleton "N" StagedProgramsDirectory })
      == [InvalidEnvironment [StagedDirectoryWithoutStaging "N"]]
  expect "only a task launch that stages has a start-up timeout" $
    startupTimeoutOf defaultTaskLaunch == Nothing
    && startupTimeoutOf (staged base) == Just defaultStartupTimeout
  setEnv "LD_LIBRARY_PATH" "/m/a::/m/b"
  master <- concreteTaskLaunch (staged base)
  given <- concreteTaskLaunch (staged base { referenceLibraryPath = LibraryPath ["/given"] })
  shared <- concreteTaskLaunch defaultTaskLaunch
  unsetEnv "LD_LIBRARY_PATH"
  expect "the master's library path is read once, without empty entries" $ master.referencePath == ["/m/a", "/m/b"]
  expect "a given reference library path is used as it is" $ given.referencePath == ["/given"]
  expect "without staging no reference library path is needed" $ null shared.referencePath

-- | Libraries this test may move to the "shared" folder: none is part of
-- glibc, so loading a copy with the system's loader is safe.
movable :: [SoName]
movable = map SoName ["libgmp.so.10", "libz.so.1", "libffi.so.8", "libffi.so.7", "libnuma.so.1"]

stagedOk :: String -> Either StagingError a -> IO a
stagedOk what = \case
  Left err -> failWith $ what <> ": " <> show err
  Right x  -> pure x

testName :: ProgramName
testName = ProgramName "hyperion-scheduler-unit-test"

-- | Every occurrence of a string replaced by another of the same length.
replaceBytes :: BS.ByteString -> BS.ByteString -> BS.ByteString -> BS.ByteString
replaceBytes from to bytes = case BS.breakSubstring from bytes of
  (before, rest)
    | BS.null rest -> before
    | otherwise -> before <> to <> replaceBytes from to (BS.drop (BS.length from) rest)

keyFiles :: OsPath -> IO [FilePath]
keyFiles dir = filter ("by-stat-" `isPrefixOf`) <$> listDirectory (toString dir)

temporaryFolders :: OsPath -> IO [FilePath]
temporaryFolders dir = filter (".staging-" `isPrefixOf`) <$> listDirectory (toString dir)

runTest :: IO ()
runTest = do
  pureTests
  configurationTests
  tmp <- getTemporaryDirectory
  pid <- getProcessID
  exe <- myExecutable
  let dir = tmp <> "/hyperion-scheduler-staging-test-" <> show pid
      osDir = fromString dir
      shared = osDir </> "shared"
      stage = osDir </> "stage"
  createDirectory dir
  createDirectory (toString shared)
  -- What this executable loads where it is, whatever the machine.
  own <- stagedOk "resolving the test executable" =<< runExceptT (resolve testName exe [])
  (so@(SoName name), path) <- case [ (n, p) | n <- movable, Just p <- [Map.lookup n own] ] of
    candidate : _ -> pure candidate
    [] -> failWith $ "the test executable loads none of the movable libraries: " <> show own
  copyFile (toString path) (toString (shared </> fromString name))
  let
    staging = (defaultStaging stage) { programs = [program "/bin/sh"] }
    sharedCopy = shared </> fromString name
  staged <- stagedOk "staging with a shared library" =<< stagePrograms staging [shared] exe
  let libDir = staged.directories.libraryDirectory
      root = staged.summary.root
  expect "a first staging builds" $ staged.outcome == Built
  expect "a library from a shared folder is copied" $ staged.summary.copiedLibraries == 1
  expect "the copy is expected to be loaded" $
    (Map.lookup so . (.locations) =<< Map.lookup testName staged.expected) == Just StagedCopy
  ok <- checkStaged staged [libDir]
  expect "the staged executable loads the copy" $ isRight ok
  wrong <- checkStaged staged []
  expect "without the staged folder it would load another file" $ case wrong of
    Left (ResolutionChanged _ [Moved so' StagedCopy _]) -> so' == so
    _ -> False
  temporaries <- temporaryFolders stage
  expect "staging leaves no temporary folder" $ null temporaries

  -- The three ways of coming to a staged set.
  again <- stagedOk "staging again" =<< stagePrograms staging [shared] exe
  expect "staging again reuses the folder, reading no source" $
    again.outcome == ReusedByStat && again.summary == staged.summary
  keys <- keyFiles stage
  expect "a key file names the folder staged from these sources" $ length keys == 1
  now <- getCurrentTime
  setModificationTime (toString sharedCopy) now
  touched <- stagedOk "staging after a touch" =<< stagePrograms staging [shared] exe
  keys' <- keyFiles stage
  expect "a touched source is hashed again and gives the same folder" $
    touched.outcome == ReusedByHash && touched.summary == staged.summary && length keys' == 2

  -- What reuse can and can't see: a same-size change of a staged file passes
  -- with the key (no contents are read), and is an error once the key is gone.
  let stagedCopy = toString (libDir </> fromString name)
  original <- BS.readFile stagedCopy
  BS.writeFile stagedCopy (BS.map (+ 1) (BS.take 16 original) <> BS.drop 16 original)
  sameSize <- stagePrograms staging [shared] exe
  expect "with the key, a same-size change of a staged file isn't seen: documented" $ isRight sameSize
  forM_ keys' $ \key -> removeFile (toString (stage </> fromString key))
  noKey <- stagePrograms staging [shared] exe
  expect "without the key, the same change is an error" $ case noKey of
    Left (StagedFileChanged p) -> takeFileName p == fromString name
    _ -> False
  BS.writeFile stagedCopy original
  restored <- stagedOk "staging with the file restored" =<< stagePrograms staging [shared] exe
  expect "with the file restored, the hashes pass and a key is written" $ restored.outcome == ReusedByHash
  BS.appendFile stagedCopy "x"
  longer <- stagePrograms staging [shared] exe
  expect "with the key, a staged file of another size is hashed, and is an error" $ case longer of
    Left (StagedFileChanged _) -> True
    _ -> False
  BS.writeFile stagedCopy original
  let manifestPath = toString (root </> "MANIFEST")
  manifest <- BS.readFile manifestPath
  BS.writeFile manifestPath (manifest <> "changed\n")
  foreignManifest <- stagePrograms staging [shared] exe
  expect "with the key, a manifest that isn't the one the folder's name says is an error" $ case foreignManifest of
    Left (StagedFileChanged p) -> takeFileName p == "MANIFEST"
    _ -> False
  keysNow <- keyFiles stage
  forM_ keysNow $ \key -> removeFile (toString (stage </> fromString key))
  foreignNoKey <- stagePrograms staging [shared] exe
  expect "without the key, so is a manifest other than the computed one" $ case foreignNoKey of
    Left (StagedFileChanged p) -> takeFileName p == "MANIFEST"
    _ -> False
  BS.writeFile manifestPath manifest
  let stagedSetPath = toString (root </> "STAGED")
  stagedSet <- BS.readFile stagedSetPath
  BS.writeFile stagedSetPath (BS8.pack "not what was written")
  corrupt <- stagePrograms staging [shared] exe
  expect "a corrupt list of staged files is an error, not an exception" $ case corrupt of
    Left (StagedFileChanged p) -> takeFileName p == "STAGED"
    _ -> False
  BS.writeFile stagedSetPath stagedSet

  -- An edit in place that keeps the size and restores the modification time
  -- (rsync --inplace -t) must not be taken for the old source. A file of its
  -- own, since it ends up with other contents.
  let
    inPlaceDir = osDir </> "in-place"
    inPlaceSource = inPlaceDir </> "script"
    stagingI = (defaultStaging (osDir </> "stage-in-place")) { programs = [program inPlaceSource] }
  createDirectory (toString inPlaceDir)
  BS.writeFile (toString inPlaceSource) "#!/bin/sh\necho one\n"
  before <- stagedOk "staging a script" =<< stagePrograms stagingI [shared] exe
  modified <- getModificationTime (toString inPlaceSource)
  threadDelay 20000
  BS.writeFile (toString inPlaceSource) "#!/bin/sh\necho two\n"
  setModificationTime (toString inPlaceSource) modified
  after <- stagedOk "staging the edited script" =<< stagePrograms stagingI [shared] exe
  expect "an edit in place with the modification time restored is seen" $
    after.outcome == Built && after.summary.manifestHash /= before.summary.manifestHash
  scriptCopy <- BS.readFile (toString (after.directories.binDirectory </> "script"))
  expect "and the new contents are staged" $ scriptCopy == "#!/bin/sh\necho two\n"

  -- A launcher's folder changes the key, and the manifest.
  let withLauncher = (defaultStaging stage)
        { programs = [MkProgram "/bin/sh" ["/nonexistent/launcher"]] }
  launcher <- stagedOk "staging with a launcher's folder" =<< stagePrograms withLauncher [shared] exe
  expect "a launcher's folder is part of a staged set's identity" $
    launcher.summary.manifestHash /= staged.summary.manifestHash && launcher.outcome == Built
  let withOtherLauncher = (defaultStaging stage)
        { programs = [MkProgram "/bin/sh" ["/nonexistent/other"]] }
  otherLauncher <- stagedOk "staging with another launcher's folder" =<< stagePrograms withOtherLauncher [shared] exe
  expect "and which folder it is, too" $
    otherLauncher.summary.manifestHash /= launcher.summary.manifestHash && otherLauncher.outcome == Built

  -- Two supervisors at once on one node.
  let concurrent = osDir </> "concurrent"
      stagingC = (defaultStaging concurrent) { programs = [program "/bin/sh"] }
  results <- do
    vars <- forM [1 :: Int .. 4] $ \_ -> do
      var <- newEmptyMVar
      _ <- forkIO $ stagePrograms stagingC [shared] exe >>= putMVar var
      pure var
    mapM takeMVar vars
  summaries <- forM results $ \r -> (.summary) <$> stagedOk "staging at once" r
  expect "staging at once in one folder gives every caller the same set" $
    all (== staged.summary.manifestHash) (map (.manifestHash) summaries)
  temporaries' <- temporaryFolders concurrent
  expect "staging at once leaves no temporary folder" $ null temporaries'

  -- Two names of one file are one file: the reference resolves through a link,
  -- and the staged program through the folder it points to.
  let
    realDir = osDir </> "real"
    linkDir = osDir </> "link"
    named = osDir </> "named"
    stagingN = (defaultStaging named) { systemLibraryDirs = defaultSystemLibraryDirs <> [linkDir, realDir] }
  createDirectory (toString realDir)
  copyFile (toString path) (toString (realDir </> fromString name))
  createDirectoryLink (toString realDir) (toString linkDir)
  viaLink <- stagedOk "staging through a link" =<< stagePrograms stagingN [linkDir] exe
  throughFolder <- checkStaged viaLink [realDir]
  expect "a library reached by another name of its folder is the same file" $ isRight throughFolder

  -- Failures.
  duplicate <- stagePrograms (defaultStaging (osDir </> "stage2")) { programs = [program exe] } [] exe
  expect "two programs with the same name are an error" $ case duplicate of
    Left (DuplicateProgramName _) -> True
    _ -> False
  missing <- stagePrograms (defaultStaging (osDir </> "stage3")) { programs = [program "/nonexistent/x"] } [] exe
  expect "a program that doesn't exist is an error" $ case missing of
    Left (NotFound "/nonexistent/x") -> True
    _ -> False
  let
    failing = osDir </> "failing"
    broken = osDir </> "broken"
  BS.writeFile (toString broken) dynamicBeyondEnd
  perms <- getPermissions (toString broken)
  setPermissions (toString broken) (setOwnerExecutable True perms)
  failed <- stagePrograms (defaultStaging failing) { programs = [program broken] } [] exe
  expect "a copy that can't be edited is an error" $ case failed of
    Left (ElfFailed p _) -> p == broken
    _ -> False
  temporaries'' <- temporaryFolders failing
  expect "a failure leaves no temporary folder" $ null temporaries''

  -- A node's preparation, for each policy.
  let
    defaultNode = NodeTaskLaunch defaultTaskLaunch []
    explicit = defaultTaskLaunch { environment = SetEnvironment defaultTaskEnvironment }
    bothNode = NodeTaskLaunch explicit { executables = NodeLocalExecutables (defaultStaging (osDir </> "both")) } [shared]
    inheritNode = NodeTaskLaunch defaultTaskLaunch { executables = NodeLocalExecutables (defaultStaging (osDir </> "inherit")) } [shared]
  plain <- stagedOk "the default" =<< prepareNodeLaunch defaultNode "/nonexistent/worker"
  expect "the default touches nothing: the executable, the environment, no report" $
    plain.workerProgram == "/nonexistent/worker" && plain.workerEnvironment == Nothing
    && plain.stagingReport == Nothing
  environmentOnly <- stagedOk "an explicit environment" =<< prepareNodeLaunch (NodeTaskLaunch explicit []) exe
  expect "an explicit environment alone: the same executable, the environment set" $
    environmentOnly.workerProgram == exe
    && (lookup "OMP_NUM_THREADS" =<< environmentOnly.workerEnvironment) == Just "1"
    && environmentOnly.stagingReport == Nothing
  missingWorker <- prepareNodeLaunch (NodeTaskLaunch explicit []) "/nonexistent/worker"
  expect "an explicit environment checks that the worker can be read" $ case missingWorker of
    Left (ElfFailed _ _) -> True
    _ -> False

  -- A program that finds a library only through the library path: a copy of
  -- this executable that needs the library under a name found nowhere else.
  let
    privateName = BS8.pack (map (const 'q') (takeWhile (/= '.') name) <> dropWhile (/= '.') name)
    needsPrivate = osDir </> "needs-private"
    privateFolder = osDir </> "private"
  image <- BS.readFile (toString exe)
  BS.writeFile (toString needsPrivate) (replaceBytes (BS8.pack name) privateName image)
  execPerms <- getPermissions (toString needsPrivate)
  setPermissions (toString needsPrivate) (setOwnerExecutable True execPerms)
  createDirectory (toString privateFolder)
  copyFile (toString path) (toString privateFolder <> "/" <> BS8.unpack privateName)
  withFolder <- checkProgramLoads needsPrivate [privateFolder]
  expect "a program finds a library through the library path it is given" $ isRight withFolder
  withoutFolder <- checkProgramLoads needsPrivate []
  expect "and without it the loader's failure is an error, naming the library" $ case withoutFolder of
    Left (LoaderFailed _ _ stderrText) -> BS8.unpack privateName `isInfixOf` stderrText
    _ -> False
  hidden <- prepareNodeLaunch (NodeTaskLaunch explicit []) needsPrivate
  expect "so an explicit environment that hides a worker's library stops the run at start" $ case hidden of
    Left (LoaderFailed _ _ _) -> True
    _ -> False
  kept <- prepareNodeLaunch (NodeTaskLaunch defaultTaskLaunch { environment = SetEnvironment defaultTaskEnvironment { libraryPath = [privateFolder] } } []) needsPrivate
  expect "while one that keeps it passes" $ isRight kept
  both <- stagedOk "staging and an environment" =<< prepareNodeLaunch bothNode exe
  expect "staging and an environment: the staged worker, staged folders first, a report" $
    (isUnder (osDir </> "both") both.workerProgram)
    && (take 1 . parseSearchPath <$> (lookup "LD_LIBRARY_PATH" =<< both.workerEnvironment))
       == Just [osDir </> "both" </> stagedName both </> "lib"]
    && (fmap (.outcome) both.stagingReport == Just Built)
    && (maybe False ((>= 0) . (.seconds)) both.stagingReport)
  inherited <- stagedOk "staging alone" =<< prepareNodeLaunch inheritNode exe
  expect "staging alone: the supervisor's environment with staged folders first" $
    (take 1 . parseSearchPath <$> (lookup "PATH" =<< inherited.workerEnvironment))
      == Just [osDir </> "inherit" </> stagedName inherited </> "bin"]

  removeDirectoryRecursive dir
  putStrLn "All Staging tests passed."
  where
    stagedName launch = maybe "" (takeFileName . (.root) . (.summary)) launch.stagingReport
