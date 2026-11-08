{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings   #-}

-- | Staging: the pure parts (the loader's output, which libraries are
-- copied, conflicts, differences), then staging this test's own executable
-- with one of its libraries moved to a temporary "shared" folder, the check
-- against the loader, and a tampered copy. Uses a temporary directory.
module Hyperion.Scheduler.Test.StagingTest (runTest) where

import Control.Exception                     (AssertionFailed (..), throwIO)
import Control.Monad                         (unless)
import Data.ByteString                       qualified as BS
import Data.Map.Strict                       qualified as Map
import Hyperion.OsPath                       ((</>))
import Hyperion.OsString                     (fromString, toString)
import Hyperion.Scheduler.TaskLaunch.Environment (StagedDirectories (..))
import Hyperion.Scheduler.TaskLaunch.Staging
import Hyperion.Util                         (myExecutable)
import System.Directory                      (copyFile, createDirectory,
                                              getTemporaryDirectory,
                                              removeDirectoryRecursive)
import System.Posix.Process                  (getProcessID)

expect :: String -> Bool -> IO ()
expect label cond = do
  unless cond $ throwIO $ AssertionFailed $ "FAILED: " <> label
  putStrLn $ "ok: " <> label

trace :: String
trace = unlines
  [ "\tlinux-vdso.so.1 (0x00007ffd2b9f1000)"
  , "\tlibgmp.so.10 => /cm/shared/apps/gmp/lib/libgmp.so.10 (0x00007f1c2a000000)"
  , "\tlibm.so.6 => /lib64/libm.so.6 (0x00007f1c29c00000)"
  , "\t/lib64/ld-linux-x86-64.so.2 (0x00007f1c2a400000)"
  ]

pureTests :: IO ()
pureTests = do
  let
    resolution = Map.fromList
      [ (SoName "libgmp.so.10", "/cm/shared/apps/gmp/lib/libgmp.so.10")
      , (SoName "libm.so.6", "/lib64/libm.so.6") ]
    native = MkProgram "/lustre/bin/blocks_3d" ["/lustre/gmp630/lib"]
    a = ProgramName "a"
    b = ProgramName "b"
  expect "the loader's output parses, without the vDSO and the loader" $
    parseLoaderTrace trace == Right resolution
  expect "libraries not found are reported" $
    parseLoaderTrace "\tlibfoo.so.1 => not found\n\tlibm.so.6 => /lib64/libm.so.6 (0x1)\n"
      == Left [SoName "libfoo.so.1"]
  expect "system libraries stay, others are copied" $
    classify defaultSystemLibraryDirs (program "/x") resolution == Map.fromList
      [ (SoName "libgmp.so.10", StagedCopy)
      , (SoName "libm.so.6", SameFile "/lib64/libm.so.6") ]
  expect "libraries in a program's prefix stay" $
    classify defaultSystemLibraryDirs native
      (Map.singleton (SoName "libgmp.so.10") "/lustre/gmp630/lib/libgmp.so.10")
      == Map.singleton (SoName "libgmp.so.10") (SameFile "/lustre/gmp630/lib/libgmp.so.10")
  expect "isUnder goes by path components" $
    isUnder "/lib" "/lib/x.so" && not (isUnder "/lib" "/lib64/x.so")
  expect "the same library for two programs is one copy" $
    libraryCopies (Map.singleton (SoName "l") [(a, "/p/l", Sha256 "h"), (b, "/q/l", Sha256 "h")])
      == Right (Map.singleton (SoName "l") ("/p/l", Sha256 "h"))
  expect "two different files under one name are a conflict" $
    libraryCopies (Map.singleton (SoName "l") [(a, "/p/l", Sha256 "h"), (b, "/q/l", Sha256 "g")])
      == Left [(SoName "l", [(a, "/p/l"), (b, "/q/l")])]
  let expected = Map.fromList [(SoName "l", StagedCopy), (SoName "m", SameFile "/lib64/m")]
  expect "no differences when the copy and the system file are loaded" $
    null (differences "/s/lib" expected (Map.fromList [(SoName "l", "/s/lib/l"), (SoName "m", "/lib64/m")]))
  expect "a library loaded from elsewhere is a difference" $
    differences "/s/lib" expected (Map.fromList [(SoName "l", "/cm/l"), (SoName "m", "/lib64/m")])
      == [Moved (SoName "l") StagedCopy "/cm/l"]
  expect "missing and unexpected libraries are differences" $
    differences "/s/lib" expected (Map.fromList [(SoName "l", "/s/lib/l"), (SoName "n", "/lib64/n")])
      == [Missing (SoName "m") (SameFile "/lib64/m"), Unexpected (SoName "n") "/lib64/n"]

-- | Libraries this test may move to the "shared" folder: none is part of
-- glibc, so loading a copy with the system's loader is safe.
movable :: [SoName]
movable = map SoName ["libgmp.so.10", "libz.so.1", "libffi.so.8", "libffi.so.7", "libnuma.so.1"]

runTest :: IO ()
runTest = do
  pureTests
  tmp <- getTemporaryDirectory
  pid <- getProcessID
  exe <- myExecutable
  let dir = tmp <> "/hyperion-scheduler-staging-test-" <> show pid
      osDir = fromString dir
      shared = osDir </> "shared"
  createDirectory dir
  createDirectory (toString shared)
  -- First, with no library path: everything this executable loads.
  first <- stagePrograms (defaultStaging (osDir </> "stage0")) [] exe
  case first of
    Left err -> throwIO $ AssertionFailed $ "FAILED: staging the test executable: " <> show err
    Right staged0 -> do
      expect "a program with system libraries only copies none" $
        staged0.summary.copiedLibraries == 0
      let
        own = maybe Map.empty snd (Map.lookup (ProgramName "hyperion-scheduler-unit-test") staged0.expected)
        candidates = [ (n, p) | n <- movable, Just (SameFile p) <- [Map.lookup n own] ]
      case candidates of
        [] -> putStrLn "skipped: the test executable loads none of the movable libraries"
        (so@(SoName name), path) : _ -> do
          copyFile (toString path) (toString (shared </> fromString name))
          let staging = (defaultStaging (osDir </> "stage")) { programs = [program "/bin/sh"] }
          result <- stagePrograms staging [shared] exe
          case result of
            Left err -> throwIO $ AssertionFailed $ "FAILED: staging with a shared library: " <> show err
            Right staged -> do
              let libDir = staged.directories.libraryDirectory
              expect "a library from a shared folder is copied" $
                staged.summary.copiedLibraries == 1
              expect "the copy is expected to be loaded" $
                (Map.lookup so . snd =<< Map.lookup (ProgramName "hyperion-scheduler-unit-test") staged.expected)
                  == Just StagedCopy
              ok <- checkStaged staged [libDir]
              expect "the staged executable loads the copy" $ either (const False) (const True) ok
              wrong <- checkStaged staged []
              expect "without the staged folder it would load another file" $ case wrong of
                Left (ResolutionChanged _ [Moved so' StagedCopy _]) -> so' == so
                _ -> False
              again <- stagePrograms staging [shared] exe
              expect "staging again reuses the folder" $
                either (const False) (\s -> s.summary == staged.summary) again
              BS.appendFile (toString (libDir </> fromString name)) "x"
              tampered <- stagePrograms staging [shared] exe
              expect "a changed staged file is an error" $ case tampered of
                Left (StagedFileChanged _) -> True
                _ -> False
  conflict <- stagePrograms (defaultStaging (osDir </> "stage2"))
    { programs = [program exe] } [] exe
  expect "two programs with the same name are an error" $ case conflict of
    Left (DuplicateProgramName _) -> True
    _ -> False
  removeDirectoryRecursive dir
  putStrLn "All Staging tests passed."
