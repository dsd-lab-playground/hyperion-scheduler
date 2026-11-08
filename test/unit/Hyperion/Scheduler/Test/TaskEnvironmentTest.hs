{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings   #-}

-- | Validation and rendering of task environments. Pure.
module Hyperion.Scheduler.Test.TaskEnvironmentTest (runTest) where

import Control.Exception                         (AssertionFailed (..),
                                                  throwIO)
import Control.Monad                             (unless)
import Data.Map.Strict                           qualified as Map
import Data.Set                                  qualified as Set
import Hyperion.Scheduler.TaskLaunch.Environment

expect :: String -> Bool -> IO ()
expect label cond = do
  unless cond $ throwIO $ AssertionFailed $ "FAILED: " <> label
  putStrLn $ "ok: " <> label

-- | A supervisor's environment as on an Expanse node after @~/.bashrc@.
own :: [(String, String)]
own =
  [ ("HOME", "/home/u")
  , ("PATH", "/cm/shared/apps/gcc/bin:/usr/bin")
  , ("LD_LIBRARY_PATH", "/cm/shared/apps/gmp/lib")
  , ("OMP_NUM_THREADS", "128")
  , ("SLURM_JOB_ID", "42")
  ]

dirs :: StagedDirectories
dirs = StagedDirectories "/scratch/u/job_42/programs/abc/bin" "/scratch/u/job_42/programs/abc/lib"

runTest :: IO ()
runTest = do
  let
    env = defaultTaskEnvironment
    with vars = env { variables = Map.fromList vars }
    render e s = Map.fromList <$> renderEnvironment e s own
  expect "the default environment is valid" $ null (validateTaskEnvironment ProgramsInPlace env)
  expect "PATH is a typed field, not a variable" $
    validateTaskEnvironment ProgramsInPlace (with [("PATH", "/x")]) == [ReservedName "PATH"]
  expect "a thread variable is a typed field" $
    validateTaskEnvironment ProgramsInPlace (with [("OMP_NUM_THREADS", "2")]) == [ReservedName "OMP_NUM_THREADS"]
  expect "a name with '=' is invalid" $
    validateTaskEnvironment ProgramsInPlace (with [("A=B", "1")]) == [InvalidName "A=B"]
  expect "a relative search path entry is invalid" $
    validateTaskEnvironment ProgramsInPlace env { searchPath = ["bin"] } == [InvalidSearchPathEntry "bin"]
  expect "a search path entry with ':' is invalid" $
    validateTaskEnvironment ProgramsInPlace env { libraryPath = ["/a:/b"] } == [InvalidSearchPathEntry "/a:/b"]
  expect "a search path entry with ';' is invalid: the loader splits on it" $
    validateTaskEnvironment ProgramsInPlace env { libraryPath = ["/a;/b"] } == [InvalidSearchPathEntry "/a;/b"]
  expect "a thread count below 1 is invalid" $
    validateTaskEnvironment ProgramsInPlace env { threads = MkThreadLimits (Just (ThreadCount 0)) Nothing }
      == [InvalidThreadCount (ThreadCount 0)]
  expect "the staged directory needs staging" $
    validateTaskEnvironment ProgramsInPlace (with [("N", StagedProgramsDirectory)])
      == [StagedDirectoryWithoutStaging "N"]
      && null (validateTaskEnvironment ProgramsStaged (with [("N", StagedProgramsDirectory)]))

  expect "no environment and no staging: inherit unchanged" $
    renderEnvironment Nothing Nothing own == Nothing
  let inheritedStaged = render Nothing (Just dirs)
  expect "inherited with staging: the staged folders go first" $
    (Map.lookup "PATH" =<< inheritedStaged)
      == Just "/scratch/u/job_42/programs/abc/bin:/cm/shared/apps/gcc/bin:/usr/bin"
    && (Map.lookup "LD_LIBRARY_PATH" =<< inheritedStaged)
      == Just "/scratch/u/job_42/programs/abc/lib:/cm/shared/apps/gmp/lib"
    && (Map.lookup "OMP_NUM_THREADS" =<< inheritedStaged) == Just "128"
  let
    withEmptyEntries = renderEnvironment Nothing (Just dirs)
      [("PATH", ":/usr/bin"), ("LD_LIBRARY_PATH", "/x::")]
  expect "inherited with staging: empty entries, which mean the current folder, are dropped" $
    (Map.toList . Map.fromList <$> withEmptyEntries) == Just
      [ ("LD_LIBRARY_PATH", "/scratch/u/job_42/programs/abc/lib:/x")
      , ("PATH", "/scratch/u/job_42/programs/abc/bin:/usr/bin") ]
  let explicit = render (Just (with [("N", StagedProgramsDirectory), ("X", "1")])) (Just dirs)
  expect "explicit with staging: PATH is the staged folder and the system folders" $
    (Map.lookup "PATH" =<< explicit) == Just "/scratch/u/job_42/programs/abc/bin:/usr/bin:/bin"
  expect "explicit with staging: LD_LIBRARY_PATH is the staged libraries only" $
    (Map.lookup "LD_LIBRARY_PATH" =<< explicit) == Just "/scratch/u/job_42/programs/abc/lib"
  expect "explicit: one BLAS and one OpenMP thread" $
    all (\n -> (Map.lookup n =<< explicit) == Just "1")
      ["OPENBLAS_NUM_THREADS", "GOTO_NUM_THREADS", "OMP_NUM_THREADS"]
  expect "explicit: other variables are inherited, and set ones added" $
    (Map.lookup "HOME" =<< explicit) == Just "/home/u"
    && (Map.lookup "X" =<< explicit) == Just "1"
  expect "StagedProgramsDirectory is the staged bin folder" $
    (Map.lookup "N" =<< explicit) == Just "/scratch/u/job_42/programs/abc/bin"
  let bare = render (Just env) Nothing
  expect "explicit without staging and no library path: no LD_LIBRARY_PATH at all" $
    (Map.member "LD_LIBRARY_PATH" <$> bare) == Just False
    && (Map.lookup "PATH" =<< bare) == Just "/usr/bin:/bin"
  expect "an empty search path without staging leaves PATH unset: empty would mean the current folder" $
    (Map.member "PATH" <$> render (Just env { searchPath = [] }) Nothing) == Just False
  expect "an empty search path with staging is the staged folder" $
    (Map.lookup "PATH" =<< render (Just env { searchPath = [] }) (Just dirs))
      == Just "/scratch/u/job_42/programs/abc/bin"
  let only = render (Just env { inherited = InheritOnly (Set.fromList ["HOME"]) }) Nothing
  expect "InheritOnly keeps only the named variables and the typed ones" $
    (Map.keys <$> only) == Just ["GOTO_NUM_THREADS", "HOME", "OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "PATH"]
  expect "unset thread limits leave inherited values" $
    (Map.lookup "OMP_NUM_THREADS" =<< render (Just env { threads = MkThreadLimits Nothing Nothing }) Nothing)
      == Just "128"
  expect "search paths parse back" $
    parseSearchPath "/a::/b:" == ["/a", "/b"]
    && searchPathOf "PATH" [("PATH", renderSearchPath ["/a", "/b"])] == ["/a", "/b"]
  putStrLn "All TaskEnvironment tests passed."
