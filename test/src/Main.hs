{-# LANGUAGE ApplicativeDo         #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE NoFieldSelectors      #-}
{-# LANGUAGE OverloadedRecordDot   #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE RecordWildCards       #-}
{-# LANGUAGE StaticPointers        #-}

-- | Driver for the LinearTransform scheduler test.
--
-- Three entry points:
--
-- > hyperion-scheduler-test local  [--shift N] [--dim N] [--base-dir DIR] [--cpus N]
-- > hyperion-scheduler-test master [--shift N] [--dim N] [--base-dir DIR] <sbatch options>
-- > hyperion-scheduler-test worker ...   -- launched by hyperion, not by hand
--
-- @local@ runs everything in this process with no SLURM and no cluster
-- database; @master@ and @worker@ are hyperion's usual cluster entry points.
--
-- The HPC site for the cluster entry points comes from the
-- @HYPERION_SCHEDULER_TEST_SITE@ environment variable (see 'siteEnvVar'), not from a
-- command-line flag: workers are launched by hyperion without our options, and
-- master and workers must agree on 'HyperionStaticConfig'. The environment is
-- inherited through @sbatch@, a flag would not be.
module Main where

import Control.Monad.IO.Class                  (liftIO)
import Control.Monad.Reader                    (local)
import Data.Maybe                              (fromMaybe)
import GHC.Conc                                (getNumProcessors)
import Hyperion                                hiding (opts)
import Hyperion.Log                            qualified as Log
import Hyperion.OsPath                         (OsPath, (</>))
import Hyperion.OsString                       (fromString, showOs)
import Hyperion.Scheduler.Test.Config          (Site)
import Hyperion.Scheduler.Test.Config          qualified as TestConfig
import Hyperion.Scheduler.Test.LinearTransform (CyclicShiftProblem (..),
                                                linearTransformJob)
import Hyperion.Slurm                          (SbatchOptions (..),
                                                sBatchOptionsParser)
import Options.Applicative                     (Parser, ParserInfo, auto,
                                                execParser, fullDesc, help,
                                                helper, info, long, metavar,
                                                option, optional, progDesc, str,
                                                value, (<**>))
import System.Console.Concurrent               (withConcurrentOutput)
import System.Directory.OsPath                 (createDirectoryIfMissing,
                                                makeAbsolute)
import System.Environment                      (getArgs, lookupEnv, setEnv,
                                                withArgs, withProgName)
import System.Exit                             (die, exitSuccess)

-- * Options

-- | Options shared by the local and cluster entry points.
data ProblemOptions = ProblemOptions
  { shift   :: Int
  , dim     :: Int
  , baseDir :: Maybe OsPath
  } deriving (Show)

problemOptsParser :: Parser ProblemOptions
problemOptsParser = do
  shift <- option auto $ long "shift" <> metavar "INT" <> value 2
    <> help "Shift the input vector cyclically by this many positions (one layer per position)"
  dim <- option auto $ long "dim" <> metavar "INT" <> value 3
    <> help "Input vector size"
  baseDir <- optional $ option (fromString <$> str) $ long "base-dir" <> metavar "DIR"
    <> help "Base directory for output files"
  pure ProblemOptions{..}

toProblem :: ProblemOptions -> CyclicShiftProblem
toProblem opts = MkCyclicShiftProblem { shift = opts.shift, dim = opts.dim }

-- | @local@ options: 'ProblemOptions' plus the size of the fake node.
data LocalOptions = LocalOptions
  { problem :: ProblemOptions
  , cpus    :: Maybe Int
  } deriving (Show)

localOptsParser :: Parser LocalOptions
localOptsParser = do
  problem <- problemOptsParser
  cpus <- optional $ option auto $ long "cpus" <> metavar "INT"
    <> help "CPUs to give the local node (default: all available)"
  pure LocalOptions{..}

localOptsInfo :: ParserInfo LocalOptions
localOptsInfo = info (localOptsParser <**> helper) $ fullDesc
  <> progDesc "Run the LinearTransform test in this process, without SLURM"

-- | @master@ options: 'ProblemOptions' plus the usual @sbatch@ options.
data MasterOptions = MasterOptions
  { problem       :: ProblemOptions
  , sbatchOptions :: SbatchOptions
  } deriving (Show)

masterOptsParser :: Parser MasterOptions
masterOptsParser = do
  problem <- problemOptsParser
  sbatchOptions <- sBatchOptionsParser
  pure MasterOptions{..}

-- * Local runs

-- | Default base directory for @local@, relative to the current directory.
defaultLocalBaseDir :: OsPath
defaultLocalBaseDir = "tmp/hyperion-scheduler-linear-transform-test"

-- | 'withConcurrentOutput' is needed because 'Log' writes through
-- "System.Console.Concurrent", which otherwise drops buffered output at exit.
-- 'hyperionMain' does the same for the cluster entry points.
runLocal :: LocalOptions -> IO ()
runLocal opts = withConcurrentOutput $ do
  baseDir <- makeAbsolute $ fromMaybe defaultLocalBaseDir opts.problem.baseDir
  cpus <- maybe getNumProcessors pure opts.cpus
  createDirectoryIfMissing True baseDir
  let
    -- 'runJobLocal' needs a 'ProgramInfo'; point it inside baseDir so a local
    -- run leaves nothing behind in the current directory.
    programInfo = ProgramInfo
      { programId       = ProgramId "local"
      , programDatabase = baseDir </> "local.sqlite"
      , programLogDir   = baseDir </> "logs"
      , programDataDir  = baseDir </> "data"
      }
    schedulerConfig = TestConfig.localSchedulerConfig (baseDir </> "node_local_storage")
  Log.info "Running locally" (baseDir, cpus)
  -- Worker processes that the scheduler's node supervisor launches inherit it.
  setEnv localWorkersEnvVar "1"
  runJobLocal defaultHyperionStaticConfig programInfo $
    -- 'runJobLocal' hardcodes one CPU; 'getJobNodes' reads this to size the node.
    local (\env -> env { jobNodeCpus = cpus }) $
      linearTransformJob (pure schedulerConfig) baseDir (toProblem opts.problem)

-- * Cluster runs

-- | Environment variable naming the HPC site, e.g. @HYPERION_SCHEDULER_TEST_SITE=expanse@.
siteEnvVar :: String
siteEnvVar = "HYPERION_SCHEDULER_TEST_SITE"

getSite :: IO Site
getSite = lookupEnv siteEnvVar >>= \case
  Nothing -> pure TestConfig.defaultSite
  Just s  -> either (die . ((siteEnvVar <> ": ") <>)) pure (TestConfig.parseSite s)

-- | A program to start every worker under, for fault injection (see
-- @test/fault/README.md@). Workers read it too, so that a node's supervisor
-- starts its workers the same way.
workerWrapperEnvVar :: String
workerWrapperEnvVar = "HYPERION_SCHEDULER_TEST_WORKER_WRAPPER"

-- | Set by @local@ for the worker processes it launches, which must use its
-- 'HyperionStaticConfig' rather than the site's.
localWorkersEnvVar :: String
localWorkersEnvVar = "HYPERION_SCHEDULER_TEST_LOCAL_WORKERS"

runOnCluster :: Site -> IO ()
runOnCluster site = do
  isLocalWorker <- (== Just "1") <$> lookupEnv localWorkersEnvVar
  wrapper <- lookupEnv workerWrapperEnvVar
  let siteConfig
        | isLocalWorker = defaultHyperionStaticConfig
        | otherwise     = TestConfig.hyperionStaticConfig site
      staticConfig = case wrapper of
        Just program | not (null program) -> siteConfig
          { workerCommand = wrapWorkerCommand (fromString program) [] . siteConfig.workerCommand }
        _ -> siteConfig
  hyperionMain masterOptsParser mkHyperionConfig staticConfig clusterComputation
  where
    -- Keep runs with different allocations side by side, so their task stats
    -- can be compared.
    workDir baseDir opts = baseDir </>
      "nodes_" <> showOs opts.sbatchOptions.nodes <>
      "_ntasks_" <> showOs opts.sbatchOptions.nTasksPerNode

    mkHyperionConfig opts =
      TestConfig.getHyperionConfig site (fromMaybe "." opts.problem.baseDir) opts.sbatchOptions

    clusterComputation opts = do
      baseDir <- liftIO $ maybe (TestConfig.getScratchDir site) makeAbsolute opts.problem.baseDir
      remoteEvalJob $ static linearTransformJob
        `cAp` (static TestConfig.getSchedulerConfig `cAp` cPure site)
        `cAp` cPure (workDir baseDir opts)
        `cAp` cPure (toProblem opts.problem)

-- * Entry point

usage :: String
usage = unlines
  [ "Usage: hyperion-scheduler-test COMMAND [OPTIONS]"
  , ""
  , "Commands:"
  , "  local   Run the LinearTransform test in this process, without SLURM"
  , "  master  Run the LinearTransform test on a SLURM cluster"
  , "  worker  Run a worker process (launched automatically by the master)"
  , ""
  , "Pass --help after a command for its options."
  , "Set " <> siteEnvVar <> " to select the HPC site for master/worker."
  ]

main :: IO ()
main = getArgs >>= \case
  "local" : rest    -> withProgName "hyperion-scheduler-test local" $
                       withArgs rest $ execParser localOptsInfo >>= runLocal
  args | needsUsage -> putStr usage >> exitSuccess
       | otherwise  -> getSite >>= runOnCluster
    where needsUsage = null args || head args `elem` ["-h", "--help", "help"]
