{-# LANGUAGE DeriveAnyClass        #-}
{-# LANGUAGE DerivingStrategies    #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE OverloadedRecordDot   #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE StaticPointers        #-}

-- | Per-site configuration for the scheduler tests: one 'Site' per HPC
-- cluster, plus the settings used when running locally without SLURM.
module Hyperion.Scheduler.Test.Config where

import Control.Monad.IO.Class    (liftIO)
import Data.Binary               (Binary)
import Data.List                 (intercalate)
import GHC.Generics              (Generic)
import Hyperion                  (Dict (..), HostNameStrategy (..),
                                  HyperionConfig (..),
                                  HyperionStaticConfig (..), Job, Static (..),
                                  defaultHyperionConfig,
                                  defaultHyperionStaticConfig)
import Hyperion.OsPath           (OsPath, (</>))
import Hyperion.OsString         (fromString)
import Hyperion.Scheduler.Config qualified as Scheduler
import Hyperion.Scheduler.TaskLaunch (EnvironmentPolicy (..),
                                      ExecutablePolicy (..), Seconds (..),
                                      Staging (..), TaskLaunch (..),
                                      defaultStaging, defaultStartupTimeout,
                                      defaultTaskEnvironment, defaultTaskLaunch,
                                      program)
import Hyperion.Slurm            (SbatchOptions (..))
import Hyperion.Slurm            qualified as Slurm
import System.Environment        (getEnv)

-- * Sites

-- | An HPC cluster we know how to run on. Add a constructor (and fill in the
-- case split below) to support a new cluster.
data Site = Expanse
  deriving (Eq, Ord, Show, Enum, Bounded, Generic, Binary)

instance Static (Binary Site) where
  closureDict = static Dict

siteName :: Site -> String
siteName = \case
  Expanse -> "expanse"

allSites :: [Site]
allSites = [minBound .. maxBound]

defaultSite :: Site
defaultSite = Expanse

parseSite :: String -> Either String Site
parseSite s = case [site | site <- allSites, siteName site == s] of
  site : _ -> Right site
  []       -> Left $
    "unknown site " <> show s <> ", expected one of: " <>
    intercalate ", " (map siteName allSites)

-- * Cluster configuration

-- | Static (compile-time) config, available to both master and workers.
hyperionStaticConfig :: Site -> HyperionStaticConfig
hyperionStaticConfig = \case
  Expanse -> defaultHyperionStaticConfig
    { hostNameStrategy           = GetHostEntriesExternal
    , nodeLauncherTimeoutRetries = Just (10, 5)
    }

-- | Default base directory, used when @--base-dir@ is not given.
getScratchDir :: Site -> IO OsPath
getScratchDir = \case
  Expanse -> do
    user <- getEnv "USER"
    pure $ "/expanse/lustre/scratch" </> fromString user </> "temp_project/hyperion-scheduler/test"

-- | Site defaults for @sbatch@. Anything given on the command line wins; see
-- 'withSiteDefaults'.
siteSbatchOptions :: Site -> SbatchOptions
siteSbatchOptions = \case
  Expanse -> Slurm.defaultSbatchOptions
    { partition = Just "compute"
    , account   = Just "yun124"
    }

-- | Fill in the optional @sbatch@ settings the user did not specify. Fields
-- that always have a value ('nodes', 'nTasksPerNode', 'time', ...) are taken
-- from the command line, which supplies its own defaults.
withSiteDefaults :: Site -> SbatchOptions -> SbatchOptions
withSiteDefaults site opts = opts
  { jobName        = opts.jobName    `orElse` defaults.jobName
  , chdir          = opts.chdir      `orElse` defaults.chdir
  , output         = opts.output     `orElse` defaults.output
  , mem            = opts.mem        `orElse` defaults.mem
  , mailType       = opts.mailType   `orElse` defaults.mailType
  , mailUser       = opts.mailUser   `orElse` defaults.mailUser
  , partition      = opts.partition  `orElse` defaults.partition
  , constraint     = opts.constraint `orElse` defaults.constraint
  , account        = opts.account    `orElse` defaults.account
  , qos            = opts.qos        `orElse` defaults.qos
  , scriptPreamble = opts.scriptPreamble `orElse` defaults.scriptPreamble
  }
  where
    defaults = siteSbatchOptions site
    orElse x y = maybe y Just x

getHyperionConfig :: Site -> OsPath -> SbatchOptions -> HyperionConfig
getHyperionConfig site workDir sbatchOptions = (defaultHyperionConfig workDir)
  { defaultSbatchOptions = withSiteDefaults site sbatchOptions
  , maxSlurmJobs         = Just 64
  }

-- | Scheduler config for a compute node of @site@. Runs on the node, so it can
-- read the SLURM environment to locate node-local scratch.
getSchedulerConfig :: Site -> LaunchOptions -> Job Scheduler.Config
getSchedulerConfig site launchOpts = case site of
  Expanse -> do
    user <- liftIO $ getEnv "USER"
    slurmJobId <- liftIO $ getEnv "SLURM_JOB_ID"
    let localStoragePath = "/scratch" </> fromString user </> "job_" <> fromString slurmJobId
    pure Scheduler.MkConfig
      { nodeMemory           = 240 * 1024 * 1024 * 1024
      , nodeLocalStorageSize = 900 * 1000 * 1000 * 1000
      , localStoragePath     = localStoragePath
      , reportInterval       = 10
      , taskLaunch           = testTaskLaunch launchOpts (localStoragePath </> "programs")
      }

-- * Local (no SLURM) configuration

-- | Scheduler config for a single local "node". @localStoragePath@ stands in
-- for node-local scratch, so the file-service and cleanup logic is exercised
-- locally too.
localSchedulerConfig :: OsPath -> TaskLaunch -> Scheduler.Config
localSchedulerConfig localStoragePath launch = Scheduler.MkConfig
  { nodeMemory           = 4 * 1024 * 1024 * 1024
  , nodeLocalStorageSize = 1024 * 1024 * 1024
  , localStoragePath     = localStoragePath
  , reportInterval       = 5
  , taskLaunch           = launch
  }

-- * Task launch

-- | Which non-default task launch settings the test uses.
data LaunchOptions = LaunchOptions
  { -- | Stage the test executable on node-local disk and launch workers from
    -- the copy.
    nodeLocalExecutables :: Bool
    -- | Launch workers with 'defaultTaskEnvironment'.
  , taskEnvironment      :: Bool
    -- | Programs to stage besides the executable. Failure tests name a missing
    -- file, or one whose reading blocks.
  , stagedPrograms       :: [OsPath]
    -- | The staging start-up timeout, in seconds, if not the default.
  , stagingTimeout       :: Maybe Int
  }
  deriving (Eq, Show, Generic, Binary)

instance Static (Binary LaunchOptions) where
  closureDict = static Dict

-- | The task launch for the options, staging in the given folder.
testTaskLaunch :: LaunchOptions -> OsPath -> TaskLaunch
testTaskLaunch opts stagingDir = defaultTaskLaunch
  { environment = if opts.taskEnvironment
      then SetEnvironment defaultTaskEnvironment
      else InheritEnvironment
  , executables = if opts.nodeLocalExecutables
      then NodeLocalExecutables (defaultStaging stagingDir)
        { programs = map program opts.stagedPrograms
        , startupTimeout = maybe defaultStartupTimeout Seconds opts.stagingTimeout
        }
      else SharedExecutables
  }
