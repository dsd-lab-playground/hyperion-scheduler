{-# LANGUAGE DeriveAnyClass      #-}
{-# LANGUAGE DerivingStrategies  #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE NoFieldSelectors    #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE StaticPointers      #-}

-- | How node supervisors launch task workers: with which environment, and
-- from which copy of the executables. The default, 'defaultTaskLaunch', is
-- what they did before: inherit the supervisor's environment and run the
-- executable where it is.
--
-- Both choices are made once per node, when its supervisor starts; the
-- master does nothing more per task. Each worker's environment is inherited
-- by everything its task starts. Processes that hyperion starts rather than a
-- supervisor (the master, file services, the reusable workers that fetch
-- files) aren't covered: they start once each, not per task.
--
-- See "Hyperion.Scheduler.TaskLaunch.Environment" and
-- "Hyperion.Scheduler.TaskLaunch.Staging".
module Hyperion.Scheduler.TaskLaunch
  ( TaskLaunch (..)
  , defaultTaskLaunch
  , isDefaultTaskLaunch
  , EnvironmentPolicy (..)
  , ExecutablePolicy (..)
  , StartupWaits (..)
  , defaultStartupWaits
  , TaskLaunchError (..)
  , validateTaskLaunch
  , startupTimeoutOf
  , NodeTaskLaunch (..)
  , concreteTaskLaunch
  , NodeLaunch (..)
  , StagingReport (..)
  , prepareNodeLaunch
  , module Hyperion.Scheduler.TaskLaunch.Environment
  , module Hyperion.Scheduler.TaskLaunch.Staging
  ) where

import Control.Monad           (forM_)
import Control.Monad.Except    (ExceptT (..), runExceptT)
import Control.Monad.IO.Class  (liftIO)
import Data.Binary             (Binary)
import GHC.Clock               (getMonotonicTime)
import GHC.Generics            (Generic)
import Hyperion                (Dict (..), Static (..))
import Hyperion.OsPath         (OsPath, isAbsolute)
import Hyperion.Scheduler.TaskLaunch.Environment
import Hyperion.Scheduler.TaskLaunch.Staging
import System.Environment      (getEnvironment, lookupEnv)

data TaskLaunch = MkTaskLaunch
  { -- | Default 'InheritEnvironment'.
    environment :: EnvironmentPolicy
    -- | Default 'SharedExecutables'.
  , executables :: ExecutablePolicy
    -- | Default 'defaultStartupWaits'.
  , startupWaits :: StartupWaits
  }
  deriving stock (Eq, Show, Generic)
  deriving anyclass (Binary)

-- | How long the master waits when supervisors fail to start.
data StartupWaits = MkStartupWaits
  { -- | After one node has failed, for the others' replies: enough to name
    -- their failures and stop those that started, and no longer, since the run
    -- is over. Default 60 s.
    afterFailure    :: Seconds
    -- | For the reply of a supervisor seen to die. One that can't launch tasks
    -- replies and then ends, and its reply and its death can arrive in either
    -- order. Default 5 s.
  , replyAfterDeath :: Seconds
  }
  deriving stock (Eq, Show, Generic)
  deriving anyclass (Binary)

defaultStartupWaits :: StartupWaits
defaultStartupWaits = MkStartupWaits (Seconds 60) (Seconds 5)

data EnvironmentPolicy
  = InheritEnvironment  -- ^ the supervisor's (with staged folders first, if staging)
  | SetEnvironment TaskEnvironment
  deriving stock (Eq, Show, Generic)
  deriving anyclass (Binary)

data ExecutablePolicy
  = SharedExecutables  -- ^ run executables where they are
  | NodeLocalExecutables Staging
  deriving stock (Eq, Show, Generic)
  deriving anyclass (Binary)

defaultTaskLaunch :: TaskLaunch
defaultTaskLaunch = MkTaskLaunch InheritEnvironment SharedExecutables defaultStartupWaits

isDefaultTaskLaunch :: TaskLaunch -> Bool
isDefaultTaskLaunch = (== defaultTaskLaunch)

data TaskLaunchError
  = InvalidEnvironment [EnvironmentError]
  -- | Its subfolders go in @PATH@ and @LD_LIBRARY_PATH@: see
  -- 'validSearchPathEntry'.
  | InvalidStagingDirectory OsPath
  | RelativeProgram OsPath
  -- | In a program's launcher's folders, a reference library path or the
  -- system folders: see 'validSearchPathEntry'.
  | InvalidLibraryFolder OsPath
  | InvalidStartupTimeout Seconds
  | InvalidStartupWait Seconds
  -- | A non-default 'TaskLaunch' needs node supervisors, which launch the
  -- workers; @HYPERION_SCHEDULER_NODE_SUPERVISORS=0@ turns them off.
  | NeedsNodeSupervisors
  deriving stock (Eq, Show, Generic)
  deriving anyclass (Binary)

-- | Every error of the configuration, checked on the master before the run
-- starts anything.
validateTaskLaunch :: TaskLaunch -> [TaskLaunchError]
validateTaskLaunch launch = environmentErrors <> stagingErrors <> waitErrors
  where
    waitErrors =
      [ InvalidStartupWait w
      | w <- [launch.startupWaits.afterFailure, launch.startupWaits.replyAfterDeath]
      , w < Seconds 0 ]
    staging = case launch.executables of
      SharedExecutables      -> ProgramsInPlace
      NodeLocalExecutables _ -> ProgramsStaged
    environmentErrors = case launch.environment of
      InheritEnvironment -> []
      SetEnvironment env -> case validateTaskEnvironment staging env of
        []   -> []
        errs -> [InvalidEnvironment errs]
    stagingErrors = case launch.executables of
      SharedExecutables      -> []
      NodeLocalExecutables s -> concat
        [ [ InvalidStagingDirectory s.directory | not (validSearchPathEntry s.directory) ]
        , [ RelativeProgram p.executable | p <- s.programs, not (isAbsolute p.executable) ]
        , [ InvalidLibraryFolder d | d <- libraryFolders s, not (validSearchPathEntry d) ]
        , [ InvalidStartupTimeout s.startupTimeout | s.startupTimeout < Seconds 1 ]
        ]
    libraryFolders s = concat
      [ concatMap (.launcherLibraryPath) s.programs
      , s.systemLibraryDirs
      , case s.referenceLibraryPath of
          MasterLibraryPath -> []
          LibraryPath path  -> path
      ]

-- | How long the master waits for supervisors to start: without staging, as
-- long as it takes, as before.
startupTimeoutOf :: TaskLaunch -> Maybe Seconds
startupTimeoutOf launch = case launch.executables of
  SharedExecutables      -> Nothing
  NodeLocalExecutables s -> Just s.startupTimeout

-- | A 'TaskLaunch' as sent to the nodes: its reference library path is
-- concrete, so that every node resolves programs the same way.
data NodeTaskLaunch = NodeTaskLaunch
  { launch        :: TaskLaunch
  , referencePath :: [OsPath]
  }
  deriving stock (Eq, Show, Generic)
  deriving anyclass (Binary)

instance Static (Binary NodeTaskLaunch) where
  closureDict = static Dict

-- | On the master: read its @LD_LIBRARY_PATH@ if the configuration asks for it.
concreteTaskLaunch :: TaskLaunch -> IO NodeTaskLaunch
concreteTaskLaunch launch = NodeTaskLaunch launch <$> case launch.executables of
  SharedExecutables      -> pure []
  NodeLocalExecutables s -> case s.referenceLibraryPath of
    LibraryPath path  -> pure path
    MasterLibraryPath -> maybe [] parseSearchPath <$> lookupEnv (envKey libraryPathVar)

-- | What a node did to prepare its task launch, for the master's log.
data StagingReport = StagingReport
  { summary :: StagingSummary
  , outcome :: StagingOutcome
    -- | Staging and checking, on this node.
  , seconds :: Double
  }
  deriving stock (Eq, Show, Generic)
  deriving anyclass (Binary)

-- | How a node's supervisor launches its workers.
data NodeLaunch = NodeLaunch
  { -- | The worker executable: the staged copy, or the supervisor's own.
    workerProgram     :: OsPath
    -- | 'Nothing': inherit the supervisor's environment unchanged.
  , workerEnvironment :: Maybe [(String, String)]
  , stagingReport     :: Maybe StagingReport
  }

-- | On a node: stage the programs if configured, render the environment, and
-- check that the worker and each staged program load what they should. Takes
-- the supervisor's own executable. With the default 'TaskLaunch' this does
-- nothing: no file is read and no process started.
prepareNodeLaunch :: NodeTaskLaunch -> OsPath -> IO (Either StagingError NodeLaunch)
prepareNodeLaunch node ownExecutable = runExceptT $ do
  own <- liftIO getEnvironment
  let
    explicit = case node.launch.environment of
      InheritEnvironment -> Nothing
      SetEnvironment env -> Just env
    runtimeLibraryPath = searchPathOf libraryPathVar
  case node.launch.executables of
    SharedExecutables -> do
      let rendered = renderEnvironment explicit Nothing own
      forM_ rendered $ ExceptT . checkProgramLoads ownExecutable . runtimeLibraryPath
      pure NodeLaunch
        { workerProgram = ownExecutable
        , workerEnvironment = rendered
        , stagingReport = Nothing
        }
    NodeLocalExecutables staging -> do
      start <- liftIO getMonotonicTime
      stagedPrograms <- ExceptT $ stagePrograms staging node.referencePath ownExecutable
      let rendered = renderEnvironment explicit (Just stagedPrograms.directories) own
      ExceptT $ checkStaged stagedPrograms (maybe [] runtimeLibraryPath rendered)
      end <- liftIO getMonotonicTime
      pure NodeLaunch
        { workerProgram = stagedPrograms.workerExecutable
        , workerEnvironment = rendered
        , stagingReport = Just StagingReport
            { summary = stagedPrograms.summary
            , outcome = stagedPrograms.outcome
            , seconds = end - start
            }
        }
