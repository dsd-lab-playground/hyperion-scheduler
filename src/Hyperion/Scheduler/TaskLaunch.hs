{-# LANGUAGE DeriveAnyClass      #-}
{-# LANGUAGE DerivingStrategies  #-}
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
-- by everything its task starts.
--
-- See "Hyperion.Scheduler.TaskLaunch.Environment" and
-- "Hyperion.Scheduler.TaskLaunch.Staging".
module Hyperion.Scheduler.TaskLaunch
  ( TaskLaunch (..)
  , defaultTaskLaunch
  , isDefaultTaskLaunch
  , EnvironmentPolicy (..)
  , ExecutablePolicy (..)
  , TaskLaunchError (..)
  , validateTaskLaunch
  , withConcreteReferencePath
  , NodeLaunch (..)
  , prepareNodeLaunch
  , module Hyperion.Scheduler.TaskLaunch.Environment
  , module Hyperion.Scheduler.TaskLaunch.Staging
  ) where

import Control.Monad.Except    (ExceptT (..), runExceptT)
import Control.Monad.IO.Class  (liftIO)
import Data.Binary             (Binary)
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
  }
  deriving stock (Eq, Show, Generic)
  deriving anyclass (Binary)

instance Static (Binary TaskLaunch) where
  closureDict = static Dict

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
defaultTaskLaunch = MkTaskLaunch InheritEnvironment SharedExecutables

isDefaultTaskLaunch :: TaskLaunch -> Bool
isDefaultTaskLaunch = (== defaultTaskLaunch)

data TaskLaunchError
  = InvalidEnvironment [EnvironmentError]
  | RelativeStagingDirectory OsPath
  -- | A non-default 'TaskLaunch' needs node supervisors, which launch the
  -- workers; @HYPERION_SCHEDULER_NODE_SUPERVISORS=0@ turns them off.
  | NeedsNodeSupervisors
  deriving stock (Eq, Show, Generic)
  deriving anyclass (Binary)

-- | Every error of the configuration, checked on the master at start.
validateTaskLaunch :: TaskLaunch -> [TaskLaunchError]
validateTaskLaunch launch = environmentErrors <> stagingErrors
  where
    staged = case launch.executables of
      SharedExecutables      -> False
      NodeLocalExecutables _ -> True
    environmentErrors = case launch.environment of
      InheritEnvironment -> []
      SetEnvironment env -> case validateTaskEnvironment staged env of
        []   -> []
        errs -> [InvalidEnvironment errs]
    stagingErrors = case launch.executables of
      NodeLocalExecutables s
        | not (isAbsolute s.directory) -> [RelativeStagingDirectory s.directory]
      _ -> []

-- | Replace 'MasterLibraryPath' by the master's @LD_LIBRARY_PATH@, so that
-- every node resolves programs the same way.
withConcreteReferencePath :: TaskLaunch -> IO TaskLaunch
withConcreteReferencePath launch = case launch.executables of
  NodeLocalExecutables s | s.referenceLibraryPath == MasterLibraryPath -> do
    path <- maybe [] parseSearchPath <$> lookupEnv "LD_LIBRARY_PATH"
    pure launch { executables = NodeLocalExecutables s { referenceLibraryPath = LibraryPath path } }
  _ -> pure launch

-- | How a node's supervisor launches its workers.
data NodeLaunch = NodeLaunch
  { -- | The worker executable: the staged copy, or the supervisor's own.
    workerProgram     :: OsPath
    -- | 'Nothing': inherit the supervisor's environment unchanged.
  , workerEnvironment :: Maybe [(String, String)]
  , stagingSummary    :: Maybe StagingSummary
  }

-- | On a node: stage the programs if configured, render the environment, and
-- check that each staged program loads what it loaded before. Takes the
-- supervisor's own executable; the reference library path must be concrete
-- (see 'withConcreteReferencePath').
prepareNodeLaunch :: TaskLaunch -> OsPath -> IO (Either StagingError NodeLaunch)
prepareNodeLaunch launch ownExecutable = runExceptT $ do
  own <- liftIO getEnvironment
  let explicit = case launch.environment of
        InheritEnvironment -> Nothing
        SetEnvironment env -> Just env
  case launch.executables of
    SharedExecutables -> pure NodeLaunch
      { workerProgram = ownExecutable
      , workerEnvironment = renderEnvironment explicit Nothing own
      , stagingSummary = Nothing
      }
    NodeLocalExecutables staging -> do
      let reference = case staging.referenceLibraryPath of
            LibraryPath p     -> p
            MasterLibraryPath -> []
      stagedPrograms <- ExceptT $ stagePrograms staging reference ownExecutable
      let
        rendered = renderEnvironment explicit (Just stagedPrograms.directories) own
        runtimeLibraryPath = maybe [] (searchPathOf (EnvName "LD_LIBRARY_PATH")) rendered
      ExceptT $ checkStaged stagedPrograms runtimeLibraryPath
      pure NodeLaunch
        { workerProgram = stagedPrograms.workerExecutable
        , workerEnvironment = rendered
        , stagingSummary = Just stagedPrograms.summary
        }
