{-# LANGUAGE DeriveAnyClass      #-}
{-# LANGUAGE DerivingStrategies  #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE NoFieldSelectors    #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings   #-}

-- | The environment of task processes: an explicit value, built once on the
-- master and applied by each node's supervisor to every worker it launches.
-- A worker's environment is inherited by everything its task starts (scripts,
-- external programs), so this replaces per-task @module load@s in scripts.
--
-- The typed fields own @PATH@, @LD_LIBRARY_PATH@ and the thread-count
-- variables; 'validateTaskEnvironment' rejects them in 'variables'.
-- Rendering to the @[(String, String)]@ of 'System.Process.CreateProcess'
-- happens in 'renderEnvironment' only.
module Hyperion.Scheduler.TaskLaunch.Environment
  ( TaskEnvironment (..)
  , defaultTaskEnvironment
  , Inheritance (..)
  , EnvName (..)
  , EnvValue (..)
  , ThreadLimits (..)
  , ThreadCount (..)
  , oneThreadEach
  , EnvironmentError (..)
  , ProgramStaging (..)
  , validateTaskEnvironment
  , validSearchPathEntry
  , StagedDirectories (..)
  , renderEnvironment
  , searchPathOf
  , renderSearchPath
  , parseSearchPath
  , envKey
  , pathVar
  , libraryPathVar
  ) where

import Data.Binary       (Binary)
import Data.List         (intercalate)
import Data.List.Extra   (splitOn)
import Data.Map.Strict   (Map)
import Data.Map.Strict   qualified as Map
import Data.Maybe        (catMaybes)
import Data.Set          (Set)
import Data.Set          qualified as Set
import Data.String       (IsString (..))
import GHC.Generics      (Generic)
import Hyperion.OsPath   (OsPath)
import Hyperion.OsString qualified as OsString

-- | The name of an environment variable.
newtype EnvName = EnvName String
  deriving stock (Eq, Ord, Show, Generic)
  deriving anyclass (Binary)

instance IsString EnvName where
  fromString = EnvName

data EnvValue
  = EnvText String
  -- | This node's staged programs directory (@bin@ of
  -- 'Hyperion.Scheduler.TaskLaunch.Staging.StagedPrograms'), for scripts that
  -- want a native's path rather than finding it in @PATH@. Requires staging.
  | StagedProgramsDirectory
  deriving stock (Eq, Ord, Show, Generic)
  deriving anyclass (Binary)

instance IsString EnvValue where
  fromString = EnvText

-- | Which of the supervisor's own variables a worker keeps.
data Inheritance
  = InheritAll
  | InheritOnly (Set EnvName)
  deriving stock (Eq, Show, Generic)
  deriving anyclass (Binary)

-- | A thread count, at least 1 (checked by 'validateTaskEnvironment').
newtype ThreadCount = ThreadCount Int
  deriving stock (Eq, Ord, Show, Generic)
  deriving anyclass (Binary)

-- | Threads of the numerical libraries that task processes load. 'Nothing'
-- leaves the variables as inherited.
data ThreadLimits = MkThreadLimits
  { -- | @OPENBLAS_NUM_THREADS@ and @GOTO_NUM_THREADS@. OpenBLAS reads them when
    -- it is loaded, i.e. when the worker starts, and otherwise starts one
    -- thread per core in every worker process (bootstrap-compute #343: one
    -- spinning pool per task).
    blasThreads   :: Maybe ThreadCount
    -- | @OMP_NUM_THREADS@. Also limits OpenMP programs that tasks start; set
    -- it only if those take their thread count from their own options (as
    -- blocks_3d does with @--num-threads@).
  , openMPThreads :: Maybe ThreadCount
  }
  deriving stock (Eq, Show, Generic)
  deriving anyclass (Binary)

-- | One BLAS thread and one OpenMP thread, the setting of bootstrap-compute
-- #343.
oneThreadEach :: ThreadLimits
oneThreadEach = MkThreadLimits (Just (ThreadCount 1)) (Just (ThreadCount 1))

data TaskEnvironment = MkTaskEnvironment
  { -- | Default 'InheritAll'.
    inherited   :: Inheritance
    -- | Set or overridden after inheriting. Default empty.
  , variables   :: Map EnvName EnvValue
    -- | @PATH@, after the staged programs' directory if there is one; unset if
    -- that leaves nothing (an empty @PATH@ would mean the current folder).
    -- Default @[\/usr\/bin, \/bin]@: no folder on a network filesystem, where
    -- every command lookup is a metadata request.
  , searchPath  :: [OsPath]
    -- | @LD_LIBRARY_PATH@, after the staged libraries' directory if there is
    -- one. Default empty.
  , libraryPath :: [OsPath]
    -- | Default 'oneThreadEach'.
  , threads     :: ThreadLimits
  }
  deriving stock (Eq, Show, Generic)
  deriving anyclass (Binary)

defaultTaskEnvironment :: TaskEnvironment
defaultTaskEnvironment = MkTaskEnvironment
  { inherited   = InheritAll
  , variables   = Map.empty
  , searchPath  = ["/usr/bin", "/bin"]
  , libraryPath = []
  , threads     = oneThreadEach
  }

data EnvironmentError
  = InvalidName EnvName
  | ReservedName EnvName          -- ^ set through a typed field instead
  | InvalidSearchPathEntry OsPath -- ^ see 'validSearchPathEntry'
  | InvalidThreadCount ThreadCount
  | StagedDirectoryWithoutStaging EnvName
  deriving stock (Eq, Show, Generic)
  deriving anyclass (Binary)

pathVar, libraryPathVar :: EnvName
pathVar = "PATH"
libraryPathVar = "LD_LIBRARY_PATH"

blasVars, openMPVars :: [EnvName]
blasVars = ["OPENBLAS_NUM_THREADS", "GOTO_NUM_THREADS"]
openMPVars = ["OMP_NUM_THREADS"]

reservedNames :: Set EnvName
reservedNames = Set.fromList $ [pathVar, libraryPathVar] <> blasVars <> openMPVars

-- | Whether programs are staged, which 'StagedProgramsDirectory' needs.
data ProgramStaging = ProgramsStaged | ProgramsInPlace
  deriving stock (Eq, Show)

-- | All errors of the environment.
validateTaskEnvironment :: ProgramStaging -> TaskEnvironment -> [EnvironmentError]
validateTaskEnvironment staging env = concat
  [ [ InvalidName n | n <- names, not (validName n) ]
  , [ ReservedName n | n <- Map.keys env.variables, n `Set.member` reservedNames ]
  , [ InvalidSearchPathEntry p | p <- env.searchPath <> env.libraryPath, not (validSearchPathEntry p) ]
  , [ InvalidThreadCount c | c@(ThreadCount k) <- threadCounts, k < 1 ]
  , [ StagedDirectoryWithoutStaging n
    | staging == ProgramsInPlace, (n, StagedProgramsDirectory) <- Map.toList env.variables ]
  ]
  where
    names = Map.keys env.variables <> case env.inherited of
      InheritAll       -> []
      InheritOnly keep -> Set.toList keep
    threadCounts = catMaybes [env.threads.blasThreads, env.threads.openMPThreads]
    validName (EnvName s) = not (null s) && all (`notElem` ['=', '\0']) s

-- | An entry of @PATH@ or @LD_LIBRARY_PATH@: absolute, and without the
-- characters that would split it (the loader splits on @;@ as well as @:@).
validSearchPathEntry :: OsPath -> Bool
validSearchPathEntry p = case OsString.toString p of
  '/' : rest -> all (`notElem` [':', ';', '\0']) rest
  _          -> False

-- | Where a node's staged programs and libraries are.
data StagedDirectories = StagedDirectories
  { binDirectory     :: OsPath
  , libraryDirectory :: OsPath
  }
  deriving stock (Eq, Show)

-- | The environment of a worker, from the supervisor's own, the policy
-- ('Nothing' for 'Hyperion.Scheduler.TaskLaunch.InheritEnvironment') and the
-- staged directories, if any. 'Nothing' means: inherit unchanged (no
-- staging, no explicit environment), which is what the supervisor did before.
renderEnvironment
  :: Maybe TaskEnvironment
  -> Maybe StagedDirectories
  -> [(String, String)]
  -> Maybe [(String, String)]
renderEnvironment Nothing Nothing _ = Nothing
renderEnvironment Nothing (Just staged) own = Just $ Map.toList $
  -- Inherit, with the staged directories first.
  Map.insert (envKey pathVar) (prepend staged.binDirectory (lookupOwn pathVar)) $
  Map.insert (envKey libraryPathVar) (prepend staged.libraryDirectory (lookupOwn libraryPathVar)) $
  Map.fromList own
  where
    lookupOwn n = lookup (envKey n) own
    -- Without the inherited value's empty entries: an empty entry means the
    -- current folder, for the loader too, and the check wouldn't see it.
    prepend dir = renderSearchPath . (dir :) . maybe [] parseSearchPath
renderEnvironment (Just env) mStaged own = Just $ Map.toList $
  -- The typed search paths replace the inherited ones; inherited thread
  -- variables stay unless a limit is set.
  Map.union explicit (Map.withoutKeys kept (Set.fromList (map envKey [pathVar, libraryPathVar])))
  where
    kept = Map.fromList $ case env.inherited of
      InheritAll       -> own
      InheritOnly keep -> filter ((`Set.member` Set.map envKey keep) . fst) own
    stagedBin = (.binDirectory) <$> mStaged
    stagedLib = (.libraryDirectory) <$> mStaged
    value = \case
      EnvText s -> Just s
      StagedProgramsDirectory -> OsString.toString <$> stagedBin
    explicit = Map.fromList $ concat
      [ [ (envKey n, v) | (n, ev) <- Map.toList env.variables, Just v <- [value ev] ]
      , [ (envKey pathVar, renderSearchPath ps)
        | let ps = maybe id (:) stagedBin env.searchPath, not (null ps) ]
      , [ (envKey libraryPathVar, renderSearchPath ps)
        | let ps = maybe id (:) stagedLib env.libraryPath, not (null ps) ]
      , threadVars blasVars env.threads.blasThreads
      , threadVars openMPVars env.threads.openMPThreads
      ]
    threadVars vars = \case
      Nothing -> []
      Just (ThreadCount k) -> [ (envKey n, show k) | n <- vars ]

envKey :: EnvName -> String
envKey (EnvName s) = s

renderSearchPath :: [OsPath] -> String
renderSearchPath = intercalate ":" . map OsString.toString

-- | The entries of a @PATH@-like value; empty entries dropped.
parseSearchPath :: String -> [OsPath]
parseSearchPath = map OsString.fromString . filter (not . null) . splitOn ":"

-- | The entries of a variable of a rendered environment.
searchPathOf :: EnvName -> [(String, String)] -> [OsPath]
searchPathOf n = maybe [] parseSearchPath . lookup (envKey n)
