{-# LANGUAGE AllowAmbiguousTypes   #-}
{-# LANGUAGE DefaultSignatures     #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE NoFieldSelectors      #-}
{-# LANGUAGE OverloadedRecordDot   #-}
{-# LANGUAGE OverloadedStrings     #-}

module Hyperion.Scheduler.Task.IsTask where

import Data.Aeson                  (ToJSON)
import Data.Map.Strict             (Map)
import Data.Map.Strict             qualified as Map
import Data.Maybe                  (isJust)
import Data.Set                    (Set)
import Data.Set                    qualified as Set
import Data.Text                   (Text)
import Data.Time.Clock             (NominalDiffTime)
import Data.Typeable               (Typeable)
import Hyperion                    (Closure, Process)
import Hyperion.Scheduler.FilePath (VirtualFilePath)
import Hyperion.Scheduler.StatKey  (EncodedSummary, SizedTaskFile (..), StatKey,
                                    TaskFile (..), unitSummary, withSize)
import Hyperion.Scheduler.Types    (Estimate (..), FileSize, MemorySize,
                                    NumCPUs, defaultRuntimeEstimate)
import Hyperion.Scheduler.Util     (typeRepText)

-- | We allow minThreads and maxThreads to depend on the stage of the
-- computation (and, at the 'Hyperion.Scheduler.Task.Task.TaskKey' level, on
-- the task config). TODO: Really, minThreads and maxThreads should be able
-- to depend on stats. How do we achieve that?
data RunStage = InitialRun | InProgressRun
  deriving (Eq, Ord, Show)

type Tag = Text

-- Everything Scheduler needs
-- HasTaskHash + IsTask + CanRemoteRunTask + 'taskShape'
class (Typeable a, ToJSON a, Eq a, Ord a) => IsTask a where
--  taskHash :: a -> ByteString
--  default taskHash :: Binary a => a -> ByteString
--  taskHash = hashBase64SafeByteString

  -- | The task's files, stat key and models: everything about it that does
  -- not depend on the sizes of its input files. They share expensive work, so
  -- they are computed together, and a
  -- 'Hyperion.Scheduler.Task.WrappedTask.WrappedTask' and an
  -- 'Hyperion.Scheduler.Task.EstimatedTask.EstimatedTask' compute them once.
  -- Its 'estimate' reuses them for each set of input sizes.
  taskShape :: a -> TaskShape

  -- | Maximum possible threads for the task
  -- TODO: get rid of RunStage?
  taskMaxThreads :: RunStage -> a -> NumCPUs
  taskMaxThreads _ _ = 1
  -- | Minimum possible threads for the task
  taskMinThreads :: RunStage -> a -> NumCPUs
  taskMinThreads _ _ = 1
  taskDefaultPriority   :: a -> Int
  taskDefaultPriority = const 0

  -- | A label indicating the type of task. If Nothing, the task
  -- will be ommitted from progress reports.
  taskTag :: a -> Maybe Tag
  taskTag = defaultTaskTag

  -- | The work of the task, given its number of CPUs. It runs on a fresh
  -- worker with one capability; for parallel Haskell, call
  -- 'Control.Concurrent.setNumCapabilities'.
  taskClosure :: a -> Maybe (NumCPUs -> Closure (Process ()))

  -- | Whether this task is a placeholder (see 'Hyperion.Scheduler.Task.Task.TaskKind')
  -- that must be replaced before running.
  -- 'Hyperion.Scheduler.Task.TaskMap.validateTaskMap' rejects maps still containing placeholders.
  taskIsPlaceholder :: a -> Bool
  taskIsPlaceholder = const False

  -- | For a placeholder task with underlying key of type @k@, recover the
  -- key. This lets replacement machinery (e.g. blocks-3d's expandBlockTasks)
  -- find placeholders of a given key type in a TaskMap without knowing the
  -- resolver or config types hidden inside the task.
  taskPlaceholderKey :: Typeable k => a -> Maybe k
  taskPlaceholderKey _ = Nothing

-- | The info, with its size, of each input file.
type InputInfos = TaskFile -> SizedTaskFile

-- | See 'taskShape'.
data TaskShape = MkTaskShape
  { inputFiles   :: Set TaskFile
  , outputFiles  :: Set TaskFile
    -- | The serialized stat key under which this task's resource usage is
    -- recorded and looked up. For a 'Hyperion.Scheduler.Task.Task.Task' this
    -- is built from the task's own stat key, which is also what the estimates
    -- are computed from.
    --
    -- 'Nothing' means the task has no identity in statistics: it is neither
    -- recorded nor looked up, and its estimates are zero. That is the right
    -- answer for tasks performing no computation (no-ops, placeholders),
    -- whose only measurable quantity would be scheduler bookkeeping latency.
    -- Setting it instead to the whole task encoded as its own stat key would
    -- put every task in a group of one, which no curve can be fitted to.
    -- 'Hyperion.Scheduler.Task.TaskMap.taskInstrumentationGaps' reports tasks
    -- that compute but leave this at 'Nothing'.
  , statKey      :: Maybe StatKey
    -- | The task's own model of its input summary, 'Nothing' if the task has
    -- no stat key. Tasks with equal stat keys must have equal models: a model
    -- is evaluated once per stat key and shared.
    -- 'Hyperion.Scheduler.Task.Task.taskShapeOf' meets this by construction,
    -- since the estimates see only the stat key and the summary.
  , model        :: Maybe (Model ResourceEstimates)
    -- | The models of the output files' sizes, of the producer summary, for
    -- the files with a file stat key. Files with equal file stat keys must
    -- have equal models.
  , outputModels :: Map VirtualFilePath (Model FileSize)
    -- | The estimation for the given infos of the input files: those produced
    -- by other tasks in the map and those on disk (see
    -- 'Hyperion.Scheduler.Task.EstimatedTaskMap.mkEstimatedTaskMap'), or the
    -- measured ones after the task ran.
  , estimate     :: InputInfos -> TaskEstimation
  }

-- | A model of a summary type that is hidden, so that statistics recorded with
-- other summaries can be compared with it without the typed stat key.
-- Decoding is a separate step, so that a decoded summary can be reused.
data Model r = forall s . MkModel
  { decode     :: EncodedSummary -> Maybe s
    -- ^ 'Nothing' if the summary does not decode, e.g. after the summary type
    -- changed.
  , estimateAt :: s -> r
  , isClose    :: s -> s -> Bool
    -- ^ Whether statistics recorded with the second summary can correct the
    -- estimates for the first (see
    -- 'Hyperion.Scheduler.StatKey.closeInputSummaries').
  }

-- | What a task's estimates are, given the sizes of its input files. See
-- 'estimate'.
data TaskEstimation = MkTaskEstimation
  { inputs          :: Set SizedTaskFile
  , outputs         :: Set SizedTaskFile
    -- | Estimated memory in bytes and runtime in seconds, with the provenance
    -- of each. Ordinary tasks give their own model (e.g.
    -- 'estimatesFromModel'); only
    -- 'Hyperion.Scheduler.Task.EstimatedTask.applyStats'
    -- ever reports a figure measured from statistics.
  , estimates       :: ResourceEstimates
    -- | The input summary 'estimates' are computed from, encoded; 'Nothing' if
    -- the task has no stat key.
  , inputSummary    :: Maybe EncodedSummary
    -- | The producer summary the sizes of the output files are computed from,
    -- encoded; 'Nothing' if no output file has a file stat key.
  , producerSummary :: Maybe EncodedSummary
  }

-- | The shape of a task with no stat key: its files and zero estimates. The
-- sizes of its output files are unknown (zero) and ignore its inputs (summary
-- '()').
filesOnlyShape
  :: Set TaskFile  -- ^ inputs
  -> Set TaskFile  -- ^ outputs
  -> TaskShape
filesOnlyShape inputs outputs = MkTaskShape
  { inputFiles   = inputs
  , outputFiles  = outputs
  , statKey      = Nothing
  , model        = Nothing
  , outputModels = Map.empty
  , estimate     = \known -> MkTaskEstimation
      { inputs          = Set.map known inputs
      , outputs         = Set.map (withSize (EstimatedByTask 0)) outputs
      , estimates       = estimatesFromModel 0
      , inputSummary    = Nothing
      , producerSummary =
          if any (isJust . (.fileStatKey)) outputs
          then Just unitSummary
          else Nothing
      }
  }

-- | See 'TaskShape'.
taskStatKey :: IsTask a => a -> Maybe StatKey
taskStatKey = (.statKey) . taskShape

-- | A task's memory and runtime estimates with their provenance.
data ResourceEstimates = MkResourceEstimates
  { memory  :: Estimate MemorySize
  , runtime :: Estimate (NumCPUs -> NominalDiffTime)
  }

-- | Default task tag = its type.
defaultTaskTagForType :: forall a. Typeable a => Maybe Tag
defaultTaskTagForType = Just $ typeRepText @a

defaultTaskTag :: forall a. Typeable a => a -> Maybe Tag
defaultTaskTag _ = defaultTaskTagForType @a

-- | A task's own model: the memory it predicts, and a runtime curve derived
-- from that memory. A task with a runtime model of its own builds
-- 'MkResourceEstimates' directly instead.
estimatesFromModel :: MemorySize -> ResourceEstimates
estimatesFromModel memory = MkResourceEstimates
  { memory  = EstimatedByTask memory
  , runtime = EstimatedByTask (defaultRuntimeEstimate memory)
  }

taskInputPaths :: IsTask a => a -> Set VirtualFilePath
taskInputPaths t = Set.map (.path) (taskShape t).inputFiles

taskOutputPaths :: IsTask a => a -> Set VirtualFilePath
taskOutputPaths t = Set.map (.path) (taskShape t).outputFiles

taskHasClosure :: IsTask a => a -> Bool
taskHasClosure = isJust . taskClosure
