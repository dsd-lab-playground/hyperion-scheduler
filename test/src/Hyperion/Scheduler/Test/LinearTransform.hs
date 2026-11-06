{-# LANGUAGE ApplicativeDo         #-}
{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE DeriveAnyClass        #-}
{-# LANGUAGE DerivingStrategies    #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE NoFieldSelectors      #-}
{-# LANGUAGE OverloadedRecordDot   #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE StaticPointers        #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE UndecidableInstances  #-}

-- | Scheduler test computing a chain of linear transformations
-- @x_{i+1} = A_i . x_i@, with one task per product @A_ik x_k@.
-- The point is to generate many small tasks with many dependencies;
-- this is not how you should multiply a matrix in practice.
--
-- This module defines the problem only. The driver ("Main") supplies the
-- 'Scheduler.Config' and the base directory, so nothing here knows about
-- SLURM or the command line.
module Hyperion.Scheduler.Test.LinearTransform where

import Bootstrap.Build           (Fetches (..))
import Control.Exception         (AssertionFailed (..))
import Control.Monad             (unless)
import Control.Monad.IO.Class    (liftIO)
import Data.Aeson                (FromJSON, ToJSON)
import Data.Binary               (Binary)
import Data.Matrix               (Matrix)
import Data.Matrix               qualified as Matrix
import Data.Maybe                (isJust)
import Data.Traversable          (for)
import Data.Typeable             (Typeable)
import Data.Vector               (Vector)
import Data.Vector               qualified as Vector
import GHC.Generics              (Generic)
import Hyperion
import Hyperion.Log              qualified as Log
import Hyperion.OsPath           (OsPath, (<.>), (</>))
import Hyperion.OsString         (fromString, showOs)
import Hyperion.Scheduler        (IsFileStatKey (..), IsStatKey (..),
                                  MemorySize, PathResolver (..),
                                  ToFileStatKey (..), TotalInputFileSize (..),
                                  encodeJsonFileAtomic, recordToStats,
                                  writeTaskStats)
import Hyperion.Scheduler        qualified as Scheduler
import Hyperion.Scheduler.Config qualified as Scheduler
import Hyperion.Scheduler.Task   (ComputeValue (..), DepKeys, FetchesKey,
                                  ReadValue, SaveValue, TaskKey (..),
                                  TaskKind (..), ValueType, getPath, mkTaskMap,
                                  readValueM, saveValueM)
import System.Directory.OsPath   (createDirectoryIfMissing, doesPathExist,
                                  removePathForcibly)
import System.Environment        (lookupEnv)
import System.Process            (callProcess)

-- * The problem

-- | A chain of 'numLayers' matrices applied to 'inputVector'.
class (ToJSON a, Ord a, Show a, Binary a, Typeable a) => LinearTransformContext a where
  numLayers :: a -> Int
  layerMatrix :: a -> Closure (Int -> Matrix Int)
  inputVector :: a -> Closure (Vector Int)
  layerMatrixDims :: Int -> a -> (Int, Int)

layerInputVectorLength :: LinearTransformContext a => Int -> a -> Int
layerInputVectorLength layerIndex ctx = ncols
  where (_nrows, ncols) = layerMatrixDims layerIndex ctx

layerOutputVectorLength :: LinearTransformContext a => Int -> a -> Int
layerOutputVectorLength layerIndex ctx = nrows
  where (nrows, _ncols) = layerMatrixDims layerIndex ctx

-- | Every task here multiplies or sums a handful of 'Int's, so what a node must
-- hold while one runs is the worker's own resident set rather than anything the
-- task allocates -- see 'memoryEstimate', which is measured per worker and
-- summed per concurrent task.
--
-- Measured at 86-95 MiB across tasks on the machine this was written on. The
-- figure is not portable, which is why the scheduler prefers recorded
-- statistics to it as soon as there are any.
workerBaselineMemory :: MemorySize
workerBaselineMemory = 96 * 1024 * 1024

-- * Key types

-- | A single product @A_ik x_k@.
data MultiplyKey a = MkMultiplyKey
  { layerIndex :: Int
  , row        :: Int
  , col        :: Int
  , ctx        :: a
  }
  deriving (Eq, Ord, Show, Generic, Binary, ToJSON, ReadValue, SaveValue)

instance (Typeable a, Static (Binary a)) => Static (Binary (MultiplyKey a)) where
  closureDict = static (\Dict -> Dict) `cAp` closureDict @(Binary a)

type instance ValueType (MultiplyKey a) = Int

type instance DepKeys (MultiplyKey a) = '[VectorKey a]

-- | Every multiply is the same scalar product, so they all share one group.
data MultiplyStatKey = MkMultiplyStatKey
  deriving (Eq, Ord, Show, Generic, ToJSON, FromJSON)

instance IsStatKey MultiplyStatKey where
  memoryEstimate _ _ = workerBaselineMemory

computeMultiplyM :: (Applicative f, FetchesKey (VectorKey a) f, LinearTransformContext a) => MultiplyKey a -> f (Process Int)
computeMultiplyM key = do
  v <- fVector
  pure $ do
    getLayerMatrix' <- unClosure (layerMatrix key.ctx)
    let
      matrix = getLayerMatrix' key.layerIndex
      -- NB: Data.Matrix elements are 1-indexed, but Data.Vector are 0-indexed
      m_ik = Matrix.getElem (key.row + 1) (key.col + 1) matrix
      v_k = v Vector.! key.col
    pure $ m_ik * v_k
  where
    fVector = fetch MkVectorKey
      { layerIndex = key.layerIndex - 1
      , length = layerInputVectorLength key.layerIndex key.ctx
      , ctx = key.ctx
      }

instance LinearTransformContext a => ComputeValue (MultiplyKey a) where
  computeValueM _ _ = computeMultiplyM

instance
  ( LinearTransformContext a
  , ToFileStatKey (MultiplyKey a)
  , ToFileStatKey (VectorKey a)
  ) => TaskKey (MultiplyKey a) where
  type StatKeyOf (MultiplyKey a) = MultiplyStatKey
  toStatKey _ _ = Just MkMultiplyStatKey
  tag _ = Just "Multiply"

-- | A file holding one serialized 'Int'. Both products and vector elements
-- write one, and a file's size is a property of what was computed rather than
-- of which task computed it, so they share a single group -- which sees every
-- such file in the run rather than a fraction of them.
--
-- Projecting a key onto itself would instead put every file in a group of its
-- own, which can report the size of a file already produced but can never
-- predict a new one.
data IntFileStatKey = MkIntFileStatKey
  deriving (Eq, Ord, Show, Generic, ToJSON, FromJSON)

instance IsFileStatKey IntFileStatKey where
  fileSizeEstimate _ _ = 8

-- NB: needs nothing of the key, hence nothing of its context either.
instance ToFileStatKey (MultiplyKey a) where
  type FileStatKeyOf (MultiplyKey a) = IntFileStatKey
  fileStatKeyOf _ = Just MkIntFileStatKey

-- | One element of an output vector, @sum_k A_ik x_k@.
data VectorElementKey a = MkVectorElementKey
  { layerIndex :: Int
  , index      :: Int
  , ctx        :: a
  }
  deriving (Eq, Ord, Show, Generic, Binary, ToJSON, ReadValue, SaveValue)

instance (Typeable a, Static (Binary a)) => Static (Binary (VectorElementKey a)) where
  closureDict = static (\Dict -> Dict) `cAp` closureDict @(Binary a)

type instance ValueType (VectorElementKey a) = Int

type instance DepKeys (VectorElementKey a) = '[MultiplyKey a]

-- | An element is a sum over the layer's input vector, so elements of equal
-- input length share statistics regardless of which layer or index they are.
newtype VectorElementStatKey = MkVectorElementStatKey { inputLength :: Int }
  deriving stock (Eq, Ord, Show)
  deriving newtype (ToJSON, FromJSON)

instance IsStatKey VectorElementStatKey where
  memoryEstimate _ _ = workerBaselineMemory

vectorElementInputKeys :: LinearTransformContext a => VectorElementKey a -> [MultiplyKey a]
vectorElementInputKeys key = map mkKey ks where
  ncols = layerInputVectorLength key.layerIndex key.ctx
  ks = [0..ncols-1]
  mkKey k = MkMultiplyKey
    { layerIndex = key.layerIndex
    , row = key.index
    , col = k
    , ctx = key.ctx
    }

-- | Pretend this shells out to @vector_element.sh in_1.bin .. in_n.bin out.bin@.
-- It exists to exercise 'CustomTask', i.e. the case where we cannot define
-- @instance ComputeValue (VectorElementKey a)@.
runVectorElementScript :: [(MultiplyKey a, OsPath)] -> (VectorElementKey a, OsPath) -> Process ()
runVectorElementScript inputs (outputKey, outputPath) = do
  Log.info "runVectorElementScript" (map snd inputs, outputPath)
  liftIO $ runSlowElement outputKey.layerIndex outputKey.index
  values <- mapM (uncurry readValueM) inputs
  saveValueM outputKey outputPath (sum values)

-- | Fault tests only: with @HYPERION_SCHEDULER_TEST_SLOW_ELEMENT=layer:index:seconds@,
-- that element's task first runs @sleep seconds@ as an external program, as a
-- long native computation does: the worker waits and sends nothing meanwhile.
-- An index of @*@ means all the layer's elements, so that some run on each node.
-- With @HYPERION_SCHEDULER_TEST_SLOW_ELEMENT_DONE=file@ it stops once that file
-- exists and writes it when it has run, so that a later pass or retry is quick.
runSlowElement :: Int -> Int -> IO ()
runSlowElement layer index = lookupEnv "HYPERION_SCHEDULER_TEST_SLOW_ELEMENT" >>= \spec ->
  case words . map (\c -> if c == ':' then ' ' else c) <$> spec of
    Just [l, i, seconds] | read l == layer, i == "*" || read i == index ->
      lookupEnv "HYPERION_SCHEDULER_TEST_SLOW_ELEMENT_DONE" >>= \done -> case done of
        Nothing   -> callProcess "sleep" [seconds]
        Just file -> do
          ran <- doesPathExist (fromString file)
          unless ran $ do
            callProcess "sleep" [seconds]
            writeFile file "ran\n"
    _ -> pure ()

instance
  ( LinearTransformContext a
  , ToFileStatKey (MultiplyKey a)
  , ToFileStatKey (VectorElementKey a)
  ) => TaskKey (VectorElementKey a) where
  type StatKeyOf (VectorElementKey a) = VectorElementStatKey
  toStatKey _ key = Just MkVectorElementStatKey
    { inputLength = layerInputVectorLength key.layerIndex key.ctx }
  tag _ = Just "VectorElement"
  taskKind = CustomTask $ \_numCpus _config key -> do
    let keys = vectorElementInputKeys key
    inputs <- zip keys <$> for keys getPath
    outputPath <- getPath key
    pure $ do
      liftIO $ Log.info "Computing vector element" (key, inputs, outputPath)
      runVectorElementScript inputs (key, outputPath)

-- | An element is one Int, exactly as a product is, so it shares that group.
-- Its /task/ statistics cannot be shared with a product's: runtime grows with
-- the number of terms summed, while the file stays one Int.
instance ToFileStatKey (VectorElementKey a) where
  type FileStatKeyOf (VectorElementKey a) = IntFileStatKey
  fileStatKeyOf _ = Just MkIntFileStatKey

-- | The vector @x_i@ entering layer @i@.
data VectorKey a = MkVectorKey
  { layerIndex :: Int
  , length     :: Int
  , ctx        :: a
  }
  deriving (Eq, Ord, Show, Generic, Binary, ToJSON, ReadValue, SaveValue)

instance (Typeable a, Static (Binary a)) => Static (Binary (VectorKey a)) where
  closureDict = static (\Dict -> Dict) `cAp` closureDict @(Binary a)

type instance ValueType (VectorKey a) = Vector Int

type instance DepKeys (VectorKey a) = '[VectorElementKey a]

computeVectorM :: (Applicative f, FetchesKey (VectorElementKey a) f, LinearTransformContext a) => VectorKey a -> f (Process (Vector Int))
computeVectorM key
  | key.layerIndex == 0 = pure $ unClosure $ inputVector key.ctx
  | otherwise = fmap pure $ sequenceA $
    Vector.generate key.length $ \i ->
      fetch MkVectorElementKey
        { layerIndex = key.layerIndex
        , index = i
        , ctx = key.ctx
        }

instance LinearTransformContext a => ComputeValue (VectorKey a) where
  computeValueM _ _ = computeVectorM

instance
 ( LinearTransformContext a
 , ToFileStatKey (VectorElementKey a)
 , ToFileStatKey (VectorKey a)
 ) => TaskKey (VectorKey a) where
  type StatKeyOf (VectorKey a) = VectorStatKey
  toStatKey _ key = Just MkVectorStatKey { size = key.length }
  tag _ = Just "Vector"

-- | Here the task stat key does double duty: a vector's file size and the work
-- of gathering it both depend on its length and nothing else, so one projection
-- -- the very same one 'toStatKey' makes -- serves both, and there is no second
-- type to declare. Reuse a task stat key only when that is true: an element's
-- runtime and file size disagree, so 'VectorElementKey' cannot.
instance ToFileStatKey (VectorKey a) where
  type FileStatKeyOf (VectorKey a) = VectorStatKey
  fileStatKeyOf key = Just MkVectorStatKey { size = key.length }

-- | A serialized @Vector Int@: one Int per element, after a length prefix.
instance IsFileStatKey VectorStatKey where
  fileSizeEstimate key _ = fromIntegral (8 * key.size + 8)

-- | Vectors of equal size share statistics, whichever layer they belong to.
newtype VectorStatKey = MkVectorStatKey { size :: Int }
  deriving stock (Eq, Ord, Show)
  deriving newtype (ToJSON, FromJSON)

-- | The memory model reads the size of the element files the vector is
-- gathered from, so that the second pass must match statistics recorded with
-- the input summary measured in the first.
instance IsStatKey VectorStatKey where
  type InputSummary VectorStatKey = TotalInputFileSize
  memoryEstimate _ (MkTotalInputFileSize total) =
    workerBaselineMemory + fromIntegral total

-- * A concrete problem: cyclic shift

-- | Take the vector @[1..dim]@ and shift it cyclically by @shift@, one
-- position per layer. The output is @[shift+1, .., dim, 1, .., shift]@.
-- @shift@ therefore controls the depth of the task graph and @dim@ its width.
data CyclicShiftProblem = MkCyclicShiftProblem
  { shift :: Int
  , dim   :: Int
  }
  deriving (Eq, Ord, Generic, Binary, ToJSON, Show)

instance Static (Binary CyclicShiftProblem) where
  closureDict = static Dict
instance Static (ToJSON CyclicShiftProblem) where
  closureDict = static Dict

instance LinearTransformContext CyclicShiftProblem where
  numLayers x = x.shift
  layerMatrix x = static getLayerMatrix `cAp` cPure x
  inputVector x = static getInputVector `cAp` cPure x
  layerMatrixDims _layer x = (x.dim, x.dim)

instance Static (LinearTransformContext CyclicShiftProblem) where
  closureDict = static Dict

getInputVector :: CyclicShiftProblem -> Vector Int
getInputVector x = Vector.enumFromN 1 x.dim

getOutputVector :: CyclicShiftProblem -> Vector Int
getOutputVector x = end Vector.++ beg where
  (beg, end) = Vector.splitAt ((-x.shift) `mod` x.dim) $ getInputVector x

-- | Each layer performs a cyclic shift by 1, i.e. 12345 -> 51234
getLayerMatrix :: CyclicShiftProblem -> Int -> Matrix Int
getLayerMatrix x _layer = Matrix.joinBlocks (tl,tr,bl,br) where
  tl = Matrix.zero 1 (x.dim - 1)
  tr = Matrix.identity 1
  bl = Matrix.identity (x.dim - 1)
  br = Matrix.zero (x.dim - 1) 1

-- * Where the task files go

-- | Intermediate values go to node-local storage, the final vector to 'outDir'.
data LinearPathResolver = MkLinearPathResolver
  { outDir  :: OsPath
  , tempDir :: OsPath
  }
  deriving (Generic, Binary, ToJSON, Show, Eq, Ord)

instance PathResolver LinearPathResolver (MultiplyKey a) where
  resolvePath r key =
    r.tempDir </>
    "layer_" <> showOs key.layerIndex </>
    "mul_" <> showOs key.row <> "_" <> showOs key.col <.> "bin"

instance PathResolver LinearPathResolver (VectorElementKey a) where
  resolvePath r key =
    r.tempDir </>
    "layer_" <> showOs key.layerIndex </>
    "vec_" <> showOs key.index <.> "bin"

instance (LinearTransformContext a) => PathResolver LinearPathResolver (VectorKey a) where
  resolvePath r key = baseDir </>
    "layer_" <> showOs key.layerIndex </>
    "vec.bin"
    where
      baseDir = if numLayers key.ctx == key.layerIndex
        then r.outDir
        else r.tempDir

-- * Static dictionaries
--
-- Needed because tasks are shipped to workers as closures.

instance Static (Binary LinearPathResolver) where
  closureDict = static Dict

instance Typeable a => Static (PathResolver LinearPathResolver (MultiplyKey a)) where
  closureDict = static Dict
instance Typeable a => Static (PathResolver LinearPathResolver (VectorElementKey a)) where
  closureDict = static Dict
instance (Typeable a, Static(LinearTransformContext a)) => Static (PathResolver LinearPathResolver (VectorKey a)) where
  closureDict = static (\Dict -> Dict) `cAp` closureDict @(LinearTransformContext a)

instance Typeable a => Static (ToFileStatKey (MultiplyKey a)) where
  closureDict = static Dict
instance Typeable a => Static (ToFileStatKey (VectorElementKey a)) where
  closureDict = static Dict
instance Typeable a => Static (ToFileStatKey (VectorKey a)) where
  closureDict = static Dict

instance
  ( Typeable a
  , Static(LinearTransformContext a)
  , Static(ToJSON a)
  ) => Static (TaskKey (MultiplyKey a)) where
  closureDict = static (\Dict -> Dict) `cAp` closureDict @(LinearTransformContext a, ToJSON a)

instance
  ( Typeable a
  , Static(LinearTransformContext a)
  , Static(ToJSON a)
  ) => Static (TaskKey (VectorElementKey a)) where
  closureDict = static (\Dict -> Dict) `cAp` closureDict @(LinearTransformContext a, ToJSON a)

instance
  ( Typeable a
  , Static(LinearTransformContext a)
  , Static(ToJSON a)
  ) => Static (TaskKey (VectorKey a)) where
  closureDict = static (\Dict -> Dict) `cAp` closureDict @(LinearTransformContext a, ToJSON a)

-- * The job

-- | Build the task map for @problem@, run it twice under @baseDir@, and check
-- each result against 'getOutputVector'.
--
-- The second pass is scheduled from the statistics the first one recorded, so
-- that the whole loop -- record, aggregate, write, read back, estimate -- runs
-- for real rather than only in unit tests. Each pass computes in its own
-- directory, so the artifacts of both survive for inspection; the two share
-- statistics because a stat key holds no paths.
--
-- The scheduler config is passed as a 'Job' action (rather than a value) so
-- that it can be evaluated on the node that actually runs the job: on a
-- cluster it reads @SLURM_JOB_ID@ to find node-local storage.
linearTransformJob :: Job Scheduler.Config -> OsPath -> CyclicShiftProblem -> Job ()
linearTransformJob getSchedulerConfig baseDir problem = do
  schedulerConfig <- getSchedulerConfig
  let
    relDir = "shift_" <> showOs problem.shift <> "_dim_" <> showOs problem.dim
    problemDir = baseDir </> relDir
    resolverFor pass = MkLinearPathResolver
      { outDir = problemDir </> pass
      , tempDir = schedulerConfig.localStoragePath </> relDir </> pass
      }
    outputVectorKey = MkVectorKey
      { layerIndex = problem.shift
      , length = problem.dim
      , ctx = problem
      }
    taskStatsFile = problemDir </> "task_stats.json"

    runPass
      :: OsPath
      -> Scheduler.TaskAndFileStats
      -> Job [Scheduler.TaskRecord Scheduler.WrappedTask]
    runPass pass stats = do
      let resolver = resolverFor pass
      Log.info "Running linear transform test" (problem, resolver)
      liftIO $ do
        removePathForcibly resolver.outDir
        createDirectoryIfMissing True resolver.outDir
      taskMap <- Scheduler.runTaskFiles schedulerConfig $
        Scheduler.mkEstimatedTaskMap stats
          =<< mkTaskMap resolver () outputVectorKey
      taskRecords <- Scheduler.runTasks schedulerConfig taskMap
      checkOutputVector resolver
      pure taskRecords

    checkOutputVector resolver = do
      outputValue <-
        readValueM outputVectorKey (resolvePath resolver outputVectorKey)
      Log.info "Computed output vector" outputValue
      let expectedValue = getOutputVector problem
      unless (expectedValue == outputValue) $
        Log.throw $ AssertionFailed $
          "Wrong output vector for problem: " <> show problem <>
          ": expected: " <> show expectedValue <>
          ": got: " <> show outputValue

  -- Nothing is known about these tasks yet, so every estimate is the task's
  -- own model.
  firstRecords <- runPass "from_model" mempty
  Log.info "Writing task records to file" (problemDir </> "task_records.json")
  encodeJsonFileAtomic (problemDir </> "task_records.json") firstRecords
  writeTaskStats taskStatsFile (foldMap recordToStats firstRecords)

  stats <- Scheduler.readTaskStats [taskStatsFile]
  secondRecords <- runPass "from_stats" stats
  encodeJsonFileAtomic
    (problemDir </> "task_records_from_stats.json") secondRecords
  assertScheduledFromStats stats secondRecords

-- | Every task with an identity in statistics must have been scheduled from
-- them, which is the point of having recorded them. A task without a stat key
-- is not looked up at all, so it is not expected to match.
assertScheduledFromStats
  :: Scheduler.TaskAndFileStats
  -> [Scheduler.TaskRecord Scheduler.WrappedTask]
  -> Job ()
assertScheduledFromStats stats records = unless (null unmeasured) $
  Log.throw $ AssertionFailed $
    "Tasks with a stat key were not scheduled from the recorded statistics: " <>
    show unmeasured
  where
    unmeasured =
      [ (Scheduler.taskTag record.task, record.taskEstimates)
      | record <- records
      , Just statKey <- [record.taskStatKey]
      , not (Scheduler.isFromStats record.taskEstimates.runtime)
        -- Memory statistics exist only where some earlier run recorded a memory
        -- figure, so require them only where these statistics hold one. Asking
        -- whether this run measured memory would be the wrong question: the two
        -- passes need not allocate the same CPUs, and a task that runs on no
        -- worker reports no memory.
        || (recordsMemory statKey
            && not (Scheduler.isFromStats record.taskEstimates.memory))
      ]
    recordsMemory statKey = any (isJust . Scheduler.maxMemory) $
      Scheduler.lookupTaskStats statKey stats
