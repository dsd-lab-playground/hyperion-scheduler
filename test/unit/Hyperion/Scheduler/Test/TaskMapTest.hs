{-# LANGUAGE DataKinds                  #-}
{-# LANGUAGE DeriveAnyClass             #-}
{-# LANGUAGE DerivingStrategies         #-}
{-# LANGUAGE DuplicateRecordFields      #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE NoFieldSelectors           #-}
{-# LANGUAGE OverloadedRecordDot        #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE TypeFamilies               #-}

-- | Pure unit tests for TaskMap logic: validateTaskMap, replaceTasks and
-- placeholder tasks. Runs locally, no cluster or filesystem access needed.
module Hyperion.Scheduler.Test.TaskMapTest where

import Control.Exception                        (AssertionFailed (..),
                                                 SomeException, evaluate,
                                                 throwIO, toException)
import Control.Monad                            (unless)
import Control.Monad.Catch                      (MonadThrow (..))
import Data.Aeson                               (ToJSON)
import Data.Binary                              (Binary, encode)
import Data.Either                              (isLeft, isRight)
import Data.IORef                               (IORef, readIORef)
import Data.List.NonEmpty                       qualified as NonEmpty
import Data.Map.Strict                          (Map)
import Data.Map.Strict                          qualified as Map
import Data.Maybe                               (isNothing)
import Data.Set                                 (Set)
import Data.Set                                 qualified as Set
import Data.Text                                qualified as Text
import GHC.Generics                             (Generic)
import Hyperion.OsPath                          (OsPath)
import Hyperion.OsString                        (fromString)
import Hyperion.Scheduler.Config                (Config (..))
import Hyperion.Scheduler.TaskLaunch (defaultTaskLaunch)
import Hyperion.Scheduler.FilePath              (VirtualFilePath (..),
                                                 isUnderLocalStorage)
import Hyperion.Scheduler.StatKey               (FileStatKey,
                                                 IsFileStatKey (..),
                                                 SizedTaskFile (..),
                                                 TaskFile (..),
                                                 ToFileStatKey (..),
                                                 encodeFileStatKey, unitSummary,
                                                 withSize)
import Hyperion.Scheduler.Stats                 (FileStats (..),
                                                 TaskAndFileStats (..),
                                                 TaskStats (..), toTrials)
import Hyperion.Scheduler.Task.EstimatedTask    (EstimatedTask, estimateTask,
                                                 prepareStats, taskInputs,
                                                 taskResourceEstimates)
import Hyperion.Scheduler.Task.EstimatedTaskMap (estimatedTasks,
                                                 mkEstimatedTaskMap)
import Hyperion.Scheduler.Task.IsTask           (InputInfos, IsTask (..),
                                                 ResourceEstimates (..),
                                                 TaskEstimation (..),
                                                 TaskShape (..), filesOnlyShape,
                                                 taskInputPaths,
                                                 taskOutputPaths, taskStatKey)
import Hyperion.Scheduler.Task.Task             (DepKeys, TaskKey (..),
                                                 TaskKind (..), dependencies,
                                                 listTaskKey, outKeys)
import Hyperion.Scheduler.Task.TaskMap          (InstrumentationGap (..),
                                                 TaskMap, placeholdersOfType,
                                                 replaceTasks,
                                                 taskInstrumentationGaps,
                                                 validateNodeLocalInputs,
                                                 validateTaskMap)
import Hyperion.Scheduler.Task.WrappedTask      (WrappedTask, wrapTask)
import Hyperion.Scheduler.TaskFiles             (MonadTaskFiles (..))
import Hyperion.Scheduler.Test.Counted          (counted, newCounter,
                                                 resetCounter)
import Hyperion.Scheduler.Types                 (Estimate (..), FileSize,
                                                 NumCPUs, schedulingEstimate)
import System.IO.Unsafe                         (unsafePerformIO)

-- * A minimal IsTask for building TaskMaps by hand

data TestTask = MkTestTask
  { name          :: String
  , inputPaths    :: Set OsPath
  , outputPaths   :: Set OsPath
  , isPlaceholder :: Bool
  , computes      :: Bool
    -- ^ Whether the task has something to run remotely. The closure itself is
    -- never forced: these tests only ask whether one exists.
  , minThreads    :: NumCPUs
  , maxThreads    :: NumCPUs
  } deriving (Eq, Ord, Show, Generic, ToJSON)

-- | A file's identity in these fixtures is just its path.
newtype PathFileStatKey = MkPathFileStatKey String
  deriving newtype (ToJSON)

instance IsFileStatKey PathFileStatKey

fileStatKeyOfPath :: OsPath -> FileStatKey
fileStatKeyOfPath path = encodeFileStatKey (MkPathFileStatKey (show path))

mkFile :: OsPath -> TaskFile
mkFile path = MkTaskFile
  { path        = VirtualFilePath path
  , fileStatKey = Just $ fileStatKeyOfPath path
  }

instance IsTask TestTask where
  taskShape t         =
    filesOnlyShape (Set.map mkFile t.inputPaths) (Set.map mkFile t.outputPaths)
  taskTag t           = Just (Text.pack t.name)
  taskClosure t
    | t.computes = Just $ error "TestTask closure is never run"
    | otherwise  = Nothing
  taskMinThreads _ t  = t.minThreads
  taskMaxThreads _ t  = t.maxThreads
  taskIsPlaceholder t = t.isPlaceholder

testTask :: String -> [OsPath] -> [OsPath] -> TestTask
testTask name ins outs = MkTestTask
  { name          = name
  , inputPaths    = Set.fromList ins
  , outputPaths   = Set.fromList outs
  , isPlaceholder = False
  , computes      = False
  , minThreads    = 1
  , maxThreads    = 1
  }

-- * A placeholder TaskKey, exercising the real 'TaskKind' machinery

newtype PKey = MkPKey String
  deriving newtype (Eq, Ord, Show, Binary, ToJSON)

type instance DepKeys PKey = '[]

-- A placeholder has no outputs, so it declares no file stat key.
instance ToFileStatKey PKey

instance TaskKey PKey where
  taskKind = PlaceholderTask

-- * Assertions

expect :: String -> Bool -> IO ()
expect label cond = do
  unless cond $ throwIO $ AssertionFailed $ "FAILED: " <> label
  putStrLn $ "ok: " <> label

-- | For tasks with no inputs.
noInputs :: InputInfos
noInputs file = error $ "Unexpected input: " <> show file.path

taskMapOf :: TestTask -> TaskMap TestTask
taskMapOf t = Map.fromList [(t, Set.empty)]

expectValid :: String -> TaskMap TestTask -> IO ()
expectValid label taskMap = case validateTaskMap taskMap of
  Right () -> putStrLn $ "ok: " <> label
  Left err -> throwIO $ AssertionFailed $ "FAILED: " <> label <> ": unexpectedly invalid: " <> show err

expectInvalid :: String -> TaskMap TestTask -> IO ()
expectInvalid label taskMap = case validateTaskMap taskMap of
  Left _   -> putStrLn $ "ok: " <> label
  Right () -> throwIO $ AssertionFailed $ "FAILED: " <> label <> ": unexpectedly valid"

-- * Tests

p1, p2, q, r, s :: OsPath
p1 = fromString "/data/p1"
p2 = fromString "/data/p2"
q  = fromString "/data/q"
r  = fromString "/data/r"
s  = fromString "/data/s"

-- | An input path produced by NO task in the map is assumed to already exist
-- on disk (its producer was pruned by checkCreated): valid.
testPrunedInputIsValid :: IO ()
testPrunedInputIsValid = do
  let
    a = testTask "A" [p1] [q]
    taskMap = Map.fromList [(a, Set.empty)]
  expectValid "input with no producer in map (pruned dependency) is valid" taskMap

-- | An input path produced by a task IN the map, with no dependency edge to
-- it: invalid (this is the broken-graph case).
testMissingEdgeIsInvalid :: IO ()
testMissingEdgeIsInvalid = do
  let
    a = testTask "A" [p1] [q]
    b = testTask "B" [] [p1]
    taskMap = Map.fromList [(a, Set.empty), (b, Set.empty)]
  expectInvalid "input produced in map but not by dependencies is invalid" taskMap

-- | The same graph with the edge present: valid.
testCorrectEdgeIsValid :: IO ()
testCorrectEdgeIsValid = do
  let
    a = testTask "A" [p1] [q]
    b = testTask "B" [] [p1]
    taskMap = Map.fromList [(a, Set.singleton b), (b, Set.empty)]
  expectValid "input produced by a dependency is valid" taskMap

-- | Unreplaced placeholder tasks are rejected.
testUnreplacedPlaceholderIsInvalid :: IO ()
testUnreplacedPlaceholderIsInvalid = do
  let
    d = (testTask "D" [] [p1]) { isPlaceholder = True }
    a = testTask "A" [p1] [q]
    taskMap = Map.fromList [(a, Set.singleton d), (d, Set.empty)]
  expectInvalid "unreplaced placeholder is invalid" taskMap

-- | replaceTasks connects dependents only to the roots of the replacement
-- fragment, not to every task in it.
testReplaceTasksConnectsToRootsOnly :: IO ()
testReplaceTasksConnectsToRootsOnly = do
  let
    d = (testTask "D" [] [p1]) { isPlaceholder = True }   -- placeholder for W
    a = testTask "A" [p1] [q]                             -- consumer
    w = testTask "W" [r] [p1]                             -- fragment root
    x = testTask "X" [] [r]                               -- fragment dependency
    taskMap  = Map.fromList [(a, Set.singleton d), (d, Set.empty)]
    fragment = Map.fromList [(w, Set.singleton x), (x, Set.empty)]
    replaced = replaceTasks (Map.singleton d fragment) taskMap
  expect "placeholder removed from keys" $ not (Map.member d replaced)
  expect "consumer depends exactly on the fragment root" $
    Map.lookup a replaced == Just (Set.singleton w)
  expect "fragment edges preserved" $
    Map.lookup w replaced == Just (Set.singleton x) && Map.lookup x replaced == Just Set.empty
  expectValid "replaced map is valid" replaced

-- | PlaceholderTask semantics at the TaskKey level: output is definitionally
-- the key itself, no dependencies, nothing to forget.
testPlaceholderTaskKeySemantics :: IO ()
testPlaceholderTaskKeySemantics = do
  let key = MkPKey "some-block"
  expect "placeholder outKeys is the key itself" $
    outKeys () key == Set.singleton key
  expect "placeholder has no dependencies" $
    Set.null (dependencies () key)

-- | NoOpTask semantics via ListTaskKey: no outputs, one dependency edge per
-- element.
testNoOpTaskKeySemantics :: IO ()
testNoOpTaskKeySemantics = do
  let listKey = listTaskKey [MkPKey "a", MkPKey "b"]
  expect "list task has no outputs" $
    Set.null (outKeys () listKey)
  expect "list task depends on all its elements" $
    Set.size (dependencies () listKey) == 2

-- | placeholdersOfType finds placeholders via taskPlaceholderKey.
testPlaceholdersOfType :: IO ()
testPlaceholdersOfType = do
  let
    a = testTask "A" [p1] [q]
    taskMap = Map.fromList [(a, Set.empty)]
  -- TestTask never implements taskPlaceholderKey, so nothing is found; the
  -- real Task r k implementation is exercised in the blocks-3d tests.
  expect "placeholdersOfType finds nothing among non-placeholders" $
    null (placeholdersOfType @TestTask @PKey taskMap)
  expect "taskPlaceholderKey defaults to Nothing" $
    isNothing (taskPlaceholderKey @TestTask @PKey a)
  expect "taskInputPaths of test task" $
    taskInputPaths a == Set.singleton (VirtualFilePath p1)

-- | A task that has something to run remotely but allows itself no CPUs would
-- throw when it ran, for want of a worker, so the map is rejected instead. The
-- minimum is a floor rather than the allocation, so such a task need not fail
-- every time -- which is why this is not left to a warning.
testComputeTasksHaveCpus :: IO ()
testComputeTasksHaveCpus = do
  let computing = (testTask "A" [] [q]) { computes = True }
  expectInvalid "a computing task that allows itself no CPUs is invalid" $
    taskMapOf computing { minThreads = 0 }
  expectValid "a computing task that asks for a CPU is valid" $
    taskMapOf computing
  -- A no-op has no closure and is expected to ask for no CPUs.
  expectValid "a task with nothing to run may ask for no CPUs" $
    taskMapOf (testTask "B" [] [r]) { minThreads = 0 }

-- | Instrumentation gaps are reported for tasks that compute, and only those:
-- declaring nothing is correct for a task that performs no computation.
testInstrumentationGaps :: IO ()
testInstrumentationGaps = do
  let
    gapsOf t = Set.fromList $ Map.keys $ taskInstrumentationGaps $
      Map.singleton (estimateTask (prepareStats mempty []) noInputs t) Set.empty
  -- 'filesOnlyShape' estimates its outputs at zero, so this task is reported on
  -- both axes: no stat key, and no declared size for the file it produces.
  expect "a computing task that declares nothing is reported on both axes" $
    gapsOf ((testTask "A" [] [q]) { computes = True })
      == Set.fromList [NoStatKey, ZeroFileSizeEstimate]
  expect "a task that computes nothing is not reported" $
    Set.null (gapsOf (testTask "A" [] [q]))

-- | A maximum below the minimum can be satisfied by no allocation, and the
-- allocator resolves it by capping at the maximum -- handing the task fewer
-- threads than it asked for, or none at all.
testThreadRangesAreOrdered :: IO ()
testThreadRangesAreOrdered = do
  let computing = (testTask "A" [] [q]) { computes = True }
  expectInvalid "maxThreads below minThreads is invalid" $
    taskMapOf computing { minThreads = 2, maxThreads = 1 }
  expectValid "maxThreads equal to minThreads is valid" $
    taskMapOf computing { minThreads = 2, maxThreads = 2 }

-- | A task declaring the size of each output file, to tell whose file info a
-- consumer ends up with.
data SizedTask = MkSizedTask
  { sizedName    :: String
  , sizedInputs  :: [OsPath]
  , sizedOutputs :: [(OsPath, FileSize)]
  , sumsInputs   :: Bool
    -- ^ Whether, given its inputs, each output's size is their total instead.
  } deriving (Eq, Ord, Show, Generic, ToJSON, Binary)

instance IsTask SizedTask where
  taskTag t     = Just (Text.pack t.sizedName)
  taskClosure _ = Nothing
  taskShape t = shape { estimate = estimate }
    where
      shape = filesOnlyShape
        (Set.fromList (map mkFile t.sizedInputs))
        (Set.fromList (map (mkFile . fst) t.sizedOutputs))
      estimate known = estimation { outputs = sizedOutputs }
        where
          estimation = shape.estimate known
          sizedOutputs = Set.fromList
            [ withSize (EstimatedByTask (if t.sumsInputs then total else size))
                (mkFile path)
            | (path, size) <- t.sizedOutputs
            ]
          total = sum $ map (schedulingEstimate . (.fileSize)) $
            Set.toList estimation.inputs

sizedTask :: String -> [OsPath] -> [(OsPath, FileSize)] -> SizedTask
sizedTask name ins outs = MkSizedTask name ins outs False

-- | A filesystem holding files of the given sizes.
newtype Disk a = MkDisk (Map OsPath FileSize -> Either SomeException a)

instance Functor Disk where
  fmap f (MkDisk g) = MkDisk (fmap f . g)

instance Applicative Disk where
  pure x = MkDisk (const (Right x))
  MkDisk f <*> MkDisk g = MkDisk (\files -> f files <*> g files)

instance Monad Disk where
  MkDisk g >>= k = MkDisk $ \files -> g files >>= \x -> runDisk files (k x)

instance MonadThrow Disk where
  throwM e = MkDisk (const (Left (toException e)))

instance MonadTaskFiles Disk where
  taskFileSize path = MkDisk (Right . Map.lookup path)

runDisk :: Map OsPath FileSize -> Disk a -> Either SomeException a
runDisk files (MkDisk f) = f files

-- | The estimated map, or the test fails.
estimateOnDisk
  :: Map OsPath FileSize -> TaskAndFileStats -> TaskMap WrappedTask
  -> IO (TaskMap (EstimatedTask WrappedTask))
estimateOnDisk files stats taskMap =
  either throwIO (pure . estimatedTasks) $
    runDisk files (mkEstimatedTaskMap stats taskMap)

testInputSizes :: IO ()
testInputSizes = do
  let
    producer = wrapTask $ sizedTask "P" [] [(q, 7)]
    -- r is on disk.
    consumer = wrapTask $ (sizedTask "C" [q, r] [(p1, 1)]) { sumsInputs = True }
    root = wrapTask $ sizedTask "R" [p1] []
    taskMap = Map.fromList
      [ (producer, Set.empty)
      , (consumer, Set.singleton producer)
      , (root, Set.singleton consumer)
      ]
    -- The size of q recorded in statistics.
    stats = MkTaskAndFileStats (MkTaskStats Map.empty) $ MkFileStats $
      Map.singleton (fileStatKeyOfPath q) $
        Map.singleton unitSummary (toTrials (NonEmpty.singleton 70))
    inputSizes t = [ (i.path, i.fileSize) | i <- Set.toList (taskInputs t) ]
    isConsumer = (== Just "C") . taskTag
    expected =
      [ (VirtualFilePath q, MeasuredFromStats 70 7)
      , (VirtualFilePath r, EstimatedByTask 3)
      ]
  estimated <- estimateOnDisk (Map.fromList [(r, 3), (q, 1000)]) stats taskMap
  let depSets = Map.elems estimated
  expect "an input takes the file info of the task producing it, after stats; \
         \another input the size on disk" $
    map inputSizes (filter isConsumer (Map.keys estimated)) == [expected]
  expect "a dependency in the map's values has the same inputs" $
    map inputSizes (filter isConsumer (concatMap Set.toList depSets))
      == [expected]
  expect "an output computed from the inputs is what its consumer sees" $
    map inputSizes (filter ((== Just "R") . taskTag) (Map.keys estimated))
      == [[(VirtualFilePath p1, EstimatedByTask 73)]]

-- | A map is validated before it is estimated, so a cycle is rejected rather
-- than estimated with inputs of no known size.
testCycleIsRejected :: IO ()
testCycleIsRejected = do
  let
    a = wrapTask $ sizedTask "A" [q] [(r, 2)]
    b = wrapTask $ sizedTask "B" [r] [(q, 4)]
    taskMap = Map.fromList [(a, Set.singleton b), (b, Set.singleton a)]
  expect "a map with a cycle is rejected" $
    isLeft $ runDisk Map.empty (mkEstimatedTaskMap mempty taskMap)

-- | An input produced by no task in the map must be on disk.
testMissingInputIsRejected :: IO ()
testMissingInputIsRejected = do
  let
    consumer = wrapTask $ sizedTask "C" [r, s] [(p1, 1)]
    taskMap = Map.fromList [(consumer, Set.empty)]
    onDisk files = runDisk files (mkEstimatedTaskMap mempty taskMap)
  expect "an input neither produced in the map nor on disk is rejected" $
    isLeft (onDisk (Map.fromList [(r, 3)]))
  expect "inputs on disk are accepted" $
    isRight (onDisk (Map.fromList [(r, 3), (s, 5)]))

-- | Two task types with the same bytes for the same name.
data TwinTaskA = MkTwinTaskA String
  deriving (Eq, Ord, Show, Generic, ToJSON, Binary)

data TwinTaskB = MkTwinTaskB String
  deriving (Eq, Ord, Show, Generic, ToJSON, Binary)

instance IsTask TwinTaskA where
  taskTag _ = Nothing
  taskClosure _ = Nothing
  taskShape _ = filesOnlyShape Set.empty Set.empty

instance IsTask TwinTaskB where
  taskTag _ = Nothing
  taskClosure _ = Nothing
  taskShape _ = filesOnlyShape Set.empty Set.empty

-- | Tasks of different types are different, even with the same bytes, e.g.
-- keys with a phantom precision.
testWrappedTaskTypeIsHashed :: IO ()
testWrappedTaskTypeIsHashed = do
  expect "the twin tasks have the same bytes" $
    encode (MkTwinTaskA "T") == encode (MkTwinTaskB "T")
  expect "wrapped tasks of different types differ" $
    wrapTask (MkTwinTaskA "T") /= wrapTask (MkTwinTaskB "T")
  expect "wrapped tasks of one type with the same bytes are equal" $
    wrapTask (MkTwinTaskA "T") == wrapTask (MkTwinTaskA "T")

-- | A task with one output, counting how often its shape is built.
data CountedTask = MkCountedTask String
  deriving (Eq, Ord, Show, Generic, ToJSON, Binary)

instance IsTask CountedTask where
  taskTag (MkCountedTask name) = Just (Text.pack name)
  taskClosure _ = Nothing
  taskShape t = counted shapeBuilds t $
    filesOnlyShape Set.empty (Set.singleton (mkFile q))

-- | @f t@, out of the optimizer's sight. The scheduler reads a task's shape in
-- different functions at different times; within one expression the optimizer
-- would merge the reads, and hide a shape that is not cached.
separately :: (t -> b) -> t -> b
separately f t = f t
{-# NOINLINE separately #-}

shapeBuilds :: IORef Int
shapeBuilds = unsafePerformIO newCounter
{-# NOINLINE shapeBuilds #-}

-- | Every way the scheduler reads a task's shape, from estimation to the
-- summaries recorded after the run, reuses one shape: 'WrappedTask' caches it
-- before estimation, and 'EstimatedTask' during the run.
testShapeIsCached :: IO ()
testShapeIsCached = do
  let
    -- What the scheduler reads during a run, each read in its own call.
    readAll t =
      separately taskStatKey t == Nothing
        && separately taskInputPaths t == Set.empty
        && separately taskOutputPaths t == Set.singleton (VirtualFilePath q)
        && schedulingEstimate (separately taskResourceEstimates t).memory == 0
        && isNothing (separately recordedSummary t)
    recordedSummary t = ((taskShape t).estimate noInputs).inputSummary
  -- Bound at run time: a constant task would be floated out and shared, and
  -- its shape built before the counter is reset.
  [wrappedName, estimatedName] <- evaluate ["W", "E"]
  resetCounter shapeBuilds
  estimated <- estimateOnDisk Map.empty mempty $
    Map.singleton (wrapTask (MkCountedTask wrappedName)) Set.empty
  expect "a wrapped task can be estimated and read" $
    all readAll (Map.keys estimated)
  wrappedBuilds <- readIORef shapeBuilds
  expect "a wrapped task's shape is built once" $ wrappedBuilds == 1
  resetCounter shapeBuilds
  expect "an estimated task can be read" $
    readAll $ estimateTask (prepareStats mempty []) noInputs $
      MkCountedTask estimatedName
  estimatedBuilds <- readIORef shapeBuilds
  expect "an estimated task's shape is built once" $ estimatedBuilds == 1

-- | A node-local input must come from a task in the map: the file service
-- could not hand a node-local file from before the run to a consumer.
testNodeLocalInputs :: IO ()
testNodeLocalInputs = do
  let
    local = fromString "/local/x"
    config = MkConfig
      { nodeMemory           = 0
      , nodeLocalStorageSize = 0
      , localStoragePath     = fromString "/local"
      , reportInterval       = 1
      , taskLaunch           = defaultTaskLaunch
      }
    producer = testTask "P" [] [local]
    consumer = testTask "C" [local, q] []
    valid taskMap = isRight
      (validateNodeLocalInputs config taskMap :: Either SomeException ())
  expect "a node-local input produced in the map is valid" $ valid $
    Map.fromList [(producer, Set.empty), (consumer, Set.singleton producer)]
  expect "a node-local input produced by no task is invalid" $
    not (valid (taskMapOf consumer))
  expect "an input that is not node-local may come from disk" $
    valid (taskMapOf (testTask "D" [q] []))

-- | A path is node-local when it is under the local storage path, compared
-- by path components.
testUnderLocalStorage :: IO ()
testUnderLocalStorage = do
  let under root path = isUnderLocalStorage (fromString root) (fromString path)
  expect "a file under the local storage is local" $ under "/scratch/u/job_1" "/scratch/u/job_1/x"
  expect "the local storage itself is local" $ under "/scratch/u/job_1/" "/scratch/u/job_1"
  expect "a non-normalised path under it is local" $ under "/scratch/u/job_1" "/scratch//u/./job_1/x"
  expect "a sibling with a longer name is not local" $ not $ under "/scratch/u/job_1" "/scratch/u/job_12/x"
  expect "a parent is not local" $ not $ under "/scratch/u/job_1" "/scratch/u"
  expect "an empty local storage path contains nothing" $ not $ under "" "/scratch/u/job_1/x"

runTest :: IO ()
runTest = do
  testUnderLocalStorage
  testNodeLocalInputs
  testInputSizes
  testCycleIsRejected
  testMissingInputIsRejected
  testShapeIsCached
  testWrappedTaskTypeIsHashed
  testPrunedInputIsValid
  testMissingEdgeIsInvalid
  testCorrectEdgeIsValid
  testUnreplacedPlaceholderIsInvalid
  testReplaceTasksConnectsToRootsOnly
  testPlaceholderTaskKeySemantics
  testNoOpTaskKeySemantics
  testPlaceholdersOfType
  testComputeTasksHaveCpus
  testThreadRangesAreOrdered
  testInstrumentationGaps
  putStrLn "All TaskMap tests passed."
