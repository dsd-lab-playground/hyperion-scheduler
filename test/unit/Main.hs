-- | Unit tests. No cluster, no SLURM: @stack test@. Only 'TaskFilesTest',
-- 'ComputeValueTest', 'WriteFileAtomicTest', 'StoreFileTest', 'ElfTest' and
-- 'StagingTest' touch the filesystem, in a temporary directory; 'StagingTest'
-- also runs the system's dynamic loader on this executable.
module Main where

import Hyperion.Scheduler.Test.ComputeValueTest    qualified as ComputeValueTest
import Hyperion.Scheduler.Test.ElfTest             qualified as ElfTest
import Hyperion.Scheduler.Test.EstimateTest        qualified as EstimateTest
import Hyperion.Scheduler.Test.InputSummaryTest    qualified as InputSummaryTest
import Hyperion.Scheduler.Test.StagingTest         qualified as StagingTest
import Hyperion.Scheduler.Test.StorageStrategyTest qualified as StorageStrategyTest
import Hyperion.Scheduler.Test.StoreFileTest       qualified as StoreFileTest
import Hyperion.Scheduler.Test.TaskEnvironmentTest qualified as TaskEnvironmentTest
import Hyperion.Scheduler.Test.TaskFilesTest       qualified as TaskFilesTest
import Hyperion.Scheduler.Test.TaskMapTest         qualified as TaskMapTest
import Hyperion.Scheduler.Test.WriteFileAtomicTest qualified as WriteFileAtomicTest

main :: IO ()
main = do
  TaskMapTest.runTest
  EstimateTest.runTest
  InputSummaryTest.runTest
  TaskFilesTest.runTest
  ComputeValueTest.runTest
  WriteFileAtomicTest.runTest
  StoreFileTest.runTest
  StorageStrategyTest.runTest
  ElfTest.runTest
  TaskEnvironmentTest.runTest
  StagingTest.runTest
