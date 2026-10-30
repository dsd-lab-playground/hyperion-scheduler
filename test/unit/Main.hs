-- | Unit tests. No cluster, no SLURM: @stack test@. Only 'TaskFilesTest',
-- 'ComputeValueTest', 'WriteFileAtomicTest' and 'StoreFileTest' touch the
-- filesystem, in a temporary directory.
module Main where

import Hyperion.Scheduler.Test.ComputeValueTest    qualified as ComputeValueTest
import Hyperion.Scheduler.Test.EstimateTest        qualified as EstimateTest
import Hyperion.Scheduler.Test.InputSummaryTest    qualified as InputSummaryTest
import Hyperion.Scheduler.Test.RunUntilTest        qualified as RunUntilTest
import Hyperion.Scheduler.Test.StorageStrategyTest qualified as StorageStrategyTest
import Hyperion.Scheduler.Test.StoreFileTest       qualified as StoreFileTest
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
  RunUntilTest.runTest
