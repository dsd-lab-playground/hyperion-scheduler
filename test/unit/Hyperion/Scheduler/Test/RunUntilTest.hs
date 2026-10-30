{-# LANGUAGE TypeApplications #-}

-- | 'runUntil'. Runs a local 'Job'.
module Hyperion.Scheduler.Test.RunUntilTest (runTest) where

import Control.Concurrent            (myThreadId, threadDelay)
import Control.Concurrent.MVar       (newEmptyMVar, putMVar, readMVar, takeMVar)
import Control.Exception             (AssertionFailed (..), ErrorCall (..),
                                      throwIO, throwTo, uninterruptibleMask_)
import Control.Monad                 (forever, unless)
import Control.Monad.Catch           (onException, try)
import Control.Monad.IO.Class        (liftIO)
import Data.Either                   (isLeft)
import Data.IORef                    (atomicModifyIORef', newIORef, readIORef)
import Data.Time.Clock               (NominalDiffTime, addUTCTime,
                                      getCurrentTime)
import Hyperion                      (Job)
import Hyperion.Job                  (runJobLocal')
import Hyperion.Scheduler.RemoteUtil (runUntil, spawnLocalJob)
import System.Timeout                (timeout)

expect :: String -> Bool -> IO ()
expect label cond = do
  unless cond $ throwIO $ AssertionFailed $ "FAILED: " <> label
  putStrLn $ "ok: " <> label

-- | 'runUntil' with a deadline the given number of seconds from now.
runFor :: NominalDiffTime -> Job a -> Job (Maybe a)
runFor seconds action = do
  now <- liftIO getCurrentTime
  runUntil (addUTCTime seconds now) action

runTest :: IO ()
runTest = do
  counter <- newIORef (0 :: Int)
  let
    count :: Job ()
    count = forever $ liftIO $ do
      atomicModifyIORef' counter (\n -> (n + 1, ()))
      threadDelay 1000
  (inTime, late, pastDeadline, countAtStop, countLater, failed) <- runJobLocal' $ do
    inTime <- runFor 10 (pure 'a')
    late <- runFor 0.05 count
    countAtStop <- liftIO $ readIORef counter
    liftIO $ threadDelay 50000
    countLater <- liftIO $ readIORef counter
    pastDeadline <- runFor (-1) count
    failed <- try @_ @ErrorCall $ runFor 10 (liftIO (throwIO (ErrorCall "test")))
    pure (inTime, late, pastDeadline, countAtStop, countLater, failed)
  expect "an action done in time returns its result" $ inTime == Just 'a'
  expect "an action not done in time returns Nothing" $ late == Nothing
  expect "a slow action ran until the deadline" $ countAtStop > 0
  expect "the action has stopped when runUntil returns" $ countLater == countAtStop
  expect "a deadline in the past returns Nothing" $ pastDeadline == Nothing
  expect "an exception of the action is rethrown" $ isLeft failed
  (interrupted, stopped) <- runJobLocal' $ do
    callerVar <- liftIO newEmptyMVar
    callerDone <- liftIO newEmptyMVar
    stoppedVar <- liftIO newEmptyMVar
    let
      -- The stop exception reaches the action only after the caller of
      -- 'runUntil' is interrupted.
      action :: Job ()
      action = liftIO $
        (uninterruptibleMask_ (threadDelay 300000) >> threadDelay 5000000)
          `onException` putMVar stoppedVar ()
    _ <- spawnLocalJob $ do
      liftIO $ myThreadId >>= putMVar callerVar
      result <- try @_ @ErrorCall $ runFor 0.1 action
      liftIO $ putMVar callerDone (isLeft result)
    liftIO $ do
      threadDelay 200000
      readMVar callerVar >>= flip throwTo (ErrorCall "interrupt")
      interrupted <- timeout 50000 $ takeMVar callerDone
      stopped <- timeout 2000000 $ takeMVar stoppedVar
      pure (interrupted, stopped)
  expect "an interrupted runUntil throws at once" $ interrupted == Just True
  expect "the action is stopped after runUntil is interrupted" $ stopped == Just ()
  putStrLn "All runUntil tests passed."
