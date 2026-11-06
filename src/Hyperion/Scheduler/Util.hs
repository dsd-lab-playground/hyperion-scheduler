{-# LANGUAGE AllowAmbiguousTypes        #-}
{-# LANGUAGE DeriveAnyClass             #-}
{-# LANGUAGE DerivingStrategies         #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedRecordDot        #-}
{-# LANGUAGE RankNTypes                 #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE StaticPointers             #-}
{-# LANGUAGE TypeApplications           #-}
{-# LANGUAGE TypeFamilies               #-}

module Hyperion.Scheduler.Util where

import Control.Exception      (IOException, catch)
import Control.Monad          (forM_, when)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.Maybe             (mapMaybe)
import Data.Text              qualified as Text
import Data.Time.Clock        (NominalDiffTime)
import Data.Typeable          (Proxy (..), Typeable, tyConModule, typeRep,
                               typeRepTyCon)
import GHC.Clock              (getMonotonicTimeNSec)
import System.Directory       (listDirectory)
import System.Posix.IO        (FdOption (CloseOnExec), setFdOption)
import Text.Read              (readMaybe)

-- | Measure real time using monotonic clock.
-- NB: Returns NominalDiffTime as it is used throughout existing code.
-- TODO: reuse this function everywhere (RunTasks.hs) instead of (non-monotonic) getCurrentTime
measureRealTime :: MonadIO m => m a -> m (NominalDiffTime, a)
measureRealTime ma = do
  start <- liftIO $ getMonotonicTimeNSec
  a <- ma
  end <- liftIO $ getMonotonicTimeNSec
  let
    diffNano = end - start
    diffTime = (fromIntegral diffNano) / 1e9
  return (diffTime, a)

-- | typeRepText @MyType = "MyType"
typeRepText :: forall a. Typeable a => Text.Text
typeRepText = Text.pack $ show $ typeRep $ Proxy @a

-- | qualifiedTypeRepText @MyType = "My.Module.MyType". Type arguments, if any,
-- are shown unqualified: their modules can be GHC internals, which move
-- between compiler versions.
qualifiedTypeRepText :: forall a. Typeable a => Text.Text
qualifiedTypeRepText =
  Text.pack $ tyConModule (typeRepTyCon rep) <> "." <> show rep
  where
    rep = typeRep (Proxy @a)

-- | Mark the file descriptors open now close-on-exec, all but stdin, stdout and
-- stderr. Network sockets aren't, so a child process inherits them, and a
-- connection stays open after this process dies, for as long as the child runs.
-- Nothing then notices the death. Sockets opened later aren't covered. Does
-- nothing where @\/proc@ doesn't list them.
closeOpenFdsOnExec :: IO ()
closeOpenFdsOnExec = do
  entries <- listDirectory "/proc/self/fd" `catch` \(_ :: IOException) -> pure []
  forM_ (mapMaybe readMaybe entries) $ \fd -> when (fd > (2 :: Int)) $
    -- The listing's own descriptor has gone by now.
    setFdOption (fromIntegral fd) CloseOnExec True
      `catch` \(_ :: IOException) -> pure ()
