{-# LANGUAGE DeriveAnyClass      #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE NoFieldSelectors    #-}
{-# LANGUAGE OverloadedRecordDot #-}

-- | The little of ELF that staging needs: a program's interpreter (the
-- dynamic loader), and removing @RPATH@ and @RUNPATH@ from a file's dynamic
-- table.
--
-- Removal drops the @DT_RPATH@ and @DT_RUNPATH@ entries before the table's
-- first @DT_NULL@, shifts the others down, and zeroes the freed entries. That
-- is the edit of @patchelf --remove-rpath@; the file's size and layout don't
-- change, and the unused string stays in @.dynstr@. Only 64-bit
-- little-endian files are supported (x86-64, AArch64).
--
-- The parsing functions are pure and total, on the bytes of the regions they
-- read; 'readElfInfo' and 'removeRunPaths' read and write those regions of a
-- file.
module Hyperion.Scheduler.TaskLaunch.Elf
  ( ElfError (..)
  , ElfInfo (..)
  , Region (..)
  , ProgramHeader (..)
  , elfHeaderSize
  , parseElfHeader
  , parseProgramHeaders
  , segment
  , withoutRunPaths
  , dynamicEntries
  , readElfInfo
  , removeRunPaths
  ) where

import Control.Exception      (IOException, try)
import Control.Monad          (unless, when)
import Data.Bits              (shiftL, shiftR, (.|.))
import Data.Binary            (Binary)
import Data.ByteString        (ByteString)
import Data.ByteString        qualified as BS
import Data.ByteString.Char8  qualified as BS8
import Data.Word              (Word16, Word32, Word64)
import GHC.Generics           (Generic)
import Hyperion.OsPath        (OsPath)
import System.File.OsPath     (withBinaryFile)
import System.IO              (Handle, IOMode (..), SeekMode (..), hSeek)

data ElfError
  = NotElf
  | UnsupportedClass Int      -- ^ EI_CLASS; only 2 (64-bit) is supported
  | UnsupportedByteOrder Int  -- ^ EI_DATA; only 1 (little-endian) is supported
  | Truncated String          -- ^ which part ends before its declared size
  | UnsupportedProgramHeaders String
  | NoDynamicTerminator       -- ^ a dynamic table without DT_NULL
  | ElfIOError String
  deriving (Eq, Show, Generic, Binary)

-- | A part of the file: offset and size in bytes.
data Region = Region
  { offset :: Word64
  , size   :: Word64
  }
  deriving (Eq, Show)

data ElfInfo = ElfInfo
  { -- | The path in @PT_INTERP@: the loader of a dynamically linked program.
    -- 'Nothing' for a statically linked program, and for most libraries.
    interpreter :: Maybe ByteString
    -- | Where the dynamic table is (@PT_DYNAMIC@), if there is one.
  , dynamic     :: Maybe Region
  }
  deriving (Eq, Show)

data ProgramHeader = ProgramHeader
  { pType   :: Word32
  , pOffset :: Word64
  , pFileSz :: Word64
  }

-- | Bytes of the ELF64 file header.
elfHeaderSize :: Int
elfHeaderSize = 64

ptDynamic, ptInterp :: Word32
ptDynamic = 2
ptInterp = 3

dtNull, dtRpath, dtRunpath :: Word64
dtNull = 0
dtRpath = 15
dtRunpath = 29

dynEntrySize :: Int
dynEntrySize = 16

-- | Little-endian unsigned integer of @n@ bytes at @i@, if the bytes are there.
word :: Int -> Int -> ByteString -> Maybe Word64
word n i bs
  | i < 0 || i + n > BS.length bs = Nothing
  | otherwise = Just $ foldl' step 0 [n - 1, n - 2 .. 0]
  where step acc k = (acc `shiftL` 8) .|. fromIntegral (BS.index bs (i + k))

word16 :: Int -> ByteString -> Maybe Word16
word16 i = fmap fromIntegral . word 2 i

word32 :: Int -> ByteString -> Maybe Word32
word32 i = fmap fromIntegral . word 4 i

word64 :: Int -> ByteString -> Maybe Word64
word64 = word 8

-- | From the first 'elfHeaderSize' bytes: where the program headers are, as
-- (offset, entry size, count).
parseElfHeader :: ByteString -> Either ElfError (Word64, Int, Int)
parseElfHeader bs = do
  unless (BS.take 4 bs == BS.pack [0x7f, 0x45, 0x4c, 0x46]) $ Left NotElf
  when (BS.length bs < elfHeaderSize) $ Left (Truncated "ELF header")
  let byteAt i = fromIntegral (BS.index bs i) :: Int
  unless (byteAt 4 == 2) $ Left (UnsupportedClass (byteAt 4))
  unless (byteAt 5 == 1) $ Left (UnsupportedByteOrder (byteAt 5))
  maybe (Left (Truncated "ELF header")) Right $ do
    phoff <- word64 0x20 bs
    phentsize <- word16 0x36 bs
    phnum <- word16 0x38 bs
    pure (phoff, fromIntegral phentsize, fromIntegral phnum)

-- | The program headers, from @count@ entries of @entrySize@ bytes.
parseProgramHeaders :: Int -> Int -> ByteString -> Either ElfError [ProgramHeader]
parseProgramHeaders entrySize count bs = do
  -- 0xffff (PN_XNUM) means the count is elsewhere; no program has that many.
  when (count == 0xffff) $ Left (UnsupportedProgramHeaders "PN_XNUM")
  when (count > 0 && entrySize < 56) $
    Left (UnsupportedProgramHeaders ("entry size " <> show entrySize))
  when (BS.length bs < entrySize * count) $ Left (Truncated "program headers")
  traverse header [0 .. count - 1]
  where
    header k = maybe (Left (Truncated "program headers")) Right $ do
      let base = k * entrySize
      ProgramHeader <$> word32 base bs <*> word64 (base + 8) bs <*> word64 (base + 32) bs

-- | The file region of the first segment of the given type.
segment :: Word32 -> [ProgramHeader] -> Maybe Region
segment t headers = case [h | h <- headers, h.pType == t] of
  h : _ -> Just (Region h.pOffset h.pFileSz)
  []    -> Nothing

-- | Entries of a dynamic table, as (tag, value), up to and without the first
-- @DT_NULL@. 'Nothing' if there is no @DT_NULL@ in the bytes.
dynamicEntries :: ByteString -> Maybe [(Word64, Word64)]
dynamicEntries bs = go 0
  where
    go i = do
      tag <- word64 i bs
      val <- word64 (i + 8) bs
      if tag == dtNull then pure [] else ((tag, val) :) <$> go (i + dynEntrySize)

-- | The dynamic table without its @DT_RPATH@ and @DT_RUNPATH@ entries, and the
-- number removed. Same length as the input; bytes from the first @DT_NULL@ on
-- are unchanged.
withoutRunPaths :: ByteString -> Either ElfError (ByteString, Int)
withoutRunPaths bs = do
  entries <- maybe (Left NoDynamicTerminator) Right (dynamicEntries bs)
  let
    isRunPath (tag, _) = tag == dtRpath || tag == dtRunpath
    kept = filter (not . isRunPath) entries
    removed = length entries - length kept
    prefixLength = length entries * dynEntrySize
    encoded = BS.concat [le64 tag <> le64 val | (tag, val) <- kept]
    zeros = BS.replicate (removed * dynEntrySize) 0
  pure (encoded <> zeros <> BS.drop prefixLength bs, removed)

le64 :: Word64 -> ByteString
le64 x = BS.pack [fromIntegral (x `shiftR` (8 * k)) | k <- [0 .. 7]]

-- | Read a file's interpreter and dynamic table location.
readElfInfo :: OsPath -> IO (Either ElfError ElfInfo)
readElfInfo path = catchIO $ withBinaryFile path ReadMode $ \h -> do
  header <- BS.hGet h elfHeaderSize
  case parseElfHeader header of
    Left err -> pure (Left err)
    Right (phoff, entrySize, count) -> do
      bytes <- readRegion h (Region phoff (fromIntegral (entrySize * count)))
      case parseProgramHeaders entrySize count bytes of
        Left err -> pure (Left err)
        Right headers -> do
          interp <- traverse (readRegion h) (segment ptInterp headers)
          pure $ Right ElfInfo
            { interpreter = BS8.takeWhile (/= '\0') <$> interp
            , dynamic = segment ptDynamic headers
            }

-- | Remove @RPATH@ and @RUNPATH@ from the file in place. Returns how many
-- entries were removed; the file is not written if there are none.
removeRunPaths :: OsPath -> IO (Either ElfError Int)
removeRunPaths path = do
  info <- readElfInfo path
  case info of
    Left err -> pure (Left err)
    Right ElfInfo { dynamic = Nothing } -> pure (Right 0)
    Right ElfInfo { dynamic = Just region } -> catchIO $
      withBinaryFile path ReadWriteMode $ \h -> do
        table <- readRegion h region
        if fromIntegral (BS.length table) < region.size
          then pure (Left (Truncated "dynamic table"))
          else case withoutRunPaths table of
            Left err -> pure (Left err)
            Right (_, 0) -> pure (Right 0)
            Right (table', n) -> do
              hSeek h AbsoluteSeek (fromIntegral region.offset)
              BS.hPut h table'
              pure (Right n)

readRegion :: Handle -> Region -> IO ByteString
readRegion h r = do
  hSeek h AbsoluteSeek (fromIntegral r.offset)
  BS.hGet h (fromIntegral r.size)

catchIO :: IO (Either ElfError a) -> IO (Either ElfError a)
catchIO act = try act >>= \case
  Left (e :: IOException) -> pure (Left (ElfIOError (show e)))
  Right r -> pure r
