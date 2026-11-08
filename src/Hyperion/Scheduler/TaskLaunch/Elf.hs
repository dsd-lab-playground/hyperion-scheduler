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
  , ElfPart (..)
  , ElfInfo (..)
  , Region (..)
  , ProgramHeaderTable (..)
  , ProgramHeader (..)
  , elfHeaderSize
  , parseElfHeader
  , parseProgramHeaders
  , segment
  , DynamicEntry (..)
  , WithoutRunPaths (..)
  , withoutRunPaths
  , dynamicEntries
  , readElfInfo
  , removeRunPaths
  ) where

import Control.Exception      (IOException, try)
import Control.Monad          (unless, when)
import Control.Monad.Except   (ExceptT (..), runExceptT, throwError)
import Control.Monad.IO.Class (liftIO)
import Data.Bits              (shiftL, shiftR, (.|.))
import Data.Binary            (Binary)
import Data.ByteString        (ByteString)
import Data.ByteString        qualified as BS
import Data.ByteString.Char8  qualified as BS8
import Data.Word              (Word16, Word32, Word64)
import GHC.Generics           (Generic)
import Hyperion.OsPath        (OsPath)
import System.File.OsPath     (withBinaryFile)
import System.IO              (Handle, IOMode (..), SeekMode (..), hFileSize,
                               hSeek)

-- | A part of an ELF file that can be missing.
data ElfPart
  = ElfHeaderPart
  | ProgramHeadersPart
  | InterpreterPart
  | DynamicTablePart
  deriving (Eq, Show, Generic, Binary)

data ElfError
  = NotElf
  | UnsupportedClass Int      -- ^ EI_CLASS; only 2 (64-bit) is supported
  | UnsupportedByteOrder Int  -- ^ EI_DATA; only 1 (little-endian) is supported
  | Truncated ElfPart         -- ^ it ends before its declared size, or beyond the file
  | TooManyProgramHeaders     -- ^ e_phnum is PN_XNUM: the count is elsewhere; no program has that many
  | ProgramHeaderTooSmall Int -- ^ e_phentsize, below the size of an ELF64 program header
  | NoDynamicTerminator       -- ^ a dynamic table without DT_NULL
  | ElfIOError String         -- ^ the operating system's message
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

-- | Where the program headers are, from the file header.
data ProgramHeaderTable = ProgramHeaderTable
  { tableOffset :: Word64
  , entrySize   :: Int
  , entryCount  :: Int
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

-- | Bytes of an ELF64 program header.
programHeaderSize :: Int
programHeaderSize = 56

-- | @e_phnum@ when the count doesn't fit in the file header.
pnXNum :: Int
pnXNum = 0xffff

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

-- | From the first 'elfHeaderSize' bytes: where the program headers are.
parseElfHeader :: ByteString -> Either ElfError ProgramHeaderTable
parseElfHeader bs = do
  unless (BS.take 4 bs == BS.pack [0x7f, 0x45, 0x4c, 0x46]) $ Left NotElf
  when (BS.length bs < elfHeaderSize) $ Left (Truncated ElfHeaderPart)
  let byteAt i = fromIntegral (BS.index bs i) :: Int
  unless (byteAt 4 == 2) $ Left (UnsupportedClass (byteAt 4))
  unless (byteAt 5 == 1) $ Left (UnsupportedByteOrder (byteAt 5))
  table <- maybe (Left (Truncated ElfHeaderPart)) Right $ do
    phoff <- word64 0x20 bs
    phentsize <- word16 0x36 bs
    phnum <- word16 0x38 bs
    pure $ ProgramHeaderTable phoff (fromIntegral phentsize) (fromIntegral phnum)
  when (table.entryCount == pnXNum) $ Left TooManyProgramHeaders
  when (table.entryCount > 0 && table.entrySize < programHeaderSize) $
    Left (ProgramHeaderTooSmall table.entrySize)
  pure table

-- | The program headers, from the bytes of their table.
parseProgramHeaders :: ProgramHeaderTable -> ByteString -> Either ElfError [ProgramHeader]
parseProgramHeaders table bs = traverse header [0 .. table.entryCount - 1]
  where
    header k = maybe (Left (Truncated ProgramHeadersPart)) Right $ do
      let base = k * table.entrySize
      ProgramHeader <$> word32 base bs <*> word64 (base + 8) bs <*> word64 (base + 32) bs

-- | The file region of the first segment of the given type.
segment :: Word32 -> [ProgramHeader] -> Maybe Region
segment t headers = case [h | h <- headers, h.pType == t] of
  h : _ -> Just (Region h.pOffset h.pFileSz)
  []    -> Nothing

-- | An entry of a dynamic table.
data DynamicEntry = DynamicEntry
  { tag   :: Word64
  , value :: Word64
  }
  deriving (Eq, Show)

-- | Entries of a dynamic table, up to and without the first @DT_NULL@.
-- 'Nothing' if there is no @DT_NULL@ in the bytes.
dynamicEntries :: ByteString -> Maybe [DynamicEntry]
dynamicEntries bs = go 0
  where
    go i = do
      tag <- word64 i bs
      val <- word64 (i + 8) bs
      if tag == dtNull then pure [] else (DynamicEntry tag val :) <$> go (i + dynEntrySize)

-- | A dynamic table after 'withoutRunPaths'.
data WithoutRunPaths = WithoutRunPaths
  { table   :: ByteString
  , removed :: Int  -- ^ the entries removed
  }
  deriving (Eq, Show)

-- | The dynamic table without its @DT_RPATH@ and @DT_RUNPATH@ entries. Same
-- length as the input; bytes from the first @DT_NULL@ on are unchanged.
withoutRunPaths :: ByteString -> Either ElfError WithoutRunPaths
withoutRunPaths bs = do
  entries <- maybe (Left NoDynamicTerminator) Right (dynamicEntries bs)
  let
    isRunPath entry = entry.tag == dtRpath || entry.tag == dtRunpath
    kept = filter (not . isRunPath) entries
    removedCount = length entries - length kept
    prefixLength = length entries * dynEntrySize
    encoded = BS.concat [le64 entry.tag <> le64 entry.value | entry <- kept]
    zeros = BS.replicate (removedCount * dynEntrySize) 0
  pure $ WithoutRunPaths (encoded <> zeros <> BS.drop prefixLength bs) removedCount

le64 :: Word64 -> ByteString
le64 x = BS.pack [fromIntegral (x `shiftR` (8 * k)) | k <- [0 .. 7]]

-- | Read a file's interpreter and dynamic table location. Declared sizes are
-- checked against the file before anything is read.
readElfInfo :: OsPath -> IO (Either ElfError ElfInfo)
readElfInfo path = catchIO $ withBinaryFile path ReadMode $ \h -> runExceptT $ do
  header <- liftIO $ BS.hGet h elfHeaderSize
  table <- ExceptT $ pure $ parseElfHeader header
  let tableRegion = Region table.tableOffset (fromIntegral (table.entrySize * table.entryCount))
  bytes <- readRegion h ProgramHeadersPart tableRegion
  headers <- ExceptT $ pure $ parseProgramHeaders table bytes
  interp <- traverse (readRegion h InterpreterPart) (segment ptInterp headers)
  interpreterPath <- traverse terminated interp
  pure ElfInfo
    { interpreter = interpreterPath
    , dynamic = segment ptDynamic headers
    }
  where
    terminated bytes = case BS8.elemIndex '\0' bytes of
      Just end -> pure (BS.take end bytes)
      Nothing  -> throwError (Truncated InterpreterPart)

-- | Remove @RPATH@ and @RUNPATH@ from the file in place. Returns how many
-- entries were removed; the file is not written if there are none.
removeRunPaths :: OsPath -> IO (Either ElfError Int)
removeRunPaths path = do
  info <- readElfInfo path
  case info of
    Left err -> pure (Left err)
    Right ElfInfo { dynamic = Nothing } -> pure (Right 0)
    Right ElfInfo { dynamic = Just region } -> catchIO $
      withBinaryFile path ReadWriteMode $ \h -> runExceptT $ do
        table <- readRegion h DynamicTablePart region
        edited <- ExceptT $ pure $ withoutRunPaths table
        when (edited.removed > 0) $ liftIO $ do
          hSeek h AbsoluteSeek (toInteger region.offset)
          BS.hPut h edited.table
        pure edited.removed

-- | The bytes of a region, if the file has all of them. A declared size is
-- never passed to the allocator unchecked.
readRegion :: Handle -> ElfPart -> Region -> ExceptT ElfError IO ByteString
readRegion h part r = do
  size <- liftIO $ hFileSize h
  when (toInteger r.offset + toInteger r.size > size) $ throwError (Truncated part)
  bytes <- liftIO $ do
    hSeek h AbsoluteSeek (toInteger r.offset)
    BS.hGet h (fromIntegral r.size)
  when (toInteger (BS.length bytes) < toInteger r.size) $ throwError (Truncated part)
  pure bytes

catchIO :: IO (Either ElfError a) -> IO (Either ElfError a)
catchIO act = try act >>= \case
  Left (e :: IOException) -> pure (Left (ElfIOError (show e)))
  Right r -> pure r
