{-# LANGUAGE OverloadedRecordDot #-}

-- | RPATH removal and ELF parsing on synthetic ELF64 images, written to a
-- temporary directory.
module Hyperion.Scheduler.Test.ElfTest (runTest, syntheticElf, dynamicBeyondEnd) where

import Control.Exception                 (AssertionFailed (..), throwIO)
import Control.Monad                     (unless)
import Data.ByteString                   (ByteString)
import Data.ByteString                   qualified as BS
import Data.ByteString.Char8             qualified as BS8
import Data.Word                         (Word64)
import Hyperion.OsString                 (fromString)
import Hyperion.Scheduler.TaskLaunch.Elf (DynamicEntry (..), ElfError (..),
                                          ElfInfo (..), ElfPart (..),
                                          Region (..), WithoutRunPaths (..),
                                          dynamicEntries, readElfInfo,
                                          removeRunPaths, withoutRunPaths)
import System.Directory                  (createDirectory,
                                          getTemporaryDirectory,
                                          removeDirectoryRecursive)
import System.FilePath                   ((</>))
import System.Posix.Process              (getProcessID)

expect :: String -> Bool -> IO ()
expect label cond = do
  unless cond $ throwIO $ AssertionFailed $ "FAILED: " <> label
  putStrLn $ "ok: " <> label

le :: Int -> Word64 -> ByteString
le n x = BS.pack [fromIntegral (x `div` (256 ^ k)) | k <- [0 .. n - 1]]

interp :: ByteString
interp = BS8.pack "/lib64/ld-linux-x86-64.so.2"

-- | Dynamic entries of the synthetic image: NEEDED, RPATH, STRTAB, RUNPATH,
-- then NULL and one NULL of padding.
dynamicTable :: [(Word64, Word64)]
dynamicTable = [(1, 1), (15, 10), (5, 0x1000), (29, 20), (0, 0), (0, 0)]

encodeDynamic :: [(Word64, Word64)] -> ByteString
encodeDynamic entries = BS.concat [le 8 t <> le 8 v | (t, v) <- entries]

-- | The ELF64 file header of an image with the given @e_phentsize@ and @e_phnum@.
fileHeader :: Word64 -> Word64 -> ByteString
fileHeader entrySize count = BS.concat
  [ BS.pack [0x7f, 0x45, 0x4c, 0x46, 2, 1, 1, 0], BS.replicate 8 0
  , le 2 2, le 2 62, le 4 1          -- e_type, e_machine (x86-64), e_version
  , le 8 0, le 8 64, le 8 0          -- e_entry, e_phoff, e_shoff
  , le 4 0, le 2 64, le 2 entrySize, le 2 count -- e_flags, e_ehsize, e_phentsize, e_phnum
  , le 2 64, le 2 0, le 2 0          -- e_shentsize, e_shnum, e_shstrndx
  ]

programHeader :: Word64 -> Word64 -> Word64 -> ByteString
programHeader t off sz = BS.concat
  [le 4 t, le 4 4, le 8 off, le 8 off, le 8 off, le 8 sz, le 8 sz, le 8 8]

-- | A 64-bit little-endian ELF image with a PT_INTERP and a PT_DYNAMIC
-- segment at 'dynamicOffset', and some trailing bytes.
syntheticElf :: [(Word64, Word64)] -> ByteString
syntheticElf entries = BS.concat
  [ fileHeader 56 2, phInterp, phDynamic
  , interp, BS.singleton 0, BS.replicate (fromIntegral dynamicOffset - interpEnd) 0
  , encodeDynamic entries
  , BS8.pack "trailing bytes"
  ]
  where
    interpOffset = 64 + 2 * 56 :: Int
    interpEnd = interpOffset + BS.length interp + 1
    phInterp = programHeader 3 (fromIntegral interpOffset) (fromIntegral (BS.length interp + 1))
    phDynamic = programHeader 2 dynamicOffset (fromIntegral (16 * length entries))

-- | An ELF image without PT_INTERP (so no loader looks at it) whose PT_DYNAMIC
-- declares bytes the file doesn't have.
dynamicBeyondEnd :: ByteString
dynamicBeyondEnd = fileHeader 56 1 <> programHeader 2 (2 ^ (40 :: Int)) (2 ^ (40 :: Int))

dynamicOffset :: Word64
dynamicOffset = 208

runTest :: IO ()
runTest = do
  tmp <- getTemporaryDirectory
  pid <- getProcessID
  let dir = tmp </> ("hyperion-scheduler-elf-test-" <> show pid)
  createDirectory dir
  let
    image = syntheticElf dynamicTable
    file = dir </> "image"
    path = fromString file
  BS.writeFile file image
  info <- readElfInfo path
  removed <- removeRunPaths path
  after <- BS.readFile file
  removedAgain <- removeRunPaths path
  afterAgain <- BS.readFile file
  let notElf = dir </> "not-elf"
      elf32 = dir </> "elf32"
      truncated = dir </> "truncated"
      bigEndian = dir </> "big-endian"
      manyHeaders = dir </> "pn-xnum"
      shortHeaders = dir </> "short-phentsize"
      headersBeyond = dir </> "headers-beyond-end"
      unterminated = dir </> "interp-unterminated"
      interpBeyond = dir </> "interp-beyond-end"
      dynamicBeyond = dir </> "dynamic-beyond-end"
      withInterp bytes = fileHeader 56 1 <> programHeader 3 120 (fromIntegral (BS.length bytes)) <> bytes
  BS.writeFile notElf (BS8.pack "#!/bin/sh\necho hello\n")
  BS.writeFile elf32 (BS.take 4 image <> BS.singleton 1 <> BS.drop 5 image)
  BS.writeFile truncated (BS.take 20 image)
  BS.writeFile bigEndian (BS.take 5 image <> BS.singleton 2 <> BS.drop 6 image)
  BS.writeFile manyHeaders (fileHeader 56 0xffff)
  BS.writeFile shortHeaders (fileHeader 8 1 <> BS.replicate 8 0)
  BS.writeFile headersBeyond (fileHeader 56 4)
  BS.writeFile unterminated (withInterp (BS8.pack "/lib64/ld"))
  BS.writeFile interpBeyond (fileHeader 56 1 <> programHeader 3 (2 ^ (40 :: Int)) (2 ^ (40 :: Int)))
  BS.writeFile dynamicBeyond dynamicBeyondEnd
  notElfInfo <- readElfInfo (fromString notElf)
  elf32Info <- readElfInfo (fromString elf32)
  truncatedInfo <- readElfInfo (fromString truncated)
  bigEndianInfo <- readElfInfo (fromString bigEndian)
  manyHeadersInfo <- readElfInfo (fromString manyHeaders)
  shortHeadersInfo <- readElfInfo (fromString shortHeaders)
  headersBeyondInfo <- readElfInfo (fromString headersBeyond)
  unterminatedInfo <- readElfInfo (fromString unterminated)
  interpBeyondInfo <- readElfInfo (fromString interpBeyond)
  dynamicBeyondInfo <- readElfInfo (fromString dynamicBeyond)
  dynamicBeyondRemoval <- removeRunPaths (fromString dynamicBeyond)
  removeDirectoryRecursive dir

  let
    dynStart = fromIntegral dynamicOffset
    dynEnd = dynStart + 16 * length dynamicTable
    expectedTable = encodeDynamic [(1, 1), (5, 0x1000), (0, 0), (0, 0), (0, 0), (0, 0)]
  expect "readElfInfo finds the interpreter" $
    fmap (.interpreter) info == Right (Just interp)
  expect "readElfInfo finds the dynamic table" $
    fmap (.dynamic) info == Right (Just (Region dynamicOffset (fromIntegral (16 * length dynamicTable))))
  expect "removeRunPaths removes DT_RPATH and DT_RUNPATH" $ removed == Right 2
  expect "the other entries are shifted down and the freed ones zeroed" $
    BS.take (dynEnd - dynStart) (BS.drop dynStart after) == expectedTable
  expect "bytes outside the dynamic table are unchanged" $
    BS.take dynStart after == BS.take dynStart image && BS.drop dynEnd after == BS.drop dynEnd image
  expect "the file keeps its size" $ BS.length after == BS.length image
  expect "a second removal finds nothing and changes nothing" $
    removedAgain == Right 0 && afterAgain == after
  expect "a file that isn't ELF is NotElf" $ notElfInfo == Left NotElf
  expect "a 32-bit file is unsupported" $ elf32Info == Left (UnsupportedClass 1)
  expect "a truncated header is an error" $ truncatedInfoIs ElfHeaderPart truncatedInfo
  expect "a big-endian file is unsupported" $ bigEndianInfo == Left (UnsupportedByteOrder 2)
  expect "PN_XNUM is unsupported" $ either (== TooManyProgramHeaders) (const False) manyHeadersInfo
  expect "a program header entry that is too short is unsupported" $
    either (== ProgramHeaderTooSmall 8) (const False) shortHeadersInfo
  expect "program headers beyond the file are truncated, not read" $
    truncatedInfoIs ProgramHeadersPart headersBeyondInfo
  expect "an interpreter path without its NUL is truncated" $
    truncatedInfoIs InterpreterPart unterminatedInfo
  expect "an interpreter declared beyond the file is truncated, not allocated" $
    truncatedInfoIs InterpreterPart interpBeyondInfo
  expect "a dynamic table declared beyond the file parses, and can't be edited" $
    fmap (.dynamic) dynamicBeyondInfo == Right (Just (Region (2 ^ (40 :: Int)) (2 ^ (40 :: Int))))
    && dynamicBeyondRemoval == Left (Truncated DynamicTablePart)
  expect "a table without DT_NULL is an error" $
    fmap (.removed) (withoutRunPaths (encodeDynamic [(1, 1), (15, 2)])) == Left NoDynamicTerminator
  expect "dynamicEntries stops at DT_NULL" $
    dynamicEntries (encodeDynamic dynamicTable)
      == Just (map (uncurry DynamicEntry) (take 4 dynamicTable))
  putStrLn "All Elf tests passed."
  where
    truncatedInfoIs part result = either (== Truncated part) (const False) result
