module Dipper.Binary where

import           Data.Binary.Get
import           Data.Binary.Put
import           Data.Bits ((.|.), (.&.), shiftL, shiftR, xor)
import           Data.ByteString as B
import           Data.ByteString.Char8 as S
import           Data.Int (Int8, Int64)
import           Data.Monoid ((<>))
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import           Data.Word (Word8)

------------------------------------------------------------------------

getText :: Get T.Text
getText = do
    len <- getVInt
    bs  <- getByteString len
    return (T.decodeUtf8With (onError bs) bs)
  where
    onError bs msg _ = error ("getText: could not decode " <> show bs <> " (" <> msg <> ")")

putText :: T.Text -> Put
putText tx = do
    putVInt (T.length tx)
    putByteString (T.encodeUtf8 tx)

getBytesWritable :: Get S.ByteString
getBytesWritable = getWord32be >>= getByteString . fromIntegral

putBytesWritable :: S.ByteString -> Put
putBytesWritable bs = do
    putWord32be (fromIntegral (S.length bs))
    putByteString bs

------------------------------------------------------------------------

getVInt :: Get Int
getVInt = fromIntegral <$> getVInt64

getVInt64 :: Get Int64
getVInt64 = withFirst . fromIntegral =<< getWord8
  where
    withFirst :: Int8 -> Get Int64
    withFirst x | size == 1 = return (fromIntegral x)
                | otherwise = fixupSign . B.foldl' go 0 <$> getByteString (size - 1)
      where
        go :: Int64 -> Word8 -> Int64
        go i b = (i `shiftL` 8) .|. fromIntegral b

        size | x >= -112 = 1
             | x <  -120 = fromIntegral (-119 - x)
             | otherwise = fromIntegral (-111 - x)

        fixupSign v = if isNegative then v `xor` (-1) else v

        isNegative = x < -120 || (x >= -112 && x < 0)

putVInt :: Int -> Put
putVInt = putVInt64 . fromIntegral

putVInt64 :: Int64 -> Put
putVInt64 i | i >= -112 && i <= 127 = putWord8 (fromIntegral i)
            | otherwise             = putWord8 (fromIntegral encLen) >> putRest len
  where
    isNegative = i < 0

    i' | isNegative = i `xor` (-1)
       | otherwise  = i

    encLen0 | isNegative = -120
                | otherwise  = -112

    encLen = go i' encLen0
      where
        go 0   n = n
        go tmp n = go (tmp `shiftR` 8) (n-1)

    len | encLen < -120 = -(encLen + 120)
        | otherwise     = -(encLen + 112)

    putRest 0   = return ()
    putRest idx = putByte idx >> putRest (idx - 1)

    putByte idx = putWord8 (fromIntegral ((i .&. mask) `shiftR` shift))
      where
        mask :: Int64
        mask  = 0xff `shiftL` shift
        shift = (idx - 1) * 8
