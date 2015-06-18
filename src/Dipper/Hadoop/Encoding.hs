{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE UndecidableInstances #-}

module Dipper.Hadoop.Encoding where

import           Data.Binary.Get
import           Data.Binary.Put
import           Data.Bits ((.|.), (.&.), xor, shiftL, shiftR)
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as L
import           Data.Int (Int8, Int32, Int64)
import           Data.Map (Map)
import qualified Data.Map as M
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import           Data.Tuple.Strict (Pair(..))
import           Data.Word (Word8)

------------------------------------------------------------------------

data Row a = Row !a !B.ByteString !B.ByteString
  deriving (Eq, Ord, Show)

------------------------------------------------------------------------

withTag :: t -> Row a -> Row t
withTag t (Row _ k v) = Row t k v

mapTag :: (a -> b) -> Row a -> Row b
mapTag f (Row tag k v) = Row (f tag) k v

hasTag :: Eq a => a -> Row a -> Bool
hasTag tag (Row tag' _ _) = tag == tag'

rowTag :: Row a -> a
rowTag (Row tag _ _) = tag

rowKey :: Row a -> B.ByteString
rowKey (Row _ key _) = key

rowValue :: Row a -> B.ByteString
rowValue (Row _ _ value) = value

------------------------------------------------------------------------

type HadoopType = T.Text

data ByteLayout =
      VarVInt      -- ^ Variable length, prefixed by Hadoop-style vint
    | VarWord32be  -- ^ Variable length, prefixed by 32-bit big-endian integer
    | Fixed Int    -- ^ Fixed length, always 'n' bytes
  deriving (Eq, Ord, Show)

data Format = Format {
    fmtType    :: !HadoopType
  , fmtLayout  :: !ByteLayout
  , fmtCompare :: B.ByteString -> B.ByteString -> Ordering
  }

type KVFormat = (Format, Format)

------------------------------------------------------------------------

instance Eq Format where
    (Format t1 l1 _) == (Format t2 l2 _) = t1 == t2 && l1 == l2

instance Ord Format where
    compare (Format t1 l1 _) (Format t2 l2 _) = compare (t1, l1) (t2, l2)

instance Show Format where
    showsPrec p (Format typ lay _) =
        showParen (p > 10) $ showString "Format "
                           . showsPrec 11 typ
                           . showString " "
                           . showsPrec 11 lay
                           . showString " {..}"

------------------------------------------------------------------------

-- | Implementations should match `org.apache.hadoop.io.Writable` where
-- possible.
class Ord a => Hadoop a where
    -- | Gets the package qualified name of the type in Java/Hadoop land. Does
    -- not inspect the value passed, simply uses it for type information.
    hadoopType :: a -> HadoopType

    -- | Describes the binary layout (i.e. variable vs fixed length)
    byteLayout :: a -> ByteLayout

    -- TODO Horrible, more efficiency to be had here
    encode :: a -> B.ByteString
    decode :: B.ByteString -> a

instance Hadoop () where
    hadoopType _ = "org.apache.hadoop.io.NullWritable"
    byteLayout _ = Fixed 0
    encode       = const B.empty
    decode       = const ()

instance Hadoop Int32 where
    hadoopType _ = "org.apache.hadoop.io.IntWritable"
    byteLayout _ = Fixed 4
    encode       = L.toStrict . runPut . putWord32be . fromIntegral
    decode       = runGet (fromIntegral <$> getWord32be) . L.fromStrict

instance Hadoop Int where
    hadoopType _ = "org.apache.hadoop.io.LongWritable"
    byteLayout _ = Fixed 8
    encode       = L.toStrict . runPut . putWord64be . fromIntegral
    decode       = runGet (fromIntegral <$> getWord64be) . L.fromStrict

instance Hadoop Int64 where
    hadoopType _ = "org.apache.hadoop.io.LongWritable"
    byteLayout _ = Fixed 8
    encode       = L.toStrict . runPut . putWord64be . fromIntegral
    decode       = runGet (fromIntegral <$> getWord64be) . L.fromStrict

instance Hadoop T.Text where
    hadoopType _ = "org.apache.hadoop.io.Text"
    byteLayout _ = VarVInt
    encode       = T.encodeUtf8
    decode       = T.decodeUtf8

instance Hadoop B.ByteString where
    hadoopType _ = "org.apache.hadoop.io.BytesWritable"
    byteLayout _ = VarWord32be
    encode       = id
    decode       = id

instance forall a. Hadoop a => Hadoop [a] where
    hadoopType _ = "org.apache.hadoop.io.BytesWritable"
    byteLayout _ = VarWord32be
    encode       = B.concat . map encode

    -- TODO likely pretty slow
    decode bs = flip runGet (L.fromStrict bs) getAll
      where
        getAll = do
          empty <- isEmpty
          if empty
             then return []
             else do
               x  <- decode <$> getLayout (byteLayout (undefined :: a))
               xs <- getAll
               return (x : xs)

------------------------------------------------------------------------

format :: forall a. Hadoop a => a -> Format
format _ = Format (hadoopType (undefined :: a))
                  (byteLayout (undefined :: a)) decodeCompare
  where
    decodeCompare x y = compare (decode x :: a) (decode y :: a)

------------------------------------------------------------------------

class HadoopKV a where
    kvFormat :: a -> KVFormat
    encodeKV :: a -> Row ()
    decodeKV :: Row t -> a

instance {-# OVERLAPPABLE #-} Hadoop a => HadoopKV a where
    kvFormat  _          = (format (), format (undefined :: a))
    encodeKV          x  = Row () (encode ()) (encode x)
    decodeKV (Row _ _ x) = decode x

instance {-# OVERLAPPING #-} (Hadoop k, Hadoop v) => HadoopKV (Pair k v) where
    kvFormat  _          = (format (undefined :: k), format (undefined :: v))
    encodeKV (x :!: y)   = Row () (encode x) (encode y)
    decodeKV (Row _ x y) = decode x :!: decode y

------------------------------------------------------------------------

getRow :: (Show a, Ord a) => Get a -> Map a KVFormat -> Get (Row a)
getRow getTag schema = do
    tag <- getTag
    case M.lookup tag schema of
      Nothing           -> fail ("getRow: invalid tag <" ++ show tag ++ ">")
      Just (kFmt, vFmt) -> Row tag <$> getLayout (fmtLayout kFmt)
                                   <*> getLayout (fmtLayout vFmt)

putRow :: (Show a, Ord a) => (a -> Put) -> Map a KVFormat -> Row a -> Put
putRow putTag schema (Row tag k v) =
    case M.lookup tag schema of
      Nothing           -> fail ("putRow: invalid tag <" ++ show tag ++ ">")
      Just (kFmt, vFmt) -> putTag tag >> putLayout (fmtLayout kFmt) k
                                      >> putLayout (fmtLayout vFmt) v

------------------------------------------------------------------------

getLayout :: ByteLayout -> Get B.ByteString
getLayout VarVInt     = getByteString =<< getVInt
getLayout VarWord32be = getByteString . fromIntegral =<< getWord32be
getLayout (Fixed n)   = getByteString n

putLayout :: ByteLayout -> B.ByteString -> Put
putLayout fmt bs = case fmt of
    VarVInt                 -> putVInt     len  >> putByteString bs
    VarWord32be             -> putWord32be lenW >> putByteString bs
    (Fixed n)   | n == len  -> putByteString bs
                | otherwise -> fail ("putLayout: incorrect size: "
                                  ++ "expected <" ++ show n ++ " bytes> "
                                  ++ "but was <" ++ show len ++ " bytes>")
  where
    len  = B.length bs
    lenW = fromIntegral (B.length bs)

------------------------------------------------------------------------

getText :: Get T.Text
getText = do
    len <- getVInt
    bs  <- getByteString len
    return (T.decodeUtf8With (onError bs) bs)
  where
    onError bs msg _ = error ("getText: could not decode " ++ show bs ++ " (" ++ msg ++ ")")

putText :: T.Text -> Put
putText tx = do
    putVInt (T.length tx)
    putByteString (T.encodeUtf8 tx)

------------------------------------------------------------------------

getBytesWritable :: Get B.ByteString
getBytesWritable = getWord32be >>= getByteString . fromIntegral

putBytesWritable :: B.ByteString -> Put
putBytesWritable bs = do
    putWord32be (fromIntegral (B.length bs))
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
