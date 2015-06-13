{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE UndecidableInstances #-}

module Dipper.Types where

import           Data.Binary.Get
import           Data.Binary.Put
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as L
import           Data.Int (Int32, Int64)
import           Data.String (IsString(..))
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import           Data.Tuple.Strict (Pair(..))
import           Data.Typeable (Typeable, typeOf)
import           Data.Word (Word8)

import           Dipper.Binary

------------------------------------------------------------------------

data Row a = Row !a !B.ByteString !B.ByteString
  deriving (Eq, Ord, Show)

------------------------------------------------------------------------

mkRow :: a -> Pair B.ByteString B.ByteString -> Row a
mkRow tag (k :!: v) = Row tag k v

unTag :: Row a -> Pair B.ByteString B.ByteString
unTag (Row _ k v) = k :!: v

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

type KVFormat = Pair Format Format

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

class KV a where
    kvFormat :: a -> KVFormat
    encodeKV :: a -> Row ()
    decodeKV :: Row t -> a

instance {-# OVERLAPPABLE #-} HadoopWritable a => KV a where
    kvFormat  _          = format () :!: format (undefined :: a)
    encodeKV          x  = Row () (encode ()) (encode x)
    decodeKV (Row _ _ x) = decode x

instance {-# OVERLAPPING #-} (HadoopWritable k, HadoopWritable v) => KV (Pair k v) where
    kvFormat  _          = format (undefined :: k) :!: format (undefined :: v)
    encodeKV (x :!: y)   = Row () (encode x) (encode y)
    decodeKV (Row _ x y) = decode x :!: decode y

instance {-# OVERLAPPING #-} (HadoopWritable k, HadoopWritable v) => KV (Pair k [v]) where
    kvFormat  _           = format (undefined :: k) :!: format (undefined :: [v])
    encodeKV (x :!: ys)   = Row () (encode x) (encode ys)
    decodeKV (Row _ x ys) = decode x :!: decode ys

------------------------------------------------------------------------

format :: forall a. HadoopWritable a => a -> Format
format _ = Format (hadoopType (undefined :: a))
                  (byteLayout (undefined :: a)) decodeCompare
  where
    decodeCompare x y = compare (decode x :: a) (decode y :: a)

-- | Implementations should match `org.apache.hadoop.io.HadoopWritable` where
-- possible.
class Ord a => HadoopWritable a where
    -- | Gets the package qualified name of 'a' in Java/Hadoop land. Does
    -- not inspect the value of 'a', simply uses it for type information.
    hadoopType :: a -> HadoopType

    -- | Describes the binary layout (i.e. variable vs fixed length)
    byteLayout :: a -> ByteLayout

    -- TODO Horrible, more efficiency to be had here
    encode :: a -> B.ByteString
    decode :: B.ByteString -> a

instance HadoopWritable () where
    hadoopType _ = "org.apache.hadoop.io.NullWritable"
    byteLayout _ = Fixed 0
    encode       = const B.empty
    decode       = const ()

instance HadoopWritable Int32 where
    hadoopType _ = "org.apache.hadoop.io.IntWritable"
    byteLayout _ = Fixed 4
    encode       = L.toStrict . runPut . putWord32be . fromIntegral
    decode       = runGet (fromIntegral <$> getWord32be) . L.fromStrict

instance HadoopWritable Int where
    hadoopType _ = "org.apache.hadoop.io.LongWritable"
    byteLayout _ = Fixed 8
    encode       = L.toStrict . runPut . putWord64be . fromIntegral
    decode       = runGet (fromIntegral <$> getWord64be) . L.fromStrict

instance HadoopWritable Int64 where
    hadoopType _ = "org.apache.hadoop.io.LongWritable"
    byteLayout _ = Fixed 8
    encode       = L.toStrict . runPut . putWord64be . fromIntegral
    decode       = runGet (fromIntegral <$> getWord64be) . L.fromStrict

instance HadoopWritable T.Text where
    hadoopType _ = "org.apache.hadoop.io.Text"
    byteLayout _ = VarVInt
    encode       = T.encodeUtf8
    decode       = T.decodeUtf8

instance HadoopWritable B.ByteString where
    hadoopType _ = "org.apache.hadoop.io.BytesWritable"
    byteLayout _ = VarWord32be
    encode       = id
    decode       = id

instance forall a. HadoopWritable a => HadoopWritable [a] where
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

newtype Name n a = Name n
    deriving (Eq, Ord, Show, Typeable)

type Tag = Word8

data Input a = MapperInput FilePath
             | ReducerInput Tag
    deriving (Eq, Ord, Show, Typeable)

data Output a = MapperOutput Tag
              | ReducerOutput FilePath
    deriving (Eq, Ord, Show, Typeable)

instance IsString (Name String a) where
    fromString = Name

------------------------------------------------------------------------

data Input' = Input' (Input ()) KVFormat
  deriving (Eq, Ord, Show, Typeable)

data Output' = Output' (Output ()) KVFormat
  deriving (Eq, Ord, Show, Typeable)

fromInput :: forall a. KV a => Input a -> Input'
fromInput x = Input' (coerceInput x) (kvFormat (undefined :: a))

fromOutput :: forall a. KV a => Output a -> Output'
fromOutput x = Output' (coerceOutput x) (kvFormat (undefined :: a))

coerceInput :: Input a -> Input b
coerceInput (MapperInput x)  = MapperInput x
coerceInput (ReducerInput x) = ReducerInput x

coerceOutput :: Output a -> Output b
coerceOutput (MapperOutput x)  = MapperOutput x
coerceOutput (ReducerOutput x) = ReducerOutput x

------------------------------------------------------------------------

data Atom n a where

    -- | Variables.
    Var :: (Typeable n, Typeable a)
        => Name n a
        -> Atom n a

    -- | Constants.
    Const :: (Typeable n, Typeable a)
          => [a]
          -> Atom n a

  deriving (Typeable)


data Tail n a where

    -- | Read from a input.
    Read :: (Typeable n, Typeable a, KV a)
         => Input a
         -> Tail n a

    -- | Flatten from the FlumeJava paper.
    Concat :: (Typeable n, Typeable a, Show a)
           => [Atom n a]
           ->  Tail n a

    -- | ParallelDo from the FlumeJava paper.
    ConcatMap :: (Typeable n, Typeable a, Typeable b, Show a, Show b)
              => (a -> [b])
              -> Atom n a
              -> Tail n b

    -- | GroupByKey from the FlumeJava paper.
    GroupByKey :: (Typeable n, Typeable k, Typeable v, Eq k, HadoopWritable k, HadoopWritable v, Show k, Show v)
               => Atom n (Pair k v)
               -> Tail n (Pair k v)

    -- | CombineValues from the FlumeJava paper.
    FoldValues :: (Typeable n, Typeable k, Typeable v, Eq k, Show k, Show v)
               => (v -> v -> v)
               -> Atom n (Pair k v)
               -> Tail n (Pair k v)

  deriving (Typeable)


data Term n a where

    -- | Result of term.
    Return :: (Typeable n)
           => Atom n a
           -> Term n a

    -- | Write to an output.
    Write :: (Typeable n, Typeable a, KV a, Show a)
          => Output a
          -> Atom n a
          -> Term n b
          -> Term n b

    -- | Let binding.
    Let :: (Typeable n, Typeable a, KV a, Show a)
        => Name n a
        -> Tail n a
        -> Term n b
        -> Term n b

  deriving (Typeable)

------------------------------------------------------------------------

instance Show n => Show (Atom n a) where
  showsPrec p x = showParen' p $ case x of
      Var n   -> showString "Var " . showForeign n
      Const _ -> showString "Const {..}"

instance Show n => Show (Tail n a) where
  showsPrec p tl = showParen' p $ case tl of
      Read         inp -> showString "Read "       . showForeign inp
      Concat       xss -> showString "Concat "     . showForeign xss
      ConcatMap  f  xs -> showString "ConcatMap "  . showForeign (typeOf f)
                                                   . showString " "
                                                   . showForeign xs
      GroupByKey    xs -> showString "GroupByKey " . showForeign xs
      FoldValues f  xs -> showString "FoldValues " . showForeign (typeOf f)
                                                   . showString " "
                                                   . showForeign xs

instance Show n => Show (Term n a) where
  showsPrec p x = showParen' p $ case x of
      Return a     -> showString "Return " . showForeign a
      Let n tl tm  -> showString "Let "    . showForeign n
                                           . showString " "
                                           . showForeign tl
                                           . showString "\n"
                                           . showForeign tm
      Write o a tm -> showString "Write "  . showForeign o
                                           . showString " "
                                           . showForeign a
                                           . showString "\n"
                                           . showForeign tm

------------------------------------------------------------------------

showForeign :: Show a => a -> ShowS
showForeign = showsPrec 11

showParen' :: Int -> ShowS -> ShowS
showParen' p = showParen (p > 10)
