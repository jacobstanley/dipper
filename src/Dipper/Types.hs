{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE FlexibleContexts #-}

module Dipper.Types where

import           Data.Binary.Get
import           Data.Binary.Put
import qualified Data.ByteString as S
import qualified Data.ByteString.Lazy as L
import           Data.Int (Int64)
import           Data.String (IsString(..))
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import           Data.Typeable (Typeable, typeOf)

import           Dipper.Binary

------------------------------------------------------------------------

data ByteFormat =
      VarVInt      -- ^ Variable length, prefixed by Hadoop-style vint
    | VarWord32be  -- ^ Variable length, prefixed by 32-bit big-endian integer
    | Fixed Int    -- ^ Fixed length, always 'n' bytes
  deriving (Eq, Ord, Show)

type KVFormat = ByteFormat :*: ByteFormat

type Row a = a :*: S.ByteString :*: S.ByteString

------------------------------------------------------------------------

hasTag :: Eq a => a -> Row a -> Bool
hasTag tag (tag' :*: _) = tag == tag'

getFormatted :: ByteFormat -> Get S.ByteString
getFormatted VarVInt     = getByteString =<< getVInt
getFormatted VarWord32be = getByteString . fromIntegral =<< getWord32be
getFormatted (Fixed n)   = getByteString n

putFormatted :: ByteFormat -> S.ByteString -> Put
putFormatted fmt bs = case fmt of
    VarVInt                 -> putVInt     len  >> putByteString bs
    VarWord32be             -> putWord32be lenW >> putByteString bs
    (Fixed n)   | n == len  -> putByteString bs
                | otherwise -> fail ("putTagged: incorrect tag size: "
                                  ++ "expected <" ++ show n ++ " bytes> "
                                  ++ "but was <" ++ show len ++ " bytes>")
  where
    len  = S.length bs
    lenW = fromIntegral (S.length bs)

------------------------------------------------------------------------

class KV a where
    kvFormat  :: a -> KVFormat
    kvType    :: a -> KVType
    encodeRow :: a -> S.ByteString :*: S.ByteString
    decodeRow :: S.ByteString :*: S.ByteString -> a

instance {-# OVERLAPPABLE #-} HadoopWritable a => KV a where
    kvFormat  _        = byteFormat () :*: byteFormat (undefined :: a)
    kvType    _        = hadoopType () :*: hadoopType (undefined :: a)
    encodeRow        x  = encode     () :*: encode x
    decodeRow (_ :*: x) = decode x

instance {-# OVERLAPPING #-} (HadoopWritable k, HadoopWritable v) => KV (k :*: v) where
    kvFormat  _        = byteFormat (undefined :: v) :*: byteFormat (undefined :: v)
    kvType    _        = hadoopType (undefined :: v) :*: hadoopType (undefined :: v)
    encodeRow (x :*: y) = encode x :*: encode y
    decodeRow (x :*: y) = decode x :*: decode y

instance {-# OVERLAPPING #-} (HadoopWritable k, HadoopWritable v) => KV (k :*: [v]) where
    kvFormat  _         = byteFormat (undefined :: v) :*: byteFormat (undefined :: [v])
    kvType    _         = hadoopType (undefined :: v) :*: hadoopType (undefined :: [v])
    encodeRow (x :*: ys) = encode x :*: encode ys
    decodeRow (x :*: ys) = decode x :*: decode ys

------------------------------------------------------------------------

type HadoopType = T.Text

type KVType = HadoopType :*: HadoopType

-- | Implementations should match `org.apache.hadoop.io.HadoopWritable` where
-- possible.
class HadoopWritable a where
    -- | Gets the package qualified name of 'a' in Java/Hadoop land. Does
    -- not inspect the value of 'a', simply uses it for type information.
    hadoopType :: a -> HadoopType

    -- | Describes the binary layout (i.e. variable vs fixed length)
    byteFormat :: a -> ByteFormat

    -- TODO Horrible, more efficiency to be had here
    encode :: a -> S.ByteString
    decode :: S.ByteString -> a

instance HadoopWritable () where
    hadoopType _ = "org.apache.hadoop.io.NullWritable"
    byteFormat _ = Fixed 0
    encode       = const S.empty
    decode       = const ()

instance HadoopWritable Int where
    hadoopType _ = "org.apache.hadoop.io.LongWritable"
    byteFormat _ = Fixed 8
    encode       = L.toStrict . runPut . putWord64be . fromIntegral
    decode       = runGet (fromIntegral <$> getWord64be) . L.fromStrict

instance HadoopWritable Int64 where
    hadoopType _ = "org.apache.hadoop.io.LongWritable"
    byteFormat _ = Fixed 8
    encode       = L.toStrict . runPut . putWord64be . fromIntegral
    decode       = runGet (fromIntegral <$> getWord64be) . L.fromStrict

instance HadoopWritable T.Text where
    hadoopType _ = "org.apache.hadoop.io.Text"
    byteFormat _ = VarVInt
    encode       = T.encodeUtf8
    decode       = T.decodeUtf8

instance HadoopWritable S.ByteString where
    hadoopType _ = "org.apache.hadoop.io.BytesWritable"
    byteFormat _ = VarWord32be
    encode       = id
    decode       = id

instance forall a. HadoopWritable a => HadoopWritable [a] where
    hadoopType _ = "org.apache.hadoop.io.BytesWritable"
    byteFormat _ = VarWord32be
    encode       = S.concat . map encode

    -- TODO likely pretty slow
    decode bs = flip runGet (L.fromStrict bs) getAll
      where
        getAll = do
          empty <- isEmpty
          if empty
             then return []
             else do
               x  <- decode <$> getFormatted (byteFormat (undefined :: a))
               xs <- getAll
               return (x : xs)

------------------------------------------------------------------------

infixr 2 :*:
infixr 2 <&>
infixr 1 :+:

data a :*: b = !a :*: !b
  deriving (Eq, Ord, Show, Typeable)

data a :+: b = L !a | R !b
  deriving (Eq, Ord, Show, Typeable)

snd' :: a :*: b -> b
snd' (_ :*: x) = x
{-# INLINE snd' #-}

-- | Sequence actions and put their resulting values into a strict product.
(<&>) :: Applicative f => f a -> f b -> f (a :*: b)
(<&>) fa fb = (:*:) <$> fa <*> fb
{-# INLINE (<&>) #-}

------------------------------------------------------------------------

newtype Name n a = Name n
    deriving (Eq, Ord, Show, Typeable)

instance IsString (Name String a) where
    fromString = Name

------------------------------------------------------------------------

data Atom n a where

    -- | Variables.
    Var   :: (Typeable n, Typeable a)
          => Name n a
          -> Atom n a

    -- | Constants.
    Const :: (Typeable n, Typeable a)
          => [a]
          -> Atom n a

  deriving (Typeable)


data Tail n a where

    -- | Flatten from the FlumeJava paper.
    Concat     :: (Typeable n, Typeable a)
               => [Atom n a]
               ->  Tail n a

    -- | ParallelDo from the FlumeJava paper.
    ConcatMap  :: (Typeable n, Typeable a, Typeable b)
               => (a -> [b])
               -> Atom n a
               -> Tail n b

    -- | GroupByKey from the FlumeJava paper.
    GroupByKey :: (Typeable n, Typeable k, Typeable v, Eq k, HadoopWritable k, HadoopWritable v)
               => Atom n (k :*:  v )
               -> Tail n (k :*: [v])

    -- | CombineValues from the FlumeJava paper.
    FoldValues :: (Typeable n, Typeable k, Typeable v)
               => (v -> v -> v)
               -> v
               -> Atom n (k :*: [v])
               -> Tail n (k :*:  v )

    -- | Read from a file.
    ReadFile   :: (Typeable n, Typeable a, KV a)
               => FilePath
               -> Tail n a

    -- | Write to a file
    WriteFile  :: (Typeable n, Typeable a, KV a)
               => FilePath
               -> Atom n a
               -> Tail n ()

  deriving (Typeable)


data Term n a where

    -- | Let binding.
    Let :: (Typeable n, Typeable a)
        => Name n a
        -> Tail n a
        -> Term n b
        -> Term n b

    -- | Let binding of ().
    Run :: (Typeable n)
        => Tail n ()
        -> Term n b
        -> Term n b

    -- | End of term.
    Return :: (Typeable n)
           => Tail n a
           -> Term n a

  deriving (Typeable)

------------------------------------------------------------------------

instance Show n => Show (Atom n a) where
  showsPrec p x = showParen (p > app) $ case x of
      Var n   -> showString "Var " . showsPrec (app+1) n
      Const _ -> showString "Const {..}"
    where
      app = 10

instance Show n => Show (Tail n a) where
  showsPrec p tl = showParen (p > app) $ case tl of
      Concat        xss -> showString "Concat "     . showsPrec (app+1) xss
      ConcatMap   f  xs -> showString "ConcatMap "  . showsPrec (app+1) (typeOf f)
                                                    . showString " "
                                                    . showsPrec (app+1) xs
      GroupByKey     xs -> showString "GroupByKey " . showsPrec (app+1) xs
      FoldValues f x xs -> showString "FoldValue "  . showsPrec (app+1) (typeOf f)
                                                    . showString " "
                                                    . showsPrec (app+1) (typeOf x)
                                                    . showString " "
                                                    . showsPrec (app+1) xs
                                                    . showsPrec (app+1) xs
      ReadFile  path    -> showString "ReadFile "   . showsPrec (app+1) path
      WriteFile path xs -> showString "WriteFile "  . showsPrec (app+1) path
                                                    . showString " "
                                                    . showsPrec (app+1) xs
    where
      app = 10

instance Show n => Show (Term n a) where
  showsPrec p x = showParen (p > app) $ case x of
      Let n tl tm -> showString "Let "    . showsPrec (app+1) n
                                          . showString " "
                                          . showsPrec (app+1) tl
                                          . showString "\n"
                                          . showsPrec (app+1) tm
      Run   tl tm -> showString "Run "    . showsPrec (app+1) tl
                                          . showString "\n"
                                          . showsPrec (app+1) tm
      Return tl   -> showString "Return " . showsPrec (app+1) tl
    where
      app = 10

