{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}

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
import           Data.Word (Word8)

------------------------------------------------------------------------

type Tag = Word8

data ByteFormat =
      VarVInt      -- ^ Variable length, prefixed by Hadoop-style vint
    | VarWord32be  -- ^ Variable length, prefixed by 32-bit big-endian integer
    | Fixed Int    -- ^ Fixed length, always 'n' bytes
  deriving (Eq, Ord, Show)

type RowFormat = ByteFormat :*: ByteFormat

type TaggedRow = Tag :*: S.ByteString :*: S.ByteString

------------------------------------------------------------------------

class Row a where
    rowFormat :: a -> RowFormat
    rowType   :: a -> RowType

instance {-# OVERLAPPABLE #-} HadoopWritable a => Row a where
    rowFormat _ = byteFormat () :*: byteFormat (undefined :: a)
    rowType   _ = hadoopType () :*: hadoopType (undefined :: a)

instance {-# OVERLAPPING #-} (HadoopWritable k, HadoopWritable v) => Row (k :*: v) where
    rowFormat _ = byteFormat (undefined :: v) :*: byteFormat (undefined :: v)
    rowType   _ = hadoopType (undefined :: v) :*: hadoopType (undefined :: v)

------------------------------------------------------------------------

type HadoopType = T.Text

type RowType = HadoopType :*: HadoopType

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

instance HadoopWritable String where
    hadoopType _ = "org.apache.hadoop.io.Text"
    byteFormat _ = VarVInt
    encode       = T.encodeUtf8 . T.pack
    decode       = T.unpack . T.decodeUtf8

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

------------------------------------------------------------------------

infixr 2 :*:
infixr 2 <&>
infixr 1 :+:

data a :*: b = !a :*: !b
  deriving (Eq, Ord, Show, Typeable)

data a :+: b = L !a | R !b
  deriving (Eq, Ord, Show, Typeable)

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
    GroupByKey :: (Typeable n, Typeable k, Typeable v)
               => Atom n (k :*:  v )
               -> Tail n (k :*: [v])

    -- | CombineValues from the FlumeJava paper.
    FoldValues :: (Typeable n, Typeable k, Typeable v)
               => (v -> v -> v)
               -> Atom n (k :*: [v])
               -> Tail n (k :*:  v )

    -- | Merge two streams of values.
    Merge      :: (Typeable n, Typeable a, Typeable b)
               => Atom n a
               -> Atom n b
               -> Tail n (a :*: b)

    -- | Read from a file.
    ReadFile   :: (Typeable n, Typeable a, Row a)
               => FilePath
               -> Tail n a

    -- | Write to a file
    WriteFile  :: (Typeable n, Typeable a, Row a)
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
  showsPrec p x = showParen (p > app) $ case x of
      Concat        xss -> showString "Concat "     . showsPrec (app+1) xss
      ConcatMap   f  xs -> showString "ConcatMap "  . showsPrec (app+1) (typeOf f)
                                                    . showString " "
                                                    . showsPrec (app+1) xs
      GroupByKey     xs -> showString "GroupByKey " . showsPrec (app+1) xs
      FoldValues  f  xs -> showString "FoldValue "  . showsPrec (app+1) (typeOf f)
                                                    . showString " "
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

