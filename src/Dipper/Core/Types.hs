{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Dipper.Core.Types where

import           Data.String (IsString(..))
import           Data.Tuple.Strict (Pair(..))
import           Data.Typeable (Typeable, typeOf)

import           Dipper.Hadoop.Encoding

------------------------------------------------------------------------

type Tag = Int

data Input a = MapperInput FilePath
             | ReducerInput Tag
    deriving (Eq, Ord, Show, Typeable)

data Output a = MapperOutput Tag
              | ReducerOutput FilePath
    deriving (Eq, Ord, Show, Typeable)

------------------------------------------------------------------------

data Input' = Input' (Input ()) KVFormat
  deriving (Eq, Ord, Show, Typeable)

data Output' = Output' (Output ()) KVFormat
  deriving (Eq, Ord, Show, Typeable)

fromInput :: forall a. HadoopKV a => Input a -> Input'
fromInput x = Input' (coerceInput x) (kvFormat (undefined :: a))

fromOutput :: forall a. HadoopKV a => Output a -> Output'
fromOutput x = Output' (coerceOutput x) (kvFormat (undefined :: a))

coerceInput :: Input a -> Input b
coerceInput (MapperInput x)  = MapperInput x
coerceInput (ReducerInput x) = ReducerInput x

coerceOutput :: Output a -> Output b
coerceOutput (MapperOutput x)  = MapperOutput x
coerceOutput (ReducerOutput x) = ReducerOutput x

------------------------------------------------------------------------

newtype Name n a = Name n
    deriving (Eq, Ord, Show, Typeable)

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
    Read :: (Typeable n, Typeable a, HadoopKV a)
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
    GroupByKey :: (Typeable n, Typeable k, Typeable v, Eq k, Hadoop k, Hadoop v, Show k, Show v)
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
    Write :: (Typeable n, Typeable a, HadoopKV a, Show a)
          => Output a
          -> Atom n a
          -> Term n b
          -> Term n b

    -- | Let binding.
    Let :: (Typeable n, Typeable a, HadoopKV a, Show a)
        => Name n a
        -> Tail n a
        -> Term n b
        -> Term n b

  deriving (Typeable)

------------------------------------------------------------------------

instance IsString (Name String a) where
    fromString = Name

instance Show n => Show (Atom n a) where
  showsPrec p x = showParen (p > 10) $ case x of
      Var n   -> showString "Var " . showsPrec 11 n
      Const _ -> showString "Const {..}"

instance Show n => Show (Tail n a) where
  showsPrec p tl = showParen (p > 10) $ case tl of
      Read         inp -> showString "Read "       . showsPrec 11 inp
      Concat       xss -> showString "Concat "     . showsPrec 11 xss
      ConcatMap  f  xs -> showString "ConcatMap "  . showsPrec 11 (typeOf f)
                                                   . showString " "
                                                   . showsPrec 11 xs
      GroupByKey    xs -> showString "GroupByKey " . showsPrec 11 xs
      FoldValues f  xs -> showString "FoldValues " . showsPrec 11 (typeOf f)
                                                   . showString " "
                                                   . showsPrec 11 xs

instance Show n => Show (Term n a) where
  showsPrec p x = showParen (p > 10) $ case x of
      Return a     -> showString "Return " . showsPrec 11 a
      Let n tl tm  -> showString "Let "    . showsPrec 11 n
                                           . showString " "
                                           . showsPrec 11 tl
                                           . showString "\n"
                                           . showsPrec 11 tm
      Write o a tm -> showString "Write "  . showsPrec 11 o
                                           . showString " "
                                           . showsPrec 11 a
                                           . showString "\n"
                                           . showsPrec 11 tm
