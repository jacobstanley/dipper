{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE FlexibleInstances #-}

module Dipper.Annotation where

import           Data.Set (Set)
import qualified Data.Set as S

------------------------------------------------------------------------

infixl 8 :@

data n :@ a = !n :@ !a

deriving instance {-# OVERLAPPABLE #-} (Show n, Show a) => Show (n :@ a)

instance Eq n => Eq (n :@ a) where
    (x :@ _) == (y :@ _) = x == y

instance Ord n => Ord (n :@ a) where
    compare (x :@ _) (y :@ _) = compare x y

------------------------------------------------------------------------

infixl 8 @@

(@@) :: Ord a => n :@ Set a -> a -> n :@ Set a
(@@) (n :@ as) a = n :@ (S.insert a as)

instance {-# OVERLAPPING #-} (Show n, Show a) => Show (n :@ Set a) where
    showsPrec p (n :@ as) = showParen (p > 10)
                          $ showsPrec 0 n
                          . foldr showOne id (S.toList as)
      where
        showOne :: Show a => a -> ShowS -> ShowS
        showOne x s = s
                    . showString " @@ "
                    . showsPrec 0 x
