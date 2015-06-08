{-# LANGUAGE TypeOperators #-}
{-# OPTIONS_GHC -funbox-strict-fields #-}

module Dipper.Product where

import Data.Typeable (Typeable)

------------------------------------------------------------------------

infixr 2 :*:
infixr 2 <&>

------------------------------------------------------------------------

data a :*: b = !a :*: !b
  deriving (Eq, Ord, Show, Typeable)

------------------------------------------------------------------------

fst' :: a :*: b -> a
fst' (x :*: _) = x
{-# INLINABLE fst' #-}

snd' :: a :*: b -> b
snd' (_ :*: x) = x
{-# INLINABLE snd' #-}

-- | Sequence actions and put their resulting values into a strict product.
(<&>) :: Applicative f => f a -> f b -> f (a :*: b)
(<&>) fa fb = (:*:) <$> fa <*> fb
{-# INLINABLE (<&>) #-}
