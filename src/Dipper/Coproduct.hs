{-# LANGUAGE TypeOperators #-}
{-# OPTIONS_GHC -funbox-strict-fields #-}

module Dipper.Coproduct where

import Control.Applicative
import Data.Typeable (Typeable)

------------------------------------------------------------------------

infixr 1 :+:
infixr 2 <%>

------------------------------------------------------------------------

data a :+: b = L !a | R !b
  deriving (Eq, Ord, Show, Typeable)

------------------------------------------------------------------------

coproduct :: (a -> c) -> (b -> c) -> a :+: b -> c
coproduct f _ (L x) = f x
coproduct _ f (R x) = f x
{-# INLINABLE coproduct #-}

(<%>) :: Alternative f => f a -> f b -> f (a :+: b)
(<%>) fa fb = L <$> fa
          <|> R <$> fb
{-# INLINABLE (<%>) #-}
