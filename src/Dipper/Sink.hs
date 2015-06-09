-- | Constant space push targets.

module Dipper.Sink where

import Data.Foldable (traverse_)

------------------------------------------------------------------------

data Sink f a = Sink (a -> f ())

instance Show (Sink f a) where
    showsPrec p _ = showParen (p > 10) (showString "Sink{..}")

-- | Push a value in to a sink.
push :: Sink f a -> a -> f ()
push (Sink io) x = io x

-- | Push many values in to a sink.
pushMany :: Applicative f => Sink f a -> [a] -> f ()
pushMany (Sink io) = traverse_ io

-- | The dual of map (aka contramap).
unmap :: (b -> a) -> Sink f a -> Sink f b
unmap f (Sink io) = Sink (io . f)

-- | The dual of concat.
unconcat :: Applicative f => Sink f a -> Sink f [a]
unconcat (Sink io) = Sink (traverse_ io)

-- | Send input to both sinks when sent the to resulting sink.
dup :: Applicative f => Sink f a -> Sink f a -> Sink f a
dup (Sink io) (Sink io') = Sink (\x -> io x *> io' x)
