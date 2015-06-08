-- | Constant space push targets.

module Dipper.Sink where

------------------------------------------------------------------------

data Sink a = Sink (a -> IO ())

-- | Push a value in to a sink.
push :: Sink a -> a -> IO ()
push (Sink io) x = io x

-- | The dual of map (aka contramap).
unmap :: (b -> a) -> Sink a -> Sink b
unmap f (Sink io) = Sink (io . f)

-- | The dual of concat.
unconcat :: Sink a -> Sink [a]
unconcat (Sink io) = Sink (mapM_ io)

-- | Send input to both sinks when sent the to resulting sink.
dup :: Sink a -> Sink a -> Sink a
dup (Sink io) (Sink io') = Sink (\x -> io x >> io' x)
