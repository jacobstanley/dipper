{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE RankNTypes #-}

-- | Constant space push targets.
module Dipper.Sink where

import Data.Tuple.Strict (Pair(..))

------------------------------------------------------------------------

data Sink m a = forall s. Sink (a -> s -> m s) s

instance Show (Sink m a) where
    showsPrec p _ = showParen (p > 10) (showString "Sink{..}")

-- | Push a value in to a sink.
push :: Applicative m => Sink m a -> a -> m ()
push (Sink feed s) x = feed x s *> pure ()

-- | Push many values in to a sink.
pushMany :: Monad m => Sink m a -> [a] -> m ()
pushMany (Sink feed s0) xs = feed' xs s0 *> pure ()
  where
    feed' []     s = pure s
    feed' (y:ys) s = feed y s >>= feed' ys

-- | The dual of map (aka contramap).
unmap :: (b -> a) -> Sink m a -> Sink m b
unmap f (Sink feed s) = Sink (feed . f) s

-- | The dual of concat.
unconcat :: Monad m => Sink m a -> Sink m [a]
unconcat (Sink feed s) = Sink feedList s
  where
    feedList []     t = return t
    feedList (x:xs) t = feed x t >>= feedList xs

-- | The dual of group.
ungroup :: (Monad m, Eq k) => Sink m (Pair k [v]) -> Sink m (Pair k v)
ungroup (Sink feed s) = Sink feed' (s, Nothing)
  where
    feed' (k :!: v) (t, Nothing) = return (t, Just (k :!: [v]))
    feed' (k :!: v) (t, Just (k' :!: vs))
        | k == k'   = return (t, Just (k' :!: (v:vs)))
        | otherwise = feed (k' :!: vs) t >>= \t' -> return (t', Just (k :!: [v]))

-- | Send input to both sinks when sent the to resulting sink.
dup :: Applicative m => Sink m a -> Sink m a -> Sink m a
dup (Sink feed s) (Sink feed' s') = Sink (\x (t, t') -> (,) <$> feed x t <*> feed' x t') (s, s')
