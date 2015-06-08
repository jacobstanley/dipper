GADT Interpretation
===================

Given
-----

    Let (Name 1) (Read (MapperInput "input1.csv"))
    Let (Name 2) (ConcatMap (Int -> [Int]) (Var (Name 1)))
    Let (Name 3) (ConcatMap (Int -> [Int]) (Var (Name 2)))
    Write (MapperOutput 2) (Var (Name 3))
    Let (Name 5) (ConcatMap (Int -> [:*: Int Int]) (Var (Name 2)))
    Let (Name 6) (ConcatMap (Int -> [:*: Int Int]) (Var (Name 2)))
    Let (Name 7) (Concat [Var (Name 5), Var (Name 6)])
    Write (MapperOutput 1) (Var (Name 7))

Run
---

    \chunk -> let x1 = chunk
                  x2 = concatMap f x1
                  x3 = concatMap g x2
                  o2 = map (\x -> 2 :*: encode x) x3
                  x5 = concatMap h x2
                  x6 = concatMap i x2
                  x7 = x5 ++ x6
                  o1 = map (\x -> 1 :*: encode x) x3
              in
                  o2 ++ o1

Sink
----

### Types

    data Sink a = Sink (a -> IO ())

    unmap :: (b -> a) -> Sink a -> Sink b

    unconcat :: Sink a -> Sink [a]

    dup :: Sink a -> Sink a -> Sink a

    stdout :: Sink (Row Tag)

### Translation

    o1 = unmap (\x -> 1 :*: encode x) sink
    x6 = unmap i (unconcat o1)
    x5 = unmap h (unconcat o1)
    o2 = unmap (\x -> 2 :*: encode x) sink
    x3 = unmap g (unconcat o2)
    x2 = unmap f (unconcat (x6 `dup` x5 `dup` x3))
    x1 = x2
