{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternGuards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeOperators #-}

{-# OPTIONS_GHC -funbox-strict-fields #-}
-- {-# OPTIONS_GHC -w #-}

module Dipper.AST where

import           Data.Dynamic
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as M

import           Dipper.Types

------------------------------------------------------------------------

freshNames :: [String]
freshNames = map (\x -> "x" ++ show x) ([1..] :: [Integer])

lets :: (Typeable n, Typeable a)
     => [(Name n a, Tail n a)]
     -> Term n b
     -> Term n b
lets []            tm = tm
lets ((n, tl):tls) tm = Let n tl (lets tls tm)

------------------------------------------------------------------------

sinkConcatOfTerm :: (Typeable n, Show n, Ord n)
                 => [n]
                 -> Map n Dynamic -- Dynamic :: Tail n _
                 -> Term n b
                 -> Term n b
sinkConcatOfTerm fresh env term = case term of

    Let (Name n)
        (ConcatMap (f :: a -> [b])
                   (Var (Name m) :: Atom n a)) tm
     | Concat xss <- (unsafeDynLookup "sinkConcatOfTerm" m env :: Tail n a)
     ->

        let
            yss             = map (ConcatMap f) xss
            (ns, n':fresh') = splitAt (length yss) fresh
            tl'             = Concat (map (Var . Name) ns) :: Tail n b
            env'            = foldr (uncurry M.insert) env
                                    ((n', toDyn tl') : zip ns (map toDyn yss))

            name = Name n'
            var  = toDyn (Var name)
            tm'  = sinkConcatOfTerm fresh' env'
                 $ substTerm (M.singleton n var) tm
        in
            lets (zip (map Name ns) yss) $
            Let name tl' tm'


    Let (Name n) tl tm ->

        let
            env' = M.insert n (toDyn tl) env
        in
            Let (Name n) tl (sinkConcatOfTerm fresh env' tm)


    Run    tl tm -> Run    tl (sinkConcatOfTerm fresh env tm)
    Return tl    -> Return tl

------------------------------------------------------------------------

--mapFusionOf :: Term n -> Term n
--mapFusionOf tm = case tm of
--  ConcatMap f (ConcatMap g xs) ->
--    mapFusionOf (ConcatMap (concatMap f . g) xs)
--
--  FromFile p     -> FromFile p
--  FromMemory xs  -> FromMemory xs
--  Concat ds      -> Concat (map mapFusionOf ds)
--  ConcatMap f d  -> ConcatMap f (mapFusionOf d)
--  GroupByKey kvs -> GroupByKey (mapFusionOf kvs)
--  Combine f kvs  -> Combine f (mapFusionOf kvs)

--mapFusionOfTerm

------------------------------------------------------------------------

renameAtom :: (Ord a, Show a, Show b, Typeable b)
           => Map a b
           -> Atom a r
           -> Atom b r
renameAtom names atom = case atom of
    Var (Name x) -> Var (Name (unsafeLookup "renameAtom" x names))
    Const x      -> Const x

renameTail :: (Ord a, Show a, Show b, Typeable b)
           => Map a b
           -> Tail a r
           -> Tail b r
renameTail names tl = case tl of
    Concat       xs  -> Concat         (map (renameAtom names) xs)
    ConcatMap  f x   -> ConcatMap  f   (renameAtom names x)
    GroupByKey   x   -> GroupByKey     (renameAtom names x)
    FoldValues f x   -> FoldValues f   (renameAtom names x)
    ReadFile  path   -> ReadFile  path
    WriteFile path x -> WriteFile path (renameAtom names x)

renameTerm :: (Ord n, Show n, Show m, Typeable m)
           => [m]
           -> Map n m
           -> Term n a
           -> ([m], Term m a)
renameTerm gen0 names term = case term of

    Let (Name n) tl tm ->

        let
            tl'         = renameTail names tl
            (n' : gen1) = gen0
            names'      = M.insert n n' names
            (gen2, tm') = renameTerm gen1 names' tm
        in
            (gen2, Let (Name n') tl' tm')

    Run tl tm ->

        let
            tl'         = renameTail names tl
            (gen1, tm') = renameTerm gen0 names tm
        in
            (gen1, Run tl' tm')

    Return tl ->

        let
            tl' = renameTail names tl
        in
            (gen0, Return tl')

------------------------------------------------------------------------

substAtom :: (Ord n, Show n)
          => Map n Dynamic -- Dynamic :: Atom n a
          -> Atom n a
          -> Atom n a
substAtom subs atom = case atom of
    Var (Name x) -> let dynAtom = toDyn atom
                        x'      = M.findWithDefault dynAtom x subs
                    in  fromDyn x' (error ("substAtom: type mismatch: "
                                         ++ show x ++ " :: " ++ show dynAtom
                                         ++ " is not of type " ++ show x'))
    Const x      -> Const x

substTail :: (Ord n, Show n)
          => Map n Dynamic -- Dynamic :: Atom n _
          -> Tail n a
          -> Tail n a
substTail subs tl = case tl of
    Concat       xs  -> Concat         (map (substAtom subs) xs)
    ConcatMap  f x   -> ConcatMap  f   (substAtom subs x)
    GroupByKey   x   -> GroupByKey     (substAtom subs x)
    FoldValues f x   -> FoldValues f   (substAtom subs x)
    ReadFile  path   -> ReadFile  path
    WriteFile path x -> WriteFile path (substAtom subs x)

substTerm :: (Ord n, Show n)
          => Map n Dynamic -- Dynamic :: Atom n _
          -> Term n a
          -> Term n a
substTerm subs term = case term of
    Let (Name n) tl tm -> Let (Name n) (substTail subs tl) (substTerm (M.delete n subs) tm)
    Run          tl tm -> Run          (substTail subs tl) (substTerm subs tm)
    Return       tl    -> Return       (substTail subs tl)

------------------------------------------------------------------------

example1 :: Term Int ()
example1 = Let x0 (ReadFile "input.csv") $
           Let x1 (ConcatMap (\x -> [x + 1]) (Var x0)) $
           Let x2 (ConcatMap (\x -> [x * 2]) (Var x0)) $
           Let x3 (Concat [Var x1, Var x2]) $
           Return (WriteFile "output.csv" (Var x3))
  where
    x0 = Name 0 :: Name Int Int
    x1 = Name 1
    x2 = Name 2
    x3 = Name 3 :: Name Int Int

------------------------------------------------------------------------

example2 :: Term String ()
example2 =
    Let i1     (ReadFile "input1") $
    Let i2     (ReadFile "input2") $
    Let i3     (ReadFile "input3") $
    Let i4     (ReadFile "input4") $
    Let a      (ConcatMap add1 (Var i1)) $
    Let write1 (ConcatMap add1 (Var a)) $
    Run        (WriteFile "output1" (Var write1)) $
    Let jtag1  (ConcatMap (tag 1) (Var a)) $
    Let b      (ConcatMap add1 (Var i2)) $
    Let c      (ConcatMap add1 (Var i3)) $
    Let fltn   (Concat [Var b, Var c]) $
    Let d      (ConcatMap add1 (Var fltn)) $
    Let jtag2  (ConcatMap (tag 2) (Var d)) $
    Let gmap   (ConcatMap kv_add1 (Var i4)) $
    Let ggbk   (GroupByKey (Var gmap)) $
    Let gcv    (FoldValues (+) (Var ggbk)) $
    Let e      (ConcatMap untag (Var gcv)) $
    Let jtag3  (ConcatMap (tag 3) (Var e)) $
    Let jfltn  (Concat [Var jtag1, Var jtag2, Var jtag3]) $
    Let jgbk   (GroupByKey (Var jfltn)) $
    Let juntag (ConcatMap untag (Var jgbk)) $
    Let f      (ConcatMap id (Var juntag)) $
    Let write2 (Concat [Var f]) $
    Return     (WriteFile "output2" (Var write2))
  where
    add1 :: Int -> [Int]
    add1 x = [x+1]

    kv_add1 :: String :*: Int -> [String :*: Int]
    kv_add1 (k :*: v) = [k :*: v+1]

    tag :: Int -> a -> [Int :*: a]
    tag ix x = [ix :*: x]

    untag :: k :*: v -> [v]
    untag (_ :*: x) = [x]

    -- names
    i1 = "1"
    i2 = "2"
    i3 = "3"
    i4 = "4"

    a  = "A"
    b  = "B"
    c  = "C"
    d  = "D"
    e  = "E"
    f  = "F"

    fltn = "Fltn"

    gmap = "G:Map"
    ggbk = "G:GBK"
    gcv  = "G:CV"

    jtag1  = "J:Tag1"
    jtag2  = "J:Tag2"
    jtag3  = "J:Tag3"
    jfltn  = "J:Fltn"
    jgbk   = "J:GBK"
    juntag = "J:Untag"

    write1 = "Write1"
    write2 = "Write2"

------------------------------------------------------------------------
-- Utils

unsafeDynLookup :: (Ord k, Show k, Typeable v) => String -> k -> Map k Dynamic -> v
unsafeDynLookup msg k kvs = fromDyn (unsafeLookup msg k kvs) (error msg')
  where
    msg' = msg ++ ": name had wrong type : " ++ show k ++ " in " ++ show (M.toList kvs)

unsafeLookup :: (Ord k, Show k, Show v) => String -> k -> Map k v -> v
unsafeLookup msg k kvs = M.findWithDefault (error msg') k kvs
  where
    msg' = msg ++ ": name not found: " ++ show k ++ " in " ++ show (M.toList kvs)
