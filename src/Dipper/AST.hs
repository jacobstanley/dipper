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
import           Data.Text (Text)
import qualified Data.Text as T

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

    Let n tl tm -> Let n tl (sinkConcatOfTerm fresh env tm)
    Ret   tl    -> Ret   tl

------------------------------------------------------------------------

renameAtom :: (Ord a, Show a, Show b, Typeable b)
           => Map a b
           -> Atom a r
           -> Atom b r
renameAtom names atom = case atom of
    Var (Name x)          -> Var (Name (unsafeLookup "renameAtom" x names))
    Var (MapperInput   x) -> Var (MapperInput   x)
    Var (MapperOutput  x) -> Var (MapperOutput  x)
    Var (ReducerInput  x) -> Var (ReducerInput  x)
    Var (ReducerOutput x) -> Var (ReducerOutput x)
    Const x               -> Const x

renameTail :: (Ord a, Show a, Show b, Typeable b)
           => Map a b
           -> Tail a r
           -> Tail b r
renameTail names tl = case tl of
    Concat        xs     -> Concat         (map (renameAtom names) xs)
    ConcatMap  f   x     -> ConcatMap  f   (renameAtom names x)
    GroupByKey     x     -> GroupByKey     (renameAtom names x)
    FoldValues f s x     -> FoldValues f s (renameAtom names x)

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

    Let n tl tm ->

        let
            n' = case n of
                   Name          _ -> error "renameTerm: impossible"
                   MapperInput   x -> MapperInput   x
                   MapperOutput  x -> MapperOutput  x
                   ReducerInput  x -> ReducerInput  x
                   ReducerOutput x -> ReducerOutput x

            tl'         = renameTail names tl
            (gen1, tm') = renameTerm gen0 names tm
        in
            (gen1, Let n' tl' tm')

    Ret tl ->

        let
            tl' = renameTail names tl
        in
            (gen0, Ret tl')

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

    Var   x -> Var x
    Const x -> Const x

substTail :: (Ord n, Show n)
          => Map n Dynamic -- Dynamic :: Atom n _
          -> Tail n a
          -> Tail n a
substTail subs tl = case tl of
    Concat        xs     -> Concat         (map (substAtom subs) xs)
    ConcatMap  f   x     -> ConcatMap  f   (substAtom subs x)
    GroupByKey     x     -> GroupByKey     (substAtom subs x)
    FoldValues f s x     -> FoldValues f s (substAtom subs x)

substTerm :: (Ord n, Show n)
          => Map n Dynamic -- Dynamic :: Atom n _
          -> Term n a
          -> Term n a
substTerm subs term = case term of
    Let (Name n) tl tm -> Let (Name n) (substTail subs tl) (substTerm (M.delete n subs) tm)
    Let       n  tl tm -> Let       n  (substTail subs tl) (substTerm subs tm)
    Ret          tl    -> Ret          (substTail subs tl)

------------------------------------------------------------------------

example1 :: Term String ()
example1 =
    Let x0     (Concat [Var input]) $
    Let x1     (ConcatMap (\x -> [x + 1]) (Var x0)) $
    Let x2     (ConcatMap (\x -> [x * 2]) (Var x0)) $
    Let x3     (Concat [Var x1, Var x2]) $
    Let output (Concat [Var x3]) $
    Ret        (Concat [])
  where
    input  = MapperInput   "input.csv"  :: Name String Int
    output = ReducerOutput "output.csv" :: Name String Int

    x0 = Name "x0" :: Name String Int
    x1 = Name "x1"
    x2 = Name "x2"
    x3 = Name "x3" :: Name String Int

------------------------------------------------------------------------

example2 :: Term String ()
example2 =
    Let a       (ConcatMap add1 (Var input1)) $
    Let output1 (ConcatMap add1 (Var a)) $
    Let jtag1   (ConcatMap (tag 1) (Var a)) $
    Let b       (ConcatMap add1 (Var input2)) $
    Let c       (ConcatMap add1 (Var input3)) $
    Let fltn    (Concat [Var b, Var c]) $
    Let d       (ConcatMap add1 (Var fltn)) $
    Let jtag2   (ConcatMap (tag 2) (Var d)) $
    Let gmap    (ConcatMap kv_add1 (Var input4)) $
    Let ggbk    (GroupByKey (Var gmap)) $
    Let gcv     (FoldValues (+) 0 (Var ggbk)) $
    Let e       (ConcatMap untag (Var gcv)) $
    Let jtag3   (ConcatMap (tag 3) (Var e)) $
    Let jfltn   (Concat [Var jtag1, Var jtag2, Var jtag3]) $
    Let jgbk    (GroupByKey (Var jfltn)) $
    Let juntag  (ConcatMap untag (Var jgbk)) $
    Let f       (ConcatMap id (Var juntag)) $
    Let output2 (Concat [Var f]) $
    Ret         (Concat [])
  where
    add1 :: Int -> [Int]
    add1 x = [x+1]

    kv_add1 :: Text :*: Int -> [Text :*: Int]
    kv_add1 (k :*: v) = [k :*: v+1]

    tag :: Int -> a -> [Int :*: a]
    tag ix x = [ix :*: x]

    untag :: k :*: v -> [v]
    untag (_ :*: x) = [x]

    -- i/o
    input1  = MapperInput "input1.csv"
    input2  = MapperInput "input2.csv"
    input3  = MapperInput "input3.csv"
    input4  = MapperInput "input4.csv"
    output1 = ReducerOutput "output1.csv"
    output2 = ReducerOutput "output2.csv"

    -- names
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

------------------------------------------------------------------------

example3 :: Term String ()
example3 =
    Let concat1 (Concat [Var input1, Var input2]) $
    Let concat2 (Concat [Var input2, Var input3]) $
    Let text1   (ConcatMap (\(k :*: v) -> [k :*: T.pack (show v)]) (Var concat1)) $
    Let output1 (GroupByKey (Var text1)) $
    Let output2 (GroupByKey (Var concat2)) $
    Ret         (Concat [])
  where
    input1  = MapperInput "in-1.csv" :: Name String (Text :*: Int)
    input2  = MapperInput "in-2.csv"
    input3  = MapperInput "in-3.csv"
    output1 = ReducerOutput "out-1.csv" :: Name String (Text :*: [Text])
    output2 = ReducerOutput "out-1.csv" :: Name String (Text :*: [Int])

    text1   = "text1"
    concat1 = "concat1"
    concat2 = "concat2"

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
