{-# LANGUAGE GADTs #-}
{-# LANGUAGE PatternGuards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeOperators #-}
{-# OPTIONS_GHC -funbox-strict-fields #-}

module Dipper.AST where

import           Data.Dynamic
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as M
import           Data.Set (Set)
import qualified Data.Set as S

import           Dipper.Types

------------------------------------------------------------------------
-- Renaming

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
    Read         i   -> Read  i
    Write        o x -> Write o (renameAtom names x)
    Concat        xs -> Concat         (map (renameAtom names) xs)
    ConcatMap  f   x -> ConcatMap  f   (renameAtom names x)
    GroupByKey     x -> GroupByKey     (renameAtom names x)
    FoldValues f s x -> FoldValues f s (renameAtom names x)

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

    Return x ->

        let
            x' = renameAtom names x
        in
            (gen0, Return x')

------------------------------------------------------------------------
-- Substitution

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

    Const x -> Const x

substTail :: (Ord n, Show n)
          => Map n Dynamic -- Dynamic :: Atom n _
          -> Tail n a
          -> Tail n a
substTail subs tl = case tl of
    Read         i   -> Read         i
    Write        o x -> Write        o (substAtom subs x)
    Concat        xs -> Concat         (map (substAtom subs) xs)
    ConcatMap  f   x -> ConcatMap  f   (substAtom subs x)
    GroupByKey     x -> GroupByKey     (substAtom subs x)
    FoldValues f s x -> FoldValues f s (substAtom subs x)

substTerm :: (Ord n, Show n)
          => Map n Dynamic -- Dynamic :: Atom n _
          -> Term n a
          -> Term n a
substTerm subs term = case term of
    Let (Name n) tl tm -> Let (Name n) (substTail subs tl) (substTerm (M.delete n subs) tm)
    Return x           -> Return (substAtom subs x)

------------------------------------------------------------------------
-- Find dependencies of a given name

dependenciesOfAtom :: Ord n => Atom n a -> Set (Name' n)
dependenciesOfAtom atom = case atom of
    Var   n -> S.singleton (coerceName n)
    Const _ -> S.empty

dependenciesOfTail :: Ord n => Tail n a -> Set (Name' n)
dependenciesOfTail tl = case tl of
    Read          _   -> S.empty
    Write         _ x -> dependenciesOfAtom x
    Concat         xs -> S.unions (map dependenciesOfAtom xs)
    ConcatMap     _ x -> dependenciesOfAtom x
    GroupByKey      x -> dependenciesOfAtom x
    FoldValues  _ _ x -> dependenciesOfAtom x

dependencies :: forall a n. Ord n => Set (Name' n) -> Term n a -> Set (Name' n)
dependencies ns term = case term of
    Let n tl tm ->
      let
          ns' = dependencies ns tm
          n'  = coerceName n
      in
          if S.member n' ns'
          then dependenciesOfTail tl `S.union` ns'
          else ns'

    Return _ -> ns

------------------------------------------------------------------------
-- Find names which have a dependency on a given name

isAtomDependent :: Ord n => Set (Name' n) -> Atom n a -> Bool
isAtomDependent ns atom = case atom of
    Var   n -> S.member (coerceName n) ns
    Const _ -> False

isTailDependent :: Ord n => ([Bool] -> Bool) -> Set (Name' n) -> Tail n a -> Bool
isTailDependent combine ns tl = case tl of
    Read          _   -> False
    Write         _ x -> isAtomDependent ns x
    Concat         xs -> combine (map (isAtomDependent ns) xs)
    ConcatMap     _ x -> isAtomDependent ns x
    GroupByKey      x -> isAtomDependent ns x
    FoldValues  _ _ x -> isAtomDependent ns x

dependents :: forall a n. Ord n => ([Bool] -> Bool) -> Set (Name' n) -> Term n a -> Set (Name' n)
dependents combine ns term = case term of
    Let n tl tm -> dependents combine (insertConnected (coerceName n) tl) tm
    Return _    -> ns
  where
    insertConnected :: Name' n -> Tail n b -> Set (Name' n)
    insertConnected n tl
      | isTailDependent combine ns tl = S.insert n ns
      | otherwise                     = ns

------------------------------------------------------------------------
-- Remove GroupByKey
--
-- Grouping is implicit in the transition from mapper to reducer.

removeGroups :: [n] -> Tag -> Term n () -> Term n ()
removeGroups names tag term = case term of
    Let n (GroupByKey kvs) tm ->

      let
          (name : names') = names

          n' = Name name

          out = Let n' (Write (MapperOutput tag) kvs)
          inp = Let n  (Read (ReducerInput tag))

          tag' = nextTag "removeGroups" tag
      in
          out (inp (removeGroups names' tag' tm))

    Let  n tl tm -> Let  n tl (removeGroups names tag tm)
    Return x     -> Return x

------------------------------------------------------------------------
-- Utils

freshNames :: [String]
freshNames = map (\x -> "x" ++ show x) ([1..] :: [Integer])

nextTag :: String -> Tag -> Tag
nextTag msg tag | tag /= maxBound = succ tag
                | otherwise       = error msg'
  where
    msg' = msg ++ ": exceeded maximum number of reducers <" ++ show tag ++ ">"

unsafeDynLookup :: (Ord k, Show k, Typeable v) => String -> k -> Map k Dynamic -> v
unsafeDynLookup msg k kvs = fromDyn (unsafeLookup msg k kvs) (error msg')
  where
    msg' = msg ++ ": name had wrong type : " ++ show k ++ " in " ++ show (M.toList kvs)

unsafeLookup :: (Ord k, Show k, Show v) => String -> k -> Map k v -> v
unsafeLookup msg k kvs = M.findWithDefault (error msg') k kvs
  where
    msg' = msg ++ ": name not found: " ++ show k ++ " in " ++ show (M.toList kvs)

------------------------------------------------------------------------
-- Sink Flattens from FlumeJava paper

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

    Return x -> Return x
  where
    lets :: (Typeable n, Typeable a)
         => [(Name n a, Tail n a)]
         -> Term n b
         -> Term n b
    lets []            tm = tm
    lets ((n, tl):tls) tm = Let n tl (lets tls tm)
