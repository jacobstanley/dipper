{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE PatternGuards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeOperators #-}
{-# OPTIONS_GHC -funbox-strict-fields #-}
{-# OPTIONS_GHC -w #-}

module Dipper.AST where

import           Data.Dynamic
import           Data.List (sort, nubBy)
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as M
import           Data.Set (Set)
import qualified Data.Set as S
import           Data.Tuple (swap)

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
    Read       i     -> Read       i
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

    Return x ->

        let
            x' = renameAtom names x
        in
            (gen0, Return x')

    Write o x tm ->

        let
            x'          = renameAtom names x
            (gen1, tm') = renameTerm gen0 names tm
        in
            (gen1, Write o x' tm')

    Let (Name n) tl tm ->

        let
            tl'         = renameTail names tl
            (n' : gen1) = gen0
            names'      = M.insert n n' names
            (gen2, tm') = renameTerm gen1 names' tm
        in
            (gen2, Let (Name n') tl' tm')

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
    Concat        xs -> Concat         (map (substAtom subs) xs)
    ConcatMap  f   x -> ConcatMap  f   (substAtom subs x)
    GroupByKey     x -> GroupByKey     (substAtom subs x)
    FoldValues f s x -> FoldValues f s (substAtom subs x)

substTerm :: (Ord n, Show n)
          => Map n Dynamic -- Dynamic :: Atom n _
          -> Term n a
          -> Term n a
substTerm subs term = case term of
    Return  x          -> Return  (substAtom subs x)
    Write o x tm       -> Write o (substAtom subs x) (substTerm subs tm)
    Let (Name n) tl tm -> Let (Name n) (substTail subs tl)
                                       (substTerm (M.delete n subs) tm)

------------------------------------------------------------------------
-- Free Variables

fvOfAtom :: Atom n a -> Set n
fvOfAtom atom = case atom of
    Var (Name n) -> S.singleton n
    Const _      -> S.empty

fvOfAtoms :: Ord n => [Atom n a] -> Set n
fvOfAtoms = S.unions . map fvOfAtom

fvOfTail :: Ord n => Tail n a -> Set n
fvOfTail tl = case tl of
    Read          _   -> S.empty
    Concat         xs -> fvOfAtoms xs
    ConcatMap     _ x -> fvOfAtom x
    GroupByKey      x -> fvOfAtom x
    FoldValues  _ _ x -> fvOfAtom x

fvOfTerm :: Ord n => Term n a -> Set n
fvOfTerm term = case term of
    Return  x          -> fvOfAtom x
    Write _ x tm       -> fvOfAtom x  `S.union` fvOfTerm tm
    Let (Name n) tl tm -> fvOfTail tl `S.union` S.delete n (fvOfTerm tm)

------------------------------------------------------------------------
-- Find dependencies of a given set of names

dependencies :: Ord n => Set n -> Term n a -> Set n
dependencies ns term = case term of
    Return _           -> ns
    Write _ _ tm       -> dependencies ns tm
    Let (Name n) tl tm ->
      let
          ns' = dependencies ns tm
      in
          if S.member n ns'
          then ns' `S.union` fvOfTail tl
          else ns'

------------------------------------------------------------------------
-- Find names which have a dependency on a given set of names

isAtomDependent :: Ord n => Set n -> Atom n a -> Bool
isAtomDependent ns atom = case atom of
    Var (Name n) -> S.member n ns
    Const _      -> False

isTailDependent :: Ord n => ([Bool] -> Bool) -> Set n -> Tail n a -> Bool
isTailDependent combine ns tl = case tl of
    Read          _   -> False
    Concat         xs -> combine (map (isAtomDependent ns) xs)
    ConcatMap     _ x -> isAtomDependent ns x
    GroupByKey      x -> isAtomDependent ns x
    FoldValues  _ _ x -> isAtomDependent ns x

dependents :: forall a n. Ord n => ([Bool] -> Bool) -> Set n -> Term n a -> Set n
dependents combine ns term = case term of
    Return  _          -> ns
    Write _ _ tm       -> dependents combine ns tm
    Let (Name n) tl tm -> dependents combine (insertConnected n tl) tm
  where
    insertConnected :: n -> Tail n b -> Set n
    insertConnected n tl
      | isTailDependent combine ns tl = S.insert n ns
      | otherwise                     = ns

------------------------------------------------------------------------
-- Remove GroupByKey
--
-- Grouping is implicit in the transition from mapper to reducer.

removeGroups :: Tag -> Term n () -> Term n ()
removeGroups tag term = case term of
    Let n (GroupByKey kvs) tm ->

      let
          out = Write (MapperOutput tag) kvs
          inp = Let n (Read (ReducerInput tag))

          tag' = nextTag "removeGroups" tag
      in
          out (inp (removeGroups tag' tm))

    Let  n tl tm -> Let  n tl (removeGroups tag tm)
    Return  x    -> Return x
    Write o x tm -> Write o x (removeGroups tag tm)


------------------------------------------------------------------------
-- Annotation

annotAtom :: (Ord n, Show n, Show m, Typeable m)
          => Map n m -> Atom n a -> Atom m a
annotAtom ns atom = case atom of
    Var (Name n) -> Var (Name (unsafeLookup "annotAtom" n ns))
    Const x      -> Const x

annotTail :: (Ord n, Show n, Show m, Typeable m)
          => Map n m -> Tail n a -> Tail m a
annotTail ns tl = case tl of
    Read          i   -> Read       i
    Concat         xs -> Concat         (map (annotAtom ns) xs)
    ConcatMap     g x -> ConcatMap  g   (annotAtom ns x)
    GroupByKey      x -> GroupByKey     (annotAtom ns x)
    FoldValues  g v x -> FoldValues g v (annotAtom ns x)

annotTerm :: (Ord n, Show n, Show m, Typeable m)
          => Map n m -> Term n a -> Term m a
annotTerm ns term = case term of
    Return  x          -> Return  (annotAtom ns x)
    Write o x tm       -> Write o (annotAtom ns x) (annotTerm ns tm)
    Let (Name n) tl tm -> Let (Name (unsafeLookup "annotTerm" n ns))
                              (annotTail ns tl)
                              (annotTerm ns tm)

------------------------------------------------------------------------
-- Distance to an input or output

data DistTo a = DistTo !a !Int
  deriving (Eq, Ord, Show, Typeable, Functor)

unDist :: DistTo a -> a
unDist (DistTo x _) = x

succDist :: DistTo a -> DistTo a
succDist (DistTo x n) = DistTo x (n+1)

succDists :: Set (DistTo a) -> Set (DistTo a)
succDists = S.mapMonotonic succDist

------------------------------------------------------------------------
-- Find Input Relations

inputsOfAtom :: (Show n, Ord n)
             => Map n (Set (DistTo Input'))
             -> Atom n a
             -> Set (DistTo Input')
inputsOfAtom env atom = case atom of
    Const _      -> S.empty
    Var (Name n) -> succDists (unsafeLookup "varInfoOfAtom" n env)

inputsOfTail :: (Show n, Ord n)
             => Map n (Set (DistTo Input'))
             -> Tail n a
             -> Set (DistTo Input')
inputsOfTail env tl = case tl of
    Read          i   -> S.singleton (DistTo (coerceInput i) 0)
    Concat         xs -> S.unions (map (inputsOfAtom env) xs)
    ConcatMap     _ x -> inputsOfAtom env x
    GroupByKey      x -> inputsOfAtom env x
    FoldValues  _ _ x -> inputsOfAtom env x

inputsOfTerm :: (Show n, Ord n)
             => Map n (Set (DistTo Input'))
             -> Term n a
             -> Map n (Set (DistTo Input'))
inputsOfTerm env term = case term of
    Return _           -> env
    Write _ _ tm       -> inputsOfTerm env tm
    Let (Name n) tl tm -> inputsOfTerm (M.insert n (inputsOfTail env tl) env) tm

------------------------------------------------------------------------
-- Find Output Relations

outputsOfTerm :: (Show n, Ord n) => Term n a -> Map n (Set (DistTo Output'))
outputsOfTerm term = case term of
    Return _     -> M.empty
    Write o x tm ->

      let
          o'tm = outputsOfTerm tm
          os   = S.singleton (DistTo (coerceOutput o) 0)
          o'x  = M.fromSet (const os) (fvOfAtom x)
      in
          M.unionWith S.union o'tm o'x

    Let (Name n) tl tm ->

      let
          o'tm = outputsOfTerm tm
          o'tl = case M.lookup n o'tm of
                   Nothing -> M.empty
                   Just os -> M.fromSet (const (succDists os)) (fvOfTail tl)
      in
          M.unionWith S.union o'tm o'tl

------------------------------------------------------------------------
-- Find Input/Output Relations

data InOut = I Input' | O Output'
  deriving (Eq, Ord, Typeable)

instance Show InOut where
    showsPrec p (I x) = showsPrec p x
    showsPrec p (O x) = showsPrec p x

ioOfTerm :: (Show n, Ord n) => Term n a -> Map n (Set (DistTo InOut))
ioOfTerm term = M.unionWith S.union is os
  where
    is = M.map (S.map (fmap I)) (inputsOfTerm M.empty term)
    os = M.map (S.map (fmap O)) (outputsOfTerm term)

------------------------------------------------------------------------
-- Input/Output Relation Helpers

consistentIO :: Set InOut -> Bool
consistentIO ios = all isM (S.toList ios)
                || all isR (S.toList ios)
  where
    isM (I (MapperInput _))  = True
    isM (O (MapperOutput _)) = True
    isM _                    = False

    isR = not . isM

printIO :: (Show n, Ord n) => Term n a -> IO ()
printIO term = mapM_ go
             . sort
             . map (\(x,y) -> (y,x))
             . filter (\(_,ios) -> not (consistentIO (S.map unDist ios)))
             $ M.toList (ioOfTerm term)
  where
    go (ios, k) = do
        putStr (show (S.toList ios))
        putStr " -> "
        print k

--------------------------------------------------------------------------
-- Find where to fix inconsistent IO for ReducerInput/MapperOutput
-- mismatches

-- TODO `findR2M` is where we split post-GBK chains and at the moment
-- TODO we are making a terrible choice! We should instead use some
-- TODO heuristic to decide which split will yield the smallest amount
-- TODO of communication.

-- | Find ReducerInput -> MapperOutput names.
findR2M :: Ord n => Map n (Set (DistTo InOut)) -> [n]
findR2M = map snd
        . nubBy eq
        . sort
        . map swap
        . M.toList
        . M.mapMaybe go
  where
    eq (DistTo x _, _) (DistTo y _, _) = x == y

    go s = case S.toAscList s of
      [ DistTo (I (ReducerInput i)) n,
        DistTo (O (MapperOutput o)) _ ] -> Just (DistTo (i, o) n)
      _                                 -> Nothing

--------------------------------------------------------------------------
-- Find where

-- | Find MapperInput -> ReducerOutput names.
findM2R :: Ord n => Map n (Set (DistTo InOut)) -> [n]
findM2R = map snd
        . nubBy eq
        . sort
        . map swap
        . M.toList
        . M.mapMaybe go
  where
    eq (DistTo x _, _) (DistTo y _, _) = x == y

    go s = case S.toAscList s of
      [ DistTo (I (MapperInput   i)) n,
        DistTo (O (ReducerOutput o)) _ ] -> Just (DistTo (i, o) n)
      _                                  -> Nothing

--------------------------------------------------------------------------
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

    Write o x tm -> Write o x (sinkConcatOfTerm fresh env tm)
    Return  x    -> Return  x
  where
    lets :: (Typeable n, Typeable a)
         => [(Name n a, Tail n a)]
         -> Term n b
         -> Term n b
    lets []            tm = tm
    lets ((n, tl):tls) tm = Let n tl (lets tls tm)
