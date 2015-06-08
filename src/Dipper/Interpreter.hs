{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeOperators #-}

{-# LANGUAGE FlexibleInstances #-}

{-# OPTIONS_GHC -w #-}

module Dipper.Interpreter where

import           Control.Monad (mplus)
import           Data.Binary.Get
import           Data.Binary.Put
import qualified Data.ByteString as B
import           Data.ByteString.Builder
import qualified Data.ByteString.Lazy as L
import           Data.Dynamic
import           Data.Int (Int32, Int64)
import           Data.List (groupBy, foldl', sort)
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as M
import           Data.Maybe (maybeToList, mapMaybe)
import           Data.Set (Set)
import qualified Data.Set as S
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import           Data.Typeable (Typeable)
import           Data.Word (Word8)

import           Dipper.AST
import           Dipper.Binary
import           Dipper.Product
import           Dipper.Sink
import           Dipper.Types

------------------------------------------------------------------------

data Formatted a = Formatted !a !KVFormat
  deriving (Eq, Ord, Show)

data Step i o = Step {
    sInput   ::  Formatted i
  , sOutputs :: [Formatted o]
  , sTerm    :: Term Int ()
  , sExec    :: Sink (Row o) -> Sink (Row ())
  }

type Mapper  = Step FilePath Tag
type Reducer = Step Tag FilePath

data Pipeline = Pipeline {
    pMappers  :: Map FilePath Mapper
  , pReducers :: Map Tag Reducer
  } deriving (Show)

------------------------------------------------------------------------

instance (Show i, Show o) => Show (Step i o) where
    showsPrec p (Step inp outs tm _) =
        showString "Step " . showsPrec 11 inp
                           . showString " "
                           . showsPrec 11 outs
                           . showString " "
                           . showsPrec 11 tm

instance Monoid Pipeline where
    mempty = Pipeline M.empty M.empty
    mappend (Pipeline ms rs)
            (Pipeline ms' rs') = Pipeline (M.union ms ms')
                                          (M.union rs rs')

------------------------------------------------------------------------

mkPipeline :: (Show n, Ord n) => Term n a -> Pipeline
mkPipeline term = foldMap mkStep
                . M.toList
                . M.mapMaybe rootInput
                . inputsOfTerm M.empty
                $ term'
  where
    term' = rename term

    mkStep (n, Input' i kvt) = case i of
        MapperInput path -> mkMapper (Formatted path kvt)
                                     (outputTags "mkPipeline" outs) pterm
        ReducerInput tag -> mkReducer (Formatted tag kvt)
                                      (outputPaths "mkPipeline" outs) pterm
      where
        pterm = partialTerm (S.singleton n) term'
        outs  = S.toList
              . S.unions
              . map (S.map unDist)
              . M.elems
              . outputsOfTerm
              $ pterm

    rootInput :: Set (DistTo Input') -> Maybe Input'
    rootInput s = case filter isRoot (S.toList s) of
      []           -> Nothing
      [DistTo i _] -> Just i
      _            -> error "mkPipeline: multiple inputs assigned to a single variable"

    isRoot (DistTo _ d) = d == 0

    kvUnit = unitFormat :*: unitFormat

------------------------------------------------------------------------

mkStep :: Formatted i -> [Formatted o] -> Term Int () -> Step i o
mkStep inp outs term = Step inp outs term undefined

mkMapper :: Formatted FilePath -> [Formatted Tag] -> Term Int () -> Pipeline
mkMapper inp@(Formatted path _) outs term =
    Pipeline (M.singleton path (Step inp outs term exec)) M.empty
  where
    exec = evalMapperTerm' term

mkReducer :: Formatted Tag -> [Formatted FilePath] -> Term Int () -> Pipeline
mkReducer inp@(Formatted tag _) outs term =
    Pipeline M.empty (M.singleton tag (mkStep inp outs term))

outputPaths :: String -> [Output'] -> [Formatted FilePath]
outputPaths msg = map go
  where
    go (Output' (ReducerOutput path) kv) = Formatted path kv
    go x = error (msg ++ ": inconsistent output: " ++ show x)

outputTags :: String -> [Output'] -> [Formatted Tag]
outputTags msg = map go
  where
    go (Output' (MapperOutput tag) kv) = Formatted tag kv
    go x = error (msg ++ ": inconsistent output: " ++ show x)

------------------------------------------------------------------------

replaceReads :: forall a n. (Ord n, Show n) => Term n a -> [Row ()] -> Term n a
replaceReads term rows = case term of
    Let (Name n :: Name n b) (Read _) tm ->

      let
           rows' :: Atom n b
           rows' = Const (map (decodeRow . dropTag) rows)
      in
           substTerm (M.singleton n (toDyn rows')) tm

    _  -> error "mkStep: initial term must be a read"

------------------------------------------------------------------------

-- TODO constants broken at the moment

type DynSink = Dynamic
type DynDup  = DynSink -> DynSink -> DynSink

instance Show (Dynamic -> Dynamic -> Dynamic) where
    showsPrec p _ = showParen (p > 10) (showString "DynamicFn")

dynDup :: forall a. Typeable a => Sink a -> DynDup
dynDup _ x y = toDyn (dup (unsafeFromDyn "dynDup" x :: Sink a)
                          (unsafeFromDyn "dynDup" y :: Sink a))

evalTail :: Ord n => DynSink -> Tail n a -> Map n (DynSink, DynDup)
evalTail sink tl = case tl of
    Read _        -> M.empty -- error "evalTail: found Read"
    GroupByKey _  -> error "evalTail: found GroupByKey"
    Concat (xs :: [Atom n a]) ->
      let
          sink' :: Sink a
          sink' = unsafeFromDyn "evalTail" sink
      in
          fvOfAtoms xs `withElem` (toDyn sink', dynDup sink')

    ConcatMap (f :: a -> [b]) x ->
      let
          sink' :: Sink a
          sink' = unmap f (unconcat (unsafeFromDyn "evalTail" sink))
      in
          fvOfAtom x `withElem` (toDyn sink', dynDup sink')

    FoldValues f v (x :: Atom n (k :*: [v])) ->
      let
          g :: k :*: [v] -> k :*: v
          g (k :*: vs) = k :*: foldl' f v vs

          sink' :: Sink (k :*: [v])
          sink' = unmap g (unsafeFromDyn "evalTail" sink)
      in
          fvOfAtom x `withElem` (toDyn sink', dynDup sink')
  where
    withElem s v = M.fromSet (const v) s

evalMapperTerm :: (Show n, Ord n) => Sink (Row Tag) -> Term n a -> Map n (DynSink, DynDup)
evalMapperTerm sink term = case term of
    Return _ -> M.empty

    Write (MapperOutput tag :: Output a) (Var (Name n)) tm ->
      let
          enc :: a -> Row Tag
          enc x = tag :*: encodeRow x

          sink' = unmap enc sink
          sinks = evalMapperTerm sink tm
      in
          M.insert n (toDyn sink', dynDup sink') sinks

    Let (Name n) tl tm ->
      let
          tm'sinks    = evalMapperTerm sink tm
          (n'sink, _) = unsafeLookup "evalMapperTerm" n tm'sinks
          tl'sinks    = evalTail n'sink tl
      in
          M.unionWith (\(s, d) (s', _) -> (d s s', d)) tm'sinks tl'sinks

evalMapperTerm' :: (Show n, Ord n) => Term n a -> Sink (Row Tag) -> Sink (Row ())
evalMapperTerm' term sink = case term of
    Let (Name n) (Read (_ :: Input a)) _ ->
      let
          (dynSink, _) = unsafeLookup "evalMapperTerm'" n sinks

          a'sink :: Sink a
          a'sink = unsafeFromDyn "evalMapperTerm'" dynSink

          row'sink = unmap (decodeRow . dropTag) a'sink
      in
          row'sink
  where
    sinks = evalMapperTerm sink term

------------------------------------------------------------------------

evalTailOld :: forall n a. (Ord n, Show n) => Map n Dynamic -> Tail n a -> [a]
evalTailOld env tl = case tl of
    Read  _           -> error "evalTailOld: Read"
    Concat        xss -> concatMap resolve xss
    ConcatMap   f  xs -> concatMap f (resolve xs)
    GroupByKey     xs -> group (resolve xs)
    FoldValues f x xs -> fold f x (resolve xs)
  where
    resolve :: (Ord n, Show n) => Atom n b -> [b]
    resolve (Var (Name n)) = unsafeDynLookup "evalTailOld" n env
    resolve (Const xs)     = xs

    fold :: (v -> v -> v) -> v -> [k :*: [v]] -> [k :*: v]
    fold f x xs = map (\(k :*: vs) -> k :*: foldl' f x vs) xs

    group :: Eq k => [k :*: v] -> [k :*: [v]]
    group = map fixGroup . groupBy keyEq

    keyEq :: Eq k => k :*: v -> k :*: v -> Bool
    keyEq (x :*: _) (y :*: _) = x == y

    fixGroup :: [k :*: v] -> k :*: [v]
    fixGroup []                = error "evalTailOld: groupBy yielded empty list: impossible"
    fixGroup ((k :*: v) : kvs) = k :*: (v : map snd' kvs)

evalTermOld :: (Ord n, Show n) => Map n Dynamic -> Term n (Row a) -> [Row a]
evalTermOld env term = case term of
    Write _ _ _           -> error "evalTermOld: Write"
    Return (Var (Name n)) -> unsafeDynLookup "evalTermOld" n env
    Return (Const xs)     -> xs
    Let (Name n) tl tm    ->
      let
          env' = M.insert n (toDyn (evalTailOld env tl)) env
      in
          evalTermOld env' tm

------------------------------------------------------------------------

decodeTagged :: Map Tag KVFormat -> L.ByteString -> [Row Tag]
decodeTagged schema bs =
    -- TODO this is unlikely to have good performance
    case runGetOrFail (getTagged schema) bs of
        Left  (_,   _, err)            -> error ("decodeTagged: " ++ err)
        Right (bs', o, x) | L.null bs' -> [x]
                          | otherwise  -> x : decodeTagged schema bs'

encodeTagged :: Map Tag KVFormat -> [Row Tag] -> L.ByteString
encodeTagged schema = runPut . mapM_ (putTagged schema)

getTagged :: Map Tag KVFormat -> Get (Row Tag)
getTagged schema = do
    tag <- getWord8
    case M.lookup tag schema of
      Nothing              -> fail ("getTagged: invalid tag <" ++ show tag ++ ">")
      Just (kFmt :*: vFmt) -> pure tag <&> getLayout (fmtLayout kFmt)
                                       <&> getLayout (fmtLayout vFmt)

putTagged :: Map Tag KVFormat -> Row Tag -> Put
putTagged schema (tag :*: k :*: v) =
    case M.lookup tag schema of
      Nothing              -> fail ("putTagged: invalid tag <" ++ show tag ++ ">")
      Just (kFmt :*: vFmt) -> putWord8 tag >> putLayout (fmtLayout kFmt) k
                                           >> putLayout (fmtLayout vFmt) v

------------------------------------------------------------------------

unitFormat :: Format
unitFormat = format (undefined :: ())

textFormat :: Format
textFormat = format (undefined :: T.Text)

bytesFormat :: Format
bytesFormat = format (undefined :: B.ByteString)

int32Format :: Format
int32Format = format (undefined :: Int32)

testEncDecTagged = take 10 (decodeTagged schema (encodeTagged schema xs))
  where
    xs = cycle [ 67 :*: "abcdefg" :*: B.empty
               , 67 :*: "123"     :*: B.empty
               , 22 :*: "1234"    :*: "Hello World!" ]

    schema = M.fromList [ (67, textFormat  :*: unitFormat)
                        , (22, int32Format :*: bytesFormat) ]
