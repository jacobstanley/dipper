{-# LANGUAGE GADTs #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeOperators #-}

{-# LANGUAGE FlexibleContexts #-}

{-# OPTIONS_GHC -w #-}

module Dipper.Interpreter where

import           Control.Monad (mplus)
import           Control.Monad.State (MonadState(..), modify, execState)
import           Data.Binary.Get
import           Data.Binary.Put
import qualified Data.ByteString as B
import           Data.ByteString.Builder
import qualified Data.ByteString.Lazy as L
import           Data.Dynamic
import           Data.Foldable (traverse_)
import           Data.Int (Int32, Int64)
import           Data.List (groupBy, foldl', sort, unfoldr, isSuffixOf)
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as M
import           Data.Maybe (maybeToList, mapMaybe)
import           Data.Set (Set)
import qualified Data.Set as S
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import           Data.Tuple.Strict (Pair(..))
import           Data.Typeable (Typeable)
import           Data.Word (Word8)

import           Dipper.AST
import           Dipper.Binary
import           Dipper.Sink
import           Dipper.Types

import           Debug.Trace

------------------------------------------------------------------------

data Formatted a = Formatted {
    fName   :: !a
  , fFormat :: !KVFormat
  } deriving (Eq, Ord, Show)

data Step i o = Step {
    stepInput   ::  Formatted i
  , stepOutputs :: [Formatted o]
  , stepTerm    :: Term Int ()
  , stepExec    :: (Applicative f, Typeable f)
                => Sink f (Row o)
                -> Sink f (Row ())
  }

type Mapper  = Step FilePath Tag
type Reducer = Step Tag FilePath

data Stage = Stage {
    stageInputs  :: Map FilePath KVFormat
  , stageOutputs :: Map FilePath KVFormat
  } deriving (Eq, Ord, Show)

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
    mappend (Pipeline ms  rs)
            (Pipeline ms' rs') = Pipeline (M.union ms ms')
                                          (M.union rs rs')

------------------------------------------------------------------------

testPipeline :: Pipeline
             -> Map FilePath L.ByteString
             -> Map FilePath L.ByteString
testPipeline p@Pipeline{..} files = foldl runStage files stages
  where
    stages :: [Stage]
    stages = stagesOfPipeline p

    runStage :: Map FilePath L.ByteString -> Stage
             -> Map FilePath L.ByteString
    runStage fs Stage{..}
      | not (null missing) = error ("testPipeline.runStage: missing inputs: " ++ show missing)
      | otherwise = M.union fs
                  . createFiles
                  . runReducer
                  . shuffle
                  . concat
                  . M.elems
                  $ M.intersectionWith runMapper pMappers avail
      where
        missing :: [FilePath]
        missing = M.keys (stageInputs `M.difference` fs)

        avail :: Map FilePath L.ByteString
        avail = fs `M.intersection` stageInputs

        createFiles :: [Row FilePath] -> Map FilePath L.ByteString
        createFiles rows = M.mapWithKey go stageOutputs
          where
            go path fmt = encodeUnitRows fmt
                        . map (withTag ())
                        . filter (hasTag path)
                        $ rows

        runMapper :: Mapper -> L.ByteString -> [Row Tag]
        runMapper Step{..} bs = orows
          where
            path   = fName stepInput
            schema = fFormat stepInput
            irows  = decodeUnitRows schema bs
            orows  = execState (pushMany sink irows) []
            sink   = stepExec tagSink

        -- TODO Detect correct target format
        shuffle :: [Row Tag] -> [Row Tag]
        shuffle = sort . map flatten . groupBy eq
          where
            eq (Row t0 k0 _) (Row t1 k1 _) = (t0, k0) == (t1, k1)

            flatten rs@(Row t k _ : _) = Row t k (B.concat (map rowValue rs))

        runReducer :: [Row Tag] -> [Row FilePath]
        runReducer irows = traceShowId orows
          where
            orows  = flip execState []
                   . traverse_ (\row -> sinkOf (rowTag row) `push` withTag () row)
                   . traceShowId
                   . decodeTagRows schema
                   . encodeTagRows schema
                   $ irows

            sinkOf tag = unsafeLookup "testPipeline.runReducer" tag sinks
            sinks      = M.map (\r -> stepExec r fileSink)   pReducers
            schema     = M.map (\r -> fFormat (stepInput r)) pReducers

    tagSink :: MonadState [Row Tag] m => Sink m (Row Tag)
    tagSink = Sink (\x -> modify (++[x]))

    fileSink :: MonadState [Row FilePath] m => Sink m (Row FilePath)
    fileSink = Sink (\x -> modify (++[x]))

------------------------------------------------------------------------

filesOfPipeline :: Pipeline -> Map FilePath KVFormat
filesOfPipeline Pipeline{..} = ins `M.union` outs
  where
    ins = M.fromList
        . map (\(Formatted n f) -> (n, f))
        . map stepInput
        . M.elems
        $ pMappers

    outs = M.fromList
         . map (\(Formatted n f) -> (n, f))
         . concatMap stepOutputs
         . M.elems
         $ pReducers

------------------------------------------------------------------------

stagesOfPipeline :: Pipeline -> [Stage]
stagesOfPipeline p@Pipeline{..} = unfoldr nextStage inputPaths
  where
    -- TODO The suffix check below is a bit dodgy, we
    -- TODO should differentiate temporary files using a
    -- TODO separate data constructor. Having said that,
    -- TODO it might be useful to be able to resume long
    -- TODO running jobs if we keep temporary files around.
    inputPaths = S.fromList
               . filter (not . (".tmp" `isSuffixOf`))
               $ M.keys pMappers

    fromSet s = filesOfPipeline p `M.intersection` M.fromSet (const ()) s

    nextStage :: Set FilePath -> Maybe (Stage, Set FilePath)
    nextStage avail | S.null m'used = Nothing
                    | otherwise     = Just (Stage (fromSet m'used)
                                                  (fromSet r'outs), avail')
      where
        (m'used, m'outs) = runnable (mkDeps pMappers)  avail
        (_,      r'outs) = runnable (mkDeps pReducers) m'outs
        avail'           = S.union (avail `S.difference` m'used) r'outs

    runnable :: (Show i, Ord i, Ord o) => Map o (Set i) -> Set i -> (Set i, Set o)
    runnable odeps avail =
        case foldr go (Right (S.empty, S.empty)) (M.toList odeps) of
          Left avail' -> runnable odeps avail'
          Right   os  -> os
      where
        go _ (Left  xs) = Left xs
        go (o, deps) (Right (is, os))
            | S.null (deps `S.difference` avail)   = Right (deps `S.union` is, S.insert o os)
            | S.null (avail `S.intersection` deps) = Right (is, os)
            | otherwise                            = Left (avail `S.difference` deps)

    mkDeps :: (Ord i, Ord o) => Map i (Step i o) -> Map o (Set i)
    mkDeps = M.unionsWith S.union
           . map mkMap
           . M.elems

    mkMap :: (Ord i, Ord o) => Step i o -> Map o (Set i)
    mkMap Step{..} =
        foldr (\o -> M.insert o (S.singleton (fName stepInput)))
                                M.empty
                                (map fName stepOutputs)

------------------------------------------------------------------------

mkPipeline :: (Ord n, Show n) => Term n () -> Pipeline
mkPipeline term = foldMap mkStepPipeline
                . M.toList
                . M.mapMaybe rootInput
                . inputsOfTerm M.empty
                $ term'
  where
    term' = rename
          . fixR2M tempPaths
          . fixM2R
          . removeGroups 0
          . rename
          $ term

    mkStepPipeline (n, Input' i kvt) = case i of
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

    kvUnit = unitFormat :!: unitFormat

------------------------------------------------------------------------

mkMapper :: Formatted FilePath -> [Formatted Tag] -> Term Int () -> Pipeline
mkMapper inp@(Formatted path _) outs term =
    Pipeline (M.singleton path step) M.empty
  where
    step = Step inp outs term (evalMapperTerm' term)

mkReducer :: Formatted Tag -> [Formatted FilePath] -> Term Int () -> Pipeline
mkReducer inp@(Formatted tag _) outs term =
    Pipeline M.empty (M.singleton tag step)
  where
    step = Step inp outs term (evalReducerTerm' term)

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

newtype DynSink (f :: * -> *) = DynSink Dynamic
    deriving (Show)

dynSink :: (Typeable f, Typeable a) => Sink f a -> DynSink f
dynSink = DynSink . toDyn

------------------------------------------------------------------------

newtype DynDup f = DynDup (DynSink f -> DynSink f -> DynSink f)

instance Show (DynDup f) where
    showsPrec p _ = showParen (p > 10) (showString "DynDup")

fromDynSink :: (Typeable f, Typeable a) => String -> DynSink f -> Sink f a
fromDynSink msg (DynSink x) = unsafeFromDyn msg x

dynDup :: forall f a. (Applicative f, Typeable f, Typeable a) => Sink f a -> DynDup f
dynDup _ = DynDup (\x y -> dynSink (dup (fromDynSink "dynDup" x :: Sink f a)
                                        (fromDynSink "dynDup" y :: Sink f a)))

dynJoin :: (DynSink f, DynDup f)
        -> (DynSink f, DynDup f)
        -> (DynSink f, DynDup f)
dynJoin (s0, DynDup d) (s1, _) = (d s0 s1, DynDup d)

------------------------------------------------------------------------

-- TODO constants broken at the moment

evalTail :: forall f n a. (Applicative f, Typeable f, Ord n)
         => DynSink f
         -> Tail n a
         -> Map n (DynSink f, DynDup f)
evalTail sink tl = case tl of
    Read _        -> M.empty
    GroupByKey _  -> error "evalTail: found GroupByKey"
    Concat (xs :: [Atom n a]) ->
      let
          sink' :: Sink f a
          sink' = fromDynSink "evalTail" sink
      in
          fvOfAtoms xs `withElem` (dynSink sink', dynDup sink')

    ConcatMap (f :: b -> [a]) x ->
      let
          sink' :: Sink f b
          sink' = unmap f (unconcat (fromDynSink "evalTail" sink))
      in
          fvOfAtom x `withElem` (dynSink sink', dynDup sink')

    FoldValues f v (x :: Atom n (Pair k [v])) ->
      let
          g :: Pair k [v] -> Pair k v
          g (k :!: vs) = k :!: foldl' f v vs

          sink' :: Sink f (Pair k [v])
          sink' = unmap g (fromDynSink "evalTail" sink)
      in
          fvOfAtom x `withElem` (dynSink sink', dynDup sink')
  where
    withElem s v = M.fromSet (const v) s

------------------------------------------------------------------------

evalMapperTerm :: (Applicative f, Typeable f, Show n, Ord n)
               => Sink f (Row Tag)
               -> Term n a
               -> Map n (DynSink f, DynDup f)
evalMapperTerm sink term = case term of
    Return _ -> M.empty

    Write (MapperOutput tag :: Output a) (Var (Name n)) tm ->
      let
          enc :: a -> Row Tag
          enc x = withTag tag (encodeKV x)

          sink' = unmap enc sink
          sinks = evalMapperTerm sink tm
      in
          M.insert n (dynSink sink', dynDup sink') sinks

    Write _ _ _ -> error "evalMapperTerm: found reducer output"

    Let (Name n) tl tm ->
      let
          tm'sinks    = evalMapperTerm sink tm
          (n'sink, _) = unsafeLookup "evalMapperTerm" n tm'sinks
          tl'sinks    = evalTail n'sink tl
      in
          M.unionWith dynJoin tm'sinks tl'sinks

evalMapperTerm' :: forall f n a. (Applicative f, Typeable f, Show n, Ord n)
                => Term n a
                -> Sink f (Row Tag)
                -> Sink f (Row ())
evalMapperTerm' term sink = case term of
    Let (Name n) (Read (_ :: Input b)) _ ->
      let
          (dynSink, _) = unsafeLookup "evalMapperTerm'" n sinks

          a'sink :: Sink f b
          a'sink =  fromDynSink "evalMapperTerm'" dynSink

          row'sink = unmap decodeKV a'sink
      in
          row'sink
  where
    sinks = evalMapperTerm sink term

------------------------------------------------------------------------

-- TODO evalReducerTerm is virtually the same as evalMapperTerm

evalReducerTerm :: (Applicative f, Typeable f, Show n, Ord n)
                => Sink f (Row FilePath)
                -> Term n a
                -> Map n (DynSink f, DynDup f)
evalReducerTerm sink term = case term of
    Return _ -> M.empty

    Write (ReducerOutput path :: Output a) (Var (Name n)) tm ->
      let
          enc :: a -> Row FilePath
          enc x = withTag path (encodeKV x)

          sink' = unmap enc sink
          sinks = evalReducerTerm sink tm
      in
          M.insert n (dynSink sink', dynDup sink') sinks

    Write _ _ _ -> error "evalReducerTerm: found reducer output"

    Let (Name n) tl tm ->
      let
          tm'sinks    = evalReducerTerm sink tm
          (n'sink, _) = unsafeLookup "evalReducerTerm" n tm'sinks
          tl'sinks    = evalTail n'sink tl
      in
          M.unionWith dynJoin tm'sinks tl'sinks

evalReducerTerm' :: forall f n a. (Applicative f, Typeable f, Show n, Ord n)
                 => Term n a
                 -> Sink f (Row FilePath)
                 -> Sink f (Row ())
evalReducerTerm' term sink = case term of
    Let (Name n) (Read (_ :: Input b)) _ ->
      let
          (dynSink, _) = unsafeLookup "evalReducerTerm'" n sinks

          a'sink :: Sink f b
          a'sink = fromDynSink "evalReducerTerm'" dynSink

          row'sink = unmap decodeKV a'sink
      in
          row'sink
  where
    sinks = evalReducerTerm sink term

------------------------------------------------------------------------

decodeUnitRows :: KVFormat -> L.ByteString -> [Row ()]
decodeUnitRows schema = decodeRows (return ()) (M.singleton () schema)

encodeUnitRows :: KVFormat -> [Row ()] -> L.ByteString
encodeUnitRows schema = encodeRows (const (return ())) (M.singleton () schema)

decodePathRows :: Map FilePath KVFormat -> L.ByteString -> [Row FilePath]
decodePathRows = decodeRows (T.unpack . T.decodeUtf8 <$> getLayout VarVInt)

encodePathRows :: Map FilePath KVFormat -> [Row FilePath] -> L.ByteString
encodePathRows = encodeRows (putLayout VarVInt . T.encodeUtf8 . T.pack)

decodeTagRows :: Map Tag KVFormat -> L.ByteString -> [Row Tag]
decodeTagRows = decodeRows getWord8

encodeTagRows :: Map Tag KVFormat -> [Row Tag] -> L.ByteString
encodeTagRows = encodeRows putWord8

------------------------------------------------------------------------

decodeRows :: (Show a, Ord a) => Get a -> Map a KVFormat -> L.ByteString -> [Row a]
decodeRows getTag schema bs =
    -- TODO this is unlikely to have good performance
    case runGetOrFail (getRow getTag schema) bs of
        Left  (_,   _, err)            -> error ("decodeRows: " ++ err)
        Right (bs', o, x) | L.null bs' -> [x]
                          | otherwise  -> x : decodeRows getTag schema bs'

encodeRows :: (Show a, Ord a) => (a -> Put) -> Map a KVFormat -> [Row a] -> L.ByteString
encodeRows putTag schema = runPut . mapM_ (putRow putTag schema)

getRow :: (Show a, Ord a) => Get a -> Map a KVFormat -> Get (Row a)
getRow getTag schema = do
    tag <- getTag
    case M.lookup tag schema of
      Nothing              -> fail ("getRow: invalid tag <" ++ show tag ++ ">")
      Just (kFmt :!: vFmt) -> Row tag <$> getLayout (fmtLayout kFmt)
                                      <*> getLayout (fmtLayout vFmt)

putRow :: (Show a, Ord a) => (a -> Put) -> Map a KVFormat -> Row a -> Put
putRow putTag schema (Row tag k v) =
    case M.lookup tag schema of
      Nothing              -> fail ("putRow: invalid tag <" ++ show tag ++ ">")
      Just (kFmt :!: vFmt) -> putTag tag >> putLayout (fmtLayout kFmt) k
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

testEncDecTagged = take 10 (decodeRows getWord8 schema (encodeRows putWord8 schema xs))
  where
    xs = cycle [ Row 67 "abcdefg" B.empty
               , Row 67 "123"     B.empty
               , Row 22 "1234"    "Hello World!" ]

    schema = M.fromList [ (67, textFormat  :!: unitFormat)
                        , (22, int32Format :!: bytesFormat) ]

sampleTextText :: L.ByteString
sampleTextText = encodeUnitRows (textFormat :!: textFormat) rows
  where
    rows = [ Row () "hello"   "hello"
           , Row () "hello"   "world"
           , Row () "goodbye" "goodbye"
           , Row () "foo"     "foo"
           , Row () "foo"     "bar"
           , Row () "foo"     "baz"
           , Row () "foo"     "quxx"
           ]
