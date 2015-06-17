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

import           Control.Exception (SomeException)
import           Control.Monad (mplus, void, forever)
import           Control.Monad.Catch (MonadThrow(..))
import           Control.Monad.Identity (Identity(..))
import           Control.Monad.State (MonadState(..), modify, runState, execState)
import           Data.Binary.Get
import           Data.Binary.Put
import qualified Data.ByteString as B
import           Data.ByteString.Builder
import qualified Data.ByteString.Lazy as L
import           Data.Conduit ((=$=), ($$), runConduit, yield, sequenceConduits, sequenceSinks)
import           Data.Conduit (Source, Sink, Consumer, Producer, Conduit)
import qualified Data.Conduit.Binary as CB
import qualified Data.Conduit.List as CL
import           Data.Conduit.Serialization.Binary (conduitGet, conduitPut)
import           Data.Dynamic
import           Data.Foldable (traverse_)
import           Data.Int (Int32, Int64)
import           Data.List (foldl', sort, sortBy, unfoldr, isSuffixOf)
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as M
import           Data.Maybe (fromMaybe)
import           Data.Maybe (maybeToList, mapMaybe)
import           Data.Ord (Ordering(..))
import           Data.Set (Set)
import qualified Data.Set as S
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import           Data.Tuple.Strict (Pair(..))
import           Data.Typeable (Typeable)
import           Data.Word (Word8)
import           System.FilePath.Posix (takeBaseName)

import           Dipper.AST
import           Dipper.Binary
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
  , stepExec    :: (Monad m, Typeable m) => Conduit (Row ()) m (Row o)
  }

type Mapper  = Step FilePath Tag
type Reducer = Step Tag FilePath

data Stage = Stage {
    stageInputs   :: Map FilePath KVFormat
  , stageShuffle  :: Map Tag      KVFormat
  , stageOutputs  :: Map FilePath KVFormat
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
      | otherwise = let
                        mapOutputs :: Map FilePath (Source (Either SomeException) (Row Tag))
                        mapOutputs = M.intersectionWith runMapper pMappers avail

                        mapOutput :: Source (Either SomeException) (Row Tag)
                        mapOutput = sequence_ (M.elems mapOutputs)

                        reduceOutput = createFiles (mapOutput =$= shuffle =$= runReducer)
                    in
                        M.union fs reduceOutput
      where
        missing :: [FilePath]
        missing = M.keys (stageInputs `M.difference` fs)

        avail :: Monad m => Map FilePath (Source m B.ByteString)
        avail = M.map CB.sourceLbs fs `M.intersection` stageInputs

        createFiles :: Source (Either SomeException) (Row FilePath) -> Map FilePath L.ByteString
        createFiles rows = M.mapWithKey go stageOutputs
          where
            go :: FilePath -> KVFormat -> L.ByteString
            go path fmt = L.fromChunks
                        . either (error . show) id
                        . runConduit
                        $ rows
                      =$= CL.filter (hasTag path)
                      =$= CL.map (withTag ())
                      =$= encodeUnitRows fmt
                      =$= CL.consume

        runMapper :: (MonadThrow m, Typeable m)
                  => Mapper
                  -> Source m B.ByteString
                  -> Source m (Row Tag)
        runMapper Step{..} = (=$= decodeUnitRows schema =$= stepExec)
          where
            path   = fName stepInput
            schema = fFormat stepInput

        runReducer :: (MonadThrow m, Typeable m) => Conduit (Row Tag) m (Row FilePath)
        runReducer = encodeTagRows schema
                 =$= decodeTagRows schema
                 =$= reducers
          where
            reducers = void
                     . sequenceConduits
                     . M.elems
                     . M.mapWithKey (\tag r -> CL.filter (hasTag tag)
                                           =$= CL.map (withTag ())
                                           =$= stepExec r)
                     $ pReducers

    fileSink :: Sink (Row FilePath) Identity [Row FilePath]
    fileSink = CL.consume

    shuffle :: Monad m => Conduit (Row Tag) m (Row Tag)
    shuffle = do
        xs <- CL.consume
        CL.sourceList (sortBy tagKeyCompare xs)

    tagKeyCompare :: Row Tag -> Row Tag -> Ordering
    tagKeyCompare (Row t1 k1 _) (Row t2 k2 _) =
        case compare t1 t2 of
            EQ  -> fmtCompare kFmt k1 k2
            ord -> ord
      where
        (kFmt :!: _) = unsafeLookup "testPipeline.tagKeyCompare" t1 schema

    schema :: Map Tag KVFormat
    schema = M.map (\r -> fFormat (stepInput r)) pReducers

------------------------------------------------------------------------

-- TODO these are almost the same

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

tagsOfPipeline :: Pipeline -> Map Tag KVFormat
tagsOfPipeline Pipeline{..} = ins `M.union` outs
  where
    ins = M.fromList
        . map (\(Formatted n f) -> (n, f))
        . concatMap stepOutputs
        . M.elems
        $ pMappers

    outs = M.fromList
         . map (\(Formatted n f) -> (n, f))
         . map stepInput
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
               . filter (\path -> takeBaseName path /= "temporary")
               $ M.keys pMappers

    fromFiles s = filesOfPipeline p `M.intersection` M.fromSet (const ()) s
    fromTags  s = tagsOfPipeline  p `M.intersection` M.fromSet (const ()) s

    nextStage :: Set FilePath -> Maybe (Stage, Set FilePath)
    nextStage avail | S.null m'used = Nothing
                    | otherwise     = Just (Stage (fromFiles m'used)
                                                  (fromTags  m'outs)
                                                  (fromFiles r'outs), avail')
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

mkPipeline :: (Ord n, Show n) => FilePath -> Term n () -> Pipeline
mkPipeline jobDir term = foldMap mkStepPipeline
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

    tempPaths = map (\x -> jobDir ++ "/temporary." ++ show x) ([1..] :: [Int])

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

newtype DynConduit (m :: * -> *) b = DynConduit Dynamic
    deriving (Show)

dynConduit :: (Typeable m, Typeable a, Typeable b) => Conduit a m b -> DynConduit m b
dynConduit = DynConduit . toDyn

------------------------------------------------------------------------

newtype DynDup m b = DynDup (DynConduit m b -> DynConduit m b -> DynConduit m b)

instance Show (DynDup m b) where
    showsPrec p _ = showParen (p > 10) (showString "DynDup")

fromDynConduit :: (Typeable m, Typeable a, Typeable b)
            => String -> DynConduit m b -> Conduit a m b
fromDynConduit msg (DynConduit x) = unsafeFromDyn msg x

dynDup :: forall a m b. (Show a, Monad m, Typeable m, Typeable a, Typeable b)
       => Conduit a m b -> DynDup m b
dynDup _ = DynDup (\x y -> dynConduit (dup (fromDynConduit "dynDup" x :: Conduit a m b)
                                        (fromDynConduit "dynDup" y :: Conduit a m b)))

dynJoin :: (DynConduit m b, DynDup m b)
        -> (DynConduit m b, DynDup m b)
        -> (DynConduit m b, DynDup m b)
dynJoin (s0, DynDup d) (s1, _) = (d s0 s1, DynDup d)

dup :: (Show a, Monad m) => Conduit a m b -> Conduit a m b -> Conduit a m b
dup c1 c2 = void (sequenceConduits [c1, c2])

------------------------------------------------------------------------

foldValues :: forall m k v. (Monad m, Eq k)
           => (v -> v -> v)
           -> Conduit (Pair k v) m (Pair k v)
foldValues append = goM =$= CL.catMaybes
  where
    goM :: Conduit (Pair k v) m (Maybe (Pair k v))
    goM = do
        s <- CL.mapAccum go Nothing
        case s of
          Nothing     -> yield Nothing
          Just (k, v) -> yield (Just (k :!: v))

    go :: Pair k v -> Maybe (k, v) -> (Maybe (k, v), Maybe (Pair k v))
    go (k :!: v) (Nothing)                   = (Just (k, v),              Nothing)
    go (k :!: v) (Just (k0, v0)) | k == k0   = (Just (k0, v0 `append` v), Nothing)
                                 | otherwise = (Just (k, v),              Just (k0 :!: v0))

------------------------------------------------------------------------

-- TODO constants broken at the moment

evalTail :: forall m n a o. (Monad m, Typeable m, Ord n, Show n, Typeable o)
         => DynConduit m o
         -> Tail n a
         -> Map n (DynConduit m o, DynDup m o)
evalTail conduit tl = case tl of
    Read _        -> M.empty
    GroupByKey _  -> error "evalTail: found GroupByKey"

    Concat (input :: [Atom n a]) ->
      let
          conduit'a :: Conduit a m o
          conduit'a = fromDynConduit "evalTail" conduit
      in
          (dynConduit conduit'a, dynDup conduit'a) `withName` fvOfAtoms input

    ConcatMap (f     :: b -> [a])
              (input :: Atom n b) ->
      let
          conduit'a :: Conduit a m o
          conduit'a = fromDynConduit "evalTail" conduit

          conduit'b :: Conduit b m o
          conduit'b = CL.concatMap f =$= conduit'a
      in
          (dynConduit conduit'b, dynDup conduit'b) `withName` fvOfAtom input

    FoldValues (step  :: v -> v -> v)
               (input :: Atom n (Pair k v)) ->
      let
          conduit'kv'o :: Conduit (Pair k v) m o
          conduit'kv'o = fromDynConduit "evalTail" conduit

          conduit'kv'i :: Conduit (Pair k v) m o
          conduit'kv'i = foldValues step =$= conduit'kv'o
      in
          (dynConduit conduit'kv'i, dynDup conduit'kv'i) `withName` fvOfAtom input
  where
    withName v s = M.fromSet (const v) s

------------------------------------------------------------------------

evalMapperTerm :: forall m n a. (Monad m, Typeable m, Show n, Ord n)
               => Term n a
               -> Map n (DynConduit m (Row Tag), DynDup m (Row Tag))
evalMapperTerm term = case term of
    Return _ -> M.empty

    Write (MapperOutput tag :: Output b) (Var (Name n)) tm ->
      let
          enc :: b -> Row Tag
          enc x = withTag tag (encodeKV x)

          conduit'b :: Conduit b m (Row Tag)
          conduit'b = CL.map enc

          conduits'tm = evalMapperTerm tm
          conduits'mo = M.singleton n (dynConduit conduit'b, dynDup conduit'b)
      in
          M.unionWith dynJoin conduits'tm conduits'mo

    Write _ _ _ -> error "evalMapperTerm: found reducer output"

    Let (Name n) tl tm ->
      let
          conduits'tm    = evalMapperTerm tm
          (conduit'n, _) = unsafeLookup "evalMapperTerm" n conduits'tm
          conduits'tl    = evalTail conduit'n tl
      in
          M.unionWith dynJoin conduits'tm conduits'tl

evalMapperTerm' :: forall m n a. (Monad m, Typeable m, Show n, Ord n)
                => Term n a
                -> Conduit (Row ()) m (Row Tag)
evalMapperTerm' term = case term of
    Let (Name n) (Read (_ :: Input b)) _ ->
      let
          (dynConduit, _) = unsafeLookup "evalMapperTerm'" n conduits

          conduit'b :: Conduit b m (Row Tag)
          conduit'b =  fromDynConduit "evalMapperTerm'" dynConduit
      in
          CL.map decodeKV =$= conduit'b
  where
    conduits = evalMapperTerm term

------------------------------------------------------------------------

-- TODO evalReducerTerm is virtually the same as evalMapperTerm

evalReducerTerm :: forall m n a. (Monad m, Typeable m, Show n, Ord n)
                => Term n a
                -> Map n (DynConduit m (Row FilePath), DynDup m (Row FilePath))
evalReducerTerm term = case term of
    Return _ -> M.empty

    Write (ReducerOutput path :: Output b) (Var (Name n)) tm ->
      let
          enc :: b -> Row FilePath
          enc x = withTag path (encodeKV x)

          conduit'b :: Conduit b m (Row FilePath)
          conduit'b = CL.map enc

          conduits'tm = evalReducerTerm tm
          conduits'ro = M.singleton n (dynConduit conduit'b, dynDup conduit'b)
      in
          M.unionWith dynJoin conduits'tm conduits'ro

    Write _ _ _ -> error "evalReducerTerm: found reducer output"

    Let (Name n) tl tm ->
      let
          conduits'tm    = evalReducerTerm tm
          (conduit'n, _) = unsafeLookup "evalReducerTerm" n conduits'tm
          conduits'tl    = evalTail conduit'n tl
      in
          M.unionWith dynJoin conduits'tm conduits'tl

evalReducerTerm' :: forall m n a. (Monad m, Typeable m, Show n, Ord n)
                 => Term n a
                 -> Conduit (Row ()) m (Row FilePath)
evalReducerTerm' term = case term of
    Let (Name n) (Read (_ :: Input b)) _ ->
      let
          (dynConduit, _) = unsafeLookup "evalReducerTerm'" n conduits

          conduit'b :: Conduit b m (Row FilePath)
          conduit'b = fromDynConduit "evalReducerTerm'" dynConduit
      in
          CL.map decodeKV =$= conduit'b
  where
    conduits = evalReducerTerm term

------------------------------------------------------------------------

decodeUnitRows :: MonadThrow m => KVFormat -> Conduit B.ByteString m (Row ())
decodeUnitRows schema = decodeRows (return ()) (M.singleton () schema)

encodeUnitRows :: MonadThrow m => KVFormat -> Conduit (Row ()) m B.ByteString
encodeUnitRows schema = encodeRows (const (return ())) (M.singleton () schema)

decodePathRows :: MonadThrow m => Map FilePath KVFormat -> Conduit B.ByteString m (Row FilePath)
decodePathRows = decodeRows (T.unpack . T.decodeUtf8 <$> getLayout VarVInt)

encodePathRows :: MonadThrow m => Map FilePath KVFormat -> Conduit (Row FilePath) m B.ByteString
encodePathRows = encodeRows (putLayout VarVInt . T.encodeUtf8 . T.pack)

decodeTagRows :: MonadThrow m => Map Tag KVFormat -> Conduit B.ByteString m (Row Tag)
decodeTagRows = decodeRows getVInt

encodeTagRows :: MonadThrow m => Map Tag KVFormat -> Conduit (Row Tag) m B.ByteString
encodeTagRows = encodeRows putVInt

------------------------------------------------------------------------

decodeRows :: (MonadThrow m, Show a, Ord a) => Get a -> Map a KVFormat -> Conduit B.ByteString m (Row a)
decodeRows getTag schema = conduitGet (getRow getTag schema)

encodeRows :: (MonadThrow m, Show a, Ord a) => (a -> Put) -> Map a KVFormat -> Conduit (Row a) m B.ByteString
encodeRows putTag schema = CL.map (putRow putTag schema) =$= conduitPut

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

intFormat :: Format
intFormat = format (undefined :: Int)

testEncDecTagged :: Either SomeException [Row Word8]
testEncDecTagged = runConduit
                 $ CL.sourceList xs
               =$= encodeRows putWord8 schema
               =$= decodeRows getWord8 schema
               =$= CL.take 10
  where
    xs = cycle [ Row 67 "abcdefg" B.empty
               , Row 67 "123"     B.empty
               , Row 22 "1234"    "Hello World!" ]

    schema = M.fromList [ (67, textFormat  :!: unitFormat)
                        , (22, int32Format :!: bytesFormat) ]

sampleTextText :: Either SomeException L.ByteString
sampleTextText = fmap L.fromChunks . runConduit
               $ CL.sourceList rows
             =$= encodeUnitRows (textFormat :!: textFormat)
             =$= CL.consume
  where
    rows = [ Row () "hello"   "hello"
           , Row () "foo"     "foo"
           , Row () "foo"     "bar"
           , Row () "goodbye" "goodbye"
           , Row () "foo"     "quxx"
           , Row () "hello"   "world"
           , Row () "foo"     "baz"
           ]

sampleTextInt :: Either SomeException L.ByteString
sampleTextInt = fmap L.fromChunks . runConduit
              $ CL.sourceList rows
            =$= encodeUnitRows (textFormat :!: intFormat)
            =$= CL.consume
  where
    rows = [ Row () "hello"   "00000000"
           , Row () "foo"     "00000001"
           , Row () "foo"     "00000002"
           , Row () "goodbye" "00000003"
           , Row () "foo"     "00000004"
           , Row () "hello"   "00000005"
           , Row () "foo"     "00000006"
           ]
