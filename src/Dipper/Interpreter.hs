{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE RecursiveDo #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeOperators #-}

{-# OPTIONS_GHC -w #-}

module Dipper.Interpreter where

import           Data.Binary.Get
import           Data.Binary.Put
import qualified Data.ByteString as S
import           Data.ByteString.Builder
import qualified Data.ByteString.Lazy as L
import           Data.Dynamic
import           Data.Int (Int32, Int64)
import           Data.List (groupBy, foldl', sort)
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as M
import           Data.Set (Set)
import qualified Data.Set as Set
import           Data.Maybe (maybeToList)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import           Data.Typeable (Typeable)
import           Data.Word (Word8)

import           Dipper.AST
import           Dipper.Binary
import           Dipper.Types

------------------------------------------------------------------------

data Formatted a = Formatted !a !KVFormat
  deriving (Eq, Ord, Show)

data Step i o = Step {
    sInput   ::  Formatted i
  , sOutputs :: [Formatted o]
  , sTerm    :: Term String ()
  } deriving (Show)

type MapperName  = FilePath
type ReducerName = Word8

type Mapper  = Step MapperName ReducerName
type Reducer = Step ReducerName MapperName

data Pipeline = Pipeline {
    pMappers  :: Map MapperName  Mapper
  , pReducers :: Map ReducerName Reducer
  } deriving (Show)

------------------------------------------------------------------------

mkPipeline :: Term String () -> Pipeline
mkPipeline = undefined

------------------------------------------------------------------------

--insertMapperOutputs :: Term n a -> Term n a
--insertMapperOutputs term = case term of
--    Let (Name n) (MapperInput _) ->

------------------------------------------------------------------------

dependencies :: forall a n. Ord n => Set n -> Term n a -> Set n
dependencies ns term = case term of
    Let (Name n) tl tm ->
      let
          ns' = dependencies ns tm
      in
          if Set.member n ns'
          then dependenciesOfTail tl `Set.union` ns'
          else ns'

    Run _ tm -> dependencies ns tm
    Ret _    -> ns

dependenciesOfTail :: Ord n => Tail n a -> Set n
dependenciesOfTail tl = case tl of
    Concat         xs -> Set.unions (map dependenciesOfAtom xs)
    ConcatMap     _ x -> dependenciesOfAtom x
    GroupByKey      x -> dependenciesOfAtom x
    FoldValues  _ _ x -> dependenciesOfAtom x
    MapperInput     _ -> Set.empty
    ReducerInput    _ -> Set.empty
    MapperOutput  _ x -> dependenciesOfAtom x
    ReducerOutput _ x -> dependenciesOfAtom x

dependenciesOfAtom :: Ord n => Atom n a -> Set n
dependenciesOfAtom atom = case atom of
    Var (Name n) -> Set.singleton n
    Const     _  -> Set.empty

------------------------------------------------------------------------

dependents :: forall a n. Eq n => ([Bool] -> Bool) -> [n] -> Term n a -> [n]
dependents combine ns term = case term of
    Let (Name n) tl tm -> dependents combine (insertConnected n tl) tm
    Run          _  tm -> dependents combine ns tm
    Ret          _     -> ns
  where
    insertConnected :: n -> Tail n b -> [n]
    insertConnected n tl
      | isTailDependent combine ns tl = ns ++ [n]
      | otherwise                     = ns

isTailDependent :: Eq n => ([Bool] -> Bool) -> [n] -> Tail n a -> Bool
isTailDependent combine ns tl = case tl of
    Concat         xs -> combine (map (isAtomDependent ns) xs)
    ConcatMap     _ x -> isAtomDependent ns x
    GroupByKey      x -> isAtomDependent ns x
    FoldValues  _ _ x -> isAtomDependent ns x
    MapperInput     _ -> False
    ReducerInput    _ -> False
    MapperOutput  _ x -> isAtomDependent ns x
    ReducerOutput _ x -> isAtomDependent ns x

isAtomDependent :: Eq n => [n] -> Atom n a -> Bool
isAtomDependent ns atom = case atom of
    Var (Name n) -> elem n ns
    Const     _  -> False

------------------------------------------------------------------------

data RowDesc = RowDesc ReducerName KVFormat
    deriving (Eq, Ord, Show)

data MSCR = MSCR {
    mscrMapper   :: [Row ReducerName] -> [Row ReducerName]
  , mscrReducer  :: [Row ReducerName] -> [Row ReducerName]
  }

------------------------------------------------------------------------

unitFormat :: Format
unitFormat = format (undefined :: ())

textFormat :: Format
textFormat = format (undefined :: T.Text)

bytesFormat :: Format
bytesFormat = format (undefined :: S.ByteString)

int32Format :: Format
int32Format = format (undefined :: Int32)

test = take 10 (decodeTagged schema (encodeTagged schema xs))
  where
    xs = cycle [ 67 :*: "abcdefg" :*: S.empty
               , 67 :*: "123"     :*: S.empty
               , 22 :*: "1234"    :*: "Hello World!" ]

    schema = M.fromList [ (67, textFormat  :*: unitFormat)
                        , (22, int32Format :*: bytesFormat) ]

test1 = mscrMapper (interpret example1) (sort (take 10 xs))
  where
    xs = cycle [ 0 :*: "" :*: "00000000"
               , 0 :*: "" :*: "00000001"
               , 0 :*: "" :*: "00000002"
               , 0 :*: "" :*: "00000003"
               , 0 :*: "" :*: "00000004"
               , 0 :*: "" :*: "00000005"
               ]

test2 = mscrMapper (interpret example2) (sort (take 10 xs))
  where
    xs = cycle [ 0 :*: "" :*: "00000000"
               , 1 :*: "" :*: "11111111"
               , 2 :*: "" :*: "22222222"
               , 3 :*: "" :*: "33333333"
               ]

test3 = mscrMapper (interpret example3) (sort (take 10 xs))
  where
    xs = cycle [ 0 :*: "X" :*: "       1"
               , 1 :*: "X" :*: "       2"
               , 2 :*: "X" :*: "       3"
               , 0 :*: "Y" :*: "      10"
               , 1 :*: "Y" :*: "      20"
               , 2 :*: "Y" :*: "      30"
               , 0 :*: "Z" :*: "     100"
               , 1 :*: "Z" :*: "     200"
               , 2 :*: "Z" :*: "     300"
               ]

------------------------------------------------------------------------

interpret :: Term String () -> MSCR
interpret term = MSCR{..}
  where
    mscrInputs      = readsOfTerm  0 term
    mscrOutputs     = writesOfTerm 0 term
    mscrMapper rows = evalTerm M.empty
                    . replaceWrites freshNames
                    . replaceReads rows
                    . removeGroups 0
                    $ term

------------------------------------------------------------------------

evalTerm :: (Ord n, Show n) => Map n Dynamic -> Term n (Row ReducerName) -> [Row ReducerName]
evalTerm env term = case term of
    Let (Name n) tl tm ->
      let
          env' = M.insert n (toDyn (evalTail env tl)) env
      in
          evalTerm env' tm

    Ret tl   -> evalTail env tl
    Run _  _ -> error ("evalTerm: cannot eval: " ++ show term)

evalTail :: forall n a. (Ord n, Show n) => Map n Dynamic -> Tail n a -> [a]
evalTail env tl = case tl of
    Concat        xss -> concatMap resolve xss
    ConcatMap   f  xs -> concatMap f (resolve xs)
    GroupByKey     xs -> group (resolve xs)
    FoldValues f x xs -> fold f x (resolve xs)
    MapperInput   _   -> error ("evalTail: cannot eval: " ++ show tl)
    ReducerInput  _   -> error ("evalTail: cannot eval: " ++ show tl)
    MapperOutput  _ _ -> error ("evalTail: cannot eval: " ++ show tl)
    ReducerOutput _ _ -> error ("evalTail: cannot eval: " ++ show tl)
  where
    resolve :: (Ord n, Show n) => Atom n b -> [b]
    resolve (Var (Name n)) = unsafeDynLookup "evalTail" n env
    resolve (Const xs)     = xs

    fold :: (v -> v -> v) -> v -> [k :*: [v]] -> [k :*: v]
    fold f x xs = map (\(k :*: vs) -> k :*: foldl' f x vs) xs

    group :: Eq k => [k :*: v] -> [k :*: [v]]
    group = map fixGroup . groupBy keyEq

    keyEq :: Eq k => k :*: v -> k :*: v -> Bool
    keyEq (x :*: _) (y :*: _) = x == y

    fixGroup :: [k :*: v] -> k :*: [v]
    fixGroup []                = error "evalTail: groupBy yielded empty list: impossible"
    fixGroup ((k :*: v) : kvs) = k :*: (v : map snd' kvs)

------------------------------------------------------------------------

removeGroups :: ReducerName -> Term n () -> Term n ()
removeGroups reducer term = case term of
    Let n (GroupByKey kvs) tm ->

      let
          out = Run   (MapperOutput reducer kvs)
          inp = Let n (ReducerInput reducer)

          reducer' = nextReducer "removeGroups" reducer
      in
          out (inp (removeGroups reducer' tm))

    Let  n tl tm -> Let  n tl (removeGroups reducer tm)
    Run    tl tm -> Run    tl (removeGroups reducer tm)
    Ret    tl    -> Ret    tl

------------------------------------------------------------------------

replaceReads :: [Row ReducerName] -> Term n a -> Term n a
replaceReads = replaceReadsOfTerm 0

replaceReadsOfTerm :: ReducerName -> [Row ReducerName] -> Term n a -> Term n a
replaceReadsOfTerm tag rows term = case term of
    Let n (MapperInput path :: Tail n b) tm ->

      let
          xs :: [b]
          xs = map (decodeRow . snd')
             $ filter (hasTag tag) rows

          tag' = nextReducer "replaceReadsOfTerm" tag
      in
          Let n (Concat [Const xs]) (replaceReadsOfTerm tag' rows tm)

    Let n  tl tm -> Let n  tl (replaceReadsOfTerm tag rows tm)
    Run    tl tm -> Run    tl (replaceReadsOfTerm tag rows tm)
    Ret    tl    -> Ret    tl

------------------------------------------------------------------------

nextReducer :: String -> ReducerName -> ReducerName
nextReducer msg name | name /= maxBound = succ name
                     | otherwise        = error msg'
  where
    msg' = msg ++ ": exceeded maximum number of reducers <" ++ show name ++ ">"

------------------------------------------------------------------------

replaceWrites :: [n] -> Term n () -> Term n (Row ReducerName)
replaceWrites = replaceWritesOfTerm 0 []

replaceWritesOfTerm :: forall n. ReducerName
                    -> [Name n (Row ReducerName)]
                    -> [n]
                    -> Term n ()
                    -> Term n (Row ReducerName)
replaceWritesOfTerm tag outs fresh term = case term of

    Run   tl tm -> case replaceWritesOfTail tag tl of
      Nothing  -> Run tl (replaceWritesOfTerm tag outs fresh tm)
      Just tl' -> replaceWithLet tl' tm

    Let n tl tm -> case replaceWritesOfTail tag tl of
      Nothing  -> Let n tl (replaceWritesOfTerm tag outs fresh tm)
      Just tl' -> replaceWithLet tl' tm

    Ret   tl    -> case replaceWritesOfTail tag tl of
      Nothing  -> Ret (Concat (map Var outs))
      Just tl' -> replaceWithLet tl' (Ret (Concat []))

  where
    replaceWithLet :: Typeable n
                   => Tail n (Row ReducerName)
                   -> Term n ()
                   -> Term n (Row ReducerName)
    replaceWithLet tl' tm =
      let
          (n:fresh') = fresh
          name       = Name n

          tag'  = nextReducer "replaceWritesOfterm" tag
          outs' = name : outs
      in
          Let name tl' (replaceWritesOfTerm tag' outs' fresh' tm)


replaceWritesOfTail :: ReducerName
                    -> Tail n a
                    -> Maybe (Tail n (Row ReducerName))
replaceWritesOfTail tag tl = case tl of
    ReducerOutput path (xs :: Atom n b) -> Just (ConcatMap go xs)
    _                               -> Nothing
  where
    go :: KV b => b -> [Row ReducerName]
    go x = [tag :*: encodeRow x]

------------------------------------------------------------------------

readsOfTerm :: ReducerName -> Term n a -> [FilePath]
readsOfTerm tag term = case term of
    Let (Name n) (MapperInput path :: Tail n b) tm ->

      let
          tag' = nextReducer "readsOfTerm" tag
          desc = RowDesc tag (kvFormat (undefined :: b))
      in
          path : readsOfTerm tag' tm

    Let _  _ tm -> readsOfTerm tag tm
    Run    _ tm -> readsOfTerm tag tm
    Ret    _    -> []

------------------------------------------------------------------------

writesOfTerm :: ReducerName -> Term n a -> [FilePath]
writesOfTerm tag term = case term of
    Run    tl tm -> go (writesOfTail tag tl) (\tg -> writesOfTerm tg tm)
    Let _  tl tm -> go (writesOfTail tag tl) (\tg -> writesOfTerm tg tm)
    Ret    tl    -> go (writesOfTail tag tl) (const [])
  where
    go (Just file) f = [file] ++ f tag'
    go Nothing     f = f tag

    tag' = nextReducer "writesOfTerm" tag

writesOfTail :: ReducerName -> Tail n a -> Maybe FilePath
writesOfTail tag tl = case tl of
    ReducerOutput path (xs :: Atom n b) ->

      let
          desc = RowDesc tag (kvFormat (undefined :: b))
      in
          Just path

    _ -> Nothing

------------------------------------------------------------------------

decodeTagged :: Map ReducerName KVFormat -> L.ByteString -> [Row ReducerName]
decodeTagged schema bs =
    -- TODO this is unlikely to have good performance
    case runGetOrFail (getTagged schema) bs of
        Left  (_,   _, err)            -> error ("decodeTagged: " ++ err)
        Right (bs', o, x) | L.null bs' -> [x]
                          | otherwise  -> x : decodeTagged schema bs'

encodeTagged :: Map ReducerName KVFormat -> [Row ReducerName] -> L.ByteString
encodeTagged schema = runPut . mapM_ (putTagged schema)

getTagged :: Map ReducerName KVFormat -> Get (Row ReducerName)
getTagged schema = do
    tag <- getWord8
    case M.lookup tag schema of
      Nothing              -> fail ("getTagged: invalid tag <" ++ show tag ++ ">")
      Just (kFmt :*: vFmt) -> pure tag <&> getLayout (fmtLayout kFmt)
                                       <&> getLayout (fmtLayout vFmt)

putTagged :: Map ReducerName KVFormat -> Row ReducerName -> Put
putTagged schema (tag :*: k :*: v) =
    case M.lookup tag schema of
      Nothing              -> fail ("putTagged: invalid tag <" ++ show tag ++ ">")
      Just (kFmt :*: vFmt) -> putWord8 tag >> putLayout (fmtLayout kFmt) k
                                           >> putLayout (fmtLayout vFmt) v
