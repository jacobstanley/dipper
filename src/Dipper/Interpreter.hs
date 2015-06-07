{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE RecursiveDo #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeOperators #-}

{-# OPTIONS_GHC -w #-}

module Dipper.Interpreter where

import           Control.Monad (mplus)
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
import           Data.Maybe (maybeToList, mapMaybe)
import           Data.Set (Set)
import qualified Data.Set as Set
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

type Mapper  = Step FilePath Tag
type Reducer = Step Tag FilePath

data Pipeline = Pipeline {
    pMappers  :: Map FilePath Mapper
  , pReducers :: Map Tag Reducer
  } deriving (Show)

------------------------------------------------------------------------

mkPipeline :: Term String () -> Pipeline
mkPipeline = undefined

------------------------------------------------------------------------

fixMapPhase :: Ord n => Term n a -> Term n a
fixMapPhase term = foldl' go term rOuts
  where
    go tm path = passReducerOutput path (nextAvailableTag "fixMapPhase" tm) tm

    rOuts = mapMaybe reducerOutput
          . Set.toList
          . dependents (any id) mIns
          $ term

    mIns = Set.map MapperInput
         . findNamesOfTerm mapperInput
         $ term

passReducerOutput :: FilePath -> Tag -> Term n a -> Term n a
passReducerOutput path tag term = case term of
    Let n@(ReducerOutput path') tl tm | path == path' ->

       Let (MapperOutput tag) tl $
       Let n (Concat [Var (ReducerInput tag)]) tm

    Let n tl tm -> Let n tl (passReducerOutput path tag tm)
    Ret   tl    -> Ret   tl

------------------------------------------------------------------------

nextAvailableTag :: String -> Term n a -> Tag
nextAvailableTag msg term = case Set.maxView (findNamesOfTerm go term) of
    Nothing     -> 0
    Just (x, _) -> nextTag msg x
  where
    go n = mapperOutput n `mplus` reducerInput n

nextTag :: String -> Tag -> Tag
nextTag msg tag | tag /= maxBound = succ tag
                | otherwise       = error msg'
  where
    msg' = msg ++ ": exceeded maximum number of reducers <" ++ show tag ++ ">"

------------------------------------------------------------------------

mapperInput :: Name n a -> Maybe FilePath
mapperInput (MapperInput x) = Just x
mapperInput _               = Nothing

mapperOutput :: Name n a -> Maybe Tag
mapperOutput (MapperOutput x) = Just x
mapperOutput _                = Nothing

reducerInput :: Name n a -> Maybe Tag
reducerInput (ReducerInput x) = Just x
reducerInput _                = Nothing

reducerOutput :: Name n a -> Maybe FilePath
reducerOutput (ReducerOutput x) = Just x
reducerOutput _                 = Nothing

------------------------------------------------------------------------

findNamesOfTerm :: Ord a => (Name' n -> Maybe a) -> Term n b -> Set a
findNamesOfTerm p term = case term of
    Ret   tl    -> findNamesOfTail p tl
    Let n tl tm -> setFromMaybe (p (coerceName n))
       `Set.union` findNamesOfTail p tl
       `Set.union` findNamesOfTerm p tm

findNamesOfTail :: Ord a => (Name' n -> Maybe a) -> Tail n b -> Set a
findNamesOfTail p tl = case tl of
    Concat        xs -> Set.unions (map (setFromMaybe . findNamesOfAtom p) xs)
    ConcatMap    _ x -> setFromMaybe (findNamesOfAtom p x)
    GroupByKey     x -> setFromMaybe (findNamesOfAtom p x)
    FoldValues _ _ x -> setFromMaybe (findNamesOfAtom p x)

findNamesOfAtom :: (Name' n -> Maybe a) -> Atom n b -> Maybe a
findNamesOfAtom p atom = case atom of
    Var   n -> p (coerceName n)
    Const _ -> Nothing

setFromMaybe :: Ord a => Maybe a -> Set a
setFromMaybe = Set.fromList . maybeToList

------------------------------------------------------------------------

dependencies :: forall a n. Ord n => Set (Name' n) -> Term n a -> Set (Name' n)
dependencies ns term = case term of
    Let n tl tm ->
      let
          ns' = dependencies ns tm
          n'  = coerceName n
      in
          if Set.member n' ns'
          then dependenciesOfTail tl `Set.union` ns'
          else ns'

    Ret _ -> ns

dependenciesOfTail :: Ord n => Tail n a -> Set (Name' n)
dependenciesOfTail tl = case tl of
    Concat         xs -> Set.unions (map dependenciesOfAtom xs)
    ConcatMap     _ x -> dependenciesOfAtom x
    GroupByKey      x -> dependenciesOfAtom x
    FoldValues  _ _ x -> dependenciesOfAtom x

dependenciesOfAtom :: Ord n => Atom n a -> Set (Name' n)
dependenciesOfAtom atom = case atom of
    Var   n -> Set.singleton (coerceName n)
    Const _ -> Set.empty

------------------------------------------------------------------------

dependents :: forall a n. Ord n => ([Bool] -> Bool) -> Set (Name' n) -> Term n a -> Set (Name' n)
dependents combine ns term = case term of
    Let n tl tm -> dependents combine (insertConnected (coerceName n) tl) tm
    Ret   _     -> ns
  where
    insertConnected :: Name' n -> Tail n b -> Set (Name' n)
    insertConnected n tl
      | isTailDependent combine ns tl = Set.insert n ns
      | otherwise                     = ns

isTailDependent :: Ord n => ([Bool] -> Bool) -> Set (Name' n) -> Tail n a -> Bool
isTailDependent combine ns tl = case tl of
    Concat         xs -> combine (map (isAtomDependent ns) xs)
    ConcatMap     _ x -> isAtomDependent ns x
    GroupByKey      x -> isAtomDependent ns x
    FoldValues  _ _ x -> isAtomDependent ns x

isAtomDependent :: Ord n => Set (Name' n) -> Atom n a -> Bool
isAtomDependent ns atom = case atom of
    Var   n -> Set.member (coerceName n) ns
    Const _ -> False

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

------------------------------------------------------------------------

evalTerm :: (Ord n, Show n) => Map n Dynamic -> Term n (Row Tag) -> [Row Tag]
evalTerm env term = case term of
    Let (Name n) tl tm ->
      let
          env' = M.insert n (toDyn (evalTail env tl)) env
      in
          evalTerm env' tm

    Ret tl   -> evalTail env tl

evalTail :: forall n a. (Ord n, Show n) => Map n Dynamic -> Tail n a -> [a]
evalTail env tl = case tl of
    Concat        xss -> concatMap resolve xss
    ConcatMap   f  xs -> concatMap f (resolve xs)
    GroupByKey     xs -> group (resolve xs)
    FoldValues f x xs -> fold f x (resolve xs)
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

removeGroups :: Tag -> Term n () -> Term n ()
removeGroups tag term = case term of
    Let n (GroupByKey kvs) tm ->

      let
          out = Let (MapperOutput tag) (Concat [kvs])
          inp = Let n (Concat [Var (ReducerInput tag)])

          tag' = nextTag "removeGroups" tag
      in
          out (inp (removeGroups tag' tm))

    Let  n tl tm -> Let  n tl (removeGroups tag tm)
    Ret    tl    -> Ret    tl

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
