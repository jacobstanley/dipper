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

evalTerm :: (Ord n, Show n) => Map n Dynamic -> Term n (Row Tag) -> [Row Tag]
evalTerm env term = case term of
    Write _ _ _           -> error "evalTerm: Write"
    Return (Var (Name n)) -> unsafeDynLookup "evalTerm" n env
    Return (Const xs)     -> xs
    Let (Name n) tl tm    ->
      let
          env' = M.insert n (toDyn (evalTail env tl)) env
      in
          evalTerm env' tm

evalTail :: forall n a. (Ord n, Show n) => Map n Dynamic -> Tail n a -> [a]
evalTail env tl = case tl of
    Read  _           -> error "evalTail: Read"
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

