{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# OPTIONS_GHC -fno-warn-missing-signatures #-}
{-# OPTIONS_GHC -w #-}

import           Control.Monad (when)
import           Data.Binary.Get (runGet, isEmpty)
import           Data.Binary.Put (runPut)
import qualified Data.ByteString.Lazy as L
import           Data.List (sort, sortBy, mapAccumL)
import           Data.Map (Map)
import qualified Data.Map as M
import           Data.Maybe (fromMaybe, catMaybes)
import           Data.Tuple.Strict (Pair(..))
import           System.Exit (exitFailure)

import           Dipper.AST (removeGroups)
import           Dipper.Interpreter hiding (foldValues)
import           Dipper.Types

import           Test.QuickCheck

import Debug.Trace (traceShow)

------------------------------------------------------------------------

prop_map (xs :: [Int]) =
    sort (map (+1) xs) === sort ys
  where
    fs  = M.singleton "xs.in" (encodeList xs)
    fs' = testPipeline (mkPipeline "" term) fs

    ys = decodeList (unsafeLookup "prop_map" "ys.out" fs')

    term :: Term String ()
    term = Let xs' (Read xs'in) $
           Let ys' (ConcatMap (\x -> [x+1]) (Var xs')) $
           Write ys'out (Var ys') $
           Return (Const [])

    xs'in  = MapperInput   "xs.in"  :: Input  Int
    ys'out = ReducerOutput "ys.out" :: Output Int
    xs' = "xs"
    ys' = "ys"

------------------------------------------------------------------------

prop_group_fold (xs :: [Pair Int Int]) =
    (sort . foldValues (+) . groupByKey $ xs) === sort zs
  where
    fs  = M.singleton "xs.in" (encodeList xs)
    fs' = testPipeline (mkPipeline "" term) fs

    zs = decodeList (unsafeLookup "prop_group_fold" "zs.out" fs')

    term :: Term String ()
    term = Let xs' (Read xs'in) $
           Let ys' (GroupByKey (Var xs')) $
           Let zs' (FoldValues (+) (Var ys')) $
           Write zs'out (Var zs') $
           Return (Const [])

    xs'in  = MapperInput   "xs.in"  :: Input  (Pair Int Int)
    zs'out = ReducerOutput "zs.out" :: Output (Pair Int Int)
    xs' = "xs"
    ys' = "ys"
    zs' = "zs"

------------------------------------------------------------------------

prop_two_outputs (xs :: [Int]) =
    sort (filter even xs) === sort evens .&&.
    sort (filter odd  xs) === sort odds
  where
    fs  = M.singleton "xs.in" (encodeList xs)
    fs' = testPipeline (mkPipeline "" term) fs

    evens = decodeList (unsafeLookup "prop_two_outputs" "evens.out" fs')
    odds  = decodeList (unsafeLookup "prop_two_outputs" "odds.out"  fs')

    term :: Term String ()
    term = Let xs'    (Read xs'in) $
           Let evens' (ConcatMap (\x -> if even x then [x] else []) (Var xs')) $
           Let odds'  (ConcatMap (\x -> if odd  x then [x] else []) (Var xs')) $
           Write evens'out (Var evens') $
           Write odds'out  (Var odds') $
           Return (Const [])

    xs'in     = MapperInput   "xs.in"     :: Input  Int
    evens'out = ReducerOutput "evens.out" :: Output Int
    odds'out  = ReducerOutput "odds.out"  :: Output Int

    xs'    = "xs"
    evens' = "evens"
    odds'  = "odds"

------------------------------------------------------------------------

prop_two_inputs (xs :: [Int], ys :: [Int]) =
    sort (xs ++ ys) === sort zs
  where
    fs' = testPipeline (mkPipeline "" term) fs
    fs  = M.fromList [ ("xs.in", encodeList xs)
                     , ("ys.in", encodeList ys) ]

    zs = decodeList (unsafeLookup "prop_map" "zs.out" fs')

    term :: Term String ()
    term = Let xs' (Read xs'in) $
           Let ys' (Read ys'in) $
           Let zs' (Concat [Var xs', Var ys']) $
           Write zs'out (Var zs') $
           Return (Const [])

    xs'in  = MapperInput   "xs.in"  :: Input  Int
    ys'in  = MapperInput   "ys.in"  :: Input  Int
    zs'out = ReducerOutput "zs.out" :: Output Int
    xs' = "xs"
    ys' = "ys"
    zs' = "zs"

------------------------------------------------------------------------

groupByKey :: Ord k => [Pair k v] -> [Pair k v]
groupByKey = sortBy keyCompare
  where
    keyCompare (k1 :!: _) (k2 :!: _) = k1 `compare` k2

foldValues :: Eq k => (v -> v -> v) -> [Pair k v] -> [Pair k v]
foldValues append xs = catMaybes (ys ++ [y])
  where
    (y, ys) = mapAccumL go Nothing xs

    go (Nothing)          (k :!: v)             = (Just (k  :!: v),             Nothing)
    go (Just (k0 :!: v0)) (k :!: v) | k0 == k   = (Just (k0 :!: v0 `append` v), Nothing)
                                    | otherwise = (Just (k  :!: v),             Just (k0 :!: v0))

------------------------------------------------------------------------

encodeList :: forall a. KV a => [a] -> L.ByteString
encodeList = runPut . mapM_ encodeRow . map encodeKV
  where
    (kf :!: vf) = kvFormat (undefined :: a)

    encodeRow (Row _ k v) = putLayout (fmtLayout kf) k
                         >> putLayout (fmtLayout vf) v

decodeList :: forall a. KV a => L.ByteString -> [a]
decodeList = map decodeKV . runGet decodeRows
  where
    decodeRows = do
        empty <- isEmpty
        if empty
           then return []
           else (:) <$> decodeRow <*> decodeRows

    decodeRow = Row () <$> getLayout (fmtLayout kf)
                       <*> getLayout (fmtLayout vf)

    (kf :!: vf) = kvFormat (undefined :: a)


unsafeLookup :: (Ord k, Show k, Show v) => String -> k -> Map k v -> v
unsafeLookup msg k m = fromMaybe (error msg') (M.lookup k m)
  where
    msg' = msg ++ ": cannot find key: " ++ show k ++ " in map: " ++ show m

------------------------------------------------------------------------
-- QuickCheck Driver

return []
main :: IO ()
main = do
    ok <- $(quickCheckAll)
    --let ok = error "switch TH back on"
    when (not ok) exitFailure
