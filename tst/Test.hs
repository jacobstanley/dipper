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
    fs' = testPipeline (mkPipeline term) fs

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
    fs' = testPipeline (mkPipeline term) fs

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
    --let ok = False
    when (not ok) exitFailure
