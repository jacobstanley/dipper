{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# OPTIONS_GHC -fno-warn-missing-signatures #-}
{-# OPTIONS_GHC -w #-}

import           Control.Monad (when)
import           Data.Binary.Get (runGet, isEmpty)
import           Data.Binary.Put (runPut)
import qualified Data.ByteString.Lazy as L
import           Data.List (sort)
import           Data.Map (Map)
import qualified Data.Map as M
import           Data.Maybe (fromMaybe)
import           Data.Tuple.Strict (Pair(..))
import           System.Exit (exitFailure)

import           Dipper.AST (removeGroups)
import           Dipper.Interpreter
import           Dipper.Types

import           Test.QuickCheck

import Debug.Trace (traceShow)

------------------------------------------------------------------------

prop_map (xs :: [Int]) =
    map (+1) (sort xs) === ys
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
