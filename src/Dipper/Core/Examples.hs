{-# LANGUAGE OverloadedStrings #-}

module Dipper.Core.Examples where

import           Data.Monoid ((<>))
import           Data.Text (Text)
import qualified Data.Text as T
import           Data.Tuple.Strict (Pair(..))

import           Dipper.Core.Types

------------------------------------------------------------------------

example1 :: Term String ()
example1 =
    Let x0 (Read input) $
    Let x1 (ConcatMap (\x -> [x + 1]) (Var x0)) $
    Let x2 (ConcatMap (\x -> [x * 2]) (Var x0)) $
    Let x3 (Concat [Var x1, Var x2]) $
    Write output (Var x3) $
    Return (Const [])
  where
    input  = MapperInput   "input.csv"  :: Input Int
    output = ReducerOutput "output.csv" :: Output Int

    x0 = "x0"
    x1 = "x1"
    x2 = "x2"
    x3 = "x3"

------------------------------------------------------------------------

example2 :: Term String ()
example2 =
    Let i1     (Read input1) $
    Let a      (ConcatMap add1 (Var i1)) $
    Let write1 (ConcatMap add1 (Var a)) $
    Write output1 (Var write1) $
    Let jtag1  (ConcatMap (tag 1) (Var a)) $
    Let i2     (Read input2) $
    Let i3     (Read input3) $
    Let b      (ConcatMap add1 (Var i2)) $
    Let c      (ConcatMap add1 (Var i3)) $
    Let fltn   (Concat [Var b, Var c]) $
    Let d      (ConcatMap add1 (Var fltn)) $
    Let jtag2  (ConcatMap (tag 2) (Var d)) $
    Let i4     (Read input4) $
    Let gmap   (ConcatMap kv_add1 (Var i4)) $
    Let ggbk   (GroupByKey (Var gmap)) $
    Let gcv    (FoldValues (+) (Var ggbk)) $
    Let e      (ConcatMap untag (Var gcv)) $
    Let jtag3  (ConcatMap (tag 3) (Var e)) $
    Let jfltn  (Concat [Var jtag1, Var jtag2, Var jtag3]) $
    Let jgbk   (GroupByKey (Var jfltn)) $
    Let juntag (ConcatMap untag (Var jgbk)) $
    Let f      (ConcatMap (:[]) (Var juntag)) $
    Let write2 (Concat [Var f]) $
    Write output2 (Var write2) $
    Return (Const [])
  where
    add1 :: Int -> [Int]
    add1 x = [x+1]

    kv_add1 :: Pair Text Int -> [Pair Text Int]
    kv_add1 (k :!: v) = [k :!: v+1]

    tag :: Int -> a -> [Pair Int a]
    tag ix x = [ix :!: x]

    untag :: Pair k v -> [v]
    untag (_ :!: x) = [x]

    -- i/o
    input1  = MapperInput "input1.csv"
    input2  = MapperInput "input2.csv"
    input3  = MapperInput "input3.csv"
    input4  = MapperInput "input4.csv"
    output1 = ReducerOutput "output1.csv"
    output2 = ReducerOutput "output2.csv"

    -- names
    i1 = "input1"
    i2 = "input2"
    i3 = "input3"
    i4 = "input4"

    a  = "A"
    b  = "B"
    c  = "C"
    d  = "D"
    e  = "E"
    f  = "F"

    fltn = "Fltn"

    gmap = "G:Map"
    ggbk = "G:GBK"
    gcv  = "G:CV"

    jtag1  = "J:Tag1"
    jtag2  = "J:Tag2"
    jtag3  = "J:Tag3"
    jfltn  = "J:Fltn"
    jgbk   = "J:GBK"
    juntag = "J:Untag"

    write1 = "write1"
    write2 = "write2"

------------------------------------------------------------------------

example3 :: Term String ()
example3 =
    Let i1        (Read input1) $
    Let i2        (Read input2) $
    Let i3        (Read input3) $
    Let concat1   (Concat [Var i1, Var i2]) $
    Let concat2   (Concat [Var i2, Var i3]) $
    Let text1     (ConcatMap (\(k :!: v) -> [k :!: T.pack (show v)]) (Var concat1)) $
    Let gbk1      (GroupByKey (Var text1)) $
    Let gbk2      (GroupByKey (Var concat2)) $
    Let mklist1   (ConcatMap (\(k :!: v) -> [k :!: [v]]) (Var gbk1)) $
    Let mklist2   (ConcatMap (\(k :!: v) -> [k :!: [v]]) (Var gbk2)) $
    Let foldlist1 (FoldValues (++) (Var mklist1)) $
    Let foldlist2 (FoldValues (++) (Var mklist2)) $
    Write output1 (Var foldlist1) $
    Write output2 (Var foldlist2) $
    Return (Const [])
  where
    input1  = MapperInput "in-1.csv" :: Input (Pair Text Int)
    input2  = MapperInput "in-2.csv"
    input3  = MapperInput "in-3.csv"
    output1 = ReducerOutput "out-1.csv" :: Output (Pair Text [Text])
    output2 = ReducerOutput "out-2.csv" :: Output (Pair Text [Int])

    i1        = "input1"
    i2        = "input2"
    i3        = "input3"
    text1     = "text1"
    concat1   = "concat1"
    concat2   = "concat2"
    gbk1      = "gbk1"
    gbk2      = "gbk2"
    mklist1   = "mklist1"
    mklist2   = "mklist2"
    foldlist1 = "foldlist1"
    foldlist2 = "foldlist2"

------------------------------------------------------------------------

example4 :: Term String ()
example4 =
    Let x0 (Read input) $
    Let x1 (ConcatMap (\(k :!: v) -> [k :!: T.toUpper v]) (Var x0)) $
    Let x2 (GroupByKey (Var x1)) $
    Write output3 (Var x2) $
    Let x3 (FoldValues (\x y -> x <> ", " <> y) (Var x2)) $
    Let x4 (GroupByKey (Var x3)) $
    Let x5 (ConcatMap (\(k :!: _) -> [k :!: (1 :: Int)]) (Var x2)) $
    Let x6 (FoldValues (+) (Var x5)) $
    Let x7 (ConcatMap (\(k :!: v) -> [k :!: T.pack (show v)]) (Var x6)) $
    Let x8 (GroupByKey (Var x7)) $
    Write output1 (Var x4) $
    Write output2 (Var x8) $
    Return (Const [])
  where
    input   = MapperInput   "input.csv"   :: Input  (Pair Text Text)
    output1 = ReducerOutput "output1.csv" :: Output (Pair Text Text)
    output2 = ReducerOutput "output2.csv" :: Output (Pair Text Text)
    output3 = ReducerOutput "output3.csv" :: Output (Pair Text Text)

    x0 = "x0"
    x1 = "x1"
    x2 = "x2"
    x3 = "x3"
    x4 = "x4"
    x5 = "x5"
    x6 = "x6"
    x7 = "x7"
    x8 = "x8"
