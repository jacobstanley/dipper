{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeOperators #-}

module Dipper.Examples where

import           Data.Text (Text)
import qualified Data.Text as T

import           Dipper.Product
import           Dipper.Types

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
    Let gcv    (FoldValues (+) 0 (Var ggbk)) $
    Let e      (ConcatMap untag (Var gcv)) $
    Let jtag3  (ConcatMap (tag 3) (Var e)) $
    Let jfltn  (Concat [Var jtag1, Var jtag2, Var jtag3]) $
    Let jgbk   (GroupByKey (Var jfltn)) $
    Let juntag (ConcatMap untag (Var jgbk)) $
    Let f      (ConcatMap id (Var juntag)) $
    Let write2 (Concat [Var f]) $
    Write output2 (Var write2) $
    Return (Const [])
  where
    add1 :: Int -> [Int]
    add1 x = [x+1]

    kv_add1 :: Text :*: Int -> [Text :*: Int]
    kv_add1 (k :*: v) = [k :*: v+1]

    tag :: Int -> a -> [Int :*: a]
    tag ix x = [ix :*: x]

    untag :: k :*: v -> [v]
    untag (_ :*: x) = [x]

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
    Let i1      (Read input1) $
    Let i2      (Read input2) $
    Let i3      (Read input3) $
    Let concat1 (Concat [Var i1, Var i2]) $
    Let concat2 (Concat [Var i2, Var i3]) $
    Let text1   (ConcatMap (\(k :*: v) -> [k :*: T.pack (show v)]) (Var concat1)) $
    Let gbk1    (GroupByKey (Var text1)) $
    Let gbk2    (GroupByKey (Var concat2)) $
    Write output1 (Var gbk1) $
    Write output2 (Var gbk2) $
    Return (Const [])
  where
    input1  = MapperInput "in-1.csv" :: Input (Text :*: Int)
    input2  = MapperInput "in-2.csv"
    input3  = MapperInput "in-3.csv"
    output1 = ReducerOutput "out-1.csv" :: Output (Text :*: [Text])
    output2 = ReducerOutput "out-1.csv" :: Output (Text :*: [Int])

    i1      = "input1"
    i2      = "input2"
    i3      = "input3"
    text1   = "text1"
    concat1 = "concat1"
    concat2 = "concat2"
    gbk1    = "gbk1"
    gbk2    = "gbk2"

