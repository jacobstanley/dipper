Example 2
=========

    as = read "in1.csv"
    bs = read "in2.csv"
    cs = read "in3.csv"
    ds = read "in4.csv"

    write1 = concatMap f as
    jtag1  = concatMap (tag 1) as
    write "out1.csv" write1

    fltn  = bs ++ cs
    es    = concatMap f fltn
    jtag2 = concatMap (tag 2) es

    dgbk  = group ds
    dcv   = combine f x dgbk
    fs    = concatMap dcv
    jtag3 = concatMap (tag 3) fs

    jfltn  = jtag1 ++ jtag2 ++ jtag3
    jgbk   = group jfltn
    juntag = concatMap untag jgbk
    k      = concatMap juntag
    write "out2.csv" k

Dependencies :: Map Input [Output]
----------------------------------

    -- Mappers
    in1.csv  -> [2, 3]
    in2.csv  -> [3]
    in3.csv  -> [3]
    in4.csv  -> [1]
    tmp1.csv -> [3]

    -- Reducers
    1 -> [tmp1.csv]
    2 -> [out1.csv]
    3 -> [out2.csv]

Execution Steps
---------------

    1. tmp1.csv <- 1 <- in4.csv

    2. out1.csv <- 2 <- in1.csv
       out2.csv <- 3 <- in1.csv, in2.csv, in3.csv, tmp1.csv

Interesting Notes
-----------------

- Mappers are named after their input file (e.g. in1.csv, tmp1.csv, etc)
- Reducers are named after their input tag (e.g. 1, 2, 3, etc)
- Reducers that depend on the same mapper (e.g. 2/3) must be run together

Types
-----

    type MapperName  = FilePath
    type ReducerName = Word8

    data Step i o = Step i [o] (Row () -> [Row o])
    type Mapper   = Step MapperName ReducerName
    type Reducer  = Step ReducerName MapperName

Mappers :: Map MapperName Mapper
--------------------------------

    case file of
      "in1.csv"  -> as     = stdin
                    write1 = concatMap f as
                    jtag1  = concatMap (tag 1) as
                    jftln  = jtag1
                    write 2 write1
                    write 3 jfltn

      "in2.csv"  -> bs    = stdin
                    fltn  = bs
                    es    = concatMap f fltn
                    jtag2 = concatMap (tag 2) es
                    jftln = jtag2
                    write 3 jfltn

      "in3.csv"  -> cs    = stdin
                    fltn  = cs
                    es    = concatMap f fltn
                    jtag2 = concatMap (tag 2) es
                    jfltn = jtag2
                    write 3 jftln

      "in4.csv"  -> ds = stdin
                    write 1 ds

      "tmp1.csv" -> dcv = stdin
                    fs    = concatMap dcv
                    jtag3 = concatMap (tag 3) fs
                    jftln = jtag3
                    write 3 jftln

Reducers :: Map ReducerName Reducer
-----------------------------------

    case stdin of
      (1, dgbk)   -> dcv = combine f x stdin
                     write "tmp1.csv" dcv

      (2, write1) -> write "out1.csv" write1

      (3, jgbk)   -> juntag = concatMap untag
                     k      = concatMap juntag
                     write "out2.csv" k
