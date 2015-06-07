Example 1
=========

    xs = read "in1.csv"
    ys = read "in2.csv"
    zs = read "in3.csv"

    c1   = xs ++ ys
    c2   = ys ++ zs
    gbk1 = group c1
    gbk2 = group c2

    write "out1.csv" gbk1
    write "out2.csv" gbk2

Mapper 1 (in1.csv)
------------------

    xs = stdin
    c1 = xs
    write stdout (1, c1)

Mapper 2 (in2.csv)
------------------

    ys = stdin
    c1 = ys
    c2 = ys
    write stdout (1, c1)
    write stdout (2, c2)

Mapper 3 (in3.csv)
------------------

    zs = stdin
    c2 = zs
    write stdout (2, c2)

Reducer (1, 2)
----------------------------

    case stdin of
      (1, gbk1) -> write stdout ("out1.csv", gbk1)
      (2, gbk2) -> write stdout ("out2.csv", gbk2)
