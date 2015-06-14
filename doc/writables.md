Tagging & Comparing Writables
=============================

| Hadoop          | Haskell    | Binary                     | Compare       |
| ------          | -------    | ------                     | -------       |
| NullWritable    | ()         | Empty                      | N/A           |
| BooleanWritable | Bool       | Fixed 1 BE                 | Unsigned      |
| ByteWritable    | Word8      | Fixed 1 BE                 | Unsigned      |
| ShortWritable   | Int16      | Fixed 2 BE                 | Signed        |
| IntWritable     | Int32      | Fixed 4 BE                 | Signed        |
| LongWritable    | Int64      | Fixed 8 BE                 | Signed        |
| FloatWritable   | Float      | Fixed 4 BE                 | Signed        |
| DoubleWritable  | Double     | Fixed 8 BE                 | Signed        |
| VIntWritable    | Int32      | VInt    BE                 | Signed        |
| VLongWritable   | Int64      | VInt    BE                 | Signed        |
| BytesWritable   | ByteString | (x : Fixed 4 BE) * Bytes x | Lexicographic |
| Text            | Text       | (x : VInt    BE) * Bytes x | Lexicographic |


Interesting snippets from Hadoop (MR1)
--------------------------------------

If the `mapred.output.key.comparator.class` option is set, then the
method `getOutputKeyComparator` creates a key comparator using
reflection, passing the `JobConf` as a constructor argument. If it is
not set, then it defaults to calling `WritableComparator.get`, passing
the map output key class as an argument.

    class JobConf extends Configuration {
        ...

        public RawComparator getOutputKeyComparator() {
            Class<? extends RawComparator> theClass = getClass("mapred.output.key.comparator.class",
                    null, RawComparator.class);

            if (theClass != null)
                return ReflectionUtils.newInstance(theClass, this);

            return WritableComparator.get(getMapOutputKeyClass().asSubclass(WritableComparable.class));
        }

        ...
    }

This is great, it means we can compare tagged keys without having to
store their type in the tag. We should be able to store the embedded
types of all the tags in the `JobConf` and access it from the
constructor of our comparator.
