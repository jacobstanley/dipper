package org.dipper;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.JobConf;

////////////////////////////////////////////////////////////////////////

public class TagKeyComparator implements RawComparator<TagKeyWritable>, Configurable {
    private final HashMap<Integer, WritableComparator> comparators;
    private DipperConf conf;

    // Called by ReflectionUtils.newInstance(..) only
    private TagKeyComparator() {
        comparators = new HashMap<Integer, WritableComparator>();
    }

    private WritableComparator comparatorOf(int tag) {
        WritableComparator comparator = comparators.get(tag);

        if (comparator == null) {
            comparator = conf.newKeyComparatorOf(tag);
            comparators.put(tag, comparator);
        }

        return comparator;
    }

    //
    // Configurable
    //

    public Configuration getConf() {
      return conf.getConf();
    }

    public void setConf(Configuration conf) {
      this.conf = new DipperConf(conf);
    }

    //
    // RawComparator<TaggedWritable>
    //

    public int compare(TagKeyWritable tk1, TagKeyWritable tk2) {
        int tag1 = tk1.getTag();
        int tag2 = tk2.getTag();

        if (tag1 != tag2) {
            return tag1 < tag2 ? -1 : 1;
        }

        return comparatorOf(tag1).compare(tk1.getKey(), tk2.getKey());
    }

    public int compare(byte[] b1, int s1, int l1,
                       byte[] b2, int s2, int l2) {
        try {
            int tag1 = WritableComparator.readVInt(b1, s1);
            int tag2 = WritableComparator.readVInt(b2, s2);

            if (tag1 != tag2) {
                return tag1 < tag2 ? -1 : 1;
            }

            int n1 = WritableUtils.getVIntSize(tag1);
            int n2 = WritableUtils.getVIntSize(tag2);

            return comparatorOf(tag1).compare(b1, s1+n1, l1-n1,
                                              b2, s2+n2, l2-n2);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }
}
