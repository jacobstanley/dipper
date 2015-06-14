package org.dipper;

import java.util.TreeMap;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.JobConf;

////////////////////////////////////////////////////////////////////////

public class TagKeyComparator implements RawComparator<TagKeyWritable>, Configurable {
    private final TreeMap<Integer, WritableComparator> comparators;
    private DipperConf conf;

    // Called by ReflectionUtils.newInstance(..) only
    private TagKeyComparator() {
        comparators = new TreeMap<Integer, WritableComparator>();
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

        if (tag1 == tag2) {
            return comparatorOf(tag1).compare(tk1.getKey(), tk2.getKey());
        }

        return tag1 < tag2 ? -1 : 1;
    }

    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        return -1;
    }
}
