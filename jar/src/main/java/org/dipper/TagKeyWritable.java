package org.dipper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

////////////////////////////////////////////////////////////////////////

public class TagKeyWritable implements Writable, Configurable {
    private final TreeMap<Integer, Writable> keys;
    private DipperConf conf;

    private int      tag;
    private Writable key;

    // Called by ReflectionUtils.newInstance(..) only
    private TagKeyWritable() {
        keys = new TreeMap<Integer, Writable>();
    }

    public void set(int tag, Writable key) {
        this.tag = tag;
        this.key = key;
    }

    public int getTag() {
        return tag;
    }

    public Writable getKey() {
        return key;
    }

    private Writable keyOf(int tag) {
        Writable key = keys.get(tag);

        if (key == null) {
            key = conf.newKeyOf(tag);
            keys.put(tag, key);
        }

        return key;
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
    // Writable
    //

    public void readFields(DataInput in) throws IOException {
        tag = WritableUtils.readVInt(in);
        key = keyOf(tag);
        key.readFields(in);
        //System.err.printf("TagKeyWritable: read: tag = %d, key = %s (%s)\n",
        //        tag, key.toString(), key.getClass().getSimpleName());
    }

    public void write(DataOutput out) throws IOException {
        //System.err.printf("TagKeyWritable: write: tag = %d, key = %s (%s)\n",
        //        tag, key.toString(), key.getClass().getSimpleName());
        WritableUtils.writeVInt(out, tag);
        key.write(out);
    }
}
