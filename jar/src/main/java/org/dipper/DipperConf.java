package org.dipper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.ReflectionUtils;

////////////////////////////////////////////////////////////////////////

public class DipperConf {
    private final Configuration conf;

    public DipperConf(Configuration conf) {
        this.conf = conf;
    }

    public Configuration getConf() {
        return conf;
    }

    ////////////////////////////////////////////////////////////////////

    public Class<? extends Writable> keyClassOf(int tag) {
        return getClass("dipper.tag." + tag + ".key").asSubclass(Writable.class);
    }

    public Class<? extends Writable> valueClassOf(int tag) {
        return getClass("dipper.tag." + tag + ".value").asSubclass(Writable.class);
    }

    public Writable newKeyOf(int tag) {
        return ReflectionUtils.newInstance(keyClassOf(tag), conf);
    }

    public Writable newValueOf(int tag) {
        return ReflectionUtils.newInstance(valueClassOf(tag), conf);
    }

    public WritableComparator newKeyComparatorOf(int tag) {
        Class<?> keyClass = getClass("dipper.tag." + tag + ".key");
        return WritableComparator.get(
            keyClass.asSubclass(WritableComparable.class), conf);
    }

    public String pathOf(int tag) {
        return conf.get("dipper.tag." + tag + ".path");
    }

    ////////////////////////////////////////////////////////////////////

    private Class<?> getClass(String configKey) {
        try {
            String className = conf.get(configKey);
            return Class.forName(className);
        } catch (ClassNotFoundException ex) {
            throw new RuntimeException("Could not create class specified by '" + configKey + "'", ex);
        }
    }
}
