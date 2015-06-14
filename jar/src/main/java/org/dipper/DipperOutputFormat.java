package org.dipper;

import java.io.IOException;
import java.util.Iterator;
import java.util.HashMap;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;

////////////////////////////////////////////////////////////////////////

public class DipperOutputFormat extends FileOutputFormat<TagKeyWritable, ValueWritable> {
    public RecordWriter<TagKeyWritable, ValueWritable> getRecordWriter(
            FileSystem fs, JobConf job, String name, Progressable progress) throws IOException {
        return new DipperRecordWriter(fs, job, name, progress);
    }
}

////////////////////////////////////////////////////////////////////////

class DipperRecordWriter implements RecordWriter<TagKeyWritable, ValueWritable> {

    private final FileSystem fs;
    private final JobConf job;
    private final String partName;
    private final Progressable progress;

    private final DipperConf conf;
    private final HashMap<Integer, RecordWriter<Writable, Writable>> writers;

    public DipperRecordWriter(FileSystem fs, JobConf job, String name, Progressable progress) {
        this.fs       = fs;
        this.job      = job;
        this.partName = name;
        this.progress = progress;
        this.conf     = new DipperConf(job);
        this.writers  = new HashMap<Integer, RecordWriter<Writable, Writable>>();
    }

    public void write(TagKeyWritable k, ValueWritable v) throws IOException {
        int      tag   = k.getTag();
        Writable key   = k.getKey();
        Writable value = v.get();

        writerOf(tag).write(key, value);
    }

    private RecordWriter<Writable, Writable> writerOf(int tag) throws IOException {
        RecordWriter<Writable, Writable> writer = writers.get(tag);

        if (writer == null) {
            writer = createWriterOf(tag);
            writers.put(tag, writer);
        }

        return writer;
    }

    public void close(Reporter reporter) throws IOException {
        Iterator<Integer> keys = writers.keySet().iterator();

        while (keys.hasNext()) {
            writers.get(keys.next()).close(reporter);
        }

        writers.clear();
    }

    ////////////////////////////////////////////////////////////////////

    private RecordWriter<Writable, Writable> createWriterOf(int tag) throws IOException {

        OutputFormat<Writable, Writable> format = conf.newFormatOf(tag);

        Class<?> keyClass   = conf.keyClassOf(tag);
        Class<?> valueClass = conf.valueClassOf(tag);
        String   path       = conf.pathOf(tag) + "/" + partName;

        Class<?> oldKeyClass   = job.getOutputKeyClass();
        Class<?> oldValueClass = job.getOutputValueClass();

        try {
            job.setOutputKeyClass(keyClass);
            job.setOutputValueClass(valueClass);

            return format.getRecordWriter(fs, job, path, progress);
        } finally {
            job.setOutputKeyClass(oldKeyClass);
            job.setOutputValueClass(oldValueClass);
        }
    }
}
