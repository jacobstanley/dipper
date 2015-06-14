package org.dipper;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Progressable;

////////////////////////////////////////////////////////////////////////

public class DipperOutputFormat extends FileOutputFormat<Writable, Writable> {
    public RecordWriter<Writable, Writable> getRecordWriter(
            FileSystem fs, JobConf job, String name, Progressable progress) throws IOException {
        return new DipperRecordWriter(fs, job, name, progress);
    }
}

////////////////////////////////////////////////////////////////////////

class DipperRecordWriter implements RecordWriter<Writable, Writable> {

    private final FileSystem fs;
    private final JobConf job;
    private final String partName;
    private final Progressable progress;

    private final TreeMap<String, RecordWriter<Writable, Writable>> writers;

    public DipperRecordWriter(FileSystem fs, JobConf job, String name, Progressable progress) {
        this.fs       = fs;
        this.job      = job;
        this.partName = name;
        this.progress = progress;
        this.writers  = new TreeMap<String, RecordWriter<Writable, Writable>>();
    }

    public void write(Writable key, Writable value) throws IOException {
        // TODO get the path based on the key
        String path = partName;

        // TODO extract the actual key/value
        Writable actualKey   = key;
        Writable actualValue = value;

        writerOf(path).write(actualKey, actualValue);
    }

    private RecordWriter<Writable, Writable> writerOf(String path) throws IOException {
        RecordWriter<Writable, Writable> writer = writers.get(path);

        if (writer == null) {
            writer = getSequenceFileWriter(fs, job, path, progress);
            writers.put(path, writer);
        }

        return writer;
    }

    public void close(Reporter reporter) throws IOException {
        Iterator<String> keys = writers.keySet().iterator();

        while (keys.hasNext()) {
            writers.get(keys.next()).close(reporter);
        }

        writers.clear();
    }

    ////////////////////////////////////////////////////////////////////

    private SequenceFileOutputFormat<Writable, Writable> sequenceFormat = null;

    protected RecordWriter<Writable, Writable> getSequenceFileWriter(
            FileSystem fs, JobConf job, String name, Progressable progress) throws IOException {
        if (sequenceFormat == null) {
            sequenceFormat = new SequenceFileOutputFormat<Writable, Writable>();
        }
        return sequenceFormat.getRecordWriter(fs, job, name, progress);
    }
}
