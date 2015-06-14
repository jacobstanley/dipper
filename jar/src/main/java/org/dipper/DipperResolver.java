package org.dipper;

import java.util.HashMap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.streaming.PipeMapRed;
import org.apache.hadoop.streaming.io.IdentifierResolver;
import org.apache.hadoop.streaming.io.InputWriter;
import org.apache.hadoop.streaming.io.OutputReader;
import org.apache.hadoop.util.ReflectionUtils;

////////////////////////////////////////////////////////////////////////

public class DipperResolver extends IdentifierResolver {
    @Override
    public void resolve(String identifier) {
        setInputWriterClass(DipperInputWriter.class);
        setOutputReaderClass(DipperOutputReader.class);
        setOutputKeyClass(TagKeyWritable.class);
        setOutputValueClass(ValueWritable.class);
    }
}

////////////////////////////////////////////////////////////////////////

class DipperInputWriter extends InputWriter<Writable, Writable> {
    private DataOutput out;

    @Override
    public void initialize(PipeMapRed pipeMapRed) throws IOException {
        super.initialize(pipeMapRed);
        out = pipeMapRed.getClientOutput();
    }

    @Override
    public void writeKey(Writable key) throws IOException {
        key.write(out);
    }

    @Override
    public void writeValue(Writable value) throws IOException {
        value.write(out);
    }
}

////////////////////////////////////////////////////////////////////////

class DipperOutputReader extends OutputReader<TagKeyWritable, ValueWritable> {
    private DataInput in;
    private DipperConf conf;
    private HashMap<Integer, ValueWritable> values;
    private TagKeyWritable key;
    private ValueWritable value;

    @Override
    public void initialize(PipeMapRed pipeMapRed) throws IOException {
        super.initialize(pipeMapRed);

        JobConf job = (JobConf)pipeMapRed.getConfiguration();

        in     = pipeMapRed.getClientInput();
        conf   = new DipperConf(job);
        key    = (TagKeyWritable)ReflectionUtils.newInstance(TagKeyWritable.class, job);
        values = new HashMap<Integer, ValueWritable>();
    }

    private ValueWritable valueOf(int tag) {
        ValueWritable value = values.get(tag);

        if (value == null) {
            value = new ValueWritable(conf.newValueOf(tag));
            values.put(tag, value);
        }

        return value;
    }

    @Override
    public boolean readKeyValue() throws IOException {
        try {
            key.readFields(in);
            value = valueOf(key.getTag());
            value.readFields(in);
            return true;
        } catch (EOFException eof) {
            return false;
        }
    }

    @Override
    public TagKeyWritable getCurrentKey() throws IOException {
        return key;
    }

    @Override
    public ValueWritable getCurrentValue() throws IOException {
        return value;
    }

    @Override
    public String getLastOutput() {
        if (key != null && value != null) {
            return "(" + key.toString() + ", " + value.toString() + ")";
        } else {
            return null;
        }
    }
}
