package org.dipper;

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

import org.apache.hadoop.streaming.io.TextInputWriter;
import org.apache.hadoop.streaming.io.TextOutputReader;

public class DipperResolver extends IdentifierResolver {
    @Override
    public void resolve(String identifier) {
        if (identifier.equalsIgnoreCase("map")) {
            setInputWriterClass(DipperInputWriter.class);
            setOutputReaderClass(DipperMapOutputReader.class);
            setOutputKeyClass(Text.class);
            setOutputValueClass(BytesWritable.class);
        } else if (identifier.equalsIgnoreCase("reduce")) {
            setInputWriterClass(DipperInputWriter.class);
            setOutputReaderClass(DipperOutputReader.class);
            setOutputKeyClass(Text.class);
            setOutputValueClass(BytesWritable.class);
        } else if (identifier.equalsIgnoreCase("text")) {
            setInputWriterClass(TextInputWriter.class);
            setOutputReaderClass(TextOutputReader.class);
            setOutputKeyClass(Text.class);
            setOutputValueClass(Text.class);
        } else {
            throw new RuntimeException("Dipper: " + identifier);
        }
    }
}

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

class DipperMapOutputReader extends DipperOutputReader {
    @Override
    protected Class<?> getKeyClass(JobConf conf) {
        return conf.getMapOutputKeyClass();
    }

    @Override
    protected Class<?> getValueClass(JobConf conf) {
        return conf.getMapOutputValueClass();
    }
}

abstract class DipperOutputReader extends OutputReader<Writable, Writable> {
    private DataInput in;
    private Writable key;
    private Writable value;

    @Override
    public void initialize(PipeMapRed pipeMapRed) throws IOException {
        super.initialize(pipeMapRed);

        JobConf job = (JobConf)pipeMapRed.getConfiguration();

        in    = pipeMapRed.getClientInput();
        key   = (Writable)ReflectionUtils.newInstance(getKeyClass(job), job);
        value = (Writable)ReflectionUtils.newInstance(getValueClass(job), job);
    }

    protected Class<?> getKeyClass(JobConf conf) {
        return conf.getOutputKeyClass();
    }

    protected Class<?> getValueClass(JobConf conf) {
        return conf.getOutputValueClass();
    }

    @Override
    public boolean readKeyValue() throws IOException {
        try {
            key.readFields(in);
            value.readFields(in);
            return true;
        } catch (EOFException eof) {
            return false;
        }
    }

    @Override
    public Writable getCurrentKey() throws IOException {
        return key;
    }

    @Override
    public Writable getCurrentValue() throws IOException {
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
