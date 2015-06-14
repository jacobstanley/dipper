package org.dipper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.Writable;

////////////////////////////////////////////////////////////////////////

public class ValueWritable implements Writable {
    private byte[] buffer     = new byte[0];
    private int    bufferUsed = 0;

    private Writable value;

    // Called by ReflectionUtils.newInstance(..) only
    private ValueWritable() {
    }

    public ValueWritable(Writable value) {
        this.value = value;
    }

    public Writable get() {
        return value;
    }

    //
    // Writable
    //

    public void readFields(DataInput in) throws IOException {
        if (value != null) {
            value.readFields(in);

            //System.err.printf("ValueWritable: read: %s (%s)\n",
            //        value.toString(), value.getClass().getSimpleName());
        } else {
            // This case occurs when we are a reducer input, reading
            // from the mapper output. We can't know at this point what
            // the type of the value is, and we don't really care. The
            // dipper process already knows how to decode the bytes as
            // it will have received the identifier tag prior to
            // receiving this value. The best thing for us to do is just
            // pass the bytes on untouched.

            // For efficiency and in order to easily capture the length
            // of the available bytes, we assume that the DataInput is a
            // DataInputBuffer and pull out its buffer directly. This
            // might stop working at some point, and we'll just have to
            // cross that bridge when we come to it.

            final DataInputBuffer src = (DataInputBuffer)in;

            final int    pos  = src.getPosition();
            final int    len  = src.getLength() - pos;
            final byte[] data = src.getData();

            //System.err.printf("ValueWritable: read:  len = %d, pos = %d, data = %s\n",
            //        len, pos, toHex(data));

            if (len > buffer.length) {
                buffer = new byte[len];
            }

            System.arraycopy(data, pos, buffer, 0, len);
            bufferUsed = len;
        }
    }

    public void write(DataOutput out) throws IOException {
        if (value != null) {
            //System.err.printf("ValueWritable: write: %s (%s)\n",
            //        value.toString(), value.getClass().getSimpleName());

            value.write(out);
        } else {
            //System.err.printf("ValueWritable: write: len = %d, data = %s\n",
            //        bufferUsed, toHex(buffer));

            out.write(buffer, 0, bufferUsed);
        }
    }

    //private static String toHex(byte[] bs) {
    //    StringBuilder sb = new StringBuilder(bs.length * 2);
    //    for(byte b : bs)
    //        sb.append(String.format("%02x ", b & 0xff));
    //    return sb.toString();
    //}
}
