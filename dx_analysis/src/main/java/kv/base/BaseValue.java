package kv.base;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public abstract class BaseValue implements Writable {
    public abstract void write(DataOutput out) throws IOException;

    public abstract void readFields(DataInput in) throws IOException;
}
