package kv.base;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public abstract class BaseDimension implements WritableComparable<BaseDimension> {

    public abstract int compareTo(BaseDimension o);
    @Override
    public abstract void write(DataOutput out) throws IOException;
    @Override
    public abstract void readFields(DataInput in) throws IOException;

}
