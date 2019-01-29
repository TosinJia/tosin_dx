package kv.key;

import kv.base.BaseDimension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class ComDimension extends BaseDimension {
    private ContactDimension contactDimension = new ContactDimension();
    private DateDimension dateDimension = new DateDimension();

    @Override
    public int compareTo(BaseDimension o) {
        ComDimension o2 = (ComDimension) o;

        int result = this.dateDimension.compareTo(o2.dateDimension);
        if(result != 0){
            return result;
        }
        result = this.contactDimension.compareTo(o2.contactDimension);
        return result;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        contactDimension.write(out);
        dateDimension.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        contactDimension.readFields(in);
        dateDimension.readFields(in);
    }

    //alt+insert hashcode equal


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ComDimension that = (ComDimension) o;
        return contactDimension.equals(that.contactDimension) &&
                dateDimension.equals(that.dateDimension);
    }

    @Override
    public int hashCode() {
        return Objects.hash(contactDimension, dateDimension);
    }
}
