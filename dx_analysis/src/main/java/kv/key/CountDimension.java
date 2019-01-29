package kv.key;

import kv.base.BaseValue;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class CountDimension extends BaseValue {
    private String callSum;
    private String callDurationSum;

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(callSum);
        out.writeUTF(callDurationSum);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        callSum = in.readUTF();
        callDurationSum = in.readUTF();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CountDimension that = (CountDimension) o;
        return Objects.equals(callSum, that.callSum) &&
                Objects.equals(callDurationSum, that.callDurationSum);
    }

    @Override
    public int hashCode() {
        return Objects.hash(callSum, callDurationSum);
    }
}
