package kv.key;

import kv.base.BaseDimension;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class DateDimension extends BaseDimension {
    private String year;
    private String month;
    private String day;

    @Override
    public int compareTo(BaseDimension o) {
        DateDimension o2 = (DateDimension) o;

        int result = this.year.compareTo(o2.year);
        if(result != 0){
            return result;
        }
        result = this.month.compareTo(o2.month);
        if(result != 0){
            return result;
        }
        result = this.day.compareTo(o2.day);
        return result;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.year);
        out.writeUTF(this.month);
        out.writeUTF(this.day);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.year = in.readUTF();
        this.month = in.readUTF();
        this.day = in.readUTF();

    }

    //快捷键生成
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DateDimension that = (DateDimension) o;
        return year.equals(that.year) &&
                month.equals(that.month) &&
                day.equals(that.day);
    }

    @Override
    public int hashCode() {
        return Objects.hash(year, month, day);
    }
}
