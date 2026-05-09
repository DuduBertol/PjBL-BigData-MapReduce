package pjbl;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MinMaxPriceWritable implements Writable {
    private long min;
    private long max;

    public MinMaxPriceWritable() {
    }

    public MinMaxPriceWritable(long min, long max) {
        this.min = min;
        this.max = max;
    }

    public long getMin() {
        return min;
    }

    public void setMin(long min) {
        this.min = min;
    }

    public long getMax() {
        return max;
    }

    public void setMax(long max) {
        this.max = max;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(min);
        out.writeLong(max);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        min = in.readLong();
        max = in.readLong();
    }

    //CSV style
    @Override
    public String toString() {
        return min + "," + max;
    }
}
