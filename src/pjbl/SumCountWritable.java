package pjbl;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// Writable customizado para calcular média:
// carrega a soma parcial e a contagem parcial, permitindo
// que o Combiner agregue sem perder informação (média não é associativa).
public class SumCountWritable implements Writable {

    private long sum;
    private long count;

    public SumCountWritable() {
    }

    public SumCountWritable(long sum, long count) {
        this.sum = sum;
        this.count = count;
    }

    public long getSum() {
        return sum;
    }

    public void setSum(long sum) {
        this.sum = sum;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public double getAverage() {
        if (count == 0) return 0.0;
        return (double) sum / (double) count;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(sum);
        out.writeLong(count);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        sum = in.readLong();
        count = in.readLong();
    }

    @Override
    public String toString() {
        return sum + "," + count;
    }
}
