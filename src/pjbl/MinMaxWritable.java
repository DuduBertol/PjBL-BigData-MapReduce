package pjbl;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MinMaxWritable implements Writable{
    private int min;
    private int max;

    public MinMaxWritable() {
    }

    public MinMaxWritable(int min, int max) {
        this.min = min;
        this.max = max;
    }

    public int getMin() {
        return min;
    }

    public void setMin(int min) {
        this.min = min;
    }

    public int getMax() {
        return max;
    }

    public void setMax(int max) {
        this.max = max;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(min);
        out.writeInt(max);
    }

    //le os campos, ou seja, lê os atributos
    @Override
    public void readFields(DataInput in) throws IOException {
        min = in.readInt();
        max = in.readInt();
    }

    //Override vai sobrepor na escrita do .txt
//    @Override
//    public String toString() {
//        return "Menor Preço: " + min + " | Maior Preço: " + max;
//    }

    //CSV style
    @Override
    public String toString() {
        return min + "," + max;
    }
}


