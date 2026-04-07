package advanced.customwritable;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.checkerframework.checker.units.qual.Temperature;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;


//Java bean
//1. atributos privados ou protected;
//2. getters e setters para todos os atributos
//3. construtor vazio

public class FireAvgTempWritable implements Writable{

    private float temp;
    private int freq;

    public FireAvgTempWritable() {
    }

    public FireAvgTempWritable(float temp, int freq) {
        this.temp = temp;
        this.freq = freq;
    }

    public float getTemp() {
        return temp;
    }

    public void setTemp(float temp) {
        this.temp = temp;
    }

    public int getFreq() {
        return freq;
    }

    public void setFreq(int freq) {
        this.freq = freq;
    }

    @Override
    public void write(DataOutput out) throws IOException {

        out.writeUTF(String.valueOf(temp));
        out.writeUTF(String.valueOf(freq));
    }

    //le os campos, ou seja, lê os atributos
    @Override
    public void readFields(DataInput in) throws IOException {

        temp = Float.parseFloat(in.readUTF());
        freq = Integer.parseInt(in.readUTF());

    }
}

