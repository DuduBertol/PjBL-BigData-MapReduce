package examples;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


//Java bean
//1. atributos privados ou protected;
//2. getters e setters para todos os atributos
//3. construtor vazio

public class MinMaxWindWritable implements Writable{

    private float wind;
    private int freq;

    public MinMaxWindWritable() {
    }

    public MinMaxWindWritable(float temp, int freq) {
        this.wind = temp;
        this.freq = freq;
    }

    public float getWind() {
        return wind;
    }

    public void setWind(float temp) {
        this.wind = wind;
    }

    public int getFreq() {
        return freq;
    }

    public void setFreq(int freq) {
        this.freq = freq;
    }

    @Override
    public void write(DataOutput out) throws IOException {

        out.writeUTF(String.valueOf(wind));
        out.writeUTF(String.valueOf(freq));
    }

    //le os campos, ou seja, lê os atributos
    @Override
    public void readFields(DataInput in) throws IOException {

        wind = Float.parseFloat(in.readUTF());
        freq = Integer.parseInt(in.readUTF());

    }
}

