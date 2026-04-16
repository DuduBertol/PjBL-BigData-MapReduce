package pjbl;

import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class YearCountryWritable implements WritableComparable<YearCountryWritable> {

    private int year;
    private String country;

    public YearCountryWritable() { }

    public YearCountryWritable(int year, String country) {
        this.year = year;
        this.country = country;
    }

    public int getYear() {
        return year;
    }
    public void setYear(int year) {
        this.year = year;
    }

    public String getCountry() {
        return country;
    }
    public void setCountry(String country) {
        this.country = country;
    }


    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(year);
        out.writeUTF(String.valueOf(country));
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        year = in.readInt();
        country = in.readUTF();
    }

    //A.compareTo(B)
    //Comparação de 2 objetos do tipo WritableComparable
    @Override
    public int compareTo(YearCountryWritable other) {
        //A < B = -1      A = B = 0       A > B = +1

        int cmpCountry = this.country.compareTo(other.country);
        if (cmpCountry != 0) { //is NOT the same?
            return cmpCountry;
        }

        int cmpYear = Integer.compare(this.year, other.year);
        return cmpYear;
    }

    //Override vai sobrepor na escrita do .txt
//    @Override
//    public String toString() {
//        return country + " (" + year + ")";
//    }

    //CSV Style
    @Override
    public String toString() {
        return country + "," + year + ",";
    }


    //COMPARE TO
        //EX. POR ANO
        /*
        int cmp = Integer.compare(this.year, obj.year);
        if (cmp != 0) { return cmp; }
        return this.pais.compareTo(o.pais);
         */

        //EX. POR PAÍS
        /*
        int cmp = this.pais.compareTo(outro.pais);
        if (cmp != 0) { return cmp; }
        return Integer.compare(this.ano, outro.ano);
         */
}
