package basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;


public class MonthSum {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path("in/forestfireinput.csv");

        // arquivo de saida
        Path output = new Path("output/monthsumv1");

        // criacao do job e seu nome
        Job j = new Job(c, "Monthsum");

        //1. registro das classes
        j.setJarByClass(MonthSum.class);
        j.setMapperClass(MapForMonthSum.class);
        j.setReducerClass(ReduceForMonthSum.class);


        //2. Definição dos tipos de saída
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(FloatWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(FloatWritable.class);


        //3. Cadatro dos arquivos de entrada e saída
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);


        //4. Finalizar o job
        System.exit(j.waitForCompletion(true)?0:1);

    }


    public static class MapForMonthSum extends Mapper<LongWritable, Text, Text, FloatWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            //7,5,mar,fri,86.2,26.2,94.3,5.1,8.2,51,6.7,0,0
            // chave pos2
            // valor pos10

            //(mar, 6.7)

            String line = value.toString();
            String[] cols = line.split(",");

            Text newKey = new Text(cols[2]);
            FloatWritable newValue = new FloatWritable(Float.parseFloat(cols[10]));

            con.write(newKey, newValue);

        }
    }


    public static class ReduceForMonthSum extends Reducer<Text, FloatWritable, Text, FloatWritable> {

        public void reduce(Text key, Iterable<FloatWritable> values, Context con)
                throws IOException, InterruptedException {

            //(mar, [12, 11, ...])
            //(apr, [7, 9, ...])

            float sum = 0;
            for (FloatWritable val : values) {
                sum += val.get();
            }

            con.write(key, new FloatWritable(sum));

        }
    }

}
