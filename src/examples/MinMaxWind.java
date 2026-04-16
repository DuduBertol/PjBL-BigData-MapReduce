
package examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class MinMaxWind {

    public static void main(String args[]) throws IOException,
            ClassNotFoundException,
            InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path("in/forestfireinput.csv");

        // arquivo de saida
        Path output = new Path("output/maxWind");

        // criacao do job e seu nome
        Job j = new Job(c, "media");


        //1. Registrar classes
        j.setJarByClass(MinMaxWind.class);
        j.setMapperClass(MapForWind.class);
        j.setReducerClass(ReduceForMaxWind.class);

        //2. Tipos de saida
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(MinMaxWindWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(FloatWritable.class);

        //3. Cadastro dos arquivos de entrada e saída
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    //key entrada, value entrada, key saida, value saida
    //7,5,mar,fri,86.2,26.2,94.3,5.1,8.2,51,6.7,0,0
    public static class MapForWind extends Mapper<LongWritable, Text, Text, MinMaxWindWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            //conversao string
            String line = value.toString();

            //colunas
            String[] columns = line.split(",");

            float newValue = Float.parseFloat(columns[10]);

            con.write(new Text("x"), new MinMaxWindWritable(newValue, 1));

            //tupla de saída
            //(x, (6.7, 1))
            //numero, frequência
        }
    }

//    public static class CombineForAverage extends Reducer<Text, FireAvgTempWritable, Text, FireAvgTempWritable>{
//        public void reduce(Text key, Iterable<FireAvgTempWritable> values, Context con)
//                throws IOException, InterruptedException {
//        }
//    }

    //(x , [(7, 1), (5, 1), ...])
    public static class ReduceForMaxWind extends Reducer<Text, MinMaxWindWritable, Text, FloatWritable> {
        public void reduce(Text key, Iterable<MinMaxWindWritable> values, Context con)
                throws IOException, InterruptedException {

            //MAX
            float maxWind = Float.MIN_VALUE;
            float minWind = Float.MAX_VALUE;

            //Iterable por algum motivo só pode ser iterado em lista 1 vez só
            for (MinMaxWindWritable v : values) {
                //MAX
                if (v.getWind() > maxWind) {
                    maxWind = v.getWind();
                }

                //MIN
                if (v.getWind() < minWind) {
                    minWind = v.getWind();
                }
            }

            //key pode ser qualquer valor
            con.write(new Text("Max Wind"), new FloatWritable(maxWind));

            con.write(new Text("Min Wind"), new FloatWritable(minWind));
        }
    }
}
