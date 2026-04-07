package advanced.customwritable;

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

public class AverageTemperature {

    public static void main(String args[]) throws IOException,
            ClassNotFoundException,
            InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path("in/forestfireinput.csv");

        // arquivo de saida
        Path output = new Path("output/mediaGlobal");

        // criacao do job e seu nome
        Job j = new Job(c, "media");


        //1. Registrar classes
        j.setJarByClass(AverageTemperature.class);
        j.setMapperClass(MapForAverage.class);
        j.setReducerClass(ReduceForAverage.class);

        //2. Tipos de saida
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(FireAvgTempWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(FloatWritable.class);

        //3. Cadastro dos arquivos de entrada e saída
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    //key entrada, value entrada, key saida, value saida
    //7,5,mar,fri,86.2,26.2,94.3,5.1,8.2,51,6.7,0,0
    public static class MapForAverage extends Mapper<LongWritable, Text, Text, FireAvgTempWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            //conversao string
            String line = value.toString();

            //colunas
            String[] columns = line.split(",");

            float newValue = Float.parseFloat(columns[8]);

            con.write(new Text("x"), new FireAvgTempWritable(newValue, 1));

            //tuplas de saída
            //(global, (8.2, 1))
            //numero, frequência
        }
    }

//    public static class CombineForAverage extends Reducer<Text, FireAvgTempWritable, Text, FireAvgTempWritable>{
//        public void reduce(Text key, Iterable<FireAvgTempWritable> values, Context con)
//                throws IOException, InterruptedException {
//        }
//    }

    //(x , [(7, 1), (5, 1), ...])
    public static class ReduceForAverage extends Reducer<Text, FireAvgTempWritable, Text, FloatWritable> {
        public void reduce(Text key, Iterable<FireAvgTempWritable> values, Context con)
                throws IOException, InterruptedException {

            int somaFreq = 0;
            float somaTemp = 0;

            for (FireAvgTempWritable v : values) {
                somaFreq += v.getFreq();
                somaTemp += v.getTemp();
            }

            float avgTemp = somaTemp / somaFreq;

            //key pode ser qualquer valor
            con.write(key, new FloatWritable(avgTemp));
        }
    }

}
