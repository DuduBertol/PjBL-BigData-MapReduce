package basic;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;


public class WordCount {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "wordcount");

        //1. registro das classes
        j.setJarByClass(WordCount.class);
        j.setMapperClass(MapForWordCount.class);
        j.setReducerClass(ReduceForWordCount.class);

        //2. definir ps tipos de saída
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(IntWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);


        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        System.out.println(j.waitForCompletion(true) ? 0 : 1);

    }

    public static class MapForWordCount extends Mapper<LongWritable, Text, Text, IntWritable> {

        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            //1:1 In the beginning God created the heaven and the earth.

            //(In, 1)
            //(the, 1)

            String line = value.toString();
            String[] words = line.split(" ");

//            String myWord = "God";
            //achar God

            for (String word : words) {
                //(chave saída, valor saída)

                Text outKey = new Text(word);
                IntWritable outValue = new IntWritable(1);

//                if (myWord.equals(word)) {
                    con.write(outKey, outValue);
//                }
            }
        }
    }


    //(God, [1,1,1,1,1])
    //(Mary, [1,1,1,1,1])
    public static class ReduceForWordCount extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            //Apenas palavras W>3000
//            if (sum >= 3000) {
                con.write(key, new IntWritable(sum));
//            }
        }
    }

}
