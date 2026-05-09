package pjbl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class YearTransactions {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        new GenericOptionsParser(c, args).getRemainingArgs();

        Path input = new Path("in/operacoes_comerciais_inteira.csv");
        Path output = new Path("output/transacoes_anual.txt");

        Job j = new Job(c, "YearTransactions");

        j.setJarByClass(YearTransactions.class);
        j.setMapperClass(YearTransactions.MapForYearTransaction.class);
        j.setCombinerClass(YearTransactions.ReduceForYearTransaction.class);
        j.setReducerClass(YearTransactions.ReduceForYearTransaction.class);

        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapForYearTransaction extends Mapper<LongWritable, Text, Text, LongWritable> {

        private boolean isHeader = true;
        private static final LongWritable UM = new LongWritable(1);

        @Override
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            if (isHeader) {
                isHeader = false;
                return;
            }

            String[] colunas = value.toString().split(";");

            if (colunas.length < 2 || colunas[1].trim().isEmpty()) {
                return;
            }

            con.write(new Text(colunas[1].trim()), UM);
        }
    }

    public static class ReduceForYearTransaction extends Reducer<Text, LongWritable, Text, LongWritable> {

        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context con)
                throws IOException, InterruptedException {

            long total = 0;
            for (LongWritable v : values) {
                total += v.get();
            }
            con.write(key, new LongWritable(total));
        }
    }


}
