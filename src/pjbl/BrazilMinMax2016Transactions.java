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

public class BrazilMinMax2016Transactions {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        new GenericOptionsParser(c, args).getRemainingArgs();

        Path input = new Path("in/operacoes_comerciais_inteira.csv");
        Path output = new Path("output/transacoes_min_max_brasil_2016.txt");

        Job j = new Job(c, "BrazilMinMax2016Transactions");

        j.setJarByClass(BrazilMinMax2016Transactions.class);
        j.setMapperClass(MapForBrazilMinMax2016Transaction.class);
        j.setCombinerClass(ReduceForBrazilMinMax2016Transaction.class);
        j.setReducerClass(ReduceForBrazilMinMax2016Transaction.class);

        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(MinMaxPriceWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(MinMaxPriceWritable.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapForBrazilMinMax2016Transaction extends Mapper<LongWritable, Text, Text, MinMaxPriceWritable> {

        private boolean isHeader = true;
        private static final Text CHAVE = new Text("Brazil_2016");

        @Override
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            if (isHeader) {
                isHeader = false;
                return;
            }

            String[] colunas = value.toString().split(";");

            if (colunas.length < 6
                    || colunas[0].trim().isEmpty()
                    || colunas[1].trim().isEmpty()
                    || colunas[5].trim().isEmpty()) {
                return;
            }

            if (!colunas[0].trim().equalsIgnoreCase("Brazil")) {
                return;
            }
            if (!colunas[1].trim().equals("2016")) {
                return;
            }

            try {
                long preco = Long.parseLong(colunas[5].trim());
                con.write(CHAVE, new MinMaxPriceWritable(preco, preco));
            } catch (NumberFormatException e) {
                //preco invalido / faltante
            }
        }
    }

    public static class ReduceForBrazilMinMax2016Transaction extends Reducer<Text, MinMaxPriceWritable, Text, MinMaxPriceWritable> {

        @Override
        public void reduce(Text key, Iterable<MinMaxPriceWritable> values, Context con)
                throws IOException, InterruptedException {

            long min = Long.MAX_VALUE;
            long max = Long.MIN_VALUE;

            for (MinMaxPriceWritable v : values) {
                if (v.getMin() < min) min = v.getMin();
                if (v.getMax() > max) max = v.getMax();
            }

            con.write(key, new MinMaxPriceWritable(min, max));
        }
    }
}
