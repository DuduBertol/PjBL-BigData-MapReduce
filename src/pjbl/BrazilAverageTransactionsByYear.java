package pjbl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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


public class BrazilAverageTransactionsByYear {

    public static void main(String[] args)
            throws IOException, ClassNotFoundException, InterruptedException {

        BasicConfigurator.configure();

        Configuration c = new Configuration();
        new GenericOptionsParser(c, args).getRemainingArgs();

        Path input  = new Path("in/operacoes_comerciais_inteira.csv");
        Path output = new Path("output/resultado_media_brasil_anual.txt");

        Job j = new Job(c, "BrazilAverageTransactionsByYear");

        j.setJarByClass(BrazilAverageTransactionsByYear.class);
        j.setMapperClass(MapForBrazilAverage.class);
        j.setCombinerClass(CombineForBrazilAverage.class);
        j.setReducerClass(ReduceForBrazilAverage.class);

            
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(SumCountWritable.class);

            
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapForBrazilAverage
            extends Mapper<LongWritable, Text, Text, SumCountWritable> {

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            if (line.startsWith("country_or_area")) {
                return;
            }

            String[] cols = line.split(";");

            if (cols.length < 6
                    || cols[0].trim().isEmpty()  
                    || cols[1].trim().isEmpty()   
                    || cols[5].trim().isEmpty()) { 
                return;
            }

            if (!cols[0].trim().equalsIgnoreCase("Brazil")) {
                return;
            }

            
            try {
                long tradeUsd = Long.parseLong(cols[5].trim());
                
                context.write(new Text(cols[1].trim()),
                              new SumCountWritable(tradeUsd, 1L));
            } catch (NumberFormatException e) {
            
            }
        }
    }


    public static class CombineForBrazilAverage
            extends Reducer<Text, SumCountWritable, Text, SumCountWritable> {

        @Override
        public void reduce(Text key, Iterable<SumCountWritable> values, Context context)
                throws IOException, InterruptedException {

            long sumParcial   = 0L;
            long countParcial = 0L;

            for (SumCountWritable v : values) {
                sumParcial   += v.getSum();
                countParcial += v.getCount();
            }

            // Emite soma parcial + contagem parcial — NÃO calcula média ainda.
            context.write(key, new SumCountWritable(sumParcial, countParcial));
        }
    }


    public static class ReduceForBrazilAverage
            extends Reducer<Text, SumCountWritable, Text, DoubleWritable> {

        @Override
        public void reduce(Text key, Iterable<SumCountWritable> values, Context context)
                throws IOException, InterruptedException {

            long sumTotal   = 0L;
            long countTotal = 0L;

            for (SumCountWritable v : values) {
                sumTotal   += v.getSum();
                countTotal += v.getCount();
            }

            // Protege divisão por zero caso nenhum registro válido exista para o ano.
            if (countTotal == 0) {
                return;
            }

            double media = (double) sumTotal / (double) countTotal;
            context.write(key, new DoubleWritable(media));
        }
    }
}
