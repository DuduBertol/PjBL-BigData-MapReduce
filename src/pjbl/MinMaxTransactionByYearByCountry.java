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

//MMTYC - Min Max Transaction by Year by Country
public class MinMaxTransactionByYearByCountry {

    public static void main(String args[]) throws IOException,
            ClassNotFoundException,
            InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path("in/operacoes_comerciais_inteira.csv");

        // arquivo de saida
        Path output = new Path("output/minMaxTransactionByYearByCount.txt");

        // criacao do job e seu nome
        Job j = new Job(c, "MinMaxTransactionByYearByCountry");


        //1. Registrar classes
        j.setJarByClass(MinMaxTransactionByYearByCountry.class);
        j.setMapperClass(MapForMinMaxTransations.class);
        j.setCombinerClass(CombineForMinMaxTransations.class);
        j.setReducerClass(ReduceForMinMaxTransations.class);

        //2. Tipos de saida
        j.setMapOutputKeyClass(YearCountryWritable.class);
        j.setMapOutputValueClass(MinMaxWritable.class);
        j.setOutputKeyClass(YearCountryWritable.class);
        j.setOutputValueClass(MinMaxWritable.class);

        //3. Cadastro dos arquivos de entrada e saída
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    //key entrada, value entrada, key saida, value saida
    //Afghanistan;2016;010410;Sheep, live;Export;6088;2339;Number of items;51;01_live_animals
    public static class MapForMinMaxTransations extends Mapper<LongWritable, Text, YearCountryWritable, MinMaxWritable> {
        boolean isHeader = true;

        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            if (isHeader) {
                isHeader = false;
                return;
            }


            String line = value.toString();
            String[] columns = line.split(";");

            try {
                int year = Integer.parseInt(columns[1]);
                String country = columns[0];
                int newValue = Integer.parseInt(columns[5]);

                //Chave = Meu Writable Ano e País | Valor = Min Max Transações
                //( (Afghanistan, 2016), [(6088;6088), ()...] )
                con.write(new YearCountryWritable(year, country), new MinMaxWritable(newValue, newValue));

            } catch (NumberFormatException e) {
                //Valores não-Int
            }
        }
    }

    //( (Afghanistan, 2016), [(5090;6088), ()...] )
    public static class CombineForMinMaxTransations extends Reducer<YearCountryWritable, MinMaxWritable, YearCountryWritable, MinMaxWritable>{
        public void reduce(YearCountryWritable key, Iterable<MinMaxWritable> values, Context con)
                throws IOException, InterruptedException {

            int max = Integer.MIN_VALUE;
            int min = Integer.MAX_VALUE;

            for (MinMaxWritable v : values) {
                max = Math.max(max, v.getMax());
                min = Math.min(min, v.getMin());
            }

            con.write(key, new MinMaxWritable(min, max));
        }
    }

    //( (Afghanistan, 2016), [(5090;6088), ()...] )
    public static class ReduceForMinMaxTransations extends Reducer<YearCountryWritable, MinMaxWritable, YearCountryWritable, MinMaxWritable> {
        public void reduce(YearCountryWritable key, Iterable<MinMaxWritable> values, Context con)
                throws IOException, InterruptedException {

            int max = Integer.MIN_VALUE;
            int min = Integer.MAX_VALUE;

            for (MinMaxWritable v : values) {
                max = Math.max(max, v.getMax());
                min = Math.min(min, v.getMin());
            }

            con.write(key, new MinMaxWritable(min, max));
        }
    }
}


