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

// Pergunta 7: Valor médio das transações por ano, somente Brasil + Export.
// Uso obrigatório do Combiner.
public class BrazilExportAverageByYear {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        new GenericOptionsParser(c, args).getRemainingArgs();

        Path input = new Path("in/operacoes_comerciais_inteira.csv");
        Path output = new Path("output/media_brasil_export_anual.txt");

        Job j = new Job(c, "BrazilExportAverageByYear");

        j.setJarByClass(BrazilExportAverageByYear.class);
        j.setMapperClass(MapForBrazilExportAverage.class);
        j.setCombinerClass(CombineForBrazilExportAverage.class);
        j.setReducerClass(ReduceForBrazilExportAverage.class);

        // Saída do mapper/combiner: (Text ano, SumCountWritable parcial)
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(SumCountWritable.class);

        // Saída final do reducer: (Text ano, DoubleWritable média)
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    // Colunas: Country(0);Year(1);CommodityCode(2);Commodity(3);Flow(4);Price(5);Weight(6);Unit(7);Amount(8);Category(9)
    public static class MapForBrazilExportAverage extends Mapper<LongWritable, Text, Text, SumCountWritable> {

        @Override
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String line = value.toString();

            // 1. Retirar cabeçalho (funciona em qualquer split, não só no primeiro)
            if (line.startsWith("country_or_area")) {
                return;
            }

            String[] colunas = line.split(";");

            // 2. Tratar dados faltantes — precisamos no mínimo até Price (índice 5)
            if (colunas.length < 6
                    || colunas[0].trim().isEmpty()
                    || colunas[1].trim().isEmpty()
                    || colunas[4].trim().isEmpty()
                    || colunas[5].trim().isEmpty()) {
                return;
            }

            // Filtro: somente Brasil
            if (!colunas[0].trim().equalsIgnoreCase("Brazil")) {
                return;
            }

            // Filtro: somente Export
            if (!colunas[4].trim().equalsIgnoreCase("Export")) {
                return;
            }

            try {
                long price = Long.parseLong(colunas[5].trim());
                con.write(new Text(colunas[1].trim()), new SumCountWritable(price, 1));
            } catch (NumberFormatException e) {
                // preço inválido / faltante
            }
        }
    }

    // Combiner: agrega (sum, count) parcialmente em cada mapper.
    // Mesma saída do mapper — não calcula média ainda.
    public static class CombineForBrazilExportAverage extends Reducer<Text, SumCountWritable, Text, SumCountWritable> {

        @Override
        public void reduce(Text key, Iterable<SumCountWritable> values, Context con)
                throws IOException, InterruptedException {

            long sumParcial = 0;
            long countParcial = 0;

            for (SumCountWritable v : values) {
                sumParcial += v.getSum();
                countParcial += v.getCount();
            }

            con.write(key, new SumCountWritable(sumParcial, countParcial));
        }
    }

    // Reducer final: agrega tudo e emite a média (DoubleWritable).
    public static class ReduceForBrazilExportAverage extends Reducer<Text, SumCountWritable, Text, DoubleWritable> {

        @Override
        public void reduce(Text key, Iterable<SumCountWritable> values, Context con)
                throws IOException, InterruptedException {

            long sumTotal = 0;
            long countTotal = 0;

            for (SumCountWritable v : values) {
                sumTotal += v.getSum();
                countTotal += v.getCount();
            }

            if (countTotal == 0) {
                return;
            }

            double media = (double) sumTotal / (double) countTotal;
            con.write(key, new DoubleWritable(media));
        }
    }
}
