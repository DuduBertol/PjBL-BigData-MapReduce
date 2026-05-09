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

// Pergunta 3: Número de transações por categoria
public class CategoryTransactions {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        new GenericOptionsParser(c, args).getRemainingArgs();

        Path input = new Path("in/operacoes_comerciais_inteira.csv");
        Path output = new Path("output/transacoes_categoria.txt");

        Job j = new Job(c, "CategoryTransactions");

        j.setJarByClass(CategoryTransactions.class);
        j.setMapperClass(MapForCategoryTransaction.class);
        j.setCombinerClass(ReduceForCategoryTransaction.class);
        j.setReducerClass(ReduceForCategoryTransaction.class);

        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    // Colunas: Country(0);Year(1);CommodityCode(2);Commodity(3);Flow(4);Price(5);Weight(6);Unit(7);Amount(8);Category(9)
    public static class MapForCategoryTransaction extends Mapper<LongWritable, Text, Text, LongWritable> {

        private boolean isHeader = true;
        private static final LongWritable UM = new LongWritable(1);

        @Override
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            // 1. Retirar cabeçalho
            if (isHeader) {
                isHeader = false;
                return;
            }

            String[] colunas = value.toString().split(";");

            // 2. Tratar dados faltantes (linha deve ter todas as 10 colunas e Category não pode ser vazia)
            if (colunas.length < 10 || colunas[9].trim().isEmpty()) {
                return;
            }

            con.write(new Text(colunas[9].trim()), UM);
        }
    }

    public static class ReduceForCategoryTransaction extends Reducer<Text, LongWritable, Text, LongWritable> {

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
