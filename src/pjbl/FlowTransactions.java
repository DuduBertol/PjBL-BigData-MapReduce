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

// Pergunta 4: Número de transações por tipo de fluxo (Flow)
public class FlowTransactions {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        new GenericOptionsParser(c, args).getRemainingArgs();

        Path input = new Path("in/operacoes_comerciais_inteira.csv");
        Path output = new Path("output/transacoes_flow.txt");

        Job j = new Job(c, "FlowTransactions");

        j.setJarByClass(FlowTransactions.class);
        j.setMapperClass(MapForFlowTransaction.class);
        j.setCombinerClass(ReduceForFlowTransaction.class);
        j.setReducerClass(ReduceForFlowTransaction.class);

        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    // Colunas: Country(0);Year(1);CommodityCode(2);Commodity(3);Flow(4);Price(5);Weight(6);Unit(7);Amount(8);Category(9)
    public static class MapForFlowTransaction extends Mapper<LongWritable, Text, Text, LongWritable> {

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

            // 2. Tratar dados faltantes (linha deve ter ao menos até a coluna Flow)
            if (colunas.length < 5 || colunas[4].trim().isEmpty()) {
                return;
            }

            con.write(new Text(colunas[4].trim()), UM);
        }
    }

    public static class ReduceForFlowTransaction extends Reducer<Text, LongWritable, Text, LongWritable> {

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
