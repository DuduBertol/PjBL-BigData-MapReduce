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

/**
 * Pergunta: Valor médio das transações por ano, somente Brasil (todos os flows).
 *
 * Requisitos:
 *  1. Writable customizado (SumCountWritable) — armazena soma + contagem para
 *     calcular média corretamente sem concatenação de strings.
 *  2. Remoção do cabeçalho (funciona em qualquer split/nó).
 *  3. Tratamento de dados faltantes (campos vazios ou não numéricos).
 *  4. NENHUMA concatenação de strings para formar chaves ou valores compostos.
 *
 * Colunas do CSV (separador ";"):
 *   [0] country_or_area  [1] year  [2] comm_code  [3] commodity
 *   [4] flow             [5] trade_usd  [6] weight_kg  [7] quantity_name
 *   [8] quantity         [9] category
 *
 * Fluxo MapReduce:
 *   Mapper  → (Text ano, SumCountWritable { sum=trade_usd, count=1 })
 *   Combiner→ (Text ano, SumCountWritable { sum parcial, count parcial })
 *   Reducer → (Text ano, DoubleWritable média)
 */
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

        // Saída do Mapper / Combiner: (Text ano, SumCountWritable)
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(SumCountWritable.class);

        // Saída final do Reducer: (Text ano, DoubleWritable média)
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    // -------------------------------------------------------------------------
    // MAPPER
    // Emite (ano, SumCountWritable{trade_usd, 1}) para cada linha do Brasil.
    // -------------------------------------------------------------------------
    public static class MapForBrazilAverage
            extends Mapper<LongWritable, Text, Text, SumCountWritable> {

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            // 1. Remover cabeçalho — funciona em qualquer split, não apenas no primeiro.
            if (line.startsWith("country_or_area")) {
                return;
            }

            String[] cols = line.split(";");

            // 2. Tratar dados faltantes: precisamos ao menos até trade_usd (índice 5).
            if (cols.length < 6
                    || cols[0].trim().isEmpty()   // country
                    || cols[1].trim().isEmpty()   // year
                    || cols[5].trim().isEmpty()) { // trade_usd
                return;
            }

            // 3. Filtro de país: somente Brasil.
            if (!cols[0].trim().equalsIgnoreCase("Brazil")) {
                return;
            }

            // 4. Emitir (ano, {trade_usd, 1}) — sem concatenação de strings.
            try {
                long tradeUsd = Long.parseLong(cols[5].trim());
                // Chave: Text com o ano (campo primitivo, sem concatenação composta)
                // Valor: SumCountWritable customizado (soma + contagem)
                context.write(new Text(cols[1].trim()),
                              new SumCountWritable(tradeUsd, 1L));
            } catch (NumberFormatException e) {
                // Valor de trade_usd inválido — ignorar linha.
            }
        }
    }

    // -------------------------------------------------------------------------
    // COMBINER
    // Agrega parcialmente no mesmo nó: reduz volume de shuffle.
    // Mantém o mesmo tipo de saída do Mapper para ser compatível como Combiner.
    // -------------------------------------------------------------------------
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

    // -------------------------------------------------------------------------
    // REDUCER
    // Recebe todos os SumCountWritable de um mesmo ano, agrega e calcula média.
    // -------------------------------------------------------------------------
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
