import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

    /**
     * MAPPER
     *
     * Recibe:
     *   <offset, linea>
     *
     * Produce:
     *   <palabra, 1>
     */
    public static class WordMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable ONE = new IntWritable(1);
        private final Text word = new Text();

        @Override
        public void map(
                Object key,
                Text value,
                Context context)
                throws IOException, InterruptedException {

            // La linea completa recibida
            String line = value.toString();

            // Dividir la linea en palabras
            StringTokenizer tokenizer =
                    new StringTokenizer(line);

            while (tokenizer.hasMoreTokens()) {

                String token = tokenizer.nextToken();

                word.set(token);

                // Emitir <palabra, 1>
                context.write(word, ONE);
            }
        }
    }


    /**
     * REDUCER
     *
     * Recibe:
     *   <palabra, [1,1,1,...]>
     *
     * Produce:
     *   <palabra, total>
     */
    public static class WordReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private final IntWritable result = new IntWritable();

        @Override
        public void reduce(
                Text key,
                Iterable<IntWritable> values,
                Context context)
                throws IOException, InterruptedException {

            int sum = 0;

            // Sumar todos los valores asociados
            // con la misma palabra
            for (IntWritable value : values) {
                sum += value.get();
            }

            result.set(sum);

            // Emitir <palabra, total>
            context.write(key, result);
        }
    }


    /**
     * DRIVER
     *
     * Configura y ejecuta el Job MapReduce.
     */
    public static void main(String[] args)
            throws Exception {

        if (args.length != 2) {
            System.err.println(
                "Uso: WordCount <input> <output>"
            );

            System.exit(2);
        }

        Configuration configuration =
                new Configuration();

        Job job = Job.getInstance(
                configuration,
                "WordCount"
        );

        // Indicar donde esta el codigo del Job
        job.setJarByClass(WordCount.class);

        // Configurar Mapper
        job.setMapperClass(WordMapper.class);

        // Configurar Reducer
        job.setReducerClass(WordReducer.class);

        // Tipo de salida del Mapper
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // Tipo de salida final
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Directorio de entrada
        FileInputFormat.addInputPath(
                job,
                new Path(args[0])
        );

        // Directorio de salida
        FileOutputFormat.setOutputPath(
                job,
                new Path(args[1])
        );

        // Esperar a que termine el Job
        System.exit(
                job.waitForCompletion(true)
                ? 0
                : 1
        );
    }
}