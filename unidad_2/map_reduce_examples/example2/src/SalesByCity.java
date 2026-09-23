import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class SalesByCity {

    /**
     * ============================================================
     * MAPPER
     * ============================================================
     *
     * Entrada:
     *
     *   Linea del archivo CSV
     *
     * Salida:
     *
     *   <ciudad, total>
     *
     * Ejemplo:
     *
     *   Bogota,250000
     *
     * se convierte en:
     *
     *   <Bogota, 250000>
     */
    public static class SalesMapper
            extends Mapper<Object, Text, Text, DoubleWritable> {

        private final Text ciudadKey = new Text();
        private final DoubleWritable totalValue =
                new DoubleWritable();

        @Override
        protected void map(
                Object key,
                Text value,
                Context context)
                throws IOException, InterruptedException {

            // Obtener la linea completa del CSV
            String linea = value.toString();

            // Ignorar la linea de encabezado
            if (linea.startsWith("id_venta")) {
                return;
            }

            // Separar las columnas
            String[] columnas = linea.split(",");

            // Verificar que tengamos las 14 columnas esperadas
            if (columnas.length != 14) {
                return;
            }

            /*
             * Posiciones de las columnas:
             *
             * 0  id_venta
             * 1  fecha
             * 2  cliente
             * 3  producto
             * 4  categoria
             * 5  cantidad
             * 6  precio_unitario
             * 7  descuento
             * 8  total
             * 9  ciudad
             * 10 metodo_pago
             * 11 canal
             * 12 vendedor
             * 13 estado
             */

            try {

                // Extraer ciudad
                String ciudad = columnas[9].trim();

                // Extraer total
                double total =
                        Double.parseDouble(columnas[8].trim());

                // Construir el par clave-valor
                ciudadKey.set(ciudad);
                totalValue.set(total);

                // Emitir:
                //
                // <ciudad, total>
                //
                context.write(
                        ciudadKey,
                        totalValue
                );

            } catch (NumberFormatException e) {

                // Ignorar registros con un total invalido
                System.err.println(
                        "Registro ignorado: " + linea
                );
            }
        }
    }


    /**
     * ============================================================
     * REDUCER
     * ============================================================
     *
     * Entrada:
     *
     *   <ciudad, [total1, total2, total3, ...]>
     *
     * Salida:
     *
     *   <ciudad, suma_total>
     */
    public static class SalesReducer
        extends Reducer<Text, DoubleWritable,
                         Text, Text> {

        private final Text result = new Text();

        @Override
        protected void reduce(
                Text ciudad,
                Iterable<DoubleWritable> valores,
                Context context)
                throws IOException, InterruptedException {

            double suma = 0.0;

            // Sumar todas las ventas de la ciudad
            for (DoubleWritable valor : valores) {
                suma += valor.get();
            }

            // Formatear el resultado con dos decimales
            String resultadoFormateado =
                    String.format("%.2f", suma);

            result.set(resultadoFormateado);

            // Emitir:
            //
            // <ciudad, suma_formateada>
            //
            context.write(
                    ciudad,
                    result
            );
        }
    }


    /**
     * ============================================================
     * DRIVER
     * ============================================================
     *
     * Configura y ejecuta el Job MapReduce.
     *
     * Argumentos:
     *
     *   args[0] = directorio de entrada en HDFS
     *   args[1] = directorio de salida en HDFS
     */
    public static void main(String[] args)
            throws Exception {

        // --------------------------------------------------------
        // Validar argumentos
        // --------------------------------------------------------

        if (args.length != 2) {

            System.err.println(
                    "Uso: SalesByCity <input> <output>"
            );

            System.exit(2);
        }


        // --------------------------------------------------------
        // Crear configuracion
        // --------------------------------------------------------

        Configuration configuration =
                new Configuration();


        // --------------------------------------------------------
        // Crear Job
        // --------------------------------------------------------

        Job job = Job.getInstance(
                configuration,
                "Sales By City"
        );


        // --------------------------------------------------------
        // Indicar la clase principal
        // --------------------------------------------------------

        job.setJarByClass(SalesByCity.class);


        // --------------------------------------------------------
        // Configurar Mapper y Reducer
        // --------------------------------------------------------

        job.setMapperClass(SalesMapper.class);
        job.setReducerClass(SalesReducer.class);


        // --------------------------------------------------------
        // Tipos de salida del Mapper
        // --------------------------------------------------------

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);


        // --------------------------------------------------------
        // Tipos de salida final
        // --------------------------------------------------------

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        // --------------------------------------------------------
        // Entrada
        // --------------------------------------------------------

        FileInputFormat.addInputPath(
                job,
                new Path(args[0])
        );


        // --------------------------------------------------------
        // Salida
        // --------------------------------------------------------

        FileOutputFormat.setOutputPath(
                job,
                new Path(args[1])
        );


        // --------------------------------------------------------
        // Ejecutar Job
        // --------------------------------------------------------

        boolean success =
                job.waitForCompletion(true);


        // Codigo de salida
        System.exit(
                success ? 0 : 1
        );
    }
}