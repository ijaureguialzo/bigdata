package com.jaureguialzo;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;

import static com.jaureguialzo.Constantes.RESULTADOS;
import static com.jaureguialzo.Contadores.Contador;

public class Reduce extends Reducer<Text, DoubleWritable, Text, Text> {

    private static final Log log = LogFactory.getLog(Reduce.class);

    private MultipleOutputs<Text, Text> mos;

    @Override
    public void setup(Context context) {
        mos = new MultipleOutputs<>(context);
        context.getCounter(Contador.NUM_REDUCERS).increment(1);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
    }

    @Override
    public void reduce(final Text key, final Iterable<DoubleWritable> values, final Context context) {

        // Entrada: (CIUDAD, LISTA_TEMPERATURAS)
        // Salida: (CIUDAD, MEDIA)

        context.getCounter(Contador.NUM_GRUPOS).increment(1);

        try {
            log.debug("Ciudad: " + key.toString()); // Ciudad

            BigDecimal total = BigDecimal.ZERO; // Total de temperatura
            long contador = 0L; // Número de valores

            for (final DoubleWritable temp : values) {

                total = total.add(BigDecimal.valueOf(temp.get()));
                contador++;

                log.debug("Clave: " + key + " Valor: " + temp);
            }

            log.debug("Valor total: " + total);
            log.debug("Contador: " + contador);

            BigDecimal media = total.divide(BigDecimal.valueOf(contador), 4, RoundingMode.HALF_UP);

            log.debug("Media: " + media);

            String salida = key.toString() + " : " + media.toString() + " ºC";

            mos.write(RESULTADOS, null, new Text(salida));

            context.getCounter(Contador.NUM_LINEAS_ESCRITAS).increment(1);

        } catch (IOException | InterruptedException e) {
            log.error("Error: " + e.getMessage());
        }
    }
}
