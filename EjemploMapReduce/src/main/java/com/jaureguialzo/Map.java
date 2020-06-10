package com.jaureguialzo;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import static com.jaureguialzo.Constantes.Campos;
import static com.jaureguialzo.Constantes.SEPARADOR;
import static com.jaureguialzo.Contadores.Contador;

public class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> { // KEYIN, VALUE_IN, KEY_OUT, VALUE_OUT  // Salida: Ciudad y media

    private final Log log = LogFactory.getLog(Map.class);

    @Override
    public void map(LongWritable key, Text value, Context context) {

        // Entrada: (ID, LINEA)
        // Salida: (CIUDAD, TEMPERATURA)

        context.getCounter(Contador.NUM_MAPPERS).increment(1);

        try {
            log.debug("Clave: " + key + " Valor: " + value);

            // Al mapper le llega una línea como entrada
            String linea = value.toString();

            // La separamos en campos individuales mpo nombre y la temperatura escribiéndolos en el context, que será la entrada al reduce
            String[] campos = linea.split(SEPARADOR);

            // Si hay dato en la columna TEMPERATURA escribimos una pareja (CIUDAD, TEMPERATURA) en el contexto para el reducer
            if (!campos[Campos.TEMPERATURA.ordinal()].isEmpty()) {
                try {
                    String ciudad = campos[Campos.CIUDAD.ordinal()];
                    double temperatura = Double.parseDouble(campos[Campos.TEMPERATURA.ordinal()]);

                    context.write(new Text(ciudad), new DoubleWritable(temperatura));
                } catch (NumberFormatException e) {
                    log.error("Error: " + e.getMessage());
                }
            }

            context.getCounter(Contador.NUM_LINEAS_MAP).increment(1);

        } catch (IOException | InterruptedException e) {
            log.error("Error: " + e.getMessage());
        }
    }
}
