package com.jaureguialzo;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import java.util.Date;

import static com.jaureguialzo.Constantes.RESULTADOS;

public class Driver extends Configured implements Tool {

    private final Log log = LogFactory.getLog(Driver.class);

    @Override
    public int run(String[] strings) throws Exception {

        // Crear y configurar el job
        Job job = Job.getInstance(getConf());

        job.setJarByClass(Driver.class);

        log.info("Comienza el Job : " + new Date());

        final String numeroReducers = job.getConfiguration().get("num-reducers");

        // Rutas de entrada y salida
        FileInputFormat.setInputPaths(job, new Path(strings[0]));
        FileOutputFormat.setOutputPath(job, new Path(strings[1]));

        // Formatos de salida del mapper para el reducer
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        // Formato de fichero para salida del job
        MultipleOutputs.addNamedOutput(job, RESULTADOS, TextOutputFormat.class, Text.class, Text.class);

        // Formatos de salida del reducer; son texto porque van a un fichero
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Definir el mapper y el reducer
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        if (numeroReducers != null && numeroReducers.length() > 0) {
            job.setNumReduceTasks(Integer.parseInt(numeroReducers));
        } else {
            job.setNumReduceTasks(9);
        }

        job.waitForCompletion(true);

        log.info("Fin del Job : " + new Date());

        return 0;
    }
}
