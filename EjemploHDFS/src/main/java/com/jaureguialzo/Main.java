package com.jaureguialzo;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class Main {

    public static void main(String[] args) {

        // Ubicación dentro del HDFS donde vamos a leer y escribir
        String rutaHDFS = "/egibide";

        String contenidoFichero = "¡Hola mundo!";
        String nombreFichero = "nuevo.txt";

        String rutaLocal = "../ficheros";
        String ficheroLocal = "local.txt";

        try {
            // Opciones de configuración de HDFS
            Configuration conf = new Configuration(true);
            conf.set("fs.defaultFS", "hdfs://namenode:8020/");
            conf.set("dfs.client.use.datanode.hostname", "true");
            System.setProperty("HADOOP_USER_NAME", "root");

            // Conectar al sistema de archivos HDFS
            FileSystem fs = FileSystem.get(conf);

            // Crear el directorio
            if (!fs.exists(new Path(rutaHDFS))) {
                fs.mkdirs(new Path(rutaHDFS));
            }

            // Crear un fichero nuevo
            Path rutaFichero = new Path(rutaHDFS + "/" + nombreFichero);
            if (!fs.exists(rutaFichero)) {
                FSDataOutputStream outputStream = fs.create(rutaFichero);

                // Convertir el texto a UTF-8 para escribir
                byte[] texto = StandardCharsets.UTF_8.encode(contenidoFichero).array();
                outputStream.write(texto);

                outputStream.close();
            }

            // Volver a leer el fichero para comprobar su contenido
            Path rutaFichero2 = new Path(rutaHDFS + "/" + nombreFichero);

            FSDataInputStream inputStream = fs.open(rutaFichero2);
            String contenido = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
            inputStream.close();

            System.out.println(contenido);

            // Acceso a las propiedades del fichero
            FileStatus status = fs.getFileStatus(rutaFichero2);

            // Modificar propiedades
            fs.setOwner(rutaFichero2, "egibide", "users");

            FsPermission permisos = FsPermission.valueOf("-rwxr-xr-x");
            fs.setPermission(rutaFichero2, permisos);

            // Copia de un fichero de local al HDFS
            fs.copyFromLocalFile(false, true, new Path(rutaLocal + "/" + ficheroLocal), new Path(rutaHDFS));

            // Copia de un fichero del HDFS a local
            fs.copyToLocalFile(false, rutaFichero2, new Path(rutaLocal));

            // Borrar el directorio
            //fs.delete(new Path(rutaHDFS), true);

            // Cerrar la conexión al HDFS
            fs.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
