package com.jaureguialzo;

import org.apache.hadoop.mapred.Counters;

public class Contadores extends Counters {

    public enum Contador {
        NUM_LINEAS_MAP,
        NUM_MAPPERS,
        NUM_REDUCERS,
        NUM_LINEAS_ESCRITAS,
        NUM_GRUPOS
    }
}
