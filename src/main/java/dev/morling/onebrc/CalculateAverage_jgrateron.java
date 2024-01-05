/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package dev.morling.onebrc;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class CalculateAverage_jgrateron {
    private static final String FILE = "./measurements.txt";
    private static int MAX_LINES = 100000;

    public static void main(String[] args) throws IOException, InterruptedException {
        // long startTime = System.nanoTime();

        var tasks = new ArrayList<TaskCalcular>();
        try (var reader = new BufferedReader(new FileReader(FILE))) {
            String line;
            var listaLineas = new LinkedList<String>();
            while ((line = reader.readLine()) != null) {
                listaLineas.add(line);
                if (listaLineas.size() > MAX_LINES) {
                    var taskCalcular = new TaskCalcular(listaLineas);
                    listaLineas = new LinkedList<String>();
                    tasks.add(taskCalcular);
                }
            }
            if (listaLineas.size() > 0) {
                var taskCalcular = new TaskCalcular(listaLineas);
                tasks.add(taskCalcular);
            }
        }
        // combinar todas las particiones
        var totalMediciones = new TreeMap<String, Medicion>();
        for (var task : tasks) {
            task.join();
            var mediciones = task.getMediciones();
            for (var entry : mediciones.entrySet()) {
                var medicion = totalMediciones.get(entry.getKey());
                if (medicion == null) {
                    totalMediciones.put(entry.getKey(), entry.getValue());
                }
                else {
                    var otraMed = entry.getValue();
                    medicion.update(otraMed.count, otraMed.tempMin, otraMed.tempMax, otraMed.tempSum);
                }
            }
        }
        var result = totalMediciones.entrySet().stream()//
                .map(e -> e.getKey() + "=" + e.getValue())//
                .collect(Collectors.joining(", "));

        System.out.println("{" + result + "}");

        // System.out.println("Total: " + (System.nanoTime() - startTime) / 1000000);
    }

    /*
     * 
     */
    static class TaskCalcular {

        private Queue<String> listaLineas;
        private Map<String, Medicion> mediciones;
        private Thread hilo;

        public TaskCalcular(Queue<String> listaLineas) {
            this.listaLineas = listaLineas;
            mediciones = new HashMap<String, Medicion>();
            hilo = Thread.ofPlatform().unstarted(() -> {
                run();
            });
            hilo.start();
        }

        public void join() throws InterruptedException {
            hilo.join();
        }

        public void run() {
            String linea;
            int pos;
            while ((linea = listaLineas.poll()) != null) {
                pos = linea.indexOf(";");
                var estacion = linea.substring(0, pos);
                var temp = Double.parseDouble(linea.substring(pos + 1));
                var medicion = mediciones.get(estacion);
                if (medicion == null) {
                    medicion = new Medicion(estacion, 1, temp, temp, temp);
                    mediciones.put(estacion, medicion);
                }
                else {
                    medicion.update(1, temp, temp, temp);
                }
            }
        }

        public Map<String, Medicion> getMediciones() {
            return mediciones;
        }
    }

    /*
     * 
     */
    static class Medicion implements Comparable<Medicion> {
        private String estacion;
        private int count;
        private double tempMin;
        private double tempMax;
        private double tempSum;

        public Medicion(String estacion, int count, double tempMin, double tempMax, double tempSum) {
            super();
            this.estacion = estacion;
            this.count = count;
            this.tempMin = tempMin;
            this.tempMax = tempMax;
            this.tempSum = tempSum;
        }

        public void update(int count, double tempMin, double tempMax, double tempSum) {
            this.count += count;
            if (tempMin < this.tempMin) {
                this.tempMin = tempMin;
            }
            if (tempMax > this.tempMax) {
                this.tempMax = tempMax;
            }
            this.tempSum += tempSum;
        }

        @Override
        public String toString() {
            double tempPro = tempSum / count;
            return "%.1f/%.1f/%.1f".formatted(tempMin, tempPro, tempMax);
        }

        @Override
        public int compareTo(Medicion medicion) {
            return estacion.compareTo(medicion.estacion);
        }
    }

}
