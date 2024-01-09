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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Map.Entry;
import java.util.stream.Collectors;

public class CalculateAverage_jgrateron {
    private static final String FILE = "./measurements.txt";
    private static final int MAX_LENGTH_LINE = 110;

    public record Particion(long offset, long size) {
    }

    /*
     * 
     */
    public static List<Particion> dividirArchivo(File archivo) throws IOException {
        var particiones = new ArrayList<Particion>();
        var buffer = new byte[MAX_LENGTH_LINE];
        var length = archivo.length();
        int cores = Runtime.getRuntime().availableProcessors();
        var sizeParticion = length / (cores * 2);
        var ini = 0l;
        try (var rfile = new RandomAccessFile(archivo, "r")) {
            for (;;) {
                var size = sizeParticion;
                var pos = ini + size;
                if (pos > length) {
                    pos = length - 1;
                    size = length - ini;
                }
                rfile.seek(pos);
                int count = rfile.read(buffer);
                if (count == -1) {
                    break;
                }
                for (int i = 0; i < count; i++) {
                    if (buffer[i] == '\n' || buffer[i] == '\r') {
                        size++;
                        break;
                    }
                    else {
                        size++;
                    }
                }
                var particion = new Particion(ini, size);
                particiones.add(particion);
                if (count != buffer.length) {
                    break;
                }
                ini += size;
            }
        }
        return particiones;
    }

    public static void main(String[] args) throws InterruptedException, IOException {
        // var startTime = System.nanoTime();
        var archivo = new File(FILE);
        var totalMediciones = new HashMap<Integer, Medicion>();
        var tareas = new ArrayList<Thread>();
        var particiones = dividirArchivo(archivo);

        for (var p : particiones) {
            var hilo = Thread.ofVirtual().start(() -> {
                var mediciones = new HashMap<Integer, Medicion>();
                try (var miArchivo = new MiArchivo(archivo, p)) {
                    for (;;) {
                        var lineas = miArchivo.readLines();
                        if (lineas.isEmpty()) {
                            break;
                        }
                        for (;;) {
                            var linea = lineas.poll();
                            if (linea == null) {
                                break;
                            }
                            int pos = linea.indexOf(";");
                            var estacion = linea.substring(0, pos);
                            var temp = Double.parseDouble(linea.substring(pos + 1));
                            var hashCode = estacion.hashCode();
                            var medicion = mediciones.get(hashCode);
                            if (medicion == null) {
                                medicion = new Medicion(estacion, 1, temp, temp, temp);
                                mediciones.put(hashCode, medicion);
                            }
                            else {
                                medicion.update(1, temp, temp, temp);
                            }
                        }
                    }
                }
                catch (IOException e) {
                    System.exit(-1);
                }
                synchronized (totalMediciones) {
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
            });
            tareas.add(hilo);
        }
        for (var hilo : tareas) {
            hilo.join();
        }

        Comparator<Entry<Integer, Medicion>> comparar = (a, b) -> {
            return a.getValue().estacion.compareTo(b.getValue().estacion);
        };

        var result = totalMediciones.entrySet().stream()//
                .sorted(comparar)//
                .map(e -> e.getValue().toString())//
                .collect(Collectors.joining(", "));

        System.out.println("{" + result + "}");
        // System.out.println("Total: " + (System.nanoTime() - startTime) / 1000000 + "ms");
    }

    /*
     * 
     */
    static class MiArchivo implements AutoCloseable {
        private final RandomAccessFile rFile;
        private final byte buffer[] = new byte[1024 * 4];
        private final byte line[] = new byte[MAX_LENGTH_LINE];
        private final byte rest[] = new byte[MAX_LENGTH_LINE];
        private int lenRest = 0;
        private long maxRead = 0;
        private long totalRead = 0;
        private Queue<String> lineas = new LinkedList<String>();

        public MiArchivo(File file, Particion particion) throws IOException {
            maxRead = particion.size;
            rFile = new RandomAccessFile(file, "r");
            rFile.seek(particion.offset);
        }

        @Override
        public void close() throws IOException {
            rFile.close();
        }

        public Queue<String> readLines() throws IOException {
            lineas.clear();
            long numBytes = rFile.read(buffer);
            if (numBytes == -1) {
                return lineas;
            }
            var totalLeidos = totalRead + numBytes;
            if (totalLeidos > maxRead) {
                numBytes = (totalLeidos - maxRead) - numBytes;
            }
            totalRead += numBytes;
            int pos = 0;
            int len = 0;
            int idx = 0;
            while (pos < numBytes) {
                if (buffer[pos] == '\n' || buffer[pos] == '\r') {
                    if (lenRest > 0) {
                        System.arraycopy(rest, 0, line, 0, lenRest);
                        System.arraycopy(buffer, idx, line, lenRest, len);
                        len += lenRest;
                        lenRest = 0;
                    }
                    else {
                        System.arraycopy(buffer, idx, line, 0, len);
                    }
                    lineas.add(new String(line, 0, len));
                    idx = pos + 1;
                    len = 0;
                }
                else {
                    len++;
                }
                pos++;
            }
            if (len > 0) {
                System.arraycopy(buffer, idx, rest, 0, len);
                lenRest = len;
            }
            return lineas;
        }
    }

    /*
     * 
     */
    static class Medicion {
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
            return "%s=%.1f/%.1f/%.1f".formatted(estacion, tempMin, tempPro, tempMax);
        }
    }
}
