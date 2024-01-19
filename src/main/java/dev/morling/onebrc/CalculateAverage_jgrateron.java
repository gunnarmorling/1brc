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
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class CalculateAverage_jgrateron {
    private static final String FILE = "./measurements.txt";
    private static final int MAX_LENGTH_LINE = 255;
    private static final int MAX_BUFFER = 1024 * 8;
    private static boolean DEBUG = false;

    public record Particion(long offset, long size) {
    }

    /*
     * Divide el archivo segun el nro de cores de la PC
     * La division se debe recalcular hasta encontrar un \n o \r (enter o return)
     */
    public static List<Particion> dividirArchivo(File archivo) throws IOException {
        var particiones = new ArrayList<Particion>();
        var buffer = new byte[MAX_LENGTH_LINE];
        var length = archivo.length();
        int cores = Runtime.getRuntime().availableProcessors();
        var sizeParticion = length / cores;
        if (sizeParticion > MAX_BUFFER) {
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
                        size++;
                        if (buffer[i] == '\n' || buffer[i] == '\r') {
                            break;
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
        }
        else {
            particiones.add(new Particion(0, length));
        }
        return particiones;
    }

    /*
     * cambiar el locale para que el separador decimal sea punto y no coma
     * crear un hilo por cada particion
     * totalizar las mediciones por cada hilo
     * ordenar y mostrar
     */
    public static void main(String[] args) throws InterruptedException, IOException {
        Locale.setDefault(Locale.US);
        var startTime = System.nanoTime();
        var archivo = new File(FILE);
        var totalMediciones = new TreeMap<String, Medicion>();
        var tareas = new ArrayList<Thread>();
        var particiones = dividirArchivo(archivo);

        for (var p : particiones) {
            var hilo = Thread.ofVirtual().start(() -> {
                try (var miTarea = new MiTarea(archivo, p)) {
                    var mediciones = miTarea.calcularMediciones();
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
                }
                catch (IOException e) {
                    System.exit(-1);
                }
            });
            tareas.add(hilo);
        }
        for (var hilo : tareas) {
            hilo.join();
        }

        var result = totalMediciones.entrySet().stream()//
                .map(e -> e.getKey() + "=" + e.getValue().toString())//
                .collect(Collectors.joining(", "));

        System.out.println("{" + result + "}");
        if (DEBUG) {
            System.out.println("Total: " + (System.nanoTime() - startTime) / 1000000 + "ms");
        }
    }

    /*
     * Clase Index para reutilizar al realizar un get en el Map
     */
    static class Index {
        private int hash;

        public Index() {
            this.hash = 0;
        }

        public Index(int hash) {
            this.hash = hash;
        }

        public void setHash(int hash) {
            this.hash = hash;
        }

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            var otro = (Index) obj;
            return this.hash == otro.hash;
        }
    }

    /*
     * Clase para procesar el archivo a la particion que corresponde
     * RandomAccessFile permite dezplazar el puntero de lectura del archivo
     * Tenemos un Map para guardar las estadisticas y un map para guardar los
     * nombres de las estaciones
     * 
     */
    static class MiTarea implements AutoCloseable {
        private final RandomAccessFile rFile;
        private long maxRead;
        private Index index = new Index();
        private Map<Index, Medicion> mediciones = new HashMap<>();
        private Map<Index, String> estaciones = new HashMap<>();

        public MiTarea(File file, Particion particion) throws IOException {
            rFile = new RandomAccessFile(file, "r");
            maxRead = particion.size;
            rFile.seek(particion.offset);
        }

        @Override
        public void close() throws IOException {
            rFile.close();
        }

        /*
         * Lee solo su particion
         * Divide el buffer por lineas usando los separadores \n o \r (enter o return)
         * obtiene la posicion de separacion ";" de la estacion y su temperatura
         * calcula el hash, convierte a double y actualiza las estadisticas
         */
        public Map<String, Medicion> calcularMediciones() throws IOException {
            var buffer = new byte[MAX_BUFFER];// buffer para lectura en el archivo
            var rest = new byte[MAX_LENGTH_LINE];// Resto que sobra en cada lectura del buffer
            var lenRest = 0;// Longitud que sobró en cada lectura del buffer
            var totalRead = 0l; // Total bytes leidos

            for (;;) {
                if (totalRead == maxRead) {
                    break;
                }
                long numBytes = rFile.read(buffer);
                if (numBytes == -1) {
                    break;
                }
                var totalLeidos = totalRead + numBytes;
                if (totalLeidos > maxRead) {
                    numBytes = maxRead - totalRead;
                }
                totalRead += numBytes;
                int pos = 0;
                int len = 0;
                int idx = 0;
                int semicolon = 0;
                while (pos < numBytes) {
                    if (buffer[pos] == '\n' || buffer[pos] == '\r') {
                        if (lenRest > 0) {
                            // concatenamos el sobrante anterior con la nueva linea
                            System.arraycopy(buffer, idx, rest, lenRest, len);
                            len += lenRest;
                            semicolon = buscarSemicolon(rest, len);
                            lenRest = 0;
                            updateMediciones(rest, 0, semicolon);
                        }
                        else {
                            updateMediciones(buffer, idx, semicolon);
                        }
                        idx = pos + 1;
                        len = 0;
                        semicolon = 0;
                    }
                    else {
                        if (buffer[pos] == ';') {
                            semicolon = len;
                        }
                        len++;
                    }
                    pos++;
                }
                if (len > 0) {
                    System.arraycopy(buffer, idx, rest, 0, len);
                    lenRest = len;
                }
            }
            return transformMediciones();
        }

        /*
         * Buscamos en reverso ya que el ; esta mas cerca de numero que la estacion
         * ademas el minimo numero 0.0 asi que quitamos tres mas
         */
        public int buscarSemicolon(byte data[], int len) {
            for (int i = len - 4; i >= 0; i--) {
                if (data[i] == ';') {
                    return i;
                }
            }
            return 0;
        }

        /*
         * Busca una medicion por su hash y crea o actualiza la temperatura
         */
        public void updateMediciones(byte data[], int pos, int semicolon) {
            var hashEstacion = calcHashCode(0, data, pos, semicolon);
            var temp = strToInt(data, pos, semicolon);
            index.setHash(hashEstacion);
            var estacion = estaciones.get(index);
            if (estacion == null) {
                estacion = new String(data, pos, semicolon);
                estaciones.put(new Index(hashEstacion), estacion);
            }
            index.setHash(hashEstacion);
            var medicion = mediciones.get(index);
            if (medicion == null) {
                medicion = new Medicion(1, temp, temp, temp);
                mediciones.put(new Index(hashEstacion), medicion);
            }
            else {
                medicion.update(1, temp, temp, temp);
            }
        }

        /*
         * Convierte las estaciones de hash a string
         */
        private Map<String, Medicion> transformMediciones() {
            var newMediciones = new HashMap<String, Medicion>();
            for (var e : mediciones.entrySet()) {
                var estacion = estaciones.get(e.getKey());
                var medicion = e.getValue();
                newMediciones.put(estacion, medicion);
            }
            return newMediciones;
        }

        /*
         * Calcula el hash de cada estacion, esto es una copia de java.internal.hashcode
         */
        private int calcHashCode(int result, byte[] a, int fromIndex, int length) {
            int end = fromIndex + length;
            for (int i = fromIndex; i < end; i++) {
                result = 31 * result + a[i];
            }
            return result;
        }

        /*
         * convierte de un arreglo de bytes a double
         */
        public int strToInt(byte linea[], int idx, int posSeparator) {
            int number = 0;
            int pos = idx + posSeparator + 1;
            boolean esNegativo = linea[pos] == '-';
            if (esNegativo) {
                pos++;
            }
            int digit1 = linea[pos] - 48;
            pos++;
            if (linea[pos] == '.') {
                pos++;
                number = (digit1 * 10) + (linea[pos] - 48);
            }
            else {
                int digit2 = linea[pos] - 48;
                pos += 2;
                number = (digit1 * 100) + (digit2 * 10) + (linea[pos] - 48);
            }
            return esNegativo ? -number : number;
        }
    }

    /*
     * Clase para reservar las estadisticas por estacion
     */
    static class Medicion {
        private int count;
        private int tempMin;
        private int tempMax;
        private int tempSum;

        public Medicion(int count, int tempMin, int tempMax, int tempSum) {
            super();
            this.count = count;
            this.tempMin = tempMin;
            this.tempMax = tempMax;
            this.tempSum = tempSum;
        }

        public void update(int count, int tempMin, int tempMax, int tempSum) {
            this.count += count;
            if (tempMin < this.tempMin) {
                this.tempMin = tempMin;
            }
            if (tempMax > this.tempMax) {
                this.tempMax = tempMax;
            }
            this.tempSum += tempSum;
        }

        public double round(double number) {
            return Math.round(number) / 10.0;
        }

        @Override
        public String toString() {
            var min = round(tempMin);
            var mid = round(1.0 * tempSum / count);
            var max = round(tempMax);
            return "%.1f/%.1f/%.1f".formatted(min, mid, max);
        }
    }
}
