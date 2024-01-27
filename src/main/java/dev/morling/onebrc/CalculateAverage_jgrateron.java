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
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

public class CalculateAverage_jgrateron {
    private static final String FILE = "./measurements.txt";
    private static final int MAX_LENGTH_LINE = 255;
    private static final int MAX_BUFFER = 1024 * 8;
    private static boolean DEBUG = false;
    public static int DECENAS[] = { 0, 10, 20, 30, 40, 50, 60, 70, 80, 90 };
    public static int CENTENAS[] = { 0, 100, 200, 300, 400, 500, 600, 700, 800, 900 };

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
        var tareas = new ArrayList<Thread>();
        var totalMediciones = new HashMap<Index, Medicion>();
        var particiones = dividirArchivo(archivo);

        for (var p : particiones) {
            var hilo = Thread.ofVirtual().start(() -> {
                try (var miTarea = new MiTarea(archivo, p)) {
                    var mediciones = miTarea.calcularMediciones();
                    for (var entry : mediciones.entrySet()) {
                        Medicion medicion;
                        synchronized (totalMediciones) {
                            medicion = totalMediciones.get(entry.getKey());
                            if (medicion == null) {
                                totalMediciones.put(entry.getKey(), entry.getValue());
                                medicion = entry.getValue();
                            }
                        }
                        synchronized (medicion) {
                            if (!medicion.equals(entry.getValue())) {
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

        Comparator<Map.Entry<Index, Medicion>> comparar = (a, b) -> {
            return a.getValue().getNombreEstacion().compareTo(b.getValue().getNombreEstacion());
        };

        for (var hilo : tareas) {
            hilo.join();
        }

        var result = totalMediciones.entrySet().stream()//
                .sorted(comparar)
                .map(e -> e.getValue().toString())//
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
        private byte[] data;
        private int fromIndex;
        private int length;

        public Index() {
            this.hash = 0;
        }

        public Index(byte data[], int fromIndex, int length) {
            this.data = data;
            this.fromIndex = fromIndex;
            this.length = length;
            this.hash = calcHashCode(length, data, fromIndex, length);
        }

        public void setData(byte data[], int fromIndex, int length) {
            this.data = data;
            this.fromIndex = fromIndex;
            this.length = length;
            this.hash = calcHashCode(length, data, fromIndex, length);
        }

        /*
         * Calcula el hash de cada estacion,
         * variation of Daniel J Bernstein's algorithm
         */
        private int calcHashCode(int result, byte[] a, int fromIndex, int length) {
            int end = fromIndex + length;
            for (int i = fromIndex; i < end; i++) {
                result = ((result << 5) + result) ^ a[i];
            }
            return result;
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
            return Arrays.equals(this.data, this.fromIndex, this.fromIndex + this.length, otro.data, otro.fromIndex,
                    otro.fromIndex + otro.length);
        }
    }

    /*
     * Clase para procesar el archivo a la particion que corresponde
     * RandomAccessFile permite dezplazar el puntero de lectura del archivo
     * Tenemos un Map para guardar las estadisticas y un map para guardar los
     * nombres de las estaciones
     */
    static class MiTarea implements AutoCloseable {
        private final RandomAccessFile rFile;
        private long maxRead;
        private Index index = new Index();
        private Map<Index, Medicion> mediciones = new HashMap<>();

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
        public Map<Index, Medicion> calcularMediciones() throws IOException {
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
                numBytes = totalRead + numBytes > maxRead ? maxRead - totalRead : numBytes;
                totalRead += numBytes;
                int pos = 0;
                int len = 0;
                int idx = 0;
                int semicolon = 0;
                while (pos < numBytes) {
                    var b = buffer[pos];
                    if (b == '\n' || b == '\r') {
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
                        if (b == ';') {
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
            return mediciones;
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
            var temp = strToInt(data, pos, semicolon);
            index.setData(data, pos, semicolon);
            var medicion = mediciones.get(index);
            if (medicion == null) {
                var estacion = new byte[semicolon];
                System.arraycopy(data, pos, estacion, 0, semicolon);
                medicion = new Medicion(estacion, 1, temp, temp, temp);
                mediciones.put(new Index(estacion, 0, semicolon), medicion);
            }
            else {
                medicion.update(1, temp, temp, temp);
            }
        }

        /*
         * convierte de un arreglo de bytes a integer
         */

        public int strToInt(byte linea[], int idx, int posSeparator) {
            int pos = idx + posSeparator + 1;
            boolean esNegativo = linea[pos] == '-';
            pos = esNegativo ? pos + 1 : pos;
            int number = linea[pos + 1] == '.' ? DECENAS[(linea[pos] - 48)] + linea[pos + 2] - 48
                    : CENTENAS[(linea[pos] - 48)] + DECENAS[(linea[pos + 1] - 48)] + (linea[pos + 3] - 48);
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
        private byte estacion[];
        private String nombreEstacion;

        public Medicion(byte estacion[], int count, int tempMin, int tempMax, int tempSum) {
            super();
            this.estacion = estacion;
            this.count = count;
            this.tempMin = tempMin;
            this.tempMax = tempMax;
            this.tempSum = tempSum;
        }

        public void update(int count, int tempMin, int tempMax, int tempSum) {
            this.count += count;
            this.tempMin = Math.min(tempMin, this.tempMin);
            this.tempMax = Math.max(tempMax, this.tempMax);
            this.tempSum += tempSum;
        }

        public double round(double number) {
            return Math.round(number) / 10.0;
        }

        public String getNombreEstacion() {
            if (nombreEstacion == null) {
                nombreEstacion = new String(estacion);
            }
            return nombreEstacion;
        }

        @Override
        public String toString() {
            var min = round(tempMin);
            var mid = round(1.0 * tempSum / count);
            var max = round(tempMax);
            var nombre = getNombreEstacion();
            return "%s=%.1f/%.1f/%.1f".formatted(nombre, min, mid, max);
        }
    }
}
