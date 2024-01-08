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

import sun.misc.Unsafe;

import java.io.*;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.groupingBy;

public class CalculateAverage_charlibot {

    private static final String FILE = "./measurements.txt";

    public static void main(String[] args) throws Exception {
        // withBufferedReaderCustomMap();
        // withCustomMap();
        // backAndForthBytesStringsInts();
        // checkCities2();
        // playingMyMap2();
        // checkCities();
        multiThreadedReadingDoItAll();
        // counter();
        // bytesMuckTransmuteCastMess();
        // withFileInputStream();
        // withBufferedReader();
    }

    public static void backAndForthBytesStringsInts() {
        String ab = "Abéché";
        byte[] array = ab.getBytes(StandardCharsets.UTF_8);
        System.out.println(Arrays.toString(array));
        int[] jas = new int[4];
        int i = 0;
        int length = array.length;
        int offset = 0;
        int jasIndex = -1;
        while (i < length) {
            byte b = array[i + offset];
            if (b == -87) {
                System.out.println("Processing -87");
                System.out.println(((b & 0xFF) << (8 * (i & 3))));
                int x = ((b & 0xFF) << (8 * (i & 3)));
                byte y = (byte) (x >>> 24);
                System.out.println(y);
            }
            // i & 3 is the same as i % 4
            if ((i & 3) == 0) { // when at i=0,4,8,12 then
                jasIndex++;
            }
            jas[jasIndex] = jas[jasIndex] | ((b & 0xFF) << (8 * (i & 3)));
            i++;
        }
        jasIndex = 0;
        byte[] buffer = new byte[32];
        while (jasIndex < 4) {
            int tmp = jas[jasIndex];
            buffer[jasIndex * 4] = (byte) tmp;
            buffer[jasIndex * 4 + 1] = (byte) (tmp >>> 8);
            buffer[jasIndex * 4 + 2] = (byte) (tmp >>> 16);
            buffer[jasIndex * 4 + 3] = (byte) (tmp >>> 24);
            jasIndex++;
        }
        length = -1;
        for (i = 0; i < buffer.length; i++) {
            if (buffer[i] == 0) {
                length = i;
                break;
            }
        }
        System.out.println(Arrays.toString(buffer));
        String city = new String(buffer, 0, length, StandardCharsets.UTF_8);
        System.out.println(city);
    }

    // 0->99998658, 1->100000112
    public static void counter() throws Exception {
        long[] startPositions = new long[]{ 0L, 1379535823L, 2759071634L, 4138607447L, 5518143263L, 6897679072L, 8277214878L, 9656750693L, 11036286506L, 12415822319L,
                13795358125L };
        FileInputStream fiss = new FileInputStream(FILE);
        int start = 1;
        fiss.skip(startPositions[start]);
        long bytesToRead = startPositions[start + 1] - startPositions[start];
        int totalBytesReadd = 0;
        int numBytesRead;
        byte[] bufferr = new byte[1024 * 8 * 64];
        int lineCount = 0;
        while ((numBytesRead = fiss.read(bufferr)) != -1 && totalBytesReadd < bytesToRead) {
            int i = 0;
            while (i < numBytesRead && totalBytesReadd < bytesToRead) {
                if (bufferr[i] == '\n') {
                    lineCount++;
                }
                i++;
                totalBytesReadd++;
            }
        }
        fiss.close();
        System.out.println(lineCount); // expecting 99999837. LOL - got 100000112. There's the problem!

        lineCount = 0;
        try (FileInputStream fis = new FileInputStream(FILE)) {
            long actualSkipped = fis.skip(startPositions[start]);
            if (actualSkipped != startPositions[start]) {
                throw new Exception("Uho oh");
            }
            // byte[] buffer = new byte[1024 * 8 * 64];
            byte[] buffer = new byte[1024 * 1024];
            long totalBytesRead = 0;
            int bytesRead;
            int currentCityLength = 0;
            int numberOfOuterReads = 0;
            int numberOfInnerReads = 0;
            String lastCity = null;
            int lastValue = -1;
            outerloop: while ((bytesRead = fis.read(buffer, currentCityLength, buffer.length - currentCityLength)) != -1) {
                numberOfOuterReads++;
                totalBytesRead -= currentCityLength;
                if (totalBytesRead >= bytesToRead && currentCityLength == 0) {
                    // we have read everything we intend to and there is no city in the buffer to finish processing
                    System.out.printf("Read everything intend to. Process=%d, totalBytesRead=%d, bytesToRead=%d\n", start, totalBytesRead, bytesToRead);
                    break;
                }
                int i = 0;
                int cityIndexStart = 0;
                int cityLength;
                int multiplier = 1;
                int value = 0;
                while (i < bytesRead + currentCityLength) {
                    if (totalBytesRead >= bytesToRead) {
                        // we have read everything we intend to for this chunk
                        System.out.printf("OOORead everything intend to inner loop. Process=%d, totalBytesRead=%d, bytesToRead=%d\n", start, totalBytesRead, bytesToRead);
                        break outerloop;
                    }
                    if (buffer[i] == ';') {
                        // lineCount += 1;
                        cityLength = i - cityIndexStart;
                        i++;
                        totalBytesRead++;
                        if (i == bytesRead + currentCityLength) {
                            System.arraycopy(buffer, cityIndexStart, buffer, 0, cityLength);
                            bytesRead = fis.read(buffer, cityLength, buffer.length - cityLength);
                            currentCityLength = cityLength;
                            cityIndexStart = 0;
                            i = cityLength;
                            numberOfInnerReads++;
                        }
                        if (buffer[i] == '-') {
                            multiplier = -1;
                            i++;
                            totalBytesRead++;
                            if (i == bytesRead + currentCityLength) {
                                System.arraycopy(buffer, cityIndexStart, buffer, 0, cityLength);
                                bytesRead = fis.read(buffer, cityLength, buffer.length - cityLength);
                                currentCityLength = cityLength;
                                cityIndexStart = 0;
                                i = cityLength;
                                numberOfInnerReads++;
                            }
                        }
                        while (buffer[i] != '\n') {
                            if (buffer[i] != '.') {
                                value = (value * 10) + (buffer[i] - '0');
                            }
                            i++;
                            totalBytesRead++;
                            if (i == bytesRead + currentCityLength) {
                                System.arraycopy(buffer, cityIndexStart, buffer, 0, cityLength);
                                bytesRead = fis.read(buffer, cityLength, buffer.length - cityLength);
                                currentCityLength = cityLength;
                                cityIndexStart = 0;
                                i = cityLength;
                                numberOfInnerReads++;
                            }
                        }
                        lineCount += 1;
                        value = value * multiplier; // is boolean check faster?
                        lastCity = new String(buffer, cityIndexStart, cityLength, StandardCharsets.UTF_8);
                        lastValue = value;
                        // buffer[i] == \n
                        if (totalBytesRead >= bytesToRead) {
                            // we have read everything we intend to for this chunk
                            System.out.printf("Read everything intend to inner loop. Process=%d, totalBytesRead=%d, bytesToRead=%d\n", start, totalBytesRead,
                                    bytesToRead);
                            System.out.printf("More info for %d. cityIndexStart=%d, cityLength=%d, value=%d, lineCount=%d, city=%s\n", start, cityIndexStart, cityLength,
                                    value, lineCount, new String(buffer, cityIndexStart, cityLength, StandardCharsets.UTF_8));
                            break outerloop;
                        }
                        cityIndexStart = i + 1;
                        value = 0;
                        multiplier = 1;
                    }
                    i++;
                    totalBytesRead++;
                }
                // System.out.printf("%d %d %d %d %d\n", i, bytesRead, currentCityLength, bytesRead + currentCityLength, buffer.length);
                currentCityLength = buffer.length - cityIndexStart;
                System.arraycopy(buffer, cityIndexStart, buffer, 0, currentCityLength);
            }
            System.out.printf("Last city=%s, value=%d\n", lastCity, lastValue);
            System.out.println(numberOfOuterReads);
            System.out.println(numberOfOuterReads);
            System.out.println(numberOfInnerReads);
        }
        System.out.println("Total read: " + totalBytesReadd);
        System.out.println("From this thing: " + lineCount);
    }

    public static void playingMyMap() {
        MyMap map = new MyMap();
        String line = "crap\nLondon;32.1\nParis;24.5\n";
        byte[] array = line.getBytes(StandardCharsets.UTF_8);
        map.insert(array, 5, 6, 321);
        map.insert(array, 17, 5, 245);
        String anotherLine = "\nParis;21.3\nLondon;-3.0\n";
        byte[] anotherArray = anotherLine.getBytes(StandardCharsets.UTF_8);
        map.insert(anotherArray, 1, 5, 213);
        map.insert(anotherArray, 12, 6, -30);
        System.out.println(Arrays.toString(map.map[1417]));
        System.out.println(Arrays.toString(map.map[3066]));
        System.out.print("{");
        System.out.print(
                map.entrySet().stream().sorted(Map.Entry.comparingByKey()).map(Object::toString).collect(Collectors.joining(", ")));
        System.out.println("}");
    }

    public static void playingMyMap2() {
        MyMap2 map = new MyMap2();
        String line = "crap\nLondon;32.1\nParis;24.5\nCity of San Marino;11.1\n";
        byte[] array = line.getBytes(StandardCharsets.UTF_8);
        // map.insert(array, 5, 6, 321);
        // map.insert(array, 17, 5, 245);
        map.insert(array, 28, 18, 111);
        System.out.println(new String(array, 28, 18, StandardCharsets.UTF_8));
        String anotherLine = "\nParis;21.3\nLondon;-3.0\nCity of San Marino;7.5\n";
        byte[] anotherArray = anotherLine.getBytes(StandardCharsets.UTF_8);
        System.out.println(Arrays.toString(new String(array, 28, 18, StandardCharsets.UTF_8).getBytes(StandardCharsets.UTF_8)));
        System.out.println(Arrays.toString(array));
        System.out.println(Arrays.toString(anotherArray));

        // map.insert(anotherArray, 1, 5, 213);
        // map.insert(anotherArray, 12, 6, -30);
        map.insert(anotherArray, 24, 18, 75);
        System.out.println(new String(anotherArray, 24, 18, StandardCharsets.UTF_8));
        // System.out.println(Arrays.deepToString(map.map));
        // System.out.println(Arrays.toString(map.map[1417]));
        // System.out.println(Arrays.toString(map.map[3066]));
        System.out.print("{");
        System.out.print(
                map.entrySet().stream().sorted(Map.Entry.comparingByKey()).map(Object::toString).collect(Collectors.joining(", ")));
        System.out.println("}");
        System.out.println(map.toMap().size());
    }

    static class MyMap {

        // MessageDigest md;
        // byte[] outDigestBuf;

        final int[][] map;
        final int capacity = 4096;

        MyMap() {
            // md = MessageDigest.getInstance("SHA-256");
            // outDigestBuf = new byte[32];
            map = new int[capacity][1 + 30 + 4]; // length of string and then the city encoded cast bytes to int. then min, max, sum, count,
        }

        public void insert(byte[] array, int offset, int length, int value) {
            // md.update(array, offset, length);
            // md.digest(outDigestBuf, 0, 32);
            int hashcode = 17;
            for (int i = offset; i < offset + length; i++) {
                // System.out.printf("i=%d, array[i]=%s, value=%d\n", i, (char) array[i], value);
                hashcode = 31 * hashcode + array[i];
            }
            int index = Math.floorMod(hashcode, capacity);
            tryInsert(index, array, offset, length, value);
        }

        private void tryInsert(int mapIndex, byte[] array, int offset, int length, int value) {
            // System.out.printf("Trying index %d for slice value %d\n", mapIndex, value);
            int[] jas = map[mapIndex];
            if (jas[0] == 0) {
                // easy case since no entry - just insert
                jas[0] = length;
                int i = offset;
                int j = 1;
                while (j < length + 1) {
                    // TODO: try putting 4 bytes into an int instead of blind copying
                    jas[j] = array[i];
                    i++;
                    j++;
                }
                jas[length + 1] = value;
                jas[length + 2] = value;
                jas[length + 3] = value;
                jas[length + 4] = 1;
            }
            else {
                if (jas[0] == length) {
                    // loop for length and see if equal then update
                    int i = offset;
                    int j = 1;
                    while (j < length + 1) {
                        if (array[i] != jas[j]) {
                            tryInsert((mapIndex + 1) % capacity, array, offset, length, value);
                            return;
                        }
                        i++;
                        j++;
                    }
                    jas[length + 1] = Math.min(value, jas[length + 1]);
                    jas[length + 2] = Math.max(value, jas[length + 2]);
                    jas[length + 3] += value;
                    jas[length + 4] += 1;
                }
                else {
                    // need to go to next entry
                    tryInsert((mapIndex + 1) % capacity, array, offset, length, value);
                }
            }
        }

        public HashMap<String, Measurement> toMap() {
            HashMap<String, Measurement> hashMap = new HashMap<>();
            for (int[] jas : map) {
                if (jas[0] != 0) {
                    int length = jas[0];
                    byte[] array = new byte[length];
                    int j = 0;
                    while (j < length) {
                        array[j] = (byte) jas[j + 1];
                        j++;
                    }
                    String city = new String(array, StandardCharsets.UTF_8);
                    Measurement m = new Measurement(0);
                    m.min = jas[length + 1];
                    m.max = jas[length + 2];
                    m.sum = jas[length + 3];
                    m.count = jas[length + 4];
                    hashMap.put(city, m);
                }
            }
            return hashMap;
        }

        public Set<Map.Entry<String, Measurement>> entrySet() {
            return toMap().entrySet();
        }
    }

    // Copied from Roy van Rijn's code
    // branchless max (unprecise for large numbers, but good enough)
    static int max(final int a, final int b) {
        final int diff = a - b;
        final int dsgn = diff >> 31;
        return a - (diff & dsgn);
    }

    // branchless min (unprecise for large numbers, but good enough)
    static int min(final int a, final int b) {
        final int diff = a - b;
        final int dsgn = diff >> 31;
        return b + (diff & dsgn);
    }

    static class MyMap2 {

        final int[][] map;
        final int capacity = 16_384; // 2^14. Might need 2^15 = 32768

        final int numIntsToStoreCity = 25; // stores up to 100 characters.
        int minPos = numIntsToStoreCity;
        int maxPos = numIntsToStoreCity + 1;
        int sumPos = numIntsToStoreCity + 2;
        int countPos = numIntsToStoreCity + 3;

        MyMap2() {
            map = new int[capacity][numIntsToStoreCity + 4]; // length of string and then the city encoded cast bytes to int. then min, max, sum, count,
        }

        public void insert(byte[] array, int offset, int length, int value) {
            int hashcode = hashArraySlice(array, offset, length);
            int index = hashcode & (capacity - 1); // same trick as in hashmap. This is the same as (% capacity).
            tryInsert(index, array, offset, length, value);
        }

        private void tryInsert(int mapIndex, byte[] array, int offset, int length, int value) {
            // jas[0] is 4 bytes of information.
            // int count = 0;
            outer: while (true) {
                int[] jas = map[mapIndex];
                if (jas[0] == 0) {
                    // easy case since no entry - just insert
                    // int curr = 0; // TODO: I don't think we need the length. Just use 0 terminated.
                    int i = 0;
                    int jasIndex = -1;
                    while (i < length) {
                        byte b = array[i + offset];
                        // i & 3 is the same as i % 4
                        if ((i & 3) == 0) { // when at i=0,4,8,12 then
                            jasIndex++;
                        }
                        jas[jasIndex] = jas[jasIndex] | ((b & 0xFF) << (8 * (i & 3)));
                        i++;
                    }
                    jas[minPos] = value;
                    jas[maxPos] = value;
                    jas[sumPos] = value;
                    jas[countPos] = 1;
                    break;
                }
                else {
                    int i = 0;
                    int jasIndex = -1;
                    while (i < length) {
                        byte b = array[i + offset];
                        if ((i & 3) == 0) { // when at i=0,4,8,12 then
                            jasIndex++;
                        }
                        byte inJas = (byte) (jas[jasIndex] >>> (8 * (i & 3)));
                        if (b != inJas) {
                            mapIndex = (mapIndex + 1) & (capacity - 1);
                            continue outer;
                        }
                        i++;
                    }
                    jas[minPos] = min(value, jas[minPos]);
                    jas[maxPos] = max(value, jas[maxPos]);
                    jas[sumPos] += value;
                    jas[countPos] += 1; // Zig would be handy here. Compile time constants of length-1, length-2, etc
                    break;
                }
            }
        }

        public HashMap<String, Measurement> toMap() {
            HashMap<String, Measurement> hashMap = new HashMap<>();
            for (int[] jas : map) {
                if (jas[0] != 0) {
                    int jasIndex = 0;
                    byte[] array = new byte[numIntsToStoreCity * 4];
                    while (jasIndex < numIntsToStoreCity) {
                        int tmp = jas[jasIndex];
                        array[jasIndex * 4] = (byte) tmp;
                        array[jasIndex * 4 + 1] = (byte) (tmp >>> 8);
                        array[jasIndex * 4 + 2] = (byte) (tmp >>> 16);
                        array[jasIndex * 4 + 3] = (byte) (tmp >>> 24);
                        jasIndex++;
                    }
                    int length = -1;
                    for (int i = 0; i < array.length; i++) {
                        if (array[i] == 0) {
                            length = i;
                            break;
                        }
                    }
                    String city = new String(array, 0, length, StandardCharsets.UTF_8);
                    Measurement m = new Measurement(0);
                    m.min = jas[minPos];
                    m.max = jas[maxPos];
                    m.sum = jas[sumPos];
                    m.count = jas[countPos];
                    hashMap.put(city, m);
                }
            }
            return hashMap;
        }

        public Set<Map.Entry<String, Measurement>> entrySet() {
            return toMap().entrySet();
        }
    }

    public static void bytesMuckTransmuteCastMess() {
        // Proves that we can initialise a normal int array and pretend like it's a byte array. Or vice versa.
        Unsafe unsafe;
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            unsafe = (Unsafe) f.get(null);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }

        int[] array = new int[3];
        array[1] = 69;
        int b = Unsafe.ARRAY_INT_BASE_OFFSET;
        int s = Unsafe.ARRAY_INT_INDEX_SCALE;
        System.out.println(b);
        System.out.println(s);
        int a = unsafe.getInt(array, b + s);
        System.out.println(a);

        System.out.printf("%d %d %d %d\n", unsafe.getByte(array, 20), unsafe.getByte(array, 21), unsafe.getByte(array, 22), unsafe.getByte(array, 23));

        System.out.println(Unsafe.ARRAY_BYTE_BASE_OFFSET);
        System.out.println(Unsafe.ARRAY_BYTE_INDEX_SCALE);
        byte[] byteArray = new byte[3 * 4];
        byteArray[4] = 69;
        byteArray[5] = 0;
        byteArray[6] = 0;
        byteArray[7] = 0;
        System.out.println(unsafe.getInt(byteArray, b + s));
    }

    public static void checkCities() throws IOException {
        Path path = Paths.get("./cities.out");
        var lines = Files.newBufferedReader(path);
        String line;
        Map<Integer, Integer> hashes = new HashMap<>();
        Map<String, Integer> yas = new HashMap<>();
        while ((line = lines.readLine()) != null) {
            byte[] bytes = line.getBytes(StandardCharsets.UTF_8);
            // int hashMod = Math.floorMod(Arrays.hashCode(bytes), 4096);
            int hashMod = hashArraySlice(bytes, 0, bytes.length) & 4095;
            hashes.merge(hashMod, 1, Integer::sum);
            yas.put(line, hashMod);
        }
        // 141=2, 3262=2 <- may have to skip extra, 289=2, 3378=2, 1421=2, 3511=2, 510=2 <- one at 511, 2613=2, 1611=2, 600=2, 3932=2, 1925=2, 916=2, 1946=2, 1965=2 <- one at 1966, 3069=3 <- one at 3071.

        // 1079=2, 95=2, 121=2, 1160=2, 3441=2, 3459=2, 432=2, 506=2, 3599=2, 1751=2, 800=2, 1918=2, 3142=3
        System.out.println(hashes.entrySet().stream().sorted(Comparator.comparing(Map.Entry::getValue)).map(Object::toString).collect(Collectors.joining(", ")));
        // System.out.println(yas);
        String x = yas.entrySet().stream().sorted(Comparator.comparingInt(Map.Entry::getValue)).map(Object::toString).collect(Collectors.joining(", "));
        System.out.println(x);
    }

    public static int hashArraySlice(byte[] array, int offset, int length) {
        int hashcode = 0;
        for (int i = offset; i < offset + length; i++) {
            // System.out.printf("i=%d, array[i]=%s, value=%d\n", i, (char) array[i], value);
            hashcode = 31 * hashcode + array[i];
        }
        // The below makes it so much worse
        // hashcode = hashcode >>> 16; // Do the same trick as-in hashmap since we're using power of 2
        return hashcode;
    }

    public static void checkCities2() throws IOException {
        Path path = Paths.get("./cities.out");
        var lines = Files.newBufferedReader(path);
        String line;
        MyMap2 myMap2 = new MyMap2();
        while ((line = lines.readLine()) != null) {
            byte[] bytes = line.getBytes(StandardCharsets.UTF_8);
            myMap2.insert(bytes, 0, line.length(), 10);
        }
        System.out.print("{");
        System.out.print(
                myMap2.entrySet().stream().sorted(Map.Entry.comparingByKey()).map(Object::toString).collect(Collectors.joining(", ")));
        System.out.println("}");
        System.out.println(myMap2.toMap().size());
        // System.out.println(yas);
    }

    public static void withBufferedReader() throws IOException {
        Path path = Paths.get(FILE);
        var lines = Files.newBufferedReader(path);
        String line = null;
        HashMap<String, Measurement> measurements = new HashMap<>(413 * 4, 1.0f);
        while ((line = lines.readLine()) != null) {
            String[] split = line.split(";");
            String city = split[0];
            int i = 0;
            int value = 0;
            int multiplier = 1;
            while (i < split[1].length()) {
                char c = split[1].charAt(i);
                if (c == '-') {
                    multiplier = -1;
                }
                else if (c == '.') {

                }
                else {
                    value = (value * 10) + (c - '0');
                }
                i++;
            }
            value = value * multiplier;
            Measurement m = measurements.get(city);
            if (m == null) {
                measurements.put(city, new Measurement(value));
            }
            else {
                m.count += 1;
                m.sum += value;
                if (value > m.max) {
                    m.max = value;
                }
                if (value < m.min) {
                    m.min = value;
                }
            }
        }
        System.out.print("{");
        System.out.print(
                measurements.entrySet().stream().sorted(Map.Entry.comparingByKey()).map(Object::toString).collect(Collectors.joining(", ")));
        System.out.println("}");
    }

    public static void withBufferedReaderCustomMap() throws IOException {
        Path path = Paths.get(FILE);
        var lines = Files.newBufferedReader(path);
        String line = null;
        MyMap measurements = new MyMap();
        while ((line = lines.readLine()) != null) {
            String[] split = line.split(";");
            String city = split[0];
            int i = 0;
            int value = 0;
            int multiplier = 1;
            while (i < split[1].length()) {
                char c = split[1].charAt(i);
                if (c == '-') {
                    multiplier = -1;
                }
                else if (c == '.') {

                }
                else {
                    value = (value * 10) + (c - '0');
                }
                i++;
            }
            value = value * multiplier;
            byte[] bytes = city.getBytes(StandardCharsets.UTF_8);
            measurements.insert(bytes, 0, bytes.length, value);
        }
        System.out.print("{");
        System.out.print(
                measurements.entrySet().stream().sorted(Map.Entry.comparingByKey()).map(Object::toString).collect(Collectors.joining(", ")));
        System.out.println("}");
    }

    static class Measurement {
        int min;
        int max;
        int sum;
        int count;

        Measurement(int value) {
            min = value;
            max = value;
            sum = value;
            count = 1;
        }

        @Override
        public String toString() {
            double minD = (double) min / 10;
            double maxD = (double) max / 10;
            double meanD = (double) sum / 10 / count;
            return round(minD) + "/" + round(meanD) + "/" + round(maxD);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }

    public static void withFileInputStream() throws IOException {
        try (FileInputStream fis = new FileInputStream(FILE)) {
            byte[] buffer = new byte[1024 * 8 * 64];
            int bytesRead = -1;
            // this could be an array of 4 integers per string instead of measurement.

            HashMap<String, Measurement> measurements = new HashMap<>(413 * 4, 1.0f);
            int currentCityLength = 0;
            int lineCount = 0;
            while ((bytesRead = fis.read(buffer, currentCityLength, buffer.length - currentCityLength)) != -1) {
                int i = 0;
                int cityIndexStart = 0;
                int cityLength;
                int multiplier = 1;
                int value = 0;
                while (i < bytesRead + currentCityLength) {
                    if (buffer[i] == ';') {
                        // TODO: Avoid making this String. I'd rather a Map<int, measurement> and a separate Map<int, String> map
                        String city = new String(buffer, cityIndexStart, i - cityIndexStart);
                        // cityLength = i - cityIndexStart;
                        i++;
                        if (i == bytesRead + currentCityLength) {
                            // System.arraycopy(buffer, cityIndexStart, buffer, 0, cityLength);
                            // bytesRead = fis.read(buffer, cityLength, buffer.length - cityLength);
                            // cityIndexStart = 0;
                            // i = cityLength;
                            // System.out.println("Reading in after city");
                            bytesRead = fis.read(buffer);
                            currentCityLength = 0;
                            i = 0;
                        }
                        if (buffer[i] == '-') {
                            multiplier = -1;
                            i++;
                            if (i == bytesRead + currentCityLength) {
                                // System.out.println("Reading in after - check");
                                bytesRead = fis.read(buffer);
                                i = 0;
                                currentCityLength = 0;
                            }
                        }
                        while (buffer[i] != '\n') {
                            if (buffer[i] != '.') {
                                value = (value * 10) + (buffer[i] - '0');
                            }
                            // if (buffer[i] - '0' > 9) {
                            // System.out.printf("City: %s, i: %d, buffer[i]: %s\n", city, i, (char) buffer[i]);
                            // }
                            i++;
                            if (i == bytesRead + currentCityLength) {
                                // int length = i - cityIndexStart;
                                // System.arraycopy(buffer, cityIndexStart, buffer, 0, length);
                                // bytesRead = fis.read(buffer, length, buffer.length - length);
                                // cityIndexStart = 0;
                                // i = length + 1;
                                // System.out.printf("Reading in during integer parsing. %s %s\n", lineCount, value);
                                // System.out.printf("City: %s, i: %d, buffer[i]: %s\n", city, i, (char) buffer[i]);
                                bytesRead = fis.read(buffer);
                                i = 0;
                                currentCityLength = 0;
                            }
                        }
                        lineCount += 1;
                        value = value * multiplier; // is boolean check faster?
                        // if (value > 10000) {
                        // System.out.printf("City: %s, value: %d, line count: %d\n", city, value, lineCount);
                        // System.out.println("_------------------");
                        // System.out.println(new String(buffer, cityIndexStart, 19));
                        // System.out.println("******************");
                        // System.out.println(new String(buffer, 0, 20));
                        // System.out.println("_------------------");
                        // }

                        Measurement m = measurements.get(city);
                        if (m == null) {
                            measurements.put(city, new Measurement(value));
                        }
                        else {
                            m.count += 1;
                            m.sum += value;
                            if (value > m.max) {
                                m.max = value;
                            }
                            if (value < m.min) {
                                m.min = value;
                            }
                        }
                        // buffer[i] == \n
                        cityIndexStart = i + 1;
                        value = 0;
                        multiplier = 1;
                    }
                    i++;
                }
                // TODO: Need to handle the carry over case here.
                // value is fine. but we need to copy the city over.
                // say read(buf) got ...Lond
                // and the next buf would be on;12.3
                // then we need to transfer Lond.
                // This is the only case.
                // We wouldn't get halfway through the value because that's in the if where we already read if we are missing
                // We only want to carry over in the case where we don
                currentCityLength = buffer.length - cityIndexStart;
                System.arraycopy(buffer, cityIndexStart, buffer, 0, currentCityLength);
                // System.out.println("|||||||||||||||");
                // System.out.println(new String(buffer, 0, currentCityLength));
                // System.out.println(lineCount);
                // System.out.println("|||||||||||||||");
            }
            System.out.println(measurements);
            System.out.print("{");
            System.out.print(
                    measurements.entrySet().stream().sorted(Map.Entry.comparingByKey()).map(Object::toString).collect(Collectors.joining(", ")));
            System.out.println("}");
            System.out.println(measurements.size());
            System.out.println(lineCount);
        }
    }

    public static void withCustomMap() throws IOException {
        try (FileInputStream fis = new FileInputStream(FILE)) {
            byte[] buffer = new byte[1024 * 8 * 64];
            int bytesRead = -1;
            // this could be an array of 4 integers per string instead of measurement.

            MyMap measurements = new MyMap();
            int currentCityLength = 0;
            int lineCount = 0;
            while ((bytesRead = fis.read(buffer, currentCityLength, buffer.length - currentCityLength)) != -1) {
                int i = 0;
                int cityIndexStart = 0;
                int cityLength;
                int multiplier = 1;
                int value = 0;
                while (i < bytesRead + currentCityLength) {
                    if (buffer[i] == ';') {
                        cityLength = i - cityIndexStart;
                        i++;
                        if (i == bytesRead + currentCityLength) {
                            System.arraycopy(buffer, cityIndexStart, buffer, 0, cityLength);
                            bytesRead = fis.read(buffer, cityLength, buffer.length - cityLength);
                            currentCityLength = cityLength;
                            cityIndexStart = 0;
                            i = cityLength;
                        }
                        if (buffer[i] == '-') {
                            multiplier = -1;
                            i++;
                            if (i == bytesRead + currentCityLength) {
                                System.arraycopy(buffer, cityIndexStart, buffer, 0, cityLength);
                                bytesRead = fis.read(buffer, cityLength, buffer.length - cityLength);
                                currentCityLength = cityLength;
                                cityIndexStart = 0;
                                i = cityLength;
                            }
                        }
                        while (buffer[i] != '\n') {
                            if (buffer[i] != '.') {
                                value = (value * 10) + (buffer[i] - '0');
                            }
                            i++;
                            if (i == bytesRead + currentCityLength) {
                                System.arraycopy(buffer, cityIndexStart, buffer, 0, cityLength);
                                bytesRead = fis.read(buffer, cityLength, buffer.length - cityLength);
                                currentCityLength = cityLength;
                                cityIndexStart = 0;
                                i = cityLength;
                            }
                        }
                        lineCount += 1;
                        value = value * multiplier; // is boolean check faster?
                        measurements.insert(buffer, cityIndexStart, cityLength, value);
                        // buffer[i] == \n
                        cityIndexStart = i + 1;
                        value = 0;
                        multiplier = 1;
                    }
                    i++;
                }
                currentCityLength = buffer.length - cityIndexStart;
                System.arraycopy(buffer, cityIndexStart, buffer, 0, currentCityLength);
            }
            System.out.println(measurements);
            System.out.print("{");
            System.out.print(
                    measurements.entrySet().stream().sorted(Map.Entry.comparingByKey()).map(Object::toString).collect(Collectors.joining(", ")));
            System.out.println("}");
            System.out.println(measurements.entrySet().size());
            System.out.println(lineCount);
        }
    }

    public static void multiThreadedReading() throws IOException, ExecutionException, InterruptedException {
        File file = new File(FILE);
        long length = file.length();
        int numProcessors = Runtime.getRuntime().availableProcessors();
        long chunkToRead = length / numProcessors;

        try (ExecutorService executorService = Executors.newWorkStealingPool(numProcessors)) {
            Future[] results = new Future[numProcessors];
            for (int processIdx = 0; processIdx < numProcessors; processIdx++) {
                long seekPoint = processIdx * chunkToRead;
                long bytesToRead = processIdx == numProcessors - 1 ? (length - seekPoint) : chunkToRead;
                Future<Integer> lineCount = executorService.submit(() -> {
                    int counter = 0;
                    try (FileInputStream fis = new FileInputStream(FILE)) {
                        long actualSkipped = fis.skip(seekPoint);
                        assert (actualSkipped != seekPoint);
                        byte[] buffer = new byte[1024 * 8 * 64];
                        long totalBytesRead = 0;
                        int bytesRead;
                        while ((bytesRead = fis.read(buffer)) != -1 && totalBytesRead < bytesToRead) {
                            int i = 0;
                            while (i < bytesRead && totalBytesRead < bytesToRead) {
                                if (buffer[i] == '\n') {
                                    counter++;
                                }
                                i++;
                                totalBytesRead++;
                            }
                        }
                    }
                    return counter;
                });
                results[processIdx] = lineCount;
            }
            int totalLineCount = 0;
            for (Future f : results) {
                totalLineCount += (Integer) f.get();
            }
            System.out.println(totalLineCount);
        }
    }

    public static void multiThreadedReadingDoItAllOLD() throws Exception {
        File file = new File(FILE);
        long length = file.length();
        int numProcessors = Runtime.getRuntime().availableProcessors();
        long chunkToRead = length / numProcessors;

        try (ExecutorService executorService = Executors.newWorkStealingPool(numProcessors)) {
            Future[] results = new Future[numProcessors];
            for (int processIdx = 0; processIdx < numProcessors; processIdx++) {
                long seekPoint = processIdx * chunkToRead;
                long bytesToRead = processIdx == numProcessors - 1 ? (length - seekPoint) : chunkToRead;
                Future<Integer> future = executorService.submit(() -> {
                    MyMap measurements = new MyMap();
                    int lineCount = 0;
                    try (FileInputStream fis = new FileInputStream(FILE)) {
                        long actualSkipped = fis.skip(seekPoint);
                        assert (actualSkipped != seekPoint);
                        byte[] buffer = new byte[1024 * 8 * 64];
                        long totalBytesRead = 0;
                        int bytesRead;
                        int currentCityLength = 0;
                        while ((bytesRead = fis.read(buffer, currentCityLength, buffer.length - currentCityLength)) != -1 && totalBytesRead < bytesToRead) {
                            int i = 0;
                            int cityIndexStart = 0;
                            int cityLength;
                            int multiplier = 1;
                            int value = 0;
                            while (i < bytesRead && totalBytesRead < bytesToRead) {
                                if (buffer[i] == ';') {
                                    cityLength = i - cityIndexStart;
                                    i++;
                                    totalBytesRead++;
                                    if (totalBytesRead == bytesToRead) {

                                        // -------|----|----|
                                        // TODO: Handle the excess by emitting it and post processing.
                                        System.out.printf("Unprocessed: %s\n", new String(buffer, cityIndexStart, i - cityIndexStart, StandardCharsets.UTF_8));
                                        return lineCount;
                                    }
                                    if (i == bytesRead + currentCityLength) {
                                        System.arraycopy(buffer, cityIndexStart, buffer, 0, cityLength);
                                        bytesRead = fis.read(buffer, cityLength, buffer.length - cityLength);
                                        currentCityLength = cityLength;
                                        cityIndexStart = 0;
                                        i = cityLength;
                                    }
                                    if (buffer[i] == '-') {
                                        multiplier = -1;
                                        i++;
                                        totalBytesRead++;
                                        if (totalBytesRead == bytesToRead) {
                                            return lineCount;
                                        }
                                        if (i == bytesRead + currentCityLength) {
                                            System.arraycopy(buffer, cityIndexStart, buffer, 0, cityLength);
                                            bytesRead = fis.read(buffer, cityLength, buffer.length - cityLength);
                                            currentCityLength = cityLength;
                                            cityIndexStart = 0;
                                            i = cityLength;
                                        }
                                    }
                                    while (buffer[i] != '\n') {
                                        if (buffer[i] != '.') {
                                            value = (value * 10) + (buffer[i] - '0');
                                        }
                                        i++;
                                        totalBytesRead++;
                                        if (totalBytesRead == bytesToRead) {
                                            return lineCount;
                                        }
                                        if (i == bytesRead + currentCityLength) {
                                            System.arraycopy(buffer, cityIndexStart, buffer, 0, cityLength);
                                            bytesRead = fis.read(buffer, cityLength, buffer.length - cityLength);
                                            currentCityLength = cityLength;
                                            cityIndexStart = 0;
                                            i = cityLength;
                                        }
                                    }
                                    lineCount += 1;
                                    value = value * multiplier; // is boolean check faster?
                                    measurements.insert(buffer, cityIndexStart, cityLength, value);
                                    // buffer[i] == \n
                                    cityIndexStart = i + 1;
                                    value = 0;
                                    multiplier = 1;
                                }
                                i++;
                                totalBytesRead++;
                            }
                            currentCityLength = buffer.length - cityIndexStart;
                            System.arraycopy(buffer, cityIndexStart, buffer, 0, currentCityLength);
                        }
                    }
                    return lineCount;
                });
                results[processIdx] = future;
            }
            int totalLineCount = 0;
            for (Future f : results) {
                totalLineCount += (Integer) f.get();
            }
            System.out.println(totalLineCount);
        }
    }

    public static void multiThreadedReadingDoItAllProgress() throws Exception {
        File file = new File(FILE);
        long length = file.length();
        int numProcessors = Runtime.getRuntime().availableProcessors();
        long chunkToRead = length / numProcessors;

        try (ExecutorService executorService = Executors.newWorkStealingPool(numProcessors)) {
            Future[] results = new Future[numProcessors];
            for (int processIdx = 0; processIdx < numProcessors; processIdx++) {
                long seekPoint = processIdx * chunkToRead;
                long bytesToRead = processIdx == numProcessors - 1 ? (length - seekPoint) : chunkToRead;
                int finalProcessIdx = processIdx;
                Future<MyMap> future = executorService.submit(() -> {
                    MyMap measurements = new MyMap();
                    int lineCount = 0;
                    try (FileInputStream fis = new FileInputStream(FILE)) {
                        long actualSkipped = fis.skip(seekPoint);
                        assert (actualSkipped != seekPoint);
                        byte[] buffer = new byte[1024 * 8 * 64];
                        long totalBytesRead = 0;
                        int bytesRead;
                        int currentCityLength = 0;
                        boolean first = true;
                        while ((bytesRead = fis.read(buffer, currentCityLength, buffer.length - currentCityLength)) != -1) {
                            if (totalBytesRead >= bytesToRead && currentCityLength == 0) {
                                // we have read everything we intend to and there is no city in the buffer to finish processing
                                System.out.printf("Read everything intend to. Process=%d, totalBytesRead=%d, bytesToRead=%d\n", finalProcessIdx, totalBytesRead,
                                        bytesToRead);
                                return measurements;
                            }
                            int i = 0;
                            int cityIndexStart = 0;
                            int cityLength;
                            int multiplier = 1;
                            int value = 0;
                            while (i < bytesRead) {
                                if (buffer[i] == ';') {
                                    cityLength = i - cityIndexStart;
                                    if (first) {
                                        first = false;
                                        System.out.printf("The first city found for %d is %s\n", finalProcessIdx,
                                                new String(buffer, cityIndexStart, cityLength, StandardCharsets.UTF_8));
                                    }
                                    i++;
                                    totalBytesRead++;
                                    if (i == bytesRead + currentCityLength) {
                                        System.arraycopy(buffer, cityIndexStart, buffer, 0, cityLength);
                                        bytesRead = fis.read(buffer, cityLength, buffer.length - cityLength);
                                        currentCityLength = cityLength;
                                        cityIndexStart = 0;
                                        i = cityLength;
                                    }
                                    if (buffer[i] == '-') {
                                        multiplier = -1;
                                        i++;
                                        totalBytesRead++;
                                        if (i == bytesRead + currentCityLength) {
                                            System.arraycopy(buffer, cityIndexStart, buffer, 0, cityLength);
                                            bytesRead = fis.read(buffer, cityLength, buffer.length - cityLength);
                                            currentCityLength = cityLength;
                                            cityIndexStart = 0;
                                            i = cityLength;
                                        }
                                    }
                                    while (buffer[i] != '\n') {
                                        if (buffer[i] != '.') {
                                            value = (value * 10) + (buffer[i] - '0');
                                        }
                                        i++;
                                        totalBytesRead++;
                                        if (i == bytesRead + currentCityLength) {
                                            System.arraycopy(buffer, cityIndexStart, buffer, 0, cityLength);
                                            bytesRead = fis.read(buffer, cityLength, buffer.length - cityLength);
                                            currentCityLength = cityLength;
                                            cityIndexStart = 0;
                                            i = cityLength;
                                        }
                                    }
                                    lineCount += 1;
                                    value = value * multiplier; // is boolean check faster?
                                    measurements.insert(buffer, cityIndexStart, cityLength, value);
                                    // buffer[i] == \n
                                    if (totalBytesRead >= bytesToRead) {
                                        // we have read everything we intend to for this chunk
                                        System.out.printf("Read everything intend to inner loop. Process=%d, totalBytesRead=%d, bytesToRead=%d\n", finalProcessIdx,
                                                totalBytesRead, bytesToRead);
                                        System.out.printf("More info for %d. cityIndexStart=%d, cityLength=%d, value=%d, lineCount=%d, city=%s\n", finalProcessIdx,
                                                cityIndexStart, cityLength, value, lineCount, new String(buffer, cityIndexStart, cityLength, StandardCharsets.UTF_8));
                                        return measurements;
                                    }
                                    cityIndexStart = i + 1;
                                    value = 0;
                                    multiplier = 1;
                                }
                                i++;
                                totalBytesRead++;
                            }
                            currentCityLength = buffer.length - cityIndexStart;
                            System.arraycopy(buffer, cityIndexStart, buffer, 0, currentCityLength);
                        }
                    }
                    System.out.println("Returning from here " + finalProcessIdx);
                    return measurements;
                });
                results[processIdx] = future;
            }
            final HashMap<String, Measurement> measurements = new HashMap<>();
            for (Future f : results) {
                HashMap<String, Measurement> m = ((MyMap) f.get()).toMap();
                m.forEach((city, measurement) -> {
                    measurements.merge(city, measurement, (oldValue, newValue) -> {
                        Measurement mmm = new Measurement(0);
                        mmm.min = Math.min(oldValue.min, newValue.min);
                        mmm.max = Math.max(oldValue.max, newValue.max);
                        mmm.sum = oldValue.sum + newValue.sum;
                        mmm.count = oldValue.count + newValue.count;
                        return mmm;
                    });
                });
            }
            System.out.print("{");
            System.out.print(
                    measurements.entrySet().stream().sorted(Map.Entry.comparingByKey()).map(Object::toString).collect(Collectors.joining(", ")));
            System.out.println("}");
        }
    }

    public static void multiThreadedReadingDoItAll() throws Exception {
        File file = new File(FILE);
        long length = file.length();
        int numProcessors = Runtime.getRuntime().availableProcessors();
        long chunkToRead = length / numProcessors;

        // make life easier by spending a bit of time up front to find line breaks around the chunks
        final long[] startPositions = new long[numProcessors + 1];
        try (RandomAccessFile raf = new RandomAccessFile(FILE, "r")) {
            byte[] buffer = new byte[256];
            for (int processIdx = 1; processIdx < numProcessors; processIdx++) {
                long initialSeekPoint = processIdx * chunkToRead;
                raf.seek(initialSeekPoint);
                int bytesRead = raf.read(buffer);
                if (bytesRead != buffer.length) {
                    throw new Exception("Actual read is not same as requested. " + bytesRead);
                }
                int i = 0;
                while (buffer[i] != '\n') {
                    i++;
                }
                initialSeekPoint += (i + 1);
                startPositions[processIdx] = initialSeekPoint;
            }
            startPositions[numProcessors] = length;
        }
        System.out.printf("Start positions %s\n", Arrays.toString(startPositions));

        try (ExecutorService executorService = Executors.newWorkStealingPool(numProcessors)) {
            Future[] results = new Future[numProcessors];
            for (int processIdx = 0; processIdx < numProcessors; processIdx++) {
                long seekPoint = startPositions[processIdx];
                long bytesToRead = startPositions[processIdx + 1] - startPositions[processIdx];
                int finalProcessIdx = processIdx;
                Future<HashMap<String, Measurement>> future = executorService.submit(() -> {
                    MyMap2 measurements = new MyMap2();
                    int lineCount = 0;
                    try (FileInputStream fis = new FileInputStream(FILE)) {
                        long actualSkipped = fis.skip(seekPoint);
                        if (actualSkipped != seekPoint) {
                            throw new Exception("Uho oh");
                        }
                        // byte[] buffer = new byte[1024 * 8 * 64];
                        byte[] buffer = new byte[1024 * 1024 * 10];
                        long totalBytesRead = 0;
                        int bytesRead;
                        int currentCityLength = 0;
                        while ((bytesRead = fis.read(buffer, currentCityLength, buffer.length - currentCityLength)) != -1) {
                            totalBytesRead -= currentCityLength; // avoid double counting. There must be a better way.
                            if (totalBytesRead >= bytesToRead && currentCityLength == 0) {
                                // we have read everything we intend to and there is no city in the buffer to finish processing
                                System.out.printf("Read everything intend to. Process=%d, totalBytesRead=%d, bytesToRead=%d\n", finalProcessIdx, totalBytesRead,
                                        bytesToRead);
                                return measurements.toMap();
                            }
                            int i = 0;
                            int cityIndexStart = 0;
                            int cityLength;
                            int multiplier = 1;
                            int value = 0;
                            while (i < bytesRead + currentCityLength) {
                                if (totalBytesRead >= bytesToRead) {
                                    // we have read everything we intend to for this chunk
                                    System.out.printf("ooRead everything intend to inner loop. Process=%d, totalBytesRead=%d, bytesToRead=%d\n", finalProcessIdx,
                                            totalBytesRead, bytesToRead);
                                    return measurements.toMap();
                                }
                                if (buffer[i] == ';') {
                                    cityLength = i - cityIndexStart;
                                    i++;
                                    totalBytesRead++;
                                    if (i == bytesRead + currentCityLength) {
                                        System.arraycopy(buffer, cityIndexStart, buffer, 0, cityLength);
                                        bytesRead = fis.read(buffer, cityLength, buffer.length - cityLength);
                                        currentCityLength = cityLength;
                                        cityIndexStart = 0;
                                        i = cityLength;
                                    }
                                    if (buffer[i] == '-') {
                                        multiplier = -1;
                                        i++;
                                        totalBytesRead++;
                                        if (i == bytesRead + currentCityLength) {
                                            System.arraycopy(buffer, cityIndexStart, buffer, 0, cityLength);
                                            bytesRead = fis.read(buffer, cityLength, buffer.length - cityLength);
                                            currentCityLength = cityLength;
                                            cityIndexStart = 0;
                                            i = cityLength;
                                        }
                                    }
                                    while (buffer[i] != '\n') {
                                        if (buffer[i] != '.') {
                                            value = (value * 10) + (buffer[i] - '0');
                                        }
                                        i++;
                                        totalBytesRead++;
                                        if (i == bytesRead + currentCityLength) {
                                            System.arraycopy(buffer, cityIndexStart, buffer, 0, cityLength);
                                            bytesRead = fis.read(buffer, cityLength, buffer.length - cityLength);
                                            currentCityLength = cityLength;
                                            cityIndexStart = 0;
                                            i = cityLength;
                                        }
                                    }
                                    lineCount += 1;
                                    value = value * multiplier; // is boolean check faster?
                                    measurements.insert(buffer, cityIndexStart, cityLength, value);
                                    // buffer[i] == \n
                                    if (totalBytesRead >= bytesToRead) {
                                        // we have read everything we intend to for this chunk
                                        System.out.printf("Read everything intend to inner loop. Process=%d, totalBytesRead=%d, bytesToRead=%d\n", finalProcessIdx,
                                                totalBytesRead, bytesToRead);
                                        System.out.printf("More info for %d. cityIndexStart=%d, cityLength=%d, value=%d, lineCount=%d, city=%s\n", finalProcessIdx,
                                                cityIndexStart, cityLength, value, lineCount, new String(buffer, cityIndexStart, cityLength, StandardCharsets.UTF_8));
                                        return measurements.toMap();
                                    }
                                    cityIndexStart = i + 1;
                                    value = 0;
                                    multiplier = 1;
                                }
                                i++;
                                totalBytesRead++;
                            }
                            currentCityLength = buffer.length - cityIndexStart;
                            System.arraycopy(buffer, cityIndexStart, buffer, 0, currentCityLength);
                        }
                    }
                    System.out.println("Returning from here " + finalProcessIdx);
                    return measurements.toMap();
                });
                results[processIdx] = future;
            }

            final HashMap<String, Measurement> measurements = new HashMap<>();
            for (Future f : results) {
                HashMap<String, Measurement> m = (HashMap<String, Measurement>) f.get();
                m.forEach((city, measurement) -> {
                    measurements.merge(city, measurement, (oldValue, newValue) -> {
                        Measurement mmm = new Measurement(0);
                        mmm.min = Math.min(oldValue.min, newValue.min);
                        mmm.max = Math.max(oldValue.max, newValue.max);
                        mmm.sum = oldValue.sum + newValue.sum;
                        mmm.count = oldValue.count + newValue.count;
                        return mmm;
                    });
                });
            }
            AtomicInteger lineCount = new AtomicInteger();
            measurements.forEach((city, m) -> lineCount.addAndGet(m.count));
            System.out.println(lineCount.get());
            System.out.print("{");
            System.out.print(
                    measurements.entrySet().stream().sorted(Map.Entry.comparingByKey()).map(Object::toString).collect(Collectors.joining(", ")));
            System.out.println("}");
        }
    }

}
