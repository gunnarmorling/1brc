package dev.morling.onebrc;

import java.io.FileInputStream;
import java.nio.file.Path;
import java.util.*;

public class CalculateAverage_reedthorngag {

    private static final String FILE = "./measurements.txt";

    private static class Data {
        public byte[] data;
    }

    private static class Station {

        int key;
        String name;
        double min;
        double max;
        double sum;
        double count;

        Station(int key, String name, double value) {
            this.key = key;
            this.name = name;
            this.min = value;
            this.max = value;
            this.sum = value;
            this.count = 1;
        }

        Station(byte[] name, double value) {
            this(Arrays.hashCode(name), new String(name), value);
        }

        public String toString() {
            return this.name + "=" + round(min) + "/" + round(sum/count) + "/" + round(max);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }

        public void add(double value) {
            if (value < this.min) this.min = value;
            else if (value > this.max) this.max = value;
            this.sum += value;
            this.count++;
        }

        public Station add(Station s) {
            if (s.min < this.min) this.min = s.min;
            if (s.max > this.max) this.max = s.max;
            this.sum += s.sum;
            this.count += s.count;
            return this;
        }
    }

    private static class Searcher implements Runnable {

        HashMap<Integer, Station> stations = new HashMap<>();
        private int start;
        private int end;

        private Data file;

        @Override
        public void run() {
            int pos = start;
            byte[] stationName = new byte[32];
            int n = 0;
            boolean nextLine = false;
            while (pos < end) {
                if (nextLine) {
                    nextLine = file.data[pos++] != '\n';
                    continue;
                }
                if (file.data[pos] == ';') {
                    String tempString = "";
                    while (file.data[++pos] != '\n')
                        tempString += (char) file.data[pos];
                    pos++;

                    double value = Double.parseDouble(tempString);

                    int key = Arrays.hashCode(stationName);
                    if (!stations.containsKey(key)) {
                        stations.put(key, new Station(key, new String(Arrays.copyOfRange(stationName,0,n)), value));
                    } else {
                        stations.get(key).add(value);
                    }

                    stationName = new byte[32];
                    n = 0;
                } else {
                    stationName[n++] = file.data[pos++];
                }
            }
        }

        public Searcher(int start, int end, Data data) {
            this.start = start;
            this.end = end;
            this.file = data;

        }
    }

    public static void main(String[] args) throws Exception {

        long startTime = System.currentTimeMillis();

        final Data file = new Data();

        FileInputStream input = new FileInputStream(Path.of(FILE).toString());
        file.data = new byte[(int) input.getChannel().size()+1];
        input.read(file.data);
        input.close();

        file.data[file.data.length-1] = '\n';

        int numThreads = 8;

        ArrayList<Thread> threads = new ArrayList<>(8);
        ArrayList<Searcher> searchers = new ArrayList<>(8);

        int approxChunkSize = file.data.length / numThreads;
        int lastChunkSize = approxChunkSize + (file.data.length - approxChunkSize * numThreads);

        int pos = 0;

        for (int i=0; i<numThreads-1; i++) {
            int start = pos;
            pos += approxChunkSize;
            while (file.data[pos++] != '\n');
            Searcher searcher = new Searcher(start,pos,file);
            Thread thread = new Thread(searcher);
            searchers.add(searcher);
            threads.add(thread);
            thread.start();
        }

        int start = pos;
        Searcher searcher = new Searcher(start,file.data.length,file);
        Thread thread = new Thread(searcher);
        thread.start();


        for (Thread t : threads)
            t.join();

        HashMap<String, Station> stations = new HashMap<>();

        for (Searcher s : searchers) {
            for (Station station : s.stations.values()) {
                if (stations.containsKey(station.name)) {
                    stations.get(station.name).add(station);
                } else {
                    stations.put(station.name, station);
                }
            }
        }

        System.out.println(stations.keySet().toArray()[0]);
        System.out.print("{");
        for (String key : stations.keySet().stream().sorted().toList()) {
            System.out.print(stations.get(key).toString());
            System.out.print(", ");
        }
        System.out.print("\b\b}");


        long end = System.currentTimeMillis();
        System.out.println("\n\nRan for " + (end - startTime) + "ms ("+ (end - startTime) / 1000.0 + "s)");
    }
}
