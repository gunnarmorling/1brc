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

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class CalculateAverage_seijikun {

    private static final String FILE = "./measurements.txt";

    private static class MeasurementAggregator {
        private int min = Integer.MAX_VALUE;
        private int max = Integer.MIN_VALUE;
        private long sum = 0;
        private long count = 0;

        private double mean = 0;

        public void finish() {
            double sum = this.sum / 10.0;
            mean = sum / (double) count;
        }

        public void printInto(PrintStream out) {
            double min = (double) this.min / 10.0;
            double max = (double) this.max / 10.0;
            out.printf("%.1f/%.1f/%.1f", min, mean, max);
        }
    }

    public static class StationIdent {
        private final int nameLength;
        private final byte[] name;
        private final int nameHash;

        public StationIdent(byte[] name, int nameHash) {
            this.nameLength = name.length;
            this.name = name;
            this.nameHash = nameHash;
        }

        @Override
        public int hashCode() {
            return nameHash;
        }

        @Override
        public boolean equals(Object obj) {
            var other = (StationIdent) obj;
            if (other.nameLength != nameLength) {
                return false;
            }
            return Arrays.equals(name, other.name);
        }
    }

    public static class ChunkReader implements Runnable {
        RandomAccessFile file;

        // Start offset of this chunk
        private final long startOffset;
        // end offset of this chunk
        private final long endOffset;

        // state
        private MappedByteBuffer buffer = null;
        private int ptr = 0;
        private HashMap<StationIdent, MeasurementAggregator> workSet;

        public ChunkReader(RandomAccessFile file, long startOffset, long endOffset) {
            this.file = file;
            this.startOffset = startOffset;
            this.endOffset = endOffset;
        }

        private StationIdent readStationName() {
            int startPtr = ptr;
            int hashCode = 0;
            int hashBytePtr = 0;
            byte c;
            while ((c = buffer.get(ptr++)) != ';') {
                hashCode ^= ((int) c) << (hashBytePtr * 8);
                hashBytePtr = (hashBytePtr + 1) % 4;
            }
            byte[] stationNameBfr = new byte[ptr - startPtr - 1];
            buffer.get(startPtr, stationNameBfr);
            hashCode ^= stationNameBfr.length;
            return new StationIdent(stationNameBfr, hashCode);
        }

        private int readTemperature() {
            int ret = 0;
            byte c = buffer.get(ptr++);
            boolean neg = (c == '-');
            if (neg) {
                c = buffer.get(ptr++);
            }

            do {
                if (c != '.') {
                    ret = ret * 10 + c - '0';
                }
            } while ((c = buffer.get(ptr++)) != '\n');

            if (neg)
                return -ret;
            return ret;
        }

        @Override
        public void run() {
            workSet = new HashMap<>();
            int chunkSize = (int) (endOffset - startOffset);
            try {
                buffer = file.getChannel().map(FileChannel.MapMode.READ_ONLY, startOffset, chunkSize);

                while (ptr < chunkSize) {
                    var station = readStationName();
                    int temp = readTemperature();
                    var stationWorkSet = workSet.get(station);
                    if (stationWorkSet == null) {
                        stationWorkSet = new MeasurementAggregator();
                        workSet.put(station, stationWorkSet);
                    }
                    stationWorkSet.min = Math.min(temp, stationWorkSet.min);
                    stationWorkSet.max = Math.max(temp, stationWorkSet.max);
                    stationWorkSet.sum += temp;
                    stationWorkSet.count += 1;
                }
            }
            catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
    }

    private static void printWorkSet(TreeMap<String, MeasurementAggregator> result, PrintStream out) {
        out.write('{');
        var iterator = result.entrySet().iterator();
        while (iterator.hasNext()) {
            var entry = iterator.next();
            out.print(entry.getKey());
            out.write('=');
            entry.getValue().printInto(out);
            if (iterator.hasNext()) {
                out.print(", ");
            }
        }
        out.println('}');
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        RandomAccessFile file = new RandomAccessFile(FILE, "r");

        int jobCnt = Runtime.getRuntime().availableProcessors();

        var chunks = new ChunkReader[jobCnt];
        long chunkSize = file.length() / jobCnt;
        long chunkStartPtr = 0;
        byte[] tmpBuffer = new byte[128];
        for (int i = 0; i < jobCnt; ++i) {
            long chunkEndPtr = chunkStartPtr + chunkSize;
            if (i != (jobCnt - 1)) { // align chunks to newlines
                file.seek(chunkEndPtr - 1);
                file.read(tmpBuffer);
                int offset = 0;
                while (tmpBuffer[offset] != '\n') {
                    offset += 1;
                }
                chunkEndPtr += offset;
            }
            else { // last chunk ends at file end
                chunkEndPtr = file.length();
            }
            chunks[i] = new ChunkReader(file, chunkStartPtr, chunkEndPtr);
            chunkStartPtr = chunkEndPtr;
        }

        try (var executor = Executors.newFixedThreadPool(jobCnt)) {
            for (int i = 0; i < jobCnt; ++i) {
                executor.submit(chunks[i]);
            }
            executor.shutdown();
            var ignored = executor.awaitTermination(1, TimeUnit.DAYS);
        }

        // merge chunks
        var result = new TreeMap<String, MeasurementAggregator>();
        for (int i = 0; i < jobCnt; ++i) {
            chunks[i].workSet.forEach((ident, otherStationWorkSet) -> {
                var identStr = new String(ident.name);
                var stationWorkSet = result.get(identStr);
                if (stationWorkSet == null) {
                    result.put(identStr, otherStationWorkSet);
                }
                else {
                    stationWorkSet.min = Math.min(stationWorkSet.min, otherStationWorkSet.min);
                    stationWorkSet.max = Math.max(stationWorkSet.max, otherStationWorkSet.max);
                    stationWorkSet.sum += otherStationWorkSet.sum;
                    stationWorkSet.count += otherStationWorkSet.count;
                }
            });
        }
        result.forEach((ignored, meas) -> meas.finish());

        // print in required format
        printWorkSet(result, System.out);

        { // result check
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            final String utf8 = StandardCharsets.UTF_8.name();
            try (PrintStream ps = new PrintStream(baos, true, utf8)) {
                printWorkSet(result, ps);
            }
            String output = baos.toString(utf8);
            final String SHOULD_OUTPUT = "{Abha=-30.8/18.0/66.3, Abidjan=-23.9/26.0/73.4, Abéché=-21.3/29.4/76.3, Accra=-24.7/26.4/75.8, Addis Ababa=-34.1/16.0/63.8, Adelaide=-33.2/17.3/68.3, Aden=-20.2/29.1/79.6, Ahvaz=-22.2/25.4/79.9, Albuquerque=-35.2/14.0/61.4, Alexandra=-38.8/11.0/63.3, Alexandria=-31.5/20.0/67.6, Algiers=-34.9/18.2/69.1, Alice Springs=-30.7/21.0/69.4, Almaty=-39.5/10.0/61.5, Amsterdam=-43.2/10.2/60.3, Anadyr=-60.7/-6.9/42.3, Anchorage=-50.7/2.8/52.7, Andorra la Vella=-39.7/9.8/62.5, Ankara=-37.8/12.0/60.2, Antananarivo=-29.5/17.9/67.3, Antsiranana=-29.4/25.2/78.4, Arkhangelsk=-47.0/1.3/55.0, Ashgabat=-32.5/17.1/64.0, Asmara=-42.9/15.6/62.9, Assab=-20.2/30.5/84.4, Astana=-53.3/3.5/54.6, Athens=-29.6/19.2/68.1, Atlanta=-32.5/17.0/65.9, Auckland=-34.8/15.2/69.0, Austin=-28.6/20.7/75.1, Baghdad=-25.2/22.8/72.7, Baguio=-31.7/19.5/67.8, Baku=-33.0/15.1/65.2, Baltimore=-37.3/13.1/63.0, Bamako=-22.9/27.8/79.7, Bangkok=-24.0/28.6/79.0, Bangui=-26.2/26.0/75.1, Banjul=-26.2/26.0/81.2, Barcelona=-32.0/18.2/69.7, Bata=-22.8/25.1/73.3, Batumi=-38.8/14.0/64.2, Beijing=-33.6/12.9/68.2, Beirut=-28.9/20.9/68.7, Belgrade=-37.2/12.5/64.1, Belize City=-24.5/26.7/78.4, Benghazi=-29.1/19.9/71.3, Bergen=-41.5/7.7/54.8, Berlin=-40.0/10.3/61.3, Bilbao=-34.5/14.7/62.6, Birao=-24.5/26.5/73.8, Bishkek=-38.2/11.3/58.4, Bissau=-24.2/27.0/76.1, Blantyre=-26.0/22.2/71.4, Bloemfontein=-35.9/15.6/64.6, Boise=-37.6/11.4/63.3, Bordeaux=-35.8/14.2/66.0, Bosaso=-22.2/30.0/78.5, Boston=-35.7/10.9/60.0, Bouaké=-23.9/26.0/76.3, Bratislava=-38.8/10.5/59.4, Brazzaville=-23.4/25.0/74.6, Bridgetown=-24.1/27.0/73.9, Brisbane=-29.0/21.4/75.9, Brussels=-40.6/10.5/62.5, Bucharest=-41.1/10.8/62.2, Budapest=-37.2/11.3/63.5, Bujumbura=-26.6/23.8/72.8, Bulawayo=-30.1/18.9/70.6, Burnie=-35.6/13.1/62.1, Busan=-32.4/15.0/64.6, Cabo San Lucas=-29.8/23.9/74.2, Cairns=-25.0/25.0/72.9, Cairo=-31.0/21.4/71.6, Calgary=-44.5/4.4/53.9, Canberra=-36.6/13.1/64.4, Cape Town=-31.2/16.2/65.6, Changsha=-32.6/17.4/68.6, Charlotte=-29.9/16.1/66.6, Chiang Mai=-24.0/25.8/78.0, Chicago=-41.4/9.8/59.5, Chihuahua=-29.5/18.6/66.2, Chittagong=-28.0/25.9/79.5, Chișinău=-43.6/10.2/62.7, Chongqing=-32.9/18.6/69.8, Christchurch=-42.4/12.2/63.0, City of San Marino=-42.6/11.8/61.3, Colombo=-25.7/27.4/75.2, Columbus=-39.9/11.7/59.1, Conakry=-22.2/26.4/78.4, Copenhagen=-38.9/9.1/62.6, Cotonou=-25.2/27.2/77.8, Cracow=-40.8/9.3/63.5, Da Lat=-33.2/17.9/67.2, Da Nang=-21.5/25.8/72.2, Dakar=-24.0/24.0/76.0, Dallas=-27.9/19.0/66.9, Damascus=-40.7/17.0/67.5, Dampier=-24.0/26.4/76.4, Dar es Salaam=-28.3/25.8/76.7, Darwin=-20.8/27.6/77.3, Denpasar=-27.1/23.7/74.1, Denver=-40.2/10.4/59.1, Detroit=-42.4/10.0/60.9, Dhaka=-23.1/25.9/81.1, Dikson=-61.1/-11.1/39.0, Dili=-23.5/26.6/79.6, Djibouti=-16.8/29.9/81.1, Dodoma=-26.6/22.7/80.0, Dolisie=-25.3/24.0/73.0, Douala=-26.3/26.7/74.4, Dubai=-21.8/26.9/80.6, Dublin=-40.5/9.8/58.8, Dunedin=-36.1/11.1/63.4, Durban=-27.5/20.6/70.3, Dushanbe=-42.4/14.7/65.6, Edinburgh=-43.3/9.3/61.1, Edmonton=-44.4/4.2/54.9, El Paso=-29.4/18.1/70.2, Entebbe=-27.6/21.0/70.2, Erbil=-29.3/19.5/67.4, Erzurum=-44.1/5.1/51.7, Fairbanks=-53.0/-2.3/51.8, Fianarantsoa=-30.6/17.9/67.0, Flores,  Petén=-23.2/26.4/73.9, Frankfurt=-37.8/10.6/60.2, Fresno=-30.9/17.9/68.6, Fukuoka=-35.1/17.0/64.4, Gaborone=-26.8/21.0/69.5, Gabès=-30.2/19.5/71.6, Gagnoa=-22.8/26.0/76.1, Gangtok=-33.5/15.2/70.8, Garissa=-22.3/29.3/77.0, Garoua=-23.4/28.3/78.7, George Town=-20.3/27.9/77.8, Ghanzi=-31.1/21.4/72.0, Gjoa Haven=-65.7/-14.4/44.0, Guadalajara=-28.3/20.9/67.1, Guangzhou=-32.8/22.4/75.7, Guatemala City=-30.5/20.4/67.1, Halifax=-41.5/7.5/56.5, Hamburg=-46.2/9.7/58.7, Hamilton=-37.0/13.8/66.3, Hanga Roa=-29.5/20.5/70.2, Hanoi=-22.8/23.6/71.6, Harare=-35.7/18.4/72.7, Harbin=-46.0/5.0/53.3, Hargeisa=-30.0/21.7/69.8, Hat Yai=-29.6/27.0/76.5, Havana=-25.3/25.2/73.9, Helsinki=-44.4/5.9/56.0, Heraklion=-32.5/18.9/68.7, Hiroshima=-30.7/16.3/66.6, Ho Chi Minh City=-25.7/27.4/80.2, Hobart=-39.6/12.7/62.1, Hong Kong=-28.4/23.3/72.5, Honiara=-25.1/26.5/75.2, Honolulu=-27.4/25.4/72.8, Houston=-28.0/20.8/70.0, Ifrane=-41.8/11.4/65.9, Indianapolis=-35.9/11.8/59.9, Iqaluit=-56.1/-9.3/43.3, Irkutsk=-51.5/1.0/54.1, Istanbul=-34.4/13.9/63.5, Jacksonville=-30.5/20.3/72.4, Jakarta=-27.5/26.7/80.3, Jayapura=-26.4/27.0/78.0, Jerusalem=-31.0/18.3/67.9, Johannesburg=-33.8/15.5/64.2, Jos=-25.9/22.8/73.2, Juba=-22.1/27.8/82.2, Kabul=-35.5/12.1/60.1, Kampala=-29.4/20.0/71.4, Kandi=-20.6/27.7/80.9, Kankan=-26.9/26.5/75.4, Kano=-27.2/26.4/76.6, Kansas City=-38.3/12.5/62.8, Karachi=-26.1/26.0/75.9, Karonga=-23.7/24.4/76.9, Kathmandu=-29.4/18.3/71.2, Khartoum=-17.5/29.9/82.9, Kingston=-21.4/27.4/75.0, Kinshasa=-23.6/25.3/73.2, Kolkata=-25.9/26.7/74.5, Kuala Lumpur=-23.9/27.3/78.1, Kumasi=-20.5/26.0/79.0, Kunming=-35.2/15.7/68.7, Kuopio=-47.6/3.4/54.6, Kuwait City=-22.3/25.7/76.8, Kyiv=-42.2/8.4/59.1, Kyoto=-31.8/15.8/69.1, La Ceiba=-21.4/26.2/77.8, La Paz=-24.5/23.7/75.3, Lagos=-20.8/26.8/77.8, Lahore=-25.8/24.3/70.7, Lake Havasu City=-25.0/23.7/71.8, Lake Tekapo=-41.2/8.7/55.3, Las Palmas de Gran Canaria=-31.7/21.2/72.6, Las Vegas=-28.5/20.3/67.9, Launceston=-36.0/13.1/65.7, Lhasa=-40.9/7.6/55.5, Libreville=-28.0/25.9/75.9, Lisbon=-34.4/17.5/64.9, Livingstone=-26.4/21.8/75.5, Ljubljana=-35.7/10.9/63.3, Lodwar=-22.6/29.3/79.9, Lomé=-24.5/26.9/73.9, London=-38.4/11.3/61.3, Los Angeles=-35.6/18.6/67.7, Louisville=-38.6/13.9/62.8, Luanda=-24.9/25.8/73.1, Lubumbashi=-27.9/20.8/69.2, Lusaka=-35.2/19.9/68.7, Luxembourg City=-41.1/9.3/61.3, Lviv=-42.9/7.8/55.8, Lyon=-36.4/12.5/65.1, Madrid=-32.7/15.0/64.8, Mahajanga=-24.3/26.3/75.4, Makassar=-27.8/26.7/76.7, Makurdi=-26.7/26.0/74.5, Malabo=-23.0/26.3/81.2, Malé=-21.9/28.0/78.9, Managua=-21.5/27.3/85.6, Manama=-25.4/26.5/78.6, Mandalay=-22.5/28.0/79.6, Mango=-25.0/28.1/79.5, Manila=-22.3/28.4/81.3, Maputo=-25.4/22.8/71.7, Marrakesh=-36.3/19.6/71.4, Marseille=-36.6/15.8/67.7, Maun=-25.4/22.4/75.8, Medan=-23.1/26.5/75.8, Mek'ele=-24.9/22.7/68.7, Melbourne=-32.4/15.1/63.4, Memphis=-31.5/17.2/68.9, Mexicali=-26.4/23.1/71.1, Mexico City=-32.2/17.5/67.4, Miami=-29.0/24.9/73.9, Milan=-34.2/13.0/64.7, Milwaukee=-42.9/8.9/60.7, Minneapolis=-42.7/7.8/56.3, Minsk=-42.5/6.7/56.6, Mogadishu=-23.1/27.1/76.8, Mombasa=-23.0/26.3/75.2, Monaco=-33.4/16.4/66.3, Moncton=-44.5/6.1/58.4, Monterrey=-27.1/22.3/71.9, Montreal=-47.6/6.8/60.8, Moscow=-43.9/5.8/56.9, Mumbai=-25.6/27.1/76.7, Murmansk=-48.0/0.6/50.8, Muscat=-19.5/28.0/79.1, Mzuzu=-30.1/17.7/68.3, N'Djamena=-21.9/28.3/79.0, Naha=-30.9/23.1/69.5, Nairobi=-29.5/17.8/68.3, Nakhon Ratchasima=-22.6/27.3/78.9, Napier=-32.7/14.6/63.4, Napoli=-36.0/15.9/63.1, Nashville=-37.7/15.4/65.0, Nassau=-27.0/24.6/74.6, Ndola=-29.4/20.3/69.6, New Delhi=-25.2/25.0/74.4, New Orleans=-27.9/20.7/72.4, New York City=-41.5/12.9/62.1, Ngaoundéré=-23.8/22.0/72.9, Niamey=-20.1/29.3/80.7, Nicosia=-32.7/19.7/69.0, Niigata=-33.7/13.9/65.5, Nouadhibou=-30.1/21.3/71.0, Nouakchott=-24.7/25.7/74.0, Novosibirsk=-49.1/1.7/53.8, Nuuk=-56.1/-1.4/46.3, Odesa=-38.5/10.7/58.3, Odienné=-25.4/26.0/75.5, Oklahoma City=-35.4/15.9/64.6, Omaha=-40.0/10.6/57.9, Oranjestad=-19.6/28.1/74.4, Oslo=-45.8/5.7/56.2, Ottawa=-46.3/6.6/59.3, Ouagadougou=-20.7/28.3/82.3, Ouahigouya=-19.4/28.6/76.8, Ouarzazate=-33.8/18.9/71.0, Oulu=-45.3/2.7/54.9, Palembang=-20.9/27.3/76.2, Palermo=-31.4/18.5/68.6, Palm Springs=-22.2/24.5/73.4, Palmerston North=-38.8/13.2/60.2, Panama City=-19.5/28.0/75.0, Parakou=-23.2/26.8/74.7, Paris=-36.0/12.3/64.0, Perth=-31.4/18.7/65.7, Petropavlovsk-Kamchatsky=-45.7/1.9/58.5, Philadelphia=-37.8/13.2/70.4, Phnom Penh=-22.2/28.3/74.8, Phoenix=-27.2/23.9/74.2, Pittsburgh=-44.2/10.8/58.2, Podgorica=-33.5/15.3/70.5, Pointe-Noire=-22.1/26.1/75.0, Pontianak=-20.8/27.7/78.4, Port Moresby=-24.3/26.9/75.4, Port Sudan=-19.7/28.4/83.4, Port Vila=-29.3/24.3/73.3, Port-Gentil=-24.9/26.0/77.0, Portland (OR)=-38.0/12.4/64.1, Porto=-32.7/15.7/65.4, Prague=-44.4/8.4/56.5, Praia=-27.9/24.4/77.2, Pretoria=-29.8/18.2/69.9, Pyongyang=-41.9/10.8/59.5, Rabat=-31.9/17.2/66.1, Rangpur=-26.9/24.4/73.8, Reggane=-23.3/28.3/76.7, Reykjavík=-45.3/4.3/51.2, Riga=-40.4/6.2/54.3, Riyadh=-25.0/26.0/78.1, Rome=-37.4/15.2/66.1, Roseau=-26.1/26.2/76.7, Rostov-on-Don=-41.2/9.9/62.1, Sacramento=-34.0/16.3/66.0, Saint Petersburg=-47.5/5.8/58.0, Saint-Pierre=-44.9/5.7/56.2, Salt Lake City=-37.5/11.6/62.3, San Antonio=-26.5/20.8/67.3, San Diego=-38.7/17.8/69.3, San Francisco=-34.1/14.6/66.5, San Jose=-35.4/16.4/67.1, San José=-28.3/22.6/71.2, San Juan=-23.3/27.2/74.0, San Salvador=-29.3/23.1/75.3, Sana'a=-28.5/20.0/67.6, Santo Domingo=-23.5/25.9/79.1, Sapporo=-38.2/8.9/59.9, Sarajevo=-45.6/10.1/62.2, Saskatoon=-47.2/3.3/51.6, Seattle=-42.3/11.3/60.6, Seoul=-37.0/12.5/62.7, Seville=-31.3/19.2/68.7, Shanghai=-37.3/16.7/64.0, Singapore=-24.3/27.0/82.8, Skopje=-37.1/12.4/59.6, Sochi=-33.6/14.2/64.0, Sofia=-36.0/10.6/57.0, Sokoto=-24.1/28.0/77.6, Split=-30.2/16.1/66.0, St. John's=-45.1/5.0/55.9, St. Louis=-33.9/13.9/66.2, Stockholm=-44.7/6.6/61.1, Surabaya=-21.3/27.1/78.2, Suva=-24.5/25.6/75.0, Suwałki=-42.9/7.2/58.6, Sydney=-31.4/17.7/67.7, Ségou=-25.5/28.0/79.5, Tabora=-29.4/23.0/72.9, Tabriz=-37.3/12.6/61.6, Taipei=-26.4/23.0/76.6, Tallinn=-41.7/6.4/55.4, Tamale=-21.7/27.9/75.6, Tamanrasset=-30.6/21.7/73.2, Tampa=-25.6/22.9/73.7, Tashkent=-33.8/14.8/63.9, Tauranga=-38.9/14.8/67.5, Tbilisi=-35.8/12.9/61.8, Tegucigalpa=-26.1/21.7/71.1, Tehran=-31.4/17.0/65.8, Tel Aviv=-28.8/20.0/69.0, Thessaloniki=-31.6/16.0/62.3, Thiès=-28.0/24.0/73.3, Tijuana=-34.8/17.8/69.1, Timbuktu=-21.4/28.0/74.8, Tirana=-33.0/15.2/62.8, Toamasina=-25.2/23.4/75.1, Tokyo=-30.5/15.4/65.3, Toliara=-26.6/24.1/74.4, Toluca=-34.1/12.4/59.8, Toronto=-41.4/9.4/60.3, Tripoli=-34.2/20.0/72.4, Tromsø=-49.3/2.9/54.8, Tucson=-30.1/20.9/70.3, Tunis=-27.3/18.4/65.8, Ulaanbaatar=-53.8/-0.4/50.3, Upington=-35.6/20.4/72.5, Vaduz=-42.8/10.1/60.3, Valencia=-30.2/18.3/71.1, Valletta=-33.8/18.8/73.2, Vancouver=-39.2/10.4/62.3, Veracruz=-24.4/25.4/77.3, Vienna=-39.9/10.4/59.3, Vientiane=-22.3/25.9/76.2, Villahermosa=-23.6/27.1/81.3, Vilnius=-44.8/6.0/60.9, Virginia Beach=-34.0/15.8/66.3, Vladivostok=-47.6/4.9/55.5, Warsaw=-42.5/8.5/55.6, Washington, D.C.=-37.0/14.6/65.4, Wau=-20.9/27.8/85.0, Wellington=-35.6/12.9/61.6, Whitehorse=-49.5/-0.1/52.8, Wichita=-35.0/13.9/62.2, Willemstad=-21.8/28.0/78.6, Winnipeg=-45.5/3.0/53.7, Wrocław=-37.3/9.6/58.8, Xi'an=-35.5/14.1/62.9, Yakutsk=-59.6/-8.8/41.5, Yangon=-22.8/27.5/74.3, Yaoundé=-24.7/23.8/71.3, Yellowknife=-55.8/-4.3/44.6, Yerevan=-44.8/12.4/63.0, Yinchuan=-42.1/9.0/57.7, Zagreb=-39.0/10.7/60.4, Zanzibar City=-25.0/26.0/75.7, Zürich=-40.8/9.3/58.9, Ürümqi=-41.4/7.4/60.5, İzmir=-35.0/17.9/69.5}\n";
            for (int i = 0; i < output.length(); ++i) {
                if (output.charAt(i) != SHOULD_OUTPUT.charAt(i)) {
                    var ctxStart = Math.max(0, i - 20);
                    var ctxEnd = Math.min(output.length() - 1, i + 20);
                    var ctxActual = output.substring(ctxStart, ctxEnd);
                    var ctxShould = SHOULD_OUTPUT.substring(ctxStart, ctxEnd);
                    System.err.println(STR."Output Error at \{i}: '\{ctxActual}' != '\{ctxShould}'");
                }
            }
            System.out.println("CHECK: OK");
        }
    }
}
