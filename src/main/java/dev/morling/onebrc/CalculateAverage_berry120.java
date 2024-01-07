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

import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

public class CalculateAverage_berry120 {

    private static final String FILE = "./measurements.txt";
    // TODO: Tweak this number?
    public static final int NUM_VIRTUAL_THREADS = 1000;
    public static final boolean DEBUG = false;

    static class TemperatureSummary implements Comparable<TemperatureSummary> {
        byte[] name;
        int min;
        int max;
        int total;
        int sampleCount;

        public TemperatureSummary(byte[] name, int min, int max, int total, int sampleCount) {
            this.name = name;
            this.min = min;
            this.max = max;
            this.total = total;
            this.sampleCount = sampleCount;
        }

        @Override
        public int compareTo(TemperatureSummary o) {
            return new String(name).compareTo(new String(o.name));
        }

        @Override
        public String toString() {
            return "TemperatureSummary{" +
                    "name=" + new String(name) +
                    ", min=" + min +
                    ", max=" + max +
                    ", total=" + total +
                    ", sampleCount=" + sampleCount +
                    '}';
        }
    }

    public static void main(String[] args) throws Exception {
        long time = System.currentTimeMillis();

        Path path = Path.of(FILE);
        RandomAccessFile file = new RandomAccessFile(path.toFile(), "r");
        FileChannel channel = file.getChannel();
        long size = Files.size(path);
        int splitSize = size < 10_000_000 ? 1 : (NUM_VIRTUAL_THREADS - 1);
        long inc = (int) (size / splitSize);

        List<Long> positions = new ArrayList<>();
        positions.add(0L);

        MemorySegment segment = channel.map(FileChannel.MapMode.READ_ONLY, 0, Files.size(path), Arena.ofShared());

        long pos = 0;
        for (int i = 0; i < splitSize; i++) {
            long endPos = pos + inc - 1;
            while (segment.get(ValueLayout.JAVA_BYTE, endPos) != '\n') {
                endPos--;
            }
            pos = endPos + 1;
            positions.add(pos);
        }
        positions.add(size);

        if (DEBUG)
            System.out.println("WORKED OUT SPLITS: " + (System.currentTimeMillis() - time));

        List<Thread> threads = new ArrayList<>(NUM_VIRTUAL_THREADS);

        List<Map<?, TemperatureSummary>> maps = Collections.synchronizedList(new ArrayList<>());

        for (int split = 0; split < positions.size() - 1; split++) {

            long position = positions.get(split);
            long positionEnd = positions.get(split + 1);

            threads.add(Thread.ofVirtual().start(() -> {

                // TODO: Custom faster map?
                Map<Integer, TemperatureSummary> map = new HashMap<>();
                maps.add(map);

                // Care much less about this map, only used if collisions in the first
                Map<String, TemperatureSummary> backupMap = new HashMap<>();
                maps.add(backupMap);

                boolean processingPlaceName = true;

                byte[] placeName = new byte[100];
                int placeNameIdx = 0;

                byte[] digits = new byte[100];
                int digitIdx = 0;

                for (long address = position; address < positionEnd; address++) {
                    byte b = segment.get(ValueLayout.JAVA_BYTE, address);

                    if (b == 10) {
                        int rollingHash = 5381;
                        for (int i = 0; i < placeNameIdx; i++) {
                            rollingHash = (((rollingHash << 5) + rollingHash) + placeName[i]) & 0xFFFFF;
                        }

                        var existingTemperatureSummary = map.get(rollingHash);
                        int num = parse(digits, digitIdx);

                        if (existingTemperatureSummary == null) {
                            byte[] thisPlace = new byte[placeNameIdx];
                            System.arraycopy(placeName, 0, thisPlace, 0, placeNameIdx);
                            map.put(rollingHash, new TemperatureSummary(thisPlace, num, num, num, 1));
                        }
                        else {
                            if (!Arrays.equals(placeName, 0, placeNameIdx, existingTemperatureSummary.name, 0, placeNameIdx)) {

                                /*
                                 * This block will be slow - don't really care, should be very rare
                                 */
                                if (DEBUG)
                                    System.out.println("BAD: COLLISION!");
                                byte[] thisPlace = new byte[placeNameIdx];
                                System.arraycopy(placeName, 0, thisPlace, 0, placeNameIdx);
                                String backupKey = new String(thisPlace);
                                var backupExistingTemperatureSummary = backupMap.get(backupKey);

                                if (backupExistingTemperatureSummary == null) {
                                    backupMap.put(backupKey, new TemperatureSummary(thisPlace, num, num, num, 1));
                                }
                                else {
                                    backupExistingTemperatureSummary.max = (Math.max(num, backupExistingTemperatureSummary.max));
                                    backupExistingTemperatureSummary.min = (Math.min(num, backupExistingTemperatureSummary.min));
                                    backupExistingTemperatureSummary.total += num;
                                    backupExistingTemperatureSummary.sampleCount++;
                                }
                                /*
                                 * End slow block
                                 */
                            }

                            existingTemperatureSummary.max = (Math.max(num, existingTemperatureSummary.max));
                            existingTemperatureSummary.min = (Math.min(num, existingTemperatureSummary.min));
                            existingTemperatureSummary.total += num;
                            existingTemperatureSummary.sampleCount++;
                        }

                        processingPlaceName = true;
                        placeNameIdx = 0;
                        digitIdx = 0;
                    }
                    else if (b == ';') {
                        processingPlaceName = false;
                    }
                    else if (processingPlaceName) {
                        placeName[placeNameIdx++] = b;
                    }
                    else {
                        digits[digitIdx++] = b;
                    }
                }
            }));

        }

        if (DEBUG) {
            System.out.println("STARTED THREADS: " + (System.currentTimeMillis() - time));
        }

        for (Thread thread : threads) {
            thread.join();
        }

        TreeMap<String, TemperatureSummary> mergedMap = new TreeMap<>();

        for (var map : maps) {
            for (TemperatureSummary t1 : map.values()) {
                if (t1 == null)
                    continue;

                var t2 = mergedMap.get(new String(t1.name));

                if (t2 == null) {
                    mergedMap.put(new String(t1.name), t1);
                }
                else {
                    var merged = new TemperatureSummary(t1.name, Math.min(t1.min, t2.min), Math.max(t1.max, t2.max), t1.total + t2.total,
                            t1.sampleCount + t2.sampleCount);
                    mergedMap.put(new String(t1.name), merged);
                }
            }
        }

        boolean first = true;
        StringBuilder output = new StringBuilder(16_000);
        output.append("{");
        for (var value : new TreeSet<>(mergedMap.values())) {
            if (first) {
                first = false;
            }
            else {
                output.append(", ");
            }
            output.append(new String(value.name)).append("=").append((double) value.min / 10).append("/")
                    .append(String.format("%.1f", ((double) value.total / value.sampleCount / 10))).append("/").append((double) value.max / 10);
        }
        output.append("}");

        System.out.println(output);
        if (DEBUG)
            System.out.println("CORRECT: " + output.toString().equals(CORRECT));

        if (DEBUG)
            System.out.println("TOTAL TIME: " + (System.currentTimeMillis() - time));

    }

    private static int parse(byte[] arr, int len) {
        // TODO: SIMD?
        return Integer.parseInt(new String(arr, 0, len).replace(".", ""));
    }

    static final String CORRECT = "{Abha=-34.8/18.0/67.0, Abidjan=-21.6/26.0/75.3, Abéché=-20.3/29.4/79.4, Accra=-22.9/26.4/74.3, Addis Ababa=-32.7/16.0/67.7, Adelaide=-33.3/17.3/66.7, Aden=-22.2/29.1/79.5, Ahvaz=-24.1/25.4/81.1, Albuquerque=-36.7/14.0/68.0, Alexandra=-42.4/11.0/60.1, Alexandria=-33.7/20.0/70.0, Algiers=-33.0/18.2/67.2, Alice Springs=-32.5/21.0/69.8, Almaty=-38.4/10.0/56.6, Amsterdam=-40.1/10.2/60.4, Anadyr=-59.3/-6.9/43.5, Anchorage=-46.2/2.8/51.8, Andorra la Vella=-44.1/9.8/62.3, Ankara=-33.7/12.0/65.9, Antananarivo=-34.8/17.9/68.8, Antsiranana=-23.7/25.2/74.3, Arkhangelsk=-48.0/1.3/50.4, Ashgabat=-31.0/17.1/66.5, Asmara=-31.5/15.6/64.3, Assab=-19.3/30.5/78.1, Astana=-48.8/3.5/51.5, Athens=-34.7/19.2/70.3, Atlanta=-30.2/17.0/66.5, Auckland=-38.5/15.2/62.6, Austin=-31.6/20.7/70.3, Baghdad=-27.2/22.8/70.9, Baguio=-30.0/19.5/71.5, Baku=-34.9/15.1/63.6, Baltimore=-38.9/13.1/62.1, Bamako=-22.0/27.8/78.3, Bangkok=-18.7/28.6/77.6, Bangui=-23.1/26.0/74.0, Banjul=-21.0/26.0/75.8, Barcelona=-33.2/18.2/72.0, Bata=-25.9/25.1/74.8, Batumi=-35.8/14.0/63.6, Beijing=-38.6/12.9/61.8, Beirut=-30.6/20.9/71.5, Belgrade=-35.7/12.5/64.0, Belize City=-22.2/26.7/78.1, Benghazi=-26.5/19.9/70.4, Bergen=-39.2/7.7/55.5, Berlin=-40.2/10.3/60.2, Bilbao=-34.4/14.7/66.0, Birao=-27.4/26.5/77.1, Bishkek=-38.6/11.3/59.9, Bissau=-29.0/27.0/74.9, Blantyre=-28.3/22.2/70.0, Bloemfontein=-36.2/15.6/67.5, Boise=-35.8/11.4/59.3, Bordeaux=-36.3/14.2/64.5, Bosaso=-17.7/30.0/80.8, Boston=-39.7/10.9/58.5, Bouaké=-29.0/26.0/76.2, Bratislava=-40.5/10.5/60.7, Brazzaville=-22.8/25.0/77.4, Bridgetown=-22.0/27.0/75.8, Brisbane=-28.1/21.4/68.7, Brussels=-36.6/10.5/61.6, Bucharest=-40.1/10.8/61.2, Budapest=-38.0/11.3/59.2, Bujumbura=-27.9/23.8/76.2, Bulawayo=-36.6/18.9/67.5, Burnie=-41.8/13.1/60.8, Busan=-36.4/15.0/65.0, Cabo San Lucas=-27.1/23.9/74.1, Cairns=-27.0/25.0/74.2, Cairo=-31.7/21.4/77.7, Calgary=-42.6/4.4/52.2, Canberra=-34.1/13.1/63.5, Cape Town=-35.0/16.2/64.3, Changsha=-33.2/17.4/76.3, Charlotte=-34.4/16.1/67.6, Chiang Mai=-23.0/25.8/71.3, Chicago=-44.3/9.8/57.5, Chihuahua=-27.7/18.6/74.1, Chittagong=-23.5/25.9/76.9, Chișinău=-38.4/10.2/61.1, Chongqing=-35.2/18.6/67.4, Christchurch=-41.1/12.2/61.5, City of San Marino=-36.5/11.8/63.6, Colombo=-21.2/27.4/80.7, Columbus=-36.5/11.7/59.8, Conakry=-27.1/26.4/75.8, Copenhagen=-38.4/9.1/58.0, Cotonou=-22.5/27.2/74.7, Cracow=-40.5/9.3/58.3, Da Lat=-29.3/17.9/67.6, Da Nang=-23.6/25.8/79.1, Dakar=-25.6/24.0/74.3, Dallas=-29.8/19.0/66.2, Damascus=-34.6/17.0/66.4, Dampier=-22.8/26.4/96.1, Dar es Salaam=-27.9/25.8/77.5, Darwin=-20.5/27.6/75.2, Denpasar=-28.3/23.7/78.4, Denver=-44.5/10.4/59.8, Detroit=-41.1/10.0/61.5, Dhaka=-22.4/25.9/75.2, Dikson=-62.7/-11.1/42.2, Dili=-22.1/26.6/76.5, Djibouti=-21.0/29.9/78.8, Dodoma=-30.6/22.7/70.8, Dolisie=-24.1/24.0/73.3, Douala=-30.0/26.7/75.5, Dubai=-21.0/26.9/74.3, Dublin=-42.6/9.8/60.0, Dunedin=-39.0/11.1/61.5, Durban=-27.0/20.6/69.1, Dushanbe=-34.2/14.7/70.3, Edinburgh=-40.6/9.3/57.5, Edmonton=-50.4/4.2/55.6, El Paso=-37.0/18.1/71.1, Entebbe=-27.6/21.0/73.7, Erbil=-33.5/19.5/71.0, Erzurum=-44.9/5.1/51.0, Fairbanks=-52.3/-2.3/49.6, Fianarantsoa=-32.0/17.9/70.3, Flores,  Petén=-23.6/26.4/74.6, Frankfurt=-37.8/10.6/61.0, Fresno=-39.3/17.9/66.3, Fukuoka=-34.8/17.0/64.9, Gaborone=-27.8/21.0/72.8, Gabès=-30.8/19.5/69.9, Gagnoa=-24.1/26.0/77.2, Gangtok=-34.0/15.2/64.4, Garissa=-17.4/29.3/88.0, Garoua=-23.3/28.3/76.7, George Town=-28.9/27.9/77.2, Ghanzi=-26.1/21.4/73.1, Gjoa Haven=-63.6/-14.4/45.2, Guadalajara=-28.8/20.9/70.1, Guangzhou=-23.9/22.4/75.6, Guatemala City=-27.4/20.4/72.2, Halifax=-44.7/7.5/58.6, Hamburg=-39.9/9.7/56.3, Hamilton=-35.0/13.8/66.1, Hanga Roa=-31.2/20.5/67.7, Hanoi=-26.5/23.6/71.7, Harare=-35.5/18.4/67.9, Harbin=-45.5/5.0/58.2, Hargeisa=-26.8/21.7/73.4, Hat Yai=-21.9/27.0/77.7, Havana=-24.5/25.2/75.9, Helsinki=-45.7/5.9/55.7, Heraklion=-31.3/18.9/72.8, Hiroshima=-39.0/16.3/65.8, Ho Chi Minh City=-22.8/27.4/78.5, Hobart=-42.8/12.7/62.5, Hong Kong=-27.9/23.3/75.3, Honiara=-23.5/26.5/75.4, Honolulu=-25.3/25.4/73.9, Houston=-28.5/20.8/72.3, Ifrane=-35.1/11.4/63.0, Indianapolis=-42.5/11.8/61.1, Iqaluit=-58.5/-9.3/41.1, Irkutsk=-48.7/1.0/49.6, Istanbul=-39.6/13.9/64.4, Jacksonville=-29.3/20.3/74.3, Jakarta=-24.2/26.7/77.7, Jayapura=-25.1/27.0/76.9, Jerusalem=-32.6/18.3/70.0, Johannesburg=-34.2/15.5/66.6, Jos=-27.5/22.8/73.1, Juba=-18.9/27.8/80.0, Kabul=-39.6/12.1/65.1, Kampala=-28.3/20.0/74.6, Kandi=-23.1/27.7/88.1, Kankan=-27.0/26.5/74.7, Kano=-21.6/26.4/74.3, Kansas City=-41.3/12.5/63.2, Karachi=-21.9/26.0/76.7, Karonga=-24.1/24.4/76.4, Kathmandu=-28.8/18.3/66.8, Khartoum=-19.6/29.9/78.1, Kingston=-23.5/27.4/73.7, Kinshasa=-22.5/25.3/76.3, Kolkata=-21.8/26.7/81.9, Kuala Lumpur=-22.0/27.3/80.2, Kumasi=-23.6/26.0/75.9, Kunming=-36.7/15.7/66.7, Kuopio=-44.8/3.4/54.8, Kuwait City=-23.7/25.7/79.4, Kyiv=-41.2/8.4/58.1, Kyoto=-38.0/15.8/70.4, La Ceiba=-26.6/26.2/77.8, La Paz=-25.7/23.7/74.3, Lagos=-21.4/26.8/77.9, Lahore=-26.5/24.3/74.4, Lake Havasu City=-31.1/23.7/75.6, Lake Tekapo=-44.5/8.7/64.3, Las Palmas de Gran Canaria=-29.0/21.2/72.3, Las Vegas=-32.9/20.3/69.8, Launceston=-34.3/13.1/64.7, Lhasa=-41.0/7.6/57.7, Libreville=-26.0/25.9/76.6, Lisbon=-41.1/17.5/68.1, Livingstone=-24.4/21.8/72.3, Ljubljana=-38.6/10.9/60.7, Lodwar=-18.9/29.3/77.9, Lomé=-24.2/26.9/83.4, London=-38.1/11.3/63.7, Los Angeles=-29.7/18.6/69.3, Louisville=-38.1/13.9/67.0, Luanda=-24.9/25.8/76.9, Lubumbashi=-27.0/20.8/69.1, Lusaka=-26.3/19.9/68.9, Luxembourg City=-43.7/9.3/60.1, Lviv=-43.2/7.8/57.7, Lyon=-38.0/12.5/65.7, Madrid=-36.9/15.0/70.5, Mahajanga=-23.0/26.3/80.9, Makassar=-22.0/26.7/78.4, Makurdi=-22.2/26.0/79.6, Malabo=-24.1/26.3/74.3, Malé=-22.9/28.0/75.0, Managua=-28.0/27.3/77.4, Manama=-25.3/26.5/77.2, Mandalay=-20.0/28.0/76.6, Mango=-27.3/28.1/81.6, Manila=-25.7/28.4/81.6, Maputo=-26.2/22.8/70.7, Marrakesh=-30.0/19.6/68.9, Marseille=-35.4/15.8/65.1, Maun=-27.9/22.4/72.5, Medan=-23.6/26.5/77.6, Mek'ele=-32.0/22.7/71.4, Melbourne=-33.4/15.1/63.2, Memphis=-34.4/17.2/67.7, Mexicali=-26.5/23.1/72.2, Mexico City=-34.9/17.5/66.1, Miami=-25.2/24.9/71.6, Milan=-35.7/13.0/62.1, Milwaukee=-42.3/8.9/58.9, Minneapolis=-42.0/7.8/56.7, Minsk=-41.2/6.7/57.6, Mogadishu=-20.9/27.1/79.0, Mombasa=-25.3/26.3/80.3, Monaco=-34.6/16.4/66.5, Moncton=-50.1/6.1/54.0, Monterrey=-28.7/22.3/70.9, Montreal=-43.6/6.8/57.2, Moscow=-45.2/5.8/53.6, Mumbai=-23.1/27.1/81.3, Murmansk=-50.7/0.6/48.5, Muscat=-23.4/28.0/81.4, Mzuzu=-31.1/17.7/68.2, N'Djamena=-25.2/28.3/79.6, Naha=-29.9/23.1/74.4, Nairobi=-32.4/17.8/67.4, Nakhon Ratchasima=-24.6/27.3/90.1, Napier=-33.9/14.6/66.1, Napoli=-38.2/15.9/67.4, Nashville=-38.5/15.4/70.3, Nassau=-25.0/24.6/84.0, Ndola=-30.7/20.3/72.3, New Delhi=-24.3/25.0/75.4, New Orleans=-31.8/20.7/72.6, New York City=-40.2/12.9/62.3, Ngaoundéré=-29.2/22.0/76.6, Niamey=-20.5/29.3/82.2, Nicosia=-30.2/19.7/66.3, Niigata=-37.7/13.9/65.3, Nouadhibou=-28.0/21.3/71.3, Nouakchott=-22.1/25.7/72.1, Novosibirsk=-48.3/1.7/55.1, Nuuk=-51.8/-1.4/53.8, Odesa=-38.7/10.7/60.7, Odienné=-22.5/26.0/76.7, Oklahoma City=-38.0/15.9/65.3, Omaha=-40.2/10.6/63.0, Oranjestad=-22.7/28.1/78.6, Oslo=-45.1/5.7/56.3, Ottawa=-41.5/6.6/54.3, Ouagadougou=-19.9/28.3/80.3, Ouahigouya=-25.2/28.6/77.3, Ouarzazate=-29.0/18.9/67.8, Oulu=-47.8/2.7/59.7, Palembang=-19.4/27.3/80.4, Palermo=-32.6/18.5/69.9, Palm Springs=-26.3/24.5/77.9, Palmerston North=-36.7/13.2/63.3, Panama City=-21.9/28.0/78.1, Parakou=-21.7/26.8/75.9, Paris=-41.4/12.3/61.9, Perth=-32.5/18.7/67.0, Petropavlovsk-Kamchatsky=-48.2/1.9/52.0, Philadelphia=-34.4/13.2/66.1, Phnom Penh=-21.2/28.3/79.2, Phoenix=-25.1/23.9/77.7, Pittsburgh=-43.5/10.8/63.1, Podgorica=-33.2/15.3/66.2, Pointe-Noire=-24.4/26.1/75.9, Pontianak=-27.4/27.7/78.5, Port Moresby=-22.1/26.9/75.9, Port Sudan=-19.4/28.4/80.9, Port Vila=-28.8/24.3/74.7, Port-Gentil=-24.1/26.0/77.4, Portland (OR)=-37.1/12.4/61.0, Porto=-35.7/15.7/65.2, Prague=-43.1/8.4/58.8, Praia=-24.8/24.4/74.8, Pretoria=-35.8/18.2/65.7, Pyongyang=-43.5/10.8/62.2, Rabat=-32.1/17.2/69.5, Rangpur=-27.4/24.4/73.9, Reggane=-22.8/28.3/83.2, Reykjavík=-47.5/4.3/53.8, Riga=-51.7/6.2/54.6, Riyadh=-22.5/26.0/75.2, Rome=-33.8/15.2/68.7, Roseau=-20.9/26.2/74.6, Rostov-on-Don=-41.8/9.9/60.8, Sacramento=-35.6/16.3/69.8, Saint Petersburg=-43.6/5.8/58.2, Saint-Pierre=-43.5/5.7/55.5, Salt Lake City=-37.8/11.6/60.3, San Antonio=-28.9/20.8/73.6, San Diego=-32.8/17.8/65.3, San Francisco=-34.4/14.6/66.9, San Jose=-31.5/16.4/67.1, San José=-26.4/22.6/73.9, San Juan=-23.4/27.2/79.2, San Salvador=-26.8/23.1/71.6, Sana'a=-31.3/20.0/71.7, Santo Domingo=-22.9/25.9/77.1, Sapporo=-40.6/8.9/57.0, Sarajevo=-39.7/10.1/59.8, Saskatoon=-47.3/3.3/53.4, Seattle=-42.2/11.3/62.7, Seoul=-38.0/12.5/63.9, Seville=-27.1/19.2/68.1, Shanghai=-31.0/16.7/64.9, Singapore=-24.5/27.0/79.8, Skopje=-37.3/12.4/61.0, Sochi=-39.3/14.2/61.9, Sofia=-38.8/10.6/58.0, Sokoto=-21.1/28.0/80.0, Split=-33.8/16.1/66.5, St. John's=-44.2/5.0/52.5, St. Louis=-40.2/13.9/67.1, Stockholm=-41.2/6.6/61.6, Surabaya=-20.3/27.1/75.6, Suva=-23.3/25.6/77.2, Suwałki=-44.0/7.2/59.9, Sydney=-38.0/17.7/67.6, Ségou=-18.9/28.0/75.5, Tabora=-29.3/23.0/73.5, Tabriz=-38.0/12.6/62.3, Taipei=-30.1/23.0/73.1, Tallinn=-40.9/6.4/60.6, Tamale=-23.5/27.9/77.8, Tamanrasset=-28.3/21.7/75.2, Tampa=-29.6/22.9/73.7, Tashkent=-33.9/14.8/62.0, Tauranga=-36.9/14.8/63.5, Tbilisi=-37.9/12.9/70.2, Tegucigalpa=-33.6/21.7/70.1, Tehran=-34.0/17.0/69.2, Tel Aviv=-30.9/20.0/74.0, Thessaloniki=-35.6/16.0/67.7, Thiès=-25.2/24.0/73.1, Tijuana=-29.8/17.8/69.2, Timbuktu=-20.7/28.0/77.6, Tirana=-33.8/15.2/66.5, Toamasina=-24.6/23.4/72.8, Tokyo=-35.6/15.4/67.1, Toliara=-22.6/24.1/75.0, Toluca=-38.6/12.4/64.7, Toronto=-41.8/9.4/57.7, Tripoli=-28.0/20.0/77.1, Tromsø=-47.3/2.9/53.1, Tucson=-29.8/20.9/72.3, Tunis=-28.4/18.4/66.6, Ulaanbaatar=-46.9/-0.4/51.3, Upington=-31.7/20.4/72.6, Vaduz=-38.7/10.1/66.2, Valencia=-30.3/18.3/71.2, Valletta=-32.3/18.8/74.6, Vancouver=-42.1/10.4/66.7, Veracruz=-22.0/25.4/82.6, Vienna=-39.9/10.4/58.9, Vientiane=-34.4/25.9/78.6, Villahermosa=-20.9/27.1/77.9, Vilnius=-44.7/6.0/55.8, Virginia Beach=-33.9/15.8/65.2, Vladivostok=-44.2/4.9/53.1, Warsaw=-42.1/8.5/61.0, Washington, D.C.=-35.0/14.6/66.6, Wau=-25.2/27.8/79.0, Wellington=-35.3/12.9/60.4, Whitehorse=-48.3/-0.1/53.4, Wichita=-35.6/13.9/64.2, Willemstad=-21.2/28.0/75.6, Winnipeg=-48.8/3.0/53.8, Wrocław=-40.0/9.6/57.0, Xi'an=-41.0/14.1/60.8, Yakutsk=-59.2/-8.8/39.4, Yangon=-23.9/27.5/77.7, Yaoundé=-29.0/23.8/73.6, Yellowknife=-52.9/-4.3/48.1, Yerevan=-37.4/12.4/64.3, Yinchuan=-40.9/9.0/58.1, Zagreb=-38.0/10.7/61.5, Zanzibar City=-22.7/26.0/76.3, Zürich=-39.8/9.3/59.5, Ürümqi=-41.8/7.4/55.3, İzmir=-32.1/17.9/66.1}";

}
