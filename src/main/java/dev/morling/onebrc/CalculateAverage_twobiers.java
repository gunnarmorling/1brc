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

import static java.util.stream.Collectors.groupingBy;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.StreamSupport;

public class CalculateAverage_twobiers {

    private static final String FILE = "./measurements.txt";
    private static final FastAveragingCollector FAST_AVERAGING_COLLECTOR = new FastAveragingCollector();
    // private static final VectorSpecies<Integer> SPECIES = IntVector.SPECIES_256;

    private static class FastAveragingCollector implements Collector<Measurement, double[], String> {
        @Override
        public Supplier<double[]> supplier() {
            // 0: current sum
            // 1: count
            // 2: current max
            // 3: current min
            return () -> new double[4];
        }

        @Override
        public BiConsumer<double[], Measurement> accumulator() {
            return (a, t) -> {
                double val = t.value();

                a[0] += val;
                a[1]++;

                if (val > a[2] || a[1] == 1) {
                    a[2] = val;
                }
                if (val < a[3] || a[1] == 1) {
                    a[3] = val;
                }
            };
        }

        @Override
        public BinaryOperator<double[]> combiner() {
            return (a, b) -> {
                a[0] += b[0];
                a[1] += b[1];
                if (b[2] > a[2]) {
                    a[2] = b[2];
                }
                if (b[3] < a[3]) {
                    a[3] = b[3];
                }
                return a;
            };
        }

        @Override
        public Function<double[], String> finisher() {
            return a -> {
                var mean = (a[1] == 0) ? 0.0d : Math.round((a[0] / a[1]) * 10.0) / 10.0;
                var max = a[2];
                var min = a[3];
                return min + "/" + mean + "/" + max;
            };
        }

        @Override
        public Set<Characteristics> characteristics() {
            return Collections.emptySet();
        }
    }

    private static class FileChannelIterator implements Iterator<ByteBuffer> {
        // File Size will be approx. 14G, 1MB Chunks sound kinda reasonable
        private static final long CHUNK_SIZE = (long) Math.pow(2, 20);

        private final FileChannel fileChannel;
        private final long size;
        private long bytesRead = 0;

        public FileChannelIterator(FileChannel fileChannel) {
            this.fileChannel = fileChannel;
            try {
                this.size = fileChannel.size();
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public boolean hasNext() {
            return bytesRead < size;
        }

        @Override
        public ByteBuffer next() {
            try {
                MappedByteBuffer mappedByteBuffer = fileChannel.map(MapMode.READ_ONLY, bytesRead, Math.min(CHUNK_SIZE, size - bytesRead));

                // Ensure the chunks will end on a newline
                int realEnd = mappedByteBuffer.limit() - 1;
                while (mappedByteBuffer.get(realEnd) != '\n') {
                    realEnd--;
                }
                mappedByteBuffer.limit(++realEnd);
                bytesRead += realEnd;

                return mappedByteBuffer;
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private record Measurement(String station, double value) {
    }

    public static void main(String[] args) throws IOException {
        try (
                var file = new RandomAccessFile(FILE, "r");
                var channel = file.getChannel();) {
            TreeMap<String, String> measurements = StreamSupport.stream(
                    Spliterators.spliteratorUnknownSize(new FileChannelIterator(channel),
                            Spliterator.IMMUTABLE),
                    true)
                    .flatMap(a -> parseMeasurements(a).stream())
                    .collect(
                            groupingBy(
                                    Measurement::station,
                                    TreeMap::new,
                                    FAST_AVERAGING_COLLECTOR));

            System.out.println(measurements);

            // Simple test for my generated file
            assert measurements.toString().equals(
                    "{Abha=-30.1/18.0/72.2, Abidjan=-23.4/26.0/79.9, Abéché=-17.3/29.4/80.5, Accra=-24.3/26.4/77.9, Addis Ababa=-33.6/16.0/65.9, Adelaide=-33.7/17.3/71.1, Aden=-21.2/29.1/81.2, Ahvaz=-30.2/25.4/80.2, Albuquerque=-39.1/14.0/60.9, Alexandra=-36.6/11.0/62.9, Alexandria=-30.4/20.0/69.8, Algiers=-33.1/18.2/68.4, Alice Springs=-27.2/21.0/72.1, Almaty=-40.5/10.0/58.5, Amsterdam=-38.9/10.2/58.9, Anadyr=-58.2/-6.9/39.9, Anchorage=-45.3/2.8/54.3, Andorra la Vella=-42.9/9.8/60.6, Ankara=-36.7/12.0/63.7, Antananarivo=-30.9/17.9/70.8, Antsiranana=-23.0/25.2/75.4, Arkhangelsk=-48.2/1.3/51.4, Ashgabat=-30.4/17.1/64.9, Asmara=-32.6/15.6/65.4, Assab=-21.2/30.5/78.7, Astana=-45.5/3.5/49.3, Athens=-28.5/19.2/68.0, Atlanta=-33.3/17.0/69.7, Auckland=-33.6/15.2/65.0, Austin=-32.1/20.7/68.5, Baghdad=-29.6/22.8/71.6, Baguio=-29.9/19.5/68.7, Baku=-32.1/15.1/66.8, Baltimore=-36.4/13.1/62.8, Bamako=-27.0/27.8/82.0, Bangkok=-20.5/28.6/79.0, Bangui=-24.2/26.0/73.8, Banjul=-27.0/26.0/75.4, Barcelona=-36.1/18.2/74.0, Bata=-25.9/25.1/73.7, Batumi=-36.8/14.0/65.7, Beijing=-34.2/12.9/65.9, Beirut=-28.9/20.9/68.5, Belgrade=-35.7/12.5/65.5, Belize City=-24.8/26.7/75.3, Benghazi=-28.6/19.9/68.9, Bergen=-48.8/7.7/58.5, Berlin=-48.1/10.3/62.5, Bilbao=-34.5/14.7/65.2, Birao=-22.7/26.5/77.9, Bishkek=-37.2/11.3/62.1, Bissau=-24.1/27.0/82.6, Blantyre=-29.2/22.2/68.9, Bloemfontein=-32.1/15.6/66.1, Boise=-36.6/11.4/62.2, Bordeaux=-34.2/14.2/63.0, Bosaso=-20.8/30.0/77.2, Boston=-39.8/10.9/58.4, Bouaké=-24.8/26.0/76.8, Bratislava=-40.5/10.5/62.6, Brazzaville=-29.3/25.0/74.6, Bridgetown=-25.0/27.0/78.4, Brisbane=-27.2/21.4/69.7, Brussels=-37.0/10.5/56.6, Bucharest=-48.7/10.8/60.8, Budapest=-39.6/11.3/61.6, Bujumbura=-26.3/23.8/73.8, Bulawayo=-32.8/18.9/68.5, Burnie=-35.3/13.1/64.4, Busan=-34.8/15.0/62.8, Cabo San Lucas=-25.0/23.9/71.6, Cairns=-23.1/25.0/74.3, Cairo=-27.6/21.4/68.6, Calgary=-46.9/4.4/56.2, Canberra=-36.9/13.1/63.4, Cape Town=-33.1/16.2/63.5, Changsha=-33.3/17.4/64.8, Charlotte=-33.7/16.1/63.5, Chiang Mai=-26.4/25.8/76.2, Chicago=-39.7/9.8/60.6, Chihuahua=-33.0/18.6/72.4, Chittagong=-20.8/25.9/76.1, Chișinău=-39.5/10.2/59.2, Chongqing=-30.8/18.6/71.4, Christchurch=-37.9/12.2/62.1, City of San Marino=-40.0/11.8/64.1, Colombo=-27.2/27.4/78.0, Columbus=-39.6/11.7/63.3, Conakry=-29.0/26.4/77.7, Copenhagen=-41.2/9.1/59.8, Cotonou=-24.6/27.2/80.1, Cracow=-39.9/9.3/63.2, Da Lat=-29.9/17.9/66.6, Da Nang=-23.4/25.8/74.5, Dakar=-27.8/24.0/74.8, Dallas=-33.8/19.0/69.2, Damascus=-33.5/17.0/68.5, Dampier=-25.0/26.4/75.8, Dar es Salaam=-22.6/25.8/78.6, Darwin=-22.0/27.6/75.4, Denpasar=-24.0/23.7/76.5, Denver=-38.2/10.4/60.4, Detroit=-38.0/10.0/60.0, Dhaka=-24.9/25.9/75.7, Dikson=-59.6/-11.1/39.0, Dili=-23.6/26.6/76.5, Djibouti=-18.9/29.9/77.8, Dodoma=-26.5/22.7/73.5, Dolisie=-24.4/24.0/71.9, Douala=-23.0/26.7/77.2, Dubai=-20.8/26.9/79.8, Dublin=-38.2/9.8/62.2, Dunedin=-39.3/11.1/61.3, Durban=-26.3/20.6/73.5, Dushanbe=-33.2/14.7/66.6, Edinburgh=-45.9/9.3/58.6, Edmonton=-41.7/4.2/56.0, El Paso=-29.6/18.1/66.5, Entebbe=-27.0/21.0/72.7, Erbil=-31.8/19.5/71.9, Erzurum=-47.0/5.1/55.4, Fairbanks=-48.9/-2.3/47.3, Fianarantsoa=-33.2/17.9/68.1, Flores,  Petén=-20.9/26.4/74.6, Frankfurt=-39.5/10.6/58.5, Fresno=-31.8/17.9/65.9, Fukuoka=-33.1/17.0/70.7, Gaborone=-29.0/21.0/73.1, Gabès=-34.3/19.5/70.3, Gagnoa=-24.6/26.0/80.8, Gangtok=-36.3/15.2/67.1, Garissa=-21.1/29.3/80.8, Garoua=-23.0/28.3/79.0, George Town=-23.4/27.9/78.1, Ghanzi=-33.3/21.4/73.2, Gjoa Haven=-64.9/-14.4/36.4, Guadalajara=-27.8/20.9/72.2, Guangzhou=-27.1/22.4/71.1, Guatemala City=-28.9/20.4/77.1, Halifax=-43.4/7.5/57.7, Hamburg=-41.5/9.7/65.0, Hamilton=-35.1/13.8/69.3, Hanga Roa=-29.1/20.5/70.8, Hanoi=-25.8/23.6/75.2, Harare=-33.2/18.4/69.1, Harbin=-45.1/5.0/55.0, Hargeisa=-33.4/21.7/71.1, Hat Yai=-22.5/27.0/74.3, Havana=-24.9/25.2/74.1, Helsinki=-42.3/5.9/55.2, Heraklion=-30.4/18.9/67.2, Hiroshima=-34.0/16.3/67.0, Ho Chi Minh City=-18.5/27.4/77.0, Hobart=-36.9/12.7/59.6, Hong Kong=-25.9/23.3/79.9, Honiara=-21.5/26.5/75.6, Honolulu=-22.9/25.4/73.7, Houston=-29.9/20.8/69.7, Ifrane=-36.8/11.4/59.3, Indianapolis=-39.8/11.8/58.4, Iqaluit=-58.2/-9.3/40.9, Irkutsk=-46.4/1.0/52.9, Istanbul=-35.2/13.9/61.9, Jacksonville=-29.4/20.3/72.0, Jakarta=-25.8/26.7/78.1, Jayapura=-21.2/27.0/74.9, Jerusalem=-31.8/18.3/66.8, Johannesburg=-32.2/15.5/63.9, Jos=-28.2/22.8/71.2, Juba=-21.4/27.8/76.2, Kabul=-37.5/12.1/61.9, Kampala=-33.6/20.0/70.1, Kandi=-23.3/27.7/77.6, Kankan=-24.6/26.5/82.7, Kano=-25.7/26.4/78.4, Kansas City=-38.7/12.5/60.9, Karachi=-24.5/26.0/74.4, Karonga=-22.5/24.4/77.5, Kathmandu=-37.5/18.3/67.3, Khartoum=-19.5/29.9/83.9, Kingston=-30.9/27.4/76.1, Kinshasa=-26.0/25.3/78.9, Kolkata=-23.0/26.7/76.4, Kuala Lumpur=-22.1/27.3/78.0, Kumasi=-22.4/26.0/80.3, Kunming=-33.1/15.7/66.1, Kuopio=-50.4/3.4/53.6, Kuwait City=-27.0/25.7/78.7, Kyiv=-43.4/8.4/57.5, Kyoto=-33.9/15.8/65.8, La Ceiba=-24.1/26.2/77.2, La Paz=-26.5/23.7/76.0, Lagos=-24.4/26.8/75.9, Lahore=-25.8/24.3/71.6, Lake Havasu City=-25.3/23.7/72.4, Lake Tekapo=-41.3/8.7/58.3, Las Palmas de Gran Canaria=-39.1/21.2/71.0, Las Vegas=-28.4/20.3/76.1, Launceston=-34.4/13.1/66.0, Lhasa=-41.2/7.6/57.5, Libreville=-24.0/25.9/73.8, Lisbon=-36.4/17.5/74.4, Livingstone=-27.5/21.8/77.2, Ljubljana=-36.4/10.9/61.4, Lodwar=-21.2/29.3/77.3, Lomé=-24.0/26.9/78.7, London=-38.3/11.3/63.4, Los Angeles=-29.8/18.6/67.1, Louisville=-40.0/13.9/61.7, Luanda=-22.7/25.8/75.3, Lubumbashi=-28.9/20.8/72.4, Lusaka=-29.1/19.9/75.4, Luxembourg City=-42.5/9.3/61.9, Lviv=-42.0/7.8/57.5, Lyon=-36.6/12.5/61.3, Madrid=-41.5/15.0/65.8, Mahajanga=-21.3/26.3/76.7, Makassar=-24.0/26.7/78.5, Makurdi=-21.6/26.0/76.9, Malabo=-20.1/26.3/73.1, Malé=-24.4/28.0/79.3, Managua=-26.7/27.3/78.7, Manama=-27.2/26.5/82.0, Mandalay=-22.3/28.0/76.9, Mango=-19.7/28.1/77.3, Manila=-27.7/28.4/76.9, Maputo=-31.7/22.8/71.0, Marrakesh=-28.0/19.6/74.1, Marseille=-39.0/15.8/66.1, Maun=-28.2/22.4/69.5, Medan=-24.4/26.5/78.9, Mek'ele=-28.4/22.7/73.7, Melbourne=-32.9/15.1/71.6, Memphis=-34.9/17.2/70.0, Mexicali=-31.9/23.1/71.6, Mexico City=-31.1/17.5/69.5, Miami=-24.6/24.9/75.0, Milan=-37.9/13.0/64.9, Milwaukee=-42.9/8.9/58.9, Minneapolis=-46.9/7.8/58.7, Minsk=-46.3/6.7/56.7, Mogadishu=-20.7/27.1/80.1, Mombasa=-21.4/26.3/77.4, Monaco=-34.1/16.4/66.0, Moncton=-42.7/6.1/53.7, Monterrey=-25.2/22.3/72.4, Montreal=-43.2/6.8/56.2, Moscow=-43.0/5.8/58.1, Mumbai=-22.5/27.1/78.4, Murmansk=-49.8/0.6/49.2, Muscat=-23.5/28.0/79.8, Mzuzu=-34.0/17.7/71.2, N'Djamena=-22.5/28.3/81.0, Naha=-24.9/23.1/71.0, Nairobi=-31.3/17.8/68.2, Nakhon Ratchasima=-29.4/27.3/75.4, Napier=-37.5/14.6/66.0, Napoli=-30.2/15.9/66.8, Nashville=-33.2/15.4/61.3, Nassau=-23.5/24.6/73.9, Ndola=-29.0/20.3/72.7, New Delhi=-23.4/25.0/73.4, New Orleans=-27.6/20.7/72.3, New York City=-36.1/12.9/63.9, Ngaoundéré=-25.2/22.0/73.8, Niamey=-20.5/29.3/79.3, Nicosia=-34.4/19.7/68.1, Niigata=-40.0/13.9/70.6, Nouadhibou=-27.3/21.3/76.0, Nouakchott=-23.3/25.7/84.5, Novosibirsk=-49.9/1.7/53.6, Nuuk=-52.0/-1.4/48.4, Odesa=-38.3/10.7/60.8, Odienné=-25.6/26.0/77.7, Oklahoma City=-31.5/15.9/64.4, Omaha=-38.6/10.6/57.4, Oranjestad=-24.7/28.1/78.5, Oslo=-43.8/5.7/56.2, Ottawa=-46.2/6.6/60.6, Ouagadougou=-22.3/28.3/76.2, Ouahigouya=-28.9/28.6/76.5, Ouarzazate=-32.1/18.9/66.6, Oulu=-51.5/2.7/53.0, Palembang=-20.6/27.3/76.1, Palermo=-33.8/18.5/68.3, Palm Springs=-25.4/24.5/71.9, Palmerston North=-38.8/13.2/60.8, Panama City=-22.3/28.0/76.3, Parakou=-26.4/26.8/76.6, Paris=-37.9/12.3/62.5, Perth=-29.8/18.7/67.9, Petropavlovsk-Kamchatsky=-47.7/1.9/56.0, Philadelphia=-34.1/13.2/60.1, Phnom Penh=-23.1/28.3/79.2, Phoenix=-26.8/23.9/72.6, Pittsburgh=-41.3/10.8/61.2, Podgorica=-32.4/15.3/65.3, Pointe-Noire=-24.8/26.1/74.1, Pontianak=-22.9/27.7/76.3, Port Moresby=-23.2/26.9/77.7, Port Sudan=-20.3/28.4/84.2, Port Vila=-23.2/24.3/74.7, Port-Gentil=-22.9/26.0/77.3, Portland (OR)=-36.2/12.4/61.0, Porto=-30.6/15.7/69.7, Prague=-46.5/8.4/57.3, Praia=-22.2/24.4/80.3, Pretoria=-34.0/18.2/66.9, Pyongyang=-41.5/10.8/59.6, Rabat=-34.9/17.2/68.1, Rangpur=-27.2/24.4/74.9, Reggane=-22.9/28.3/77.8, Reykjavík=-45.4/4.3/55.7, Riga=-39.8/6.2/61.3, Riyadh=-23.5/26.0/74.9, Rome=-35.5/15.2/64.5, Roseau=-22.3/26.2/81.4, Rostov-on-Don=-36.8/9.9/61.2, Sacramento=-30.9/16.3/65.1, Saint Petersburg=-43.9/5.8/54.5, Saint-Pierre=-45.1/5.7/53.5, Salt Lake City=-40.6/11.6/67.5, San Antonio=-29.7/20.8/72.8, San Diego=-31.6/17.8/69.0, San Francisco=-36.6/14.6/66.4, San Jose=-30.3/16.4/64.0, San José=-32.8/22.6/73.9, San Juan=-24.9/27.2/76.5, San Salvador=-27.9/23.1/74.4, Sana'a=-31.2/20.0/71.0, Santo Domingo=-26.5/25.9/76.4, Sapporo=-41.4/8.9/60.3, Sarajevo=-43.1/10.1/61.1, Saskatoon=-46.7/3.3/55.8, Seattle=-40.2/11.3/60.1, Seoul=-40.7/12.5/62.3, Seville=-35.5/19.2/68.8, Shanghai=-34.9/16.7/64.6, Singapore=-21.8/27.0/76.5, Skopje=-40.1/12.4/62.3, Sochi=-33.6/14.2/62.6, Sofia=-36.6/10.6/63.1, Sokoto=-21.7/28.0/76.1, Split=-33.9/16.1/63.2, St. John's=-47.5/5.0/57.9, St. Louis=-35.4/13.9/64.0, Stockholm=-43.6/6.6/57.6, Surabaya=-25.2/27.1/77.0, Suva=-26.2/25.6/74.0, Suwałki=-46.4/7.2/57.1, Sydney=-33.9/17.7/71.3, Ségou=-21.3/28.0/75.1, Tabora=-23.8/23.0/73.8, Tabriz=-40.5/12.6/63.5, Taipei=-23.5/23.0/72.8, Tallinn=-48.3/6.4/54.1, Tamale=-24.6/27.9/80.0, Tamanrasset=-26.1/21.7/70.3, Tampa=-28.4/22.9/74.7, Tashkent=-33.6/14.8/64.1, Tauranga=-37.1/14.8/69.5, Tbilisi=-40.9/12.9/63.9, Tegucigalpa=-28.9/21.7/69.4, Tehran=-36.2/17.0/70.8, Tel Aviv=-34.0/20.0/70.2, Thessaloniki=-35.1/16.0/66.3, Thiès=-23.8/24.0/70.9, Tijuana=-32.8/17.8/71.3, Timbuktu=-19.7/28.0/83.9, Tirana=-39.9/15.2/64.4, Toamasina=-30.3/23.4/72.8, Tokyo=-32.7/15.4/65.4, Toliara=-26.2/24.1/75.0, Toluca=-37.1/12.4/59.1, Toronto=-41.2/9.4/59.0, Tripoli=-32.2/20.0/66.5, Tromsø=-47.6/2.9/60.0, Tucson=-33.5/20.9/69.9, Tunis=-30.9/18.4/68.2, Ulaanbaatar=-54.1/-0.4/49.7, Upington=-27.2/20.4/78.8, Vaduz=-41.9/10.1/60.8, Valencia=-31.7/18.3/65.8, Valletta=-32.4/18.8/71.4, Vancouver=-41.1/10.4/60.9, Veracruz=-25.9/25.4/74.6, Vienna=-44.2/10.4/63.4, Vientiane=-23.2/25.9/83.4, Villahermosa=-20.0/27.1/76.7, Vilnius=-44.9/6.0/56.9, Virginia Beach=-35.9/15.8/64.7, Vladivostok=-46.3/4.9/60.2, Warsaw=-41.0/8.5/59.9, Washington, D.C.=-36.3/14.6/65.5, Wau=-20.5/27.8/79.2, Wellington=-33.0/12.9/64.0, Whitehorse=-47.5/-0.1/50.1, Wichita=-35.2/13.9/60.6, Willemstad=-21.7/28.0/77.8, Winnipeg=-51.2/3.0/53.5, Wrocław=-41.4/9.6/60.5, Xi'an=-39.7/14.1/73.7, Yakutsk=-57.8/-8.8/46.7, Yangon=-24.3/27.5/78.7, Yaoundé=-24.8/23.8/71.4, Yellowknife=-51.9/-4.3/50.0, Yerevan=-45.6/12.4/60.0, Yinchuan=-39.0/9.0/63.5, Zagreb=-41.6/10.7/58.5, Zanzibar City=-25.4/26.0/75.7, Zürich=-40.6/9.3/55.4, Ürümqi=-43.5/7.4/58.3, İzmir=-34.7/17.9/68.3}");
        }
    }

    private static List<Measurement> parseMeasurements(ByteBuffer byteBuffer) {
        // Most of the code here is derived from @bjhara's implementation
        // https://github.com/gunnarmorling/1brc/pull/10
        var measurements = new ArrayList<Measurement>(100_000);

        final int limit = byteBuffer.limit();
        final byte[] buffer = new byte[128];

        while (byteBuffer.position() < limit) {
            final int start = byteBuffer.position();

            int separatorPosition = start;
            while (separatorPosition != limit &&
                    byteBuffer.get(separatorPosition) != ';') {
                separatorPosition++;
            }

            int endOfLinePosition = separatorPosition; // must be after the separator
            while (endOfLinePosition != limit &&
                    byteBuffer.get(endOfLinePosition) != '\n') {
                endOfLinePosition++;
            }

            int nameOffset = separatorPosition - start;
            byteBuffer.get(buffer, 0, nameOffset);
            String key = new String(buffer, 0, nameOffset);

            byteBuffer.get(); // Skip separator

            int valueLength = endOfLinePosition - separatorPosition - 1;
            byteBuffer.get(buffer, 0, valueLength);
            double value = fastParseDouble(buffer, valueLength);

            byteBuffer.get(); // Skip newline

            measurements.add(new Measurement(key, value));
        }

        return measurements;
    }

    private static double fastParseDouble(byte[] bytes, int length) {
        long value = 0;
        int exp = 0;
        boolean negative = false;
        int decimalPlaces = Integer.MIN_VALUE;
        for (int i = 0; i < length; i++) {
            byte ch = bytes[i];
            if (ch >= '0' && ch <= '9') {
                value = value * 10 + (ch - '0');
                decimalPlaces++;
            }
            else if (ch == '-') {
                negative = true;
            }
            else if (ch == '.') {
                decimalPlaces = 0;
            }
        }

        return asDouble(value, exp, negative, decimalPlaces);
    }

    private static double asDouble(long value, int exp, boolean negative, int decimalPlaces) {
        if (decimalPlaces > 0 && value < Long.MAX_VALUE / 2) {
            if (value < Long.MAX_VALUE / (1L << 32)) {
                exp -= 32;
                value <<= 32;
            }
            if (value < Long.MAX_VALUE / (1L << 16)) {
                exp -= 16;
                value <<= 16;
            }
            if (value < Long.MAX_VALUE / (1L << 8)) {
                exp -= 8;
                value <<= 8;
            }
            if (value < Long.MAX_VALUE / (1L << 4)) {
                exp -= 4;
                value <<= 4;
            }
            if (value < Long.MAX_VALUE / (1L << 2)) {
                exp -= 2;
                value <<= 2;
            }
            if (value < Long.MAX_VALUE / (1L << 1)) {
                exp -= 1;
                value <<= 1;
            }
        }
        for (; decimalPlaces > 0; decimalPlaces--) {
            exp--;
            long mod = value % 5;
            value /= 5;
            int modDiv = 1;
            if (value < Long.MAX_VALUE / (1L << 4)) {
                exp -= 4;
                value <<= 4;
                modDiv <<= 4;
            }
            if (value < Long.MAX_VALUE / (1L << 2)) {
                exp -= 2;
                value <<= 2;
                modDiv <<= 2;
            }
            if (value < Long.MAX_VALUE / (1L << 1)) {
                exp -= 1;
                value <<= 1;
                modDiv <<= 1;
            }
            if (decimalPlaces > 1) {
                value += modDiv * mod / 5;
            }
            else {
                value += (modDiv * mod + 4) / 5;
            }
        }
        final double d = Math.scalb((double) value, exp);
        return negative ? -d : d;
    }
}
