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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

public class CalculateAverage_twobiers {

    private static final String FILE = "./measurements.txt";
    private static final FastAveragingCollector FAST_AVERAGING_COLLECTOR = new FastAveragingCollector();
    // private static final VectorSpecies<Integer> SPECIES = IntVector.SPECIES_256;

    private static class FastAveragingCollector implements Collector<String[], double[], String> {
        @Override
        public Supplier<double[]> supplier() {
            // 0: summand 1
            // 1: summand 2
            // 2: count
            // 3: parsed value
            // 4: current max
            // 5: current min
            return () -> new double[6];
        }

        @Override
        public BiConsumer<double[], String[]> accumulator() {
            return (a, t) -> {
                double val = fastParseDouble(t[1]);
                sumWithCompensation(a, val);
                a[2]++;
                a[3] += val;
                if (val > a[4]) {
                    a[4] = val;
                }
                if (val < a[5]) {
                    a[5] = val;
                }
            };
        }

        @Override
        public BinaryOperator<double[]> combiner() {
            return (a, b) -> {
                sumWithCompensation(a, b[0]);
                // Subtract compensation bits
                sumWithCompensation(a, -b[1]);
                a[2] += b[2];
                a[3] += b[3];
                if (b[4] > a[4]) {
                    a[4] = b[4];
                }
                if (b[5] < a[5]) {
                    a[5] = b[5];
                }
                return a;
            };
        }

        @Override
        public Function<double[], String> finisher() {
            return a -> {
                var mean = (a[2] == 0) ? 0.0d : Math.round(((a[0] + a[1]) / a[2]) * 10.0) / 10.0;
                var max = a[4];
                var min = a[5];
                return min + "/" + mean + "/" + max;
            };
        }

        @Override
        public Set<Characteristics> characteristics() {
            return Collections.emptySet();
        }
    }

    public static void main(String[] args) throws IOException {
        TreeMap<String, String> measurements = Files.lines(Paths.get(FILE))
                .parallel()
                .map(l -> fastSplit(l))
                .collect(
                        groupingBy(
                                m -> m[0],
                                TreeMap::new,
                                FAST_AVERAGING_COLLECTOR));

        System.out.println(measurements);

        // Simple test for my generated file
        assert measurements.toString().equals(
                "{Abha=-26.9/18.0/61.8, Abidjan=-23.3/26.0/71.5, Abéché=-19.4/29.4/72.3, Accra=-20.5/26.4/69.9, Addis Ababa=-29.5/16.0/62.3, Adelaide=-26.8/17.3/66.2, Aden=-14.8/29.1/76.1, Ahvaz=-20.7/25.4/70.3, Albuquerque=-33.5/14.0/56.9, Alexandra=-32.5/11.0/63.1, Alexandria=-26.7/20.0/62.5, Algiers=-30.7/18.2/63.5, Alice Springs=-27.6/21.0/66.7, Almaty=-35.9/10.0/53.7, Amsterdam=-34.9/10.2/55.0, Anadyr=-50.7/-6.9/42.5, Anchorage=-47.1/2.8/47.0, Andorra la Vella=-34.3/9.8/56.1, Ankara=-31.6/12.0/63.8, Antananarivo=-32.2/17.9/63.7, Antsiranana=-22.1/25.2/67.8, Arkhangelsk=-46.2/1.3/46.7, Ashgabat=-29.0/17.1/62.1, Asmara=-29.3/15.6/59.3, Assab=-14.5/30.5/76.5, Astana=-40.9/3.5/48.2, Athens=-29.2/19.2/61.6, Atlanta=-28.2/17.0/62.5, Auckland=-28.9/15.2/59.3, Austin=-23.4/20.7/63.5, Baghdad=-29.5/22.8/68.7, Baguio=-24.5/19.5/60.7, Baku=-27.7/15.1/59.7, Baltimore=-30.8/13.1/60.9, Bamako=-20.5/27.8/73.7, Bangkok=-18.6/28.6/75.1, Bangui=-21.1/26.0/71.6, Banjul=-18.0/26.0/70.5, Barcelona=-27.1/18.2/72.3, Bata=-30.7/25.1/69.2, Batumi=-30.6/14.0/59.2, Beijing=-32.4/12.9/55.0, Beirut=-24.7/20.9/67.3, Belgrade=-34.9/12.5/55.0, Belize City=-18.3/26.7/72.4, Benghazi=-27.4/19.9/65.6, Bergen=-35.2/7.7/51.9, Berlin=-38.5/10.3/59.7, Bilbao=-28.6/14.7/58.7, Birao=-23.7/26.5/73.5, Bishkek=-36.9/11.3/59.1, Bissau=-15.1/27.0/72.7, Blantyre=-25.4/22.2/75.5, Bloemfontein=-34.1/15.6/57.5, Boise=-34.6/11.4/60.5, Bordeaux=-29.9/14.2/57.0, Bosaso=-16.8/30.0/73.8, Boston=-32.7/10.9/56.2, Bouaké=-16.0/26.0/71.0, Bratislava=-32.3/10.5/55.6, Brazzaville=-19.3/25.0/70.8, Bridgetown=-16.3/27.0/70.4, Brisbane=-23.0/21.4/69.2, Brussels=-34.8/10.5/52.6, Bucharest=-30.0/10.8/56.4, Budapest=-36.4/11.3/54.6, Bujumbura=-24.3/23.8/69.5, Bulawayo=-23.5/18.9/60.7, Burnie=-32.2/13.0/57.4, Busan=-35.9/15.0/57.9, Cabo San Lucas=-18.4/23.9/67.8, Cairns=-18.9/25.0/69.6, Cairo=-28.0/21.4/64.9, Calgary=-41.2/4.4/51.4, Canberra=-28.6/13.1/56.3, Cape Town=-29.8/16.2/58.9, Changsha=-27.3/17.4/60.3, Charlotte=-29.6/16.1/60.9, Chiang Mai=-20.8/25.8/70.0, Chicago=-35.2/9.8/59.5, Chihuahua=-34.0/18.6/63.3, Chittagong=-19.8/25.9/74.8, Chișinău=-30.7/10.2/54.8, Chongqing=-24.6/18.6/66.6, Christchurch=-32.6/12.2/59.0, City of San Marino=-36.9/11.8/63.4, Colombo=-16.2/27.4/71.4, Columbus=-32.4/11.7/62.0, Conakry=-17.9/26.4/71.4, Copenhagen=-38.2/9.1/65.3, Cotonou=-19.8/27.2/73.2, Cracow=-33.4/9.3/58.5, Da Lat=-23.8/17.9/61.2, Da Nang=-19.2/25.8/70.1, Dakar=-20.7/24.0/68.6, Dallas=-25.3/18.9/63.5, Damascus=-28.4/17.0/63.7, Dampier=-18.6/26.4/70.2, Dar es Salaam=-17.7/25.8/69.2, Darwin=-14.5/27.6/74.8, Denpasar=-21.6/23.7/68.4, Denver=-32.1/10.4/59.0, Detroit=-36.8/10.0/56.6, Dhaka=-19.2/25.9/70.7, Dikson=-54.4/-11.1/37.8, Dili=-18.3/26.6/71.0, Djibouti=-18.2/29.9/76.6, Dodoma=-23.2/22.7/68.4, Dolisie=-20.9/24.0/66.2, Douala=-15.9/26.7/72.4, Dubai=-17.8/26.9/71.2, Dublin=-38.7/9.8/56.8, Dunedin=-35.2/11.1/55.4, Durban=-21.5/20.6/65.6, Dushanbe=-26.9/14.7/59.7, Edinburgh=-33.0/9.3/54.2, Edmonton=-38.9/4.2/48.5, El Paso=-24.3/18.1/63.7, Entebbe=-24.5/21.0/68.1, Erbil=-25.8/19.5/65.9, Erzurum=-38.8/5.1/49.0, Fairbanks=-47.4/-2.3/44.2, Fianarantsoa=-26.8/17.9/63.1, Flores,  Petén=-21.1/26.4/73.4, Frankfurt=-35.6/10.6/56.2, Fresno=-23.9/17.9/67.4, Fukuoka=-28.0/17.0/62.0, Gaborone=-25.2/21.0/64.9, Gabès=-30.0/19.5/64.7, Gagnoa=-19.2/26.0/72.1, Gangtok=-33.3/15.2/63.1, Garissa=-19.4/29.3/83.4, Garoua=-16.4/28.3/80.1, George Town=-16.6/27.9/71.3, Ghanzi=-22.4/21.4/64.0, Gjoa Haven=-63.9/-14.4/30.8, Guadalajara=-22.7/20.9/69.1, Guangzhou=-34.0/22.4/69.4, Guatemala City=-26.7/20.4/63.4, Halifax=-35.5/7.5/50.5, Hamburg=-36.2/9.7/57.3, Hamilton=-28.9/13.8/58.9, Hanga Roa=-22.8/20.5/66.6, Hanoi=-20.2/23.6/70.3, Harare=-25.5/18.4/61.5, Harbin=-43.4/5.0/50.3, Hargeisa=-21.6/21.7/71.2, Hat Yai=-17.0/27.0/70.6, Havana=-28.3/25.2/74.0, Helsinki=-38.7/5.9/55.5, Heraklion=-25.4/18.9/64.9, Hiroshima=-28.7/16.3/60.7, Ho Chi Minh City=-21.3/27.4/69.2, Hobart=-34.4/12.7/60.7, Hong Kong=-23.9/23.3/66.6, Honiara=-17.9/26.5/72.1, Honolulu=-20.9/25.4/75.6, Houston=-28.3/20.8/64.4, Ifrane=-31.0/11.4/56.1, Indianapolis=-35.1/11.8/58.0, Iqaluit=-61.8/-9.3/38.7, Irkutsk=-49.6/1.0/46.7, Istanbul=-31.6/13.9/68.0, Jacksonville=-21.9/20.3/65.3, Jakarta=-18.2/26.7/74.0, Jayapura=-15.5/27.0/70.6, Jerusalem=-25.4/18.3/70.5, Johannesburg=-29.2/15.5/58.5, Jos=-22.4/22.8/66.3, Juba=-17.1/27.8/69.5, Kabul=-29.6/12.1/59.8, Kampala=-26.0/20.0/66.3, Kandi=-17.3/27.7/74.9, Kankan=-20.0/26.5/74.1, Kano=-20.0/26.4/69.8, Kansas City=-32.9/12.5/58.3, Karachi=-21.6/26.0/71.6, Karonga=-21.0/24.3/71.6, Kathmandu=-27.7/18.3/62.5, Khartoum=-20.3/29.9/78.0, Kingston=-17.6/27.4/69.9, Kinshasa=-22.6/25.3/70.1, Kolkata=-14.8/26.7/71.8, Kuala Lumpur=-17.6/27.3/73.1, Kumasi=-19.2/25.9/75.1, Kunming=-32.0/15.7/58.5, Kuopio=-45.8/3.4/46.5, Kuwait City=-19.2/25.7/71.2, Kyiv=-37.8/8.4/55.1, Kyoto=-27.2/15.8/64.9, La Ceiba=-17.8/26.2/72.7, La Paz=-23.7/23.7/66.4, Lagos=-16.6/26.8/74.6, Lahore=-19.8/24.3/70.3, Lake Havasu City=-24.8/23.7/68.3, Lake Tekapo=-36.3/8.7/54.1, Las Palmas de Gran Canaria=-29.4/21.2/66.7, Las Vegas=-24.9/20.3/64.8, Launceston=-32.0/13.1/57.2, Lhasa=-38.8/7.6/52.9, Libreville=-21.5/25.9/71.7, Lisbon=-34.9/17.5/65.6, Livingstone=-23.9/21.8/65.5, Ljubljana=-30.3/10.9/57.1, Lodwar=-14.8/29.3/73.1, Lomé=-17.5/26.9/70.7, London=-34.8/11.3/55.7, Los Angeles=-30.7/18.6/62.6, Louisville=-32.1/13.9/62.9, Luanda=-17.6/25.8/72.3, Lubumbashi=-26.0/20.8/66.9, Lusaka=-23.8/19.9/62.1, Luxembourg City=-33.0/9.3/56.0, Lviv=-36.1/7.8/51.7, Lyon=-36.7/12.5/56.8, Madrid=-30.4/15.0/59.1, Mahajanga=-17.2/26.3/67.9, Makassar=-14.9/26.7/73.1, Makurdi=-23.8/26.0/69.2, Malabo=-20.5/26.3/71.7, Malé=-21.6/28.0/75.4, Managua=-29.5/27.3/74.3, Manama=-20.3/26.5/72.8, Mandalay=-15.3/28.0/73.1, Mango=-14.7/28.1/74.5, Manila=-18.1/28.4/72.1, Maputo=-22.2/22.8/70.5, Marrakesh=-27.6/19.6/71.3, Marseille=-29.7/15.8/60.7, Maun=-21.5/22.4/65.6, Medan=-18.2/26.5/69.2, Mek'ele=-29.0/22.7/69.3, Melbourne=-29.6/15.1/64.2, Memphis=-27.3/17.2/61.6, Mexicali=-24.4/23.1/71.6, Mexico City=-27.1/17.5/63.1, Miami=-18.7/24.9/71.4, Milan=-32.9/13.0/59.2, Milwaukee=-33.9/8.9/55.1, Minneapolis=-35.5/7.8/50.9, Minsk=-35.2/6.8/53.5, Mogadishu=-18.6/27.1/73.9, Mombasa=-17.3/26.3/72.9, Monaco=-36.0/16.4/64.7, Moncton=-38.9/6.1/49.2, Monterrey=-20.0/22.3/66.4, Montreal=-41.5/6.8/53.1, Moscow=-40.5/5.8/50.6, Mumbai=-19.7/27.1/70.7, Murmansk=-47.6/0.6/49.6, Muscat=-16.9/28.0/78.6, Mzuzu=-25.7/17.7/60.6, N'Djamena=-15.5/28.3/71.7, Naha=-22.1/23.1/75.7, Nairobi=-29.7/17.8/60.1, Nakhon Ratchasima=-17.3/27.3/79.0, Napier=-33.7/14.6/58.0, Napoli=-25.7/15.9/61.1, Nashville=-33.2/15.4/59.1, Nassau=-20.4/24.6/67.9, Ndola=-23.4/20.3/64.5, New Delhi=-19.6/25.0/70.1, New Orleans=-24.3/20.7/64.5, New York City=-32.1/12.9/60.8, Ngaoundéré=-26.7/22.0/64.6, Niamey=-18.9/29.3/73.3, Nicosia=-27.2/19.7/64.8, Niigata=-29.8/13.9/60.0, Nouadhibou=-24.4/21.3/65.7, Nouakchott=-22.2/25.7/71.2, Novosibirsk=-40.9/1.7/45.7, Nuuk=-47.0/-1.4/48.1, Odesa=-41.9/10.7/53.1, Odienné=-25.7/26.0/70.0, Oklahoma City=-25.9/15.9/59.8, Omaha=-37.7/10.6/56.4, Oranjestad=-14.4/28.1/70.7, Oslo=-38.9/5.7/54.5, Ottawa=-39.2/6.6/48.7, Ouagadougou=-16.9/28.3/75.4, Ouahigouya=-15.7/28.6/75.0, Ouarzazate=-27.6/18.9/62.1, Oulu=-42.3/2.7/45.1, Palembang=-19.7/27.3/71.1, Palermo=-27.2/18.5/63.0, Palm Springs=-26.7/24.5/73.4, Palmerston North=-36.3/13.2/56.0, Panama City=-19.2/28.0/76.6, Parakou=-16.4/26.8/69.9, Paris=-33.4/12.3/57.6, Perth=-28.3/18.7/67.7, Petropavlovsk-Kamchatsky=-41.7/1.9/49.3, Philadelphia=-32.9/13.2/59.4, Phnom Penh=-15.2/28.3/73.5, Phoenix=-18.7/23.9/69.1, Pittsburgh=-38.1/10.8/57.1, Podgorica=-32.2/15.3/58.9, Pointe-Noire=-20.8/26.1/70.7, Pontianak=-18.5/27.7/75.5, Port Moresby=-16.6/26.9/69.7, Port Sudan=-12.9/28.4/75.4, Port Vila=-20.4/24.3/71.3, Port-Gentil=-16.6/26.0/74.4, Portland (OR)=-31.7/12.4/60.2, Porto=-29.1/15.7/57.6, Prague=-41.1/8.4/54.7, Praia=-21.5/24.4/70.7, Pretoria=-26.5/18.2/64.6, Pyongyang=-32.6/10.8/55.7, Rabat=-28.3/17.2/61.1, Rangpur=-19.4/24.4/67.5, Reggane=-19.0/28.3/70.8, Reykjavík=-41.3/4.3/47.3, Riga=-41.8/6.2/54.3, Riyadh=-26.1/26.1/69.6, Rome=-27.6/15.2/58.4, Roseau=-23.1/26.2/67.8, Rostov-on-Don=-34.0/9.9/58.3, Sacramento=-31.3/16.3/59.9, Saint Petersburg=-37.5/5.8/48.8, Saint-Pierre=-39.5/5.7/49.8, Salt Lake City=-35.6/11.6/55.5, San Antonio=-28.7/20.8/66.1, San Diego=-26.0/17.8/67.9, San Francisco=-28.4/14.6/64.1, San Jose=-25.6/16.4/58.4, San José=-20.7/22.6/67.3, San Juan=-15.3/27.2/72.2, San Salvador=-20.9/23.1/65.7, Sana'a=-28.1/20.0/65.4, Santo Domingo=-19.2/25.9/70.6, Sapporo=-36.1/8.9/54.1, Sarajevo=-42.5/10.1/62.6, Saskatoon=-42.5/3.3/49.9, Seattle=-34.8/11.3/56.1, Seoul=-30.7/12.5/58.2, Seville=-22.1/19.1/64.5, Shanghai=-25.4/16.7/68.4, Singapore=-19.2/27.0/74.2, Skopje=-33.3/12.4/57.6, Sochi=-29.8/14.2/62.2, Sofia=-35.1/10.6/53.6, Sokoto=-19.1/28.0/75.1, Split=-26.8/16.1/61.5, St. John's=-38.6/5.0/51.1, St. Louis=-27.3/13.9/61.6, Stockholm=-35.5/6.6/50.0, Surabaya=-15.5/27.1/76.1, Suva=-19.2/25.6/70.5, Suwałki=-40.9/7.2/50.1, Sydney=-28.0/17.6/69.6, Ségou=-23.0/28.0/73.0, Tabora=-24.6/23.0/71.2, Tabriz=-31.8/12.6/57.8, Taipei=-19.9/23.0/70.7, Tallinn=-36.7/6.4/51.4, Tamale=-16.7/27.9/75.9, Tamanrasset=-24.8/21.7/69.0, Tampa=-25.2/22.9/71.6, Tashkent=-31.2/14.8/65.3, Tauranga=-35.5/14.8/62.8, Tbilisi=-32.4/12.9/59.3, Tegucigalpa=-25.5/21.7/66.8, Tehran=-28.7/17.0/61.7, Tel Aviv=-24.3/20.0/64.7, Thessaloniki=-29.7/16.0/62.1, Thiès=-22.4/24.0/70.6, Tijuana=-25.8/17.8/64.8, Timbuktu=-15.3/28.0/80.0, Tirana=-34.3/15.2/61.9, Toamasina=-20.1/23.4/67.4, Tokyo=-33.6/15.4/60.6, Toliara=-24.5/24.1/74.4, Toluca=-31.6/12.4/58.0, Toronto=-37.2/9.4/51.9, Tripoli=-27.3/20.0/63.1, Tromsø=-40.8/2.9/48.1, Tucson=-25.6/20.9/62.6, Tunis=-24.6/18.4/60.0, Ulaanbaatar=-48.7/-0.4/46.3, Upington=-22.3/20.4/63.2, Vaduz=-37.0/10.1/55.8, Valencia=-25.9/18.3/68.5, Valletta=-25.2/18.8/66.4, Vancouver=-35.2/10.4/53.4, Veracruz=-22.5/25.4/67.7, Vienna=-33.4/10.4/55.0, Vientiane=-21.2/25.9/69.2, Villahermosa=-15.2/27.1/73.3, Vilnius=-40.0/6.0/52.7, Virginia Beach=-28.8/15.8/59.9, Vladivostok=-39.0/4.9/49.3, Warsaw=-34.0/8.5/53.5, Washington, D.C.=-30.2/14.6/59.8, Wau=-17.4/27.8/70.2, Wellington=-31.0/12.9/59.2, Whitehorse=-50.4/-0.1/46.7, Wichita=-29.7/13.9/60.1, Willemstad=-19.1/28.0/73.5, Winnipeg=-39.3/3.0/53.5, Wrocław=-41.4/9.6/53.7, Xi'an=-31.9/14.1/57.2, Yakutsk=-54.0/-8.8/33.5, Yangon=-19.6/27.5/76.4, Yaoundé=-23.8/23.8/69.9, Yellowknife=-55.5/-4.3/38.6, Yerevan=-30.0/12.4/61.0, Yinchuan=-36.8/9.0/53.4, Zagreb=-36.3/10.7/56.9, Zanzibar City=-20.4/26.0/71.5, Zürich=-36.0/9.3/56.3, Ürümqi=-40.5/7.4/54.5, İzmir=-29.5/17.9/61.2}");
    }

    private static String[] fastSplit(String str) {
        var splitArray = new String[2];
        var chars = str.toCharArray();

        int i = 0;
        for (char c : chars) {
            if (c == ';') {
                splitArray[0] = new String(Arrays.copyOfRange(chars, 0, i));
                break;
            }
            i++;
        }

        splitArray[1] = new String(Arrays.copyOfRange(chars, i + 1, chars.length));
        return splitArray;
    }

    private static Double fastParseDouble(String str) {
        long value = 0;
        int exp = 0;
        boolean negative = false;
        int decimalPlaces = Integer.MIN_VALUE;
        var chars = str.toCharArray();
        for (char ch : chars) {
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

    private static double[] sumWithCompensation(double[] intermediateSum, double value) {
        double tmp = value - intermediateSum[1];
        double sum = intermediateSum[0];
        double velvel = sum + tmp; // Little wolf of rounding error
        intermediateSum[1] = (velvel - sum) - tmp;
        intermediateSum[0] = velvel;
        return intermediateSum;
    }

}
