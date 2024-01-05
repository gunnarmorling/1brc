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
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.LongStream;

public class CalculateAverage_lawrey_magic {

    static final String[] stations = {"Abha", "Abidjan", "Abéché", "Accra", "Addis Ababa", "Adelaide", "Aden", "Ahvaz", "Albuquerque", "Alexandra", "Alexandria",
            "Algiers", "Alice Springs", "Almaty", "Amsterdam", "Anadyr", "Anchorage", "Andorra la Vella", "Ankara", "Antananarivo", "Antsiranana", "Arkhangelsk",
            "Ashgabat", "Asmara", "Assab", "Astana", "Athens", "Atlanta", "Auckland", "Austin", "Baghdad", "Baguio", "Baku", "Baltimore", "Bamako", "Bangkok", "Bangui",
            "Banjul", "Barcelona", "Bata", "Batumi", "Beijing", "Beirut", "Belgrade", "Belize City", "Benghazi", "Bergen", "Berlin", "Bilbao", "Birao", "Bishkek",
            "Bissau", "Blantyre", "Bloemfontein", "Boise", "Bordeaux", "Bosaso", "Boston", "Bouaké", "Bratislava", "Brazzaville", "Bridgetown", "Brisbane", "Brussels",
            "Bucharest", "Budapest", "Bujumbura", "Bulawayo", "Burnie", "Busan", "Cabo San Lucas", "Cairns", "Cairo", "Calgary", "Canberra", "Cape Town", "Changsha",
            "Charlotte", "Chiang Mai", "Chicago", "Chihuahua", "Chișinău", "Chittagong", "Chongqing", "Christchurch", "City of San Marino", "Colombo", "Columbus",
            "Conakry", "Copenhagen", "Cotonou", "Cracow", "Da Lat", "Da Nang", "Dakar", "Dallas", "Damascus", "Dampier", "Dar es Salaam", "Darwin", "Denpasar", "Denver",
            "Detroit", "Dhaka", "Dikson", "Dili", "Djibouti", "Dodoma", "Dolisie", "Douala", "Dubai", "Dublin", "Dunedin", "Durban", "Dushanbe", "Edinburgh", "Edmonton",
            "El Paso", "Entebbe", "Erbil", "Erzurum", "Fairbanks", "Fianarantsoa", "Flores,  Petén", "Frankfurt", "Fresno", "Fukuoka", "Gabès", "Gaborone", "Gagnoa",
            "Gangtok", "Garissa", "Garoua", "George Town", "Ghanzi", "Gjoa Haven", "Guadalajara", "Guangzhou", "Guatemala City", "Halifax", "Hamburg", "Hamilton",
            "Hanga Roa", "Hanoi", "Harare", "Harbin", "Hargeisa", "Hat Yai", "Havana", "Helsinki", "Heraklion", "Hiroshima", "Ho Chi Minh City", "Hobart", "Hong Kong",
            "Honiara", "Honolulu", "Houston", "Ifrane", "Indianapolis", "Iqaluit", "Irkutsk", "Istanbul", "İzmir", "Jacksonville", "Jakarta", "Jayapura", "Jerusalem",
            "Johannesburg", "Jos", "Juba", "Kabul", "Kampala", "Kandi", "Kankan", "Kano", "Kansas City", "Karachi", "Karonga", "Kathmandu", "Khartoum", "Kingston",
            "Kinshasa", "Kolkata", "Kuala Lumpur", "Kumasi", "Kunming", "Kuopio", "Kuwait City", "Kyiv", "Kyoto", "La Ceiba", "La Paz", "Lagos", "Lahore",
            "Lake Havasu City", "Lake Tekapo", "Las Palmas de Gran Canaria", "Las Vegas", "Launceston", "Lhasa", "Libreville", "Lisbon", "Livingstone", "Ljubljana",
            "Lodwar", "Lomé", "London", "Los Angeles", "Louisville", "Luanda", "Lubumbashi", "Lusaka", "Luxembourg City", "Lviv", "Lyon", "Madrid", "Mahajanga",
            "Makassar", "Makurdi", "Malabo", "Malé", "Managua", "Manama", "Mandalay", "Mango", "Manila", "Maputo", "Marrakesh", "Marseille", "Maun", "Medan", "Mek'ele",
            "Melbourne", "Memphis", "Mexicali", "Mexico City", "Miami", "Milan", "Milwaukee", "Minneapolis", "Minsk", "Mogadishu", "Mombasa", "Monaco", "Moncton",
            "Monterrey", "Montreal", "Moscow", "Mumbai", "Murmansk", "Muscat", "Mzuzu", "N'Djamena", "Naha", "Nairobi", "Nakhon Ratchasima", "Napier", "Napoli",
            "Nashville", "Nassau", "Ndola", "New Delhi", "New Orleans", "New York City", "Ngaoundéré", "Niamey", "Nicosia", "Niigata", "Nouadhibou", "Nouakchott",
            "Novosibirsk", "Nuuk", "Odesa", "Odienné", "Oklahoma City", "Omaha", "Oranjestad", "Oslo", "Ottawa", "Ouagadougou", "Ouahigouya", "Ouarzazate", "Oulu",
            "Palembang", "Palermo", "Palm Springs", "Palmerston North", "Panama City", "Parakou", "Paris", "Perth", "Petropavlovsk-Kamchatsky", "Philadelphia",
            "Phnom Penh", "Phoenix", "Pittsburgh", "Podgorica", "Pointe-Noire", "Pontianak", "Port Moresby", "Port Sudan", "Port Vila", "Port-Gentil", "Portland (OR)",
            "Porto", "Prague", "Praia", "Pretoria", "Pyongyang", "Rabat", "Rangpur", "Reggane", "Reykjavík", "Riga", "Riyadh", "Rome", "Roseau", "Rostov-on-Don",
            "Sacramento", "Saint Petersburg", "Saint-Pierre", "Salt Lake City", "San Antonio", "San Diego", "San Francisco", "San Jose", "San José", "San Juan",
            "San Salvador", "Sana'a", "Santo Domingo", "Sapporo", "Sarajevo", "Saskatoon", "Seattle", "Ségou", "Seoul", "Seville", "Shanghai", "Singapore", "Skopje",
            "Sochi", "Sofia", "Sokoto", "Split", "St. John's", "St. Louis", "Stockholm", "Surabaya", "Suva", "Suwałki", "Sydney", "Tabora", "Tabriz", "Taipei", "Tallinn",
            "Tamale", "Tamanrasset", "Tampa", "Tashkent", "Tauranga", "Tbilisi", "Tegucigalpa", "Tehran", "Tel Aviv", "Thessaloniki", "Thiès", "Tijuana", "Timbuktu",
            "Tirana", "Toamasina", "Tokyo", "Toliara", "Toluca", "Toronto", "Tripoli", "Tromsø", "Tucson", "Tunis", "Ulaanbaatar", "Upington", "Ürümqi", "Vaduz",
            "Valencia", "Valletta", "Vancouver", "Veracruz", "Vienna", "Vientiane", "Villahermosa", "Vilnius", "Virginia Beach", "Vladivostok", "Warsaw",
            "Washington, D.C.", "Wau", "Wellington", "Whitehorse", "Wichita", "Willemstad", "Winnipeg", "Wrocław", "Xi'an", "Yakutsk", "Yangon", "Yaoundé", "Yellowknife",
            "Yerevan", "Yinchuan", "Zagreb", "Zanzibar City", "Zürich",};
    static final int[] stationHash = {4176, 5994, 1095, 2143, 4973, 5724, 5290, 1457, 4864, 5434, 5024, 3443, 1081, 3712, 4108, 3241, 3568, 4550, 3158, 828, 5404, 1368,
            3637, 75, 4621, 2868, 3377, 4662, 671, 3103, 2579, 1933, 5584, 458, 2355, 3666, 1672, 1241, 3480, 1956, 461, 540, 2448, 2517, 4223, 905, 3985, 3393, 505,
            2288, 2139, 4799, 5011, 867, 4454, 759, 5437, 5223, 2415, 2794, 5279, 4649, 1327, 392, 435, 3117, 4007, 331, 5811, 14, 4073, 4011, 3644, 2061, 4199, 3136,
            5797, 166, 4979, 1816, 1341, 1856, 4636, 4390, 6072, 5273, 1238, 960, 1466, 5030, 3768, 4584, 93, 3128, 3409, 4760, 2955, 2298, 1948, 3236, 5731, 180, 1934,
            2896, 3981, 1008, 5042, 5997, 5070, 652, 1754, 2560, 3230, 523, 5839, 4569, 3476, 3659, 3563, 1458, 1063, 5905, 3809, 5791, 5676, 2822, 2214, 3201, 4792,
            4427, 5806, 5371, 5392, 563, 2828, 3316, 5949, 5480, 4767, 4405, 2963, 4106, 805, 599, 1678, 2552, 597, 4149, 861, 2473, 5146, 5725, 5181, 3460, 629, 4806,
            1471, 3949, 2936, 5473, 1338, 1911, 1873, 4357, 5698, 3963, 2771, 5810, 3778, 5886, 1168, 211, 4144, 3191, 2015, 1186, 1628, 604, 4187, 3300, 3540, 1615,
            5329, 1964, 5641, 1127, 5826, 5045, 3551, 349, 1762, 5972, 5321, 4413, 4177, 5566, 6004, 6008, 1403, 1575, 1669, 3430, 1073, 3261, 573, 5159, 1285, 2220,
            2391, 708, 849, 6017, 3840, 5157, 2229, 2206, 3121, 3302, 1316, 3319, 3472, 5362, 5232, 1536, 2404, 4916, 5474, 1832, 4423, 5298, 3731, 2899, 2903, 2329, 375,
            2339, 3227, 299, 4041, 3092, 3326, 4083, 1990, 2158, 5819, 1517, 617, 5270, 3149, 1831, 52, 1514, 5975, 1597, 1212, 2356, 4594, 4822, 4332, 6040, 725, 4366,
            3679, 5129, 2918, 5831, 4615, 5046, 3330, 3820, 1703, 726, 5423, 1530, 830, 3159, 3876, 2762, 2975, 1414, 4397, 344, 2420, 5776, 4832, 4703, 1251, 4515, 2792,
            3725, 5459, 1371, 2757, 4506, 2680, 1887, 1359, 2674, 3134, 5940, 6067, 2041, 5126, 131, 4677, 611, 2731, 2572, 2756, 3906, 1920, 4341, 5981, 3901, 2422,
            3124, 1957, 5471, 5897, 5901, 1910, 243, 2054, 1002, 442, 1410, 3998, 2123, 4706, 5098, 5207, 3682, 2407, 3017, 3555, 6085, 2265, 1683, 5992, 325, 5433, 2930,
            1513, 1007, 2199, 5106, 5479, 3826, 575, 3661, 1276, 2976, 5016, 5568, 1329, 2730, 5569, 3724, 3217, 799, 3716, 4514, 3498, 3107, 549, 5673, 3829, 2421, 4227,
            4466, 951, 4128, 3812, 2814, 53, 3615, 1557, 2443, 3515, 3671, 2029, 5501, 4403, 3437, 416, 3250, 5800, 426, 645, 3645, 1746, 1523, 1129, 1019, 5446, 2362,
            1265, 4360, 5862, 5351, 2272, 2279, 2893, 5089, 5128, 4796, 4410, 1757, 55, 1344, 1380, 2154, 580};
    static final int[] stationHash2 = stationHash.clone();
    // Path to the file containing temperature measurements.
    private static final String FILE = "./measurements.txt";

    static {
        Arrays.sort(stationHash2);
    }

    public static void main(String[] args) throws IOException {
        // Open the file for reading.
        File file = new File(FILE);
        long length = file.length();
        long chunk = 1 << 28; // Size of the chunk to be processed.
        RandomAccessFile raf = new RandomAccessFile(file, "r");

        // Process the file in chunks and merge the results.
        Measurement[] allMeasurementsMap = LongStream.range(0, length / chunk + 1)
                .parallel()
                .mapToObj(i -> extractMeasurementsFromChunk(i, chunk, length, raf))
                .reduce((a, b) -> mergeMeasurementMaps(a, b))
                .orElseThrow();

        // Sort the measurements and print them.
        Map<String, Measurement> sortedMeasurementsMap = new TreeMap<>();
        for (int i = 0; i < stations.length; i++) {
            String station = stations[i];
            int n = stationHash[i];
            sortedMeasurementsMap.put(station, allMeasurementsMap[n]);
        }
        System.out.print('{');
        String sep = "";
        for (Map.Entry<String, Measurement> entry : sortedMeasurementsMap.entrySet()) {
            System.out.print(sep);
            System.out.print(entry);
            sep = ", ";
        }
        System.out.println("}");
    }

    // Merges two measurement maps.
    private static Measurement[] mergeMeasurementMaps(Measurement[] a, Measurement[] b) {
        for (int i : stationHash2) {
            a[i].merge(b[i]);
        }
        return a;
    }

    // Extracts measurements from a chunk of the file.
    private static Measurement[] extractMeasurementsFromChunk(long i, long chunk, long length, RandomAccessFile raf) {
        long start = i * chunk;
        long size = Math.min(length, start + chunk + 64 * 1024) - start;
        Measurement[] array = new Measurement[6086];
        for (int j : stationHash2) {
            array[j] = new Measurement();
        }
        try {
            MappedByteBuffer mbb = raf.getChannel().map(FileChannel.MapMode.READ_ONLY, start, size);
            mbb.order(ByteOrder.nativeOrder());
            if (i > 0)
                skipToFirstLine(mbb);
            while (mbb.remaining() > 0 && mbb.position() <= chunk) {
                int key = readKey(mbb);
                int temp = readTemperatureFromBuffer(mbb);
                Measurement m = array[key];
                m.sample(temp / 10.0);
            }

        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
        return array;
    }

    // Reads a temperature value from the buffer.
    private static int readTemperatureFromBuffer(MappedByteBuffer mbb) {
        int temp = 0;
        boolean negative = false;
        outer:
        while (mbb.remaining() > 0) {
            int b = mbb.get();
            switch (b) {
                case '-':
                    negative = true;
                    break;
                default:
                    temp = 10 * temp + (b - '0');
                    break;
                case '.':
                    b = mbb.get();
                    temp = 10 * temp + (b - '0');
                case '\r':
                    mbb.get();
                case '\n':
                    break outer;
            }
        }
        if (negative)
            temp = -temp;
        return temp;
    }

    // Skips to the first line in the buffer, used for chunk processing.
    private static void skipToFirstLine(MappedByteBuffer mbb) {
        while ((mbb.get() & 0xFF) >= ' ') {
            // Skip bytes until reaching the start of a line.
        }
    }

    static int readKey(MappedByteBuffer mbb) {
        long hash = mbb.getInt();
        int rewind = -1;

        if ((hash & 0xFF000000) != (';' << 24)) {
            do {
                int s = mbb.getInt();
                if ((s & 0xFF) == ';') {
                    rewind = 3;
                    s = ';';
                } else if ((s & 0xFF00) == (';' << 8)) {
                    rewind = 2;
                    s &= 0xFFFF;
                } else if ((s & 0xFF0000) == (';' << 16)) {
                    rewind = 1;
                    s &= 0xFFFFFF;
                } else if ((s & 0xFF000000) == (';' << 24)) {
                    rewind = 0;
                }
                hash = hash * 21503 + s;
            } while (rewind == -1);
        }
        hash += hash >>> 1;
        mbb.position(mbb.position() - rewind);

        int abs = (int) Math.abs(hash % 6121);
        return abs;
    }

    static String peek(MappedByteBuffer mbb) {
        int pos = mbb.position();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 16; i++)
            sb.append((char) (mbb.get(pos + i) & 0xff));
        return sb.toString();
    }

    // Inner class representing a measurement with min, max, and average calculations.
    static class Measurement {
        double min = Double.POSITIVE_INFINITY;
        double max = Double.NEGATIVE_INFINITY;
        double sum = 0.0;
        long count = 0;

        // Default constructor for Measurement.
        public Measurement() {
        }

        // Adds a new temperature sample and updates min, max, and average.
        public void sample(double temp) {
            min = Math.min(min, temp);
            max = Math.max(max, temp);
            sum += temp;
            count++;
        }

        // Returns a formatted string representing min, average, and max.
        public String toString() {
            return round(min) + "/" + round(sum / count) + "/" + round(max);
        }

        // Helper method to round a double value to one decimal place.
        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }

        // Merges this Measurement with another Measurement.
        public Measurement merge(Measurement m2) {
            min = Math.min(min, m2.min);
            max = Math.max(max, m2.max);
            sum += m2.sum;
            count += m2.count;
            return this;
        }
    }
}
