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

import java.io.IOException;
import java.lang.foreign.Arena;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.stream.Stream;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toMap;

// Note: don't be abused by the optimizations for the challenge
// * yes Double.parseDouble is slow...but impl is more generic and it is fast enough for most apps
// * yes HashMap/Hashtable structures are slow...but really fast for most usages (and saner than the list backed map like - Aggregation/Stations - but here hashcode was too expensive)
// * yes iterating over an array can be slow...seriously i'll not explain this one
// * yes using a DTO (Measurement) is slow...guess you got the idea
// * yes using String is slow...until you have another marshalling (avro/parquet even json or xml) it is still not that bad
// * yes yes yes, equals/hashcode should always respect their contract...not like in Bytes
// * /!\ however loading in mem a buffer is often accurate and even reusing buffer (even if the JVM GC is great) and can be kept in all apps
public class CalculateAverage_rmannibucau {

    private static final String FILE = "./measurements.txt";

    public static void main(final String... args) throws Throwable {
        new CalculateAverage_rmannibucau().run(args);
    }

    private void run(final String... args) throws Throwable {
        final var input = Paths.get(args.length == 0 ? FILE : args[0]);

        final long size = Files.size(input);
        final var allowedThreadCount = Math.max(2, Runtime.getRuntime().availableProcessors());
        final long chunkSize = Math.max(1, size / allowedThreadCount);

        final List<ForkJoinTask<Map<Bytes, MeasurementAggregator>>> tasks;
        try (final var arena = Arena.ofConfined()) { // don't let the gc catch up our poor programming model so let's control it
            final var offsets = offsetsFor(input, size, chunkSize, arena);

            // tested structured concurrency but it does not help (in style nor perf indeed), only in thread optim due to IO nature, not critical here
            // "map" phase of the map/reduce
            final var parallelism = Math.min(offsets.size(), allowedThreadCount);
            try (final var threads = new ForkJoinPool(parallelism)) { // commonPool() is a design joke even if it would work there
                tasks = offsets.stream()
                        .filter(it -> it.start() != it.end())
                        .map(i -> threads.submit(() -> parseChunk(i)))
                        .toList();
            }
        }

        // await + merge results
        final var globalResult = tasks.stream().flatMap(f -> {
            try {
                return f.get().entrySet().stream();
            }
            catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
            catch (final ExecutionException e) {
                throw new IllegalStateException(e.getCause());
            }
        }).collect(toMap(Map.Entry::getKey, Map.Entry::getValue, this::merge));

        // just format the output
        System.out.println(formatOutput(globalResult));
    }

    private Map<Bytes, MeasurementAggregator> parseChunk(final Offset offset) {
        final Map<Bytes, MeasurementAggregator> res = new HashMap<>(512); // there are only ~413 stations so avoid resizing
        long memIndex = offset.start();
        while (memIndex < offset.end()) {

            final long nameStart = memIndex;
            // save a a loop by computing the hash with finding ';'
            // IMPORTANT: if you change it use ComputePotentialStationDiffsPerHash to ensure you didn't break hash distribution
            int hash = UNSAFE.getByte(memIndex++);
            byte c;
            while ((c = UNSAFE.getByte(memIndex++)) != ';') {
                hash *= 31;
                hash += c;
            }
            final long nameEnd = memIndex - 1;

            // indeed, in real life better handle error cases and don't be so minimalistic, FastDoubleParser is not bad
            final boolean negative;
            if ((c = UNSAFE.getByte(memIndex++)) == '-') {
                negative = true;
                c = UNSAFE.getByte(memIndex++);
            }
            else {
                negative = false;
            }

            int integer = c - '0';
            while ((c = UNSAFE.getByte(memIndex++)) != '\n') {
                if (c == '.') {
                    if (memIndex < offset.end() && (c = UNSAFE.getByte(memIndex++)) != '\n') { // no decimal part actually if '\n'
                        integer = integer * 10 + (c - '0');
                    }
                    else {
                        integer *= 10;
                    }
                    break;
                }
                integer = 10 * integer + (c - '0');
            }
            if (negative) {
                integer = -integer;
            }

            final var tempKey = new Bytes(null, hash);
            var agg = res.get(tempKey);
            if (agg == null) {
                final var name = new byte[(int) (nameEnd - nameStart)];
                UNSAFE.copyMemory(null, nameStart, name, Unsafe.ARRAY_BYTE_BASE_OFFSET, name.length);
                res.put(
                        new Bytes(
                                name,
                                tempKey.hashCode()),
                        new MeasurementAggregator(integer, integer, integer, 1));
            }
            else {
                appendValue(agg, integer);
            }
            memIndex++; // EOL
        }
        return res;
    }

    private List<Offset> offsetsFor(final Path input, final long size, final long chunkSize, final Arena arena) {
        final var firstSegmentEnd = Math.min(size, chunkSize);
        try (final var channel = FileChannel.open(input, EnumSet.of(StandardOpenOption.READ))) {
            // thanks Thomas Wue! we don't use the memory segments to bypass checks (see jdk.internal.misc.ScopedMemoryAccess.getByteInternal)
            final long ptr = channel.map(READ_ONLY, 0, size, arena).address();
            return Stream.iterate(
                    new Offset(ptr, ptr + firstSegmentEnd + (firstSegmentEnd == size ? 0 : eolNextOffset(ptr + firstSegmentEnd, false))),
                    Objects::nonNull,
                    e -> {
                        if (e.end() == ptr + size) {
                            return null;
                        }
                        final var splitStart = e.end();
                        final var splitEnd = Math.min(ptr + size, splitStart + chunkSize);
                        try { // align on the right ranges current segment - normally done in the iterator in big data but a bit slower and here it can be done locally
                            final long start = splitStart + (splitStart > ptr ? eolNextOffset(splitStart, true) : 0);
                            final long end = splitEnd + (splitEnd < ptr + size ? eolNextOffset(splitEnd, false) : 0);
                            return new Offset(start, end);
                        }
                        catch (final IOException ioe) {
                            throw new IllegalStateException(ioe);
                        }
                    })
                    .toList();
        }
        catch (final IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private Map<String, ResultRow> formatOutput(final Map<Bytes, MeasurementAggregator> measurements) {
        return new TreeMap<>(measurements.entrySet().stream()
                .collect(toMap(
                        e -> e.getKey().toString(),
                        e -> {
                            final var agg = e.getValue();
                            return new ResultRow(agg.min / 10f, agg.sum * 0.1f / agg.total, agg.max / 10f);
                        })));
    }

    private MeasurementAggregator merge(final MeasurementAggregator agg1, final MeasurementAggregator agg2) {
        return new MeasurementAggregator(Math.min(agg1.min, agg2.min), Math.max(agg1.max, agg2.max), agg1.sum + agg2.sum, agg1.total + agg2.total);
    }

    private void appendValue(final MeasurementAggregator agg, final int value) {
        if (value < agg.min) {
            agg.min = value;
        }
        // if we were a min we can't be a max and if we are a max we can't be a min since we init both with the same value
        else if (value > agg.max) {
            agg.max = value;
        }
        agg.sum += value;
        agg.total++;
    }

    private int eolNextOffset(final long position, final boolean start) throws IOException {
        long i = position;
        while (UNSAFE.getByte(i) != '\n') {
            i++;
        }
        if (UNSAFE.getByte(i) == '\n') {
            return (int) (i + (start ? 1 : 0) - position);
        }
        return (int) (i - position);
    }

    private static final Unsafe UNSAFE = initUnsafe();

    private static Unsafe initUnsafe() {
        try {
            final var theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            return (Unsafe) theUnsafe.get(Unsafe.class);
        }
        catch (final NoSuchFieldException | IllegalAccessException e) {
            throw new IllegalStateException(e);
        }
    }

    private static class Bytes {
        private final byte[] value;
        private final int hash;

        private Bytes(final byte[] value, final int precomputedHash) {
            this.value = value;
            this.hash = precomputedHash;
        }

        @Override
        public String toString() {
            return new String(value, UTF_8);
        }

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public boolean equals(final Object obj) { // we don't need to check the type since it is an internal usage "we know"
            // important: we used ComputePotentialStationDiffsPerHash to ensure that if the hash is the same there it is the same value
            // so no need to do anything there
            return true;
        }
    }

    private record Offset(long start, long end) {
    }

    private record ResultRow(float min, float mean, float max) {
        public String toString() {
            return STR."\{round(min)}/\{round(mean)}/\{round(max)}";
        }

        private double round(final float value) {
            return Math.round(value * 10.f) / 10.;
        }
    }

    private static class MeasurementAggregator {
        private int min;
        private int max;
        private long sum;
        private long total;

        private MeasurementAggregator(final int min, final int max, final long sum, final long total) {
            this.min = min;
            this.max = max;
            this.sum = sum;
            this.total = total;
        }
    }

    // NOTE: this is a work code which shouldn't stay but kept to explain the rational behind some impl choice (in Bytes) for the exercise
    // we assume the station set is closed as of today and optimize Bytes equals impl by checking conflicts only per hash
    public static class ComputePotentialStationDiffsPerHash {
        private ComputePotentialStationDiffsPerHash() {
            // no-op
        }

        public static void main(final String... args) {
            final var stations = List.of("Abha",
                    "Abidjan",
                    "Abéché",
                    "Accra",
                    "Addis Ababa",
                    "Adelaide",
                    "Aden",
                    "Ahvaz",
                    "Albuquerque",
                    "Alexandra",
                    "Alexandria",
                    "Algiers",
                    "Alice Springs",
                    "Almaty",
                    "Amsterdam",
                    "Anadyr",
                    "Anchorage",
                    "Andorra la Vella",
                    "Ankara",
                    "Antananarivo",
                    "Antsiranana",
                    "Arkhangelsk",
                    "Ashgabat",
                    "Asmara",
                    "Assab",
                    "Astana",
                    "Athens",
                    "Atlanta",
                    "Auckland",
                    "Austin",
                    "Baghdad",
                    "Baguio",
                    "Baku",
                    "Baltimore",
                    "Bamako",
                    "Bangkok",
                    "Bangui",
                    "Banjul",
                    "Barcelona",
                    "Bata",
                    "Batumi",
                    "Beijing",
                    "Beirut",
                    "Belgrade",
                    "Belize City",
                    "Benghazi",
                    "Bergen",
                    "Berlin",
                    "Bilbao",
                    "Birao",
                    "Bishkek",
                    "Bissau",
                    "Blantyre",
                    "Bloemfontein",
                    "Boise",
                    "Bordeaux",
                    "Bosaso",
                    "Boston",
                    "Bouaké",
                    "Bratislava",
                    "Brazzaville",
                    "Bridgetown",
                    "Brisbane",
                    "Brussels",
                    "Bucharest",
                    "Budapest",
                    "Bujumbura",
                    "Bulawayo",
                    "Burnie",
                    "Busan",
                    "Cabo San Lucas",
                    "Cairns",
                    "Cairo",
                    "Calgary",
                    "Canberra",
                    "Cape Town",
                    "Changsha",
                    "Charlotte",
                    "Chiang Mai",
                    "Chicago",
                    "Chihuahua",
                    "Chișinău",
                    "Chittagong",
                    "Chongqing",
                    "Christchurch",
                    "City of San Marino",
                    "Colombo",
                    "Columbus",
                    "Conakry",
                    "Copenhagen",
                    "Cotonou",
                    "Cracow",
                    "Da Lat",
                    "Da Nang",
                    "Dakar",
                    "Dallas",
                    "Damascus",
                    "Dampier",
                    "Dar es Salaam",
                    "Darwin",
                    "Denpasar",
                    "Denver",
                    "Detroit",
                    "Dhaka",
                    "Dikson",
                    "Dili",
                    "Djibouti",
                    "Dodoma",
                    "Dolisie",
                    "Douala",
                    "Dubai",
                    "Dublin",
                    "Dunedin",
                    "Durban",
                    "Dushanbe",
                    "Edinburgh",
                    "Edmonton",
                    "El Paso",
                    "Entebbe",
                    "Erbil",
                    "Erzurum",
                    "Fairbanks",
                    "Fianarantsoa",
                    "Flores,  Petén",
                    "Frankfurt",
                    "Fresno",
                    "Fukuoka",
                    "Gabès",
                    "Gaborone",
                    "Gagnoa",
                    "Gangtok",
                    "Garissa",
                    "Garoua",
                    "George Town",
                    "Ghanzi",
                    "Gjoa Haven",
                    "Guadalajara",
                    "Guangzhou",
                    "Guatemala City",
                    "Halifax",
                    "Hamburg",
                    "Hamilton",
                    "Hanga Roa",
                    "Hanoi",
                    "Harare",
                    "Harbin",
                    "Hargeisa",
                    "Hat Yai",
                    "Havana",
                    "Helsinki",
                    "Heraklion",
                    "Hiroshima",
                    "Ho Chi Minh City",
                    "Hobart",
                    "Hong Kong",
                    "Honiara",
                    "Honolulu",
                    "Houston",
                    "Ifrane",
                    "Indianapolis",
                    "Iqaluit",
                    "Irkutsk",
                    "Istanbul",
                    "İzmir",
                    "Jacksonville",
                    "Jakarta",
                    "Jayapura",
                    "Jerusalem",
                    "Johannesburg",
                    "Jos",
                    "Juba",
                    "Kabul",
                    "Kampala",
                    "Kandi",
                    "Kankan",
                    "Kano",
                    "Kansas City",
                    "Karachi",
                    "Karonga",
                    "Kathmandu",
                    "Khartoum",
                    "Kingston",
                    "Kinshasa",
                    "Kolkata",
                    "Kuala Lumpur",
                    "Kumasi",
                    "Kunming",
                    "Kuopio",
                    "Kuwait City",
                    "Kyiv",
                    "Kyoto",
                    "La Ceiba",
                    "La Paz",
                    "Lagos",
                    "Lahore",
                    "Lake Havasu City",
                    "Lake Tekapo",
                    "Las Palmas de Gran Canaria",
                    "Las Vegas",
                    "Launceston",
                    "Lhasa",
                    "Libreville",
                    "Lisbon",
                    "Livingstone",
                    "Ljubljana",
                    "Lodwar",
                    "Lomé",
                    "London",
                    "Los Angeles",
                    "Louisville",
                    "Luanda",
                    "Lubumbashi",
                    "Lusaka",
                    "Luxembourg City",
                    "Lviv",
                    "Lyon",
                    "Madrid",
                    "Mahajanga",
                    "Makassar",
                    "Makurdi",
                    "Malabo",
                    "Malé",
                    "Managua",
                    "Manama",
                    "Mandalay",
                    "Mango",
                    "Manila",
                    "Maputo",
                    "Marrakesh",
                    "Marseille",
                    "Maun",
                    "Medan",
                    "Mek'ele",
                    "Melbourne",
                    "Memphis",
                    "Mexicali",
                    "Mexico City",
                    "Miami",
                    "Milan",
                    "Milwaukee",
                    "Minneapolis",
                    "Minsk",
                    "Mogadishu",
                    "Mombasa",
                    "Monaco",
                    "Moncton",
                    "Monterrey",
                    "Montreal",
                    "Moscow",
                    "Mumbai",
                    "Murmansk",
                    "Muscat",
                    "Mzuzu",
                    "N'Djamena",
                    "Naha",
                    "Nairobi",
                    "Nakhon Ratchasima",
                    "Napier",
                    "Napoli",
                    "Nashville",
                    "Nassau",
                    "Ndola",
                    "New Delhi",
                    "New Orleans",
                    "New York City",
                    "Ngaoundéré",
                    "Niamey",
                    "Nicosia",
                    "Niigata",
                    "Nouadhibou",
                    "Nouakchott",
                    "Novosibirsk",
                    "Nuuk",
                    "Odesa",
                    "Odienné",
                    "Oklahoma City",
                    "Omaha",
                    "Oranjestad",
                    "Oslo",
                    "Ottawa",
                    "Ouagadougou",
                    "Ouahigouya",
                    "Ouarzazate",
                    "Oulu",
                    "Palembang",
                    "Palermo",
                    "Palm Springs",
                    "Palmerston North",
                    "Panama City",
                    "Parakou",
                    "Paris",
                    "Perth",
                    "Petropavlovsk-Kamchatsky",
                    "Philadelphia",
                    "Phnom Penh",
                    "Phoenix",
                    "Pittsburgh",
                    "Podgorica",
                    "Pointe-Noire",
                    "Pontianak",
                    "Port Moresby",
                    "Port Sudan",
                    "Port Vila",
                    "Port-Gentil",
                    "Portland (OR)",
                    "Porto",
                    "Prague",
                    "Praia",
                    "Pretoria",
                    "Pyongyang",
                    "Rabat",
                    "Rangpur",
                    "Reggane",
                    "Reykjavík",
                    "Riga",
                    "Riyadh",
                    "Rome",
                    "Roseau",
                    "Rostov-on-Don",
                    "Sacramento",
                    "Saint Petersburg",
                    "Saint-Pierre",
                    "Salt Lake City",
                    "San Antonio",
                    "San Diego",
                    "San Francisco",
                    "San Jose",
                    "San José",
                    "San Juan",
                    "San Salvador",
                    "Sana'a",
                    "Santo Domingo",
                    "Sapporo",
                    "Sarajevo",
                    "Saskatoon",
                    "Seattle",
                    "Ségou",
                    "Seoul",
                    "Seville",
                    "Shanghai",
                    "Singapore",
                    "Skopje",
                    "Sochi",
                    "Sofia",
                    "Sokoto",
                    "Split",
                    "St. John's",
                    "St. Louis",
                    "Stockholm",
                    "Surabaya",
                    "Suva",
                    "Suwałki",
                    "Sydney",
                    "Tabora",
                    "Tabriz",
                    "Taipei",
                    "Tallinn",
                    "Tamale",
                    "Tamanrasset",
                    "Tampa",
                    "Tashkent",
                    "Tauranga",
                    "Tbilisi",
                    "Tegucigalpa",
                    "Tehran",
                    "Tel Aviv",
                    "Thessaloniki",
                    "Thiès",
                    "Tijuana",
                    "Timbuktu",
                    "Tirana",
                    "Toamasina",
                    "Tokyo",
                    "Toliara",
                    "Toluca",
                    "Toronto",
                    "Tripoli",
                    "Tromsø",
                    "Tucson",
                    "Tunis",
                    "Ulaanbaatar",
                    "Upington",
                    "Ürümqi",
                    "Vaduz",
                    "Valencia",
                    "Valletta",
                    "Vancouver",
                    "Veracruz",
                    "Vienna",
                    "Vientiane",
                    "Villahermosa",
                    "Vilnius",
                    "Virginia Beach",
                    "Vladivostok",
                    "Warsaw",
                    "Washington, D.C.",
                    "Wau",
                    "Wellington",
                    "Whitehorse",
                    "Wichita",
                    "Willemstad",
                    "Winnipeg",
                    "Wrocław",
                    "Xi'an",
                    "Yakutsk",
                    "Yangon",
                    "Yaoundé",
                    "Yellowknife",
                    "Yerevan",
                    "Yinchuan",
                    "Zagreb",
                    "Zanzibar City",
                    "Zürich");
            final var conflicts = stations.stream()
                    .map(it -> {
                        final var bytes = it.getBytes(UTF_8);
                        int hash = bytes[0];
                        for (int i = 1; i < bytes.length; i++) {
                            hash *= 31;
                            hash += bytes[i];
                        }
                        return new Bytes(bytes, hash);
                    })
                    .collect(groupingBy(it -> it.hash)).entrySet().stream()
                    .filter(it -> it.getValue().size() > 1)
                    .collect(toMap(Map.Entry::getKey, e -> e.getValue().stream()
                            .collect(toMap(identity(), identity(), (a, b) -> {
                                throw new IllegalStateException("oops: " + a + "/" + b);
                            }, () -> new HashMap<>(512)))));
            System.out.println(conflicts); // 22 conflicts but last 2 chars (and bytes) are never the same
        }
    }
}
