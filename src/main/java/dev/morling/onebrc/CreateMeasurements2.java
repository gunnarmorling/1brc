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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.rschwietzke.CheaperCharBuffer;
import org.rschwietzke.FastRandom;

/**
 * Faster version with some data faking instead of a real Gaussian distribution
 * Good enough for our purppose I guess.
 */
public class CreateMeasurements2 {

    private static final String FILE = "./measurements2.txt";

    static class WeatherStation {
        final static char[] NUMBERS = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9' };

        final String id;
        final int meanTemperature;

        final char[] firstPart;
        final FastRandom r = new FastRandom(ThreadLocalRandom.current().nextLong());

        WeatherStation(String id, double meanTemperature) {
            this.id = id;
            this.meanTemperature = (int) meanTemperature;
            // make it directly copyable
            this.firstPart = (id + ";").toCharArray();
        }

        /**
         * We write out data into the buffer to avoid string conversion
         * We also no longer use double and gaussian, because for our
         * purpose, the fake numbers here will do it. Less
         *
         * @param buffer the buffer to append to
         */
        void measurement(final CheaperCharBuffer buffer) {
            // fake -10.9 to +10.9 variance without double operations and rounding
            // gives us -10 to +10
            int m = meanTemperature + (r.nextInt(21) - 10);
            // gives us a decimal digit 0 to 9 as char
            char d = NUMBERS[r.nextInt(10)];

            // just append, only one number has to be converted and we can do
            // better... if we watn
            buffer.append(firstPart, 0, firstPart.length)
                    .append(String.valueOf(m)).append('.').append(d)
                    .append('\n');
        }
    }

    public static void main(String[] args) throws Exception {
        long start = System.currentTimeMillis();

        if (args.length != 1) {
            System.out.println("Usage: create_measurements.sh <number of records to create>");
            System.exit(1);
        }

        int size = 0;
        try {
            size = Integer.parseInt(args[0]);
        }
        catch (NumberFormatException e) {
            System.out.println("Invalid value for <number of records to create>");
            System.out.println("Usage: CreateMeasurements <number of records to create>");
            System.exit(1);
        }

        // @formatter:off
        // data from https://en.wikipedia.org/wiki/List_of_cities_by_average_temperature;
        // converted using https://wikitable2csv.ggor.de/
        // brought to form using DuckDB:
        // D copy (
        //     select City, regexp_extract(Year,'(.*)\n.*', 1) as AverageTemp
        //     from (
        //         select City,Year
        //         from read_csv_auto('List_of_cities_by_average_temperature_1.csv', header = true)
        //         union
        //         select City,Year
        //         from read_csv_auto('List_of_cities_by_average_temperature_2.csv', header = true)
        //         union
        //         select City,Year
        //         from read_csv_auto('List_of_cities_by_average_temperature_3.csv', header = true)
        //         union
        //         select City,Year
        //         from read_csv_auto('List_of_cities_by_average_temperature_4.csv', header = true)
        //         union
        //         select City,Year
        //         from read_csv_auto('List_of_cities_by_average_temperature_5.csv', header = true)
        //         )
        // ) TO 'output.csv' (HEADER, DELIMITER ',');
        // @formatter:on
        final List<WeatherStation> stations = Arrays.asList(
                new WeatherStation("Abha", 18.0),
                new WeatherStation("Abidjan", 26.0),
                new WeatherStation("Abéché", 29.4),
                new WeatherStation("Accra", 26.4),
                new WeatherStation("Addis Ababa", 16.0),
                new WeatherStation("Adelaide", 17.3),
                new WeatherStation("Aden", 29.1),
                new WeatherStation("Ahvaz", 25.4),
                new WeatherStation("Albuquerque", 14.0),
                new WeatherStation("Alexandra", 11.0),
                new WeatherStation("Alexandria", 20.0),
                new WeatherStation("Algiers", 18.2),
                new WeatherStation("Alice Springs", 21.0),
                new WeatherStation("Almaty", 10.0),
                new WeatherStation("Amsterdam", 10.2),
                new WeatherStation("Anadyr", -6.9),
                new WeatherStation("Anchorage", 2.8),
                new WeatherStation("Andorra la Vella", 9.8),
                new WeatherStation("Ankara", 12.0),
                new WeatherStation("Antananarivo", 17.9),
                new WeatherStation("Antsiranana", 25.2),
                new WeatherStation("Arkhangelsk", 1.3),
                new WeatherStation("Ashgabat", 17.1),
                new WeatherStation("Asmara", 15.6),
                new WeatherStation("Assab", 30.5),
                new WeatherStation("Astana", 3.5),
                new WeatherStation("Athens", 19.2),
                new WeatherStation("Atlanta", 17.0),
                new WeatherStation("Auckland", 15.2),
                new WeatherStation("Austin", 20.7),
                new WeatherStation("Baghdad", 22.77),
                new WeatherStation("Baguio", 19.5),
                new WeatherStation("Baku", 15.1),
                new WeatherStation("Baltimore", 13.1),
                new WeatherStation("Bamako", 27.8),
                new WeatherStation("Bangkok", 28.6),
                new WeatherStation("Bangui", 26.0),
                new WeatherStation("Banjul", 26.0),
                new WeatherStation("Barcelona", 18.2),
                new WeatherStation("Bata", 25.1),
                new WeatherStation("Batumi", 14.0),
                new WeatherStation("Beijing", 12.9),
                new WeatherStation("Beirut", 20.9),
                new WeatherStation("Belgrade", 12.5),
                new WeatherStation("Belize City", 26.7),
                new WeatherStation("Benghazi", 19.9),
                new WeatherStation("Bergen", 7.7),
                new WeatherStation("Berlin", 10.3),
                new WeatherStation("Bilbao", 14.7),
                new WeatherStation("Birao", 26.5),
                new WeatherStation("Bishkek", 11.3),
                new WeatherStation("Bissau", 27.0),
                new WeatherStation("Blantyre", 22.2),
                new WeatherStation("Bloemfontein", 15.6),
                new WeatherStation("Boise", 11.4),
                new WeatherStation("Bordeaux", 14.2),
                new WeatherStation("Bosaso", 30.0),
                new WeatherStation("Boston", 10.9),
                new WeatherStation("Bouaké", 26.0),
                new WeatherStation("Bratislava", 10.5),
                new WeatherStation("Brazzaville", 25.0),
                new WeatherStation("Bridgetown", 27.0),
                new WeatherStation("Brisbane", 21.4),
                new WeatherStation("Brussels", 10.5),
                new WeatherStation("Bucharest", 10.8),
                new WeatherStation("Budapest", 11.3),
                new WeatherStation("Bujumbura", 23.8),
                new WeatherStation("Bulawayo", 18.9),
                new WeatherStation("Burnie", 13.1),
                new WeatherStation("Busan", 15.0),
                new WeatherStation("Cabo San Lucas", 23.9),
                new WeatherStation("Cairns", 25.0),
                new WeatherStation("Cairo", 21.4),
                new WeatherStation("Calgary", 4.4),
                new WeatherStation("Canberra", 13.1),
                new WeatherStation("Cape Town", 16.2),
                new WeatherStation("Changsha", 17.4),
                new WeatherStation("Charlotte", 16.1),
                new WeatherStation("Chiang Mai", 25.8),
                new WeatherStation("Chicago", 9.8),
                new WeatherStation("Chihuahua", 18.6),
                new WeatherStation("Chișinău", 10.2),
                new WeatherStation("Chittagong", 25.9),
                new WeatherStation("Chongqing", 18.6),
                new WeatherStation("Christchurch", 12.2),
                new WeatherStation("City of San Marino", 11.8),
                new WeatherStation("Colombo", 27.4),
                new WeatherStation("Columbus", 11.7),
                new WeatherStation("Conakry", 26.4),
                new WeatherStation("Copenhagen", 9.1),
                new WeatherStation("Cotonou", 27.2),
                new WeatherStation("Cracow", 9.3),
                new WeatherStation("Da Lat", 17.9),
                new WeatherStation("Da Nang", 25.8),
                new WeatherStation("Dakar", 24.0),
                new WeatherStation("Dallas", 19.0),
                new WeatherStation("Damascus", 17.0),
                new WeatherStation("Dampier", 26.4),
                new WeatherStation("Dar es Salaam", 25.8),
                new WeatherStation("Darwin", 27.6),
                new WeatherStation("Denpasar", 23.7),
                new WeatherStation("Denver", 10.4),
                new WeatherStation("Detroit", 10.0),
                new WeatherStation("Dhaka", 25.9),
                new WeatherStation("Dikson", -11.1),
                new WeatherStation("Dili", 26.6),
                new WeatherStation("Djibouti", 29.9),
                new WeatherStation("Dodoma", 22.7),
                new WeatherStation("Dolisie", 24.0),
                new WeatherStation("Douala", 26.7),
                new WeatherStation("Dubai", 26.9),
                new WeatherStation("Dublin", 9.8),
                new WeatherStation("Dunedin", 11.1),
                new WeatherStation("Durban", 20.6),
                new WeatherStation("Dushanbe", 14.7),
                new WeatherStation("Edinburgh", 9.3),
                new WeatherStation("Edmonton", 4.2),
                new WeatherStation("El Paso", 18.1),
                new WeatherStation("Entebbe", 21.0),
                new WeatherStation("Erbil", 19.5),
                new WeatherStation("Erzurum", 5.1),
                new WeatherStation("Fairbanks", -2.3),
                new WeatherStation("Fianarantsoa", 17.9),
                new WeatherStation("Flores,  Petén", 26.4),
                new WeatherStation("Frankfurt", 10.6),
                new WeatherStation("Fresno", 17.9),
                new WeatherStation("Fukuoka", 17.0),
                new WeatherStation("Gabès", 19.5),
                new WeatherStation("Gaborone", 21.0),
                new WeatherStation("Gagnoa", 26.0),
                new WeatherStation("Gangtok", 15.2),
                new WeatherStation("Garissa", 29.3),
                new WeatherStation("Garoua", 28.3),
                new WeatherStation("George Town", 27.9),
                new WeatherStation("Ghanzi", 21.4),
                new WeatherStation("Gjoa Haven", -14.4),
                new WeatherStation("Guadalajara", 20.9),
                new WeatherStation("Guangzhou", 22.4),
                new WeatherStation("Guatemala City", 20.4),
                new WeatherStation("Halifax", 7.5),
                new WeatherStation("Hamburg", 9.7),
                new WeatherStation("Hamilton", 13.8),
                new WeatherStation("Hanga Roa", 20.5),
                new WeatherStation("Hanoi", 23.6),
                new WeatherStation("Harare", 18.4),
                new WeatherStation("Harbin", 5.0),
                new WeatherStation("Hargeisa", 21.7),
                new WeatherStation("Hat Yai", 27.0),
                new WeatherStation("Havana", 25.2),
                new WeatherStation("Helsinki", 5.9),
                new WeatherStation("Heraklion", 18.9),
                new WeatherStation("Hiroshima", 16.3),
                new WeatherStation("Ho Chi Minh City", 27.4),
                new WeatherStation("Hobart", 12.7),
                new WeatherStation("Hong Kong", 23.3),
                new WeatherStation("Honiara", 26.5),
                new WeatherStation("Honolulu", 25.4),
                new WeatherStation("Houston", 20.8),
                new WeatherStation("Ifrane", 11.4),
                new WeatherStation("Indianapolis", 11.8),
                new WeatherStation("Iqaluit", -9.3),
                new WeatherStation("Irkutsk", 1.0),
                new WeatherStation("Istanbul", 13.9),
                new WeatherStation("İzmir", 17.9),
                new WeatherStation("Jacksonville", 20.3),
                new WeatherStation("Jakarta", 26.7),
                new WeatherStation("Jayapura", 27.0),
                new WeatherStation("Jerusalem", 18.3),
                new WeatherStation("Johannesburg", 15.5),
                new WeatherStation("Jos", 22.8),
                new WeatherStation("Juba", 27.8),
                new WeatherStation("Kabul", 12.1),
                new WeatherStation("Kampala", 20.0),
                new WeatherStation("Kandi", 27.7),
                new WeatherStation("Kankan", 26.5),
                new WeatherStation("Kano", 26.4),
                new WeatherStation("Kansas City", 12.5),
                new WeatherStation("Karachi", 26.0),
                new WeatherStation("Karonga", 24.4),
                new WeatherStation("Kathmandu", 18.3),
                new WeatherStation("Khartoum", 29.9),
                new WeatherStation("Kingston", 27.4),
                new WeatherStation("Kinshasa", 25.3),
                new WeatherStation("Kolkata", 26.7),
                new WeatherStation("Kuala Lumpur", 27.3),
                new WeatherStation("Kumasi", 26.0),
                new WeatherStation("Kunming", 15.7),
                new WeatherStation("Kuopio", 3.4),
                new WeatherStation("Kuwait City", 25.7),
                new WeatherStation("Kyiv", 8.4),
                new WeatherStation("Kyoto", 15.8),
                new WeatherStation("La Ceiba", 26.2),
                new WeatherStation("La Paz", 23.7),
                new WeatherStation("Lagos", 26.8),
                new WeatherStation("Lahore", 24.3),
                new WeatherStation("Lake Havasu City", 23.7),
                new WeatherStation("Lake Tekapo", 8.7),
                new WeatherStation("Las Palmas de Gran Canaria", 21.2),
                new WeatherStation("Las Vegas", 20.3),
                new WeatherStation("Launceston", 13.1),
                new WeatherStation("Lhasa", 7.6),
                new WeatherStation("Libreville", 25.9),
                new WeatherStation("Lisbon", 17.5),
                new WeatherStation("Livingstone", 21.8),
                new WeatherStation("Ljubljana", 10.9),
                new WeatherStation("Lodwar", 29.3),
                new WeatherStation("Lomé", 26.9),
                new WeatherStation("London", 11.3),
                new WeatherStation("Los Angeles", 18.6),
                new WeatherStation("Louisville", 13.9),
                new WeatherStation("Luanda", 25.8),
                new WeatherStation("Lubumbashi", 20.8),
                new WeatherStation("Lusaka", 19.9),
                new WeatherStation("Luxembourg City", 9.3),
                new WeatherStation("Lviv", 7.8),
                new WeatherStation("Lyon", 12.5),
                new WeatherStation("Madrid", 15.0),
                new WeatherStation("Mahajanga", 26.3),
                new WeatherStation("Makassar", 26.7),
                new WeatherStation("Makurdi", 26.0),
                new WeatherStation("Malabo", 26.3),
                new WeatherStation("Malé", 28.0),
                new WeatherStation("Managua", 27.3),
                new WeatherStation("Manama", 26.5),
                new WeatherStation("Mandalay", 28.0),
                new WeatherStation("Mango", 28.1),
                new WeatherStation("Manila", 28.4),
                new WeatherStation("Maputo", 22.8),
                new WeatherStation("Marrakesh", 19.6),
                new WeatherStation("Marseille", 15.8),
                new WeatherStation("Maun", 22.4),
                new WeatherStation("Medan", 26.5),
                new WeatherStation("Mek'ele", 22.7),
                new WeatherStation("Melbourne", 15.1),
                new WeatherStation("Memphis", 17.2),
                new WeatherStation("Mexicali", 23.1),
                new WeatherStation("Mexico City", 17.5),
                new WeatherStation("Miami", 24.9),
                new WeatherStation("Milan", 13.0),
                new WeatherStation("Milwaukee", 8.9),
                new WeatherStation("Minneapolis", 7.8),
                new WeatherStation("Minsk", 6.7),
                new WeatherStation("Mogadishu", 27.1),
                new WeatherStation("Mombasa", 26.3),
                new WeatherStation("Monaco", 16.4),
                new WeatherStation("Moncton", 6.1),
                new WeatherStation("Monterrey", 22.3),
                new WeatherStation("Montreal", 6.8),
                new WeatherStation("Moscow", 5.8),
                new WeatherStation("Mumbai", 27.1),
                new WeatherStation("Murmansk", 0.6),
                new WeatherStation("Muscat", 28.0),
                new WeatherStation("Mzuzu", 17.7),
                new WeatherStation("N'Djamena", 28.3),
                new WeatherStation("Naha", 23.1),
                new WeatherStation("Nairobi", 17.8),
                new WeatherStation("Nakhon Ratchasima", 27.3),
                new WeatherStation("Napier", 14.6),
                new WeatherStation("Napoli", 15.9),
                new WeatherStation("Nashville", 15.4),
                new WeatherStation("Nassau", 24.6),
                new WeatherStation("Ndola", 20.3),
                new WeatherStation("New Delhi", 25.0),
                new WeatherStation("New Orleans", 20.7),
                new WeatherStation("New York City", 12.9),
                new WeatherStation("Ngaoundéré", 22.0),
                new WeatherStation("Niamey", 29.3),
                new WeatherStation("Nicosia", 19.7),
                new WeatherStation("Niigata", 13.9),
                new WeatherStation("Nouadhibou", 21.3),
                new WeatherStation("Nouakchott", 25.7),
                new WeatherStation("Novosibirsk", 1.7),
                new WeatherStation("Nuuk", -1.4),
                new WeatherStation("Odesa", 10.7),
                new WeatherStation("Odienné", 26.0),
                new WeatherStation("Oklahoma City", 15.9),
                new WeatherStation("Omaha", 10.6),
                new WeatherStation("Oranjestad", 28.1),
                new WeatherStation("Oslo", 5.7),
                new WeatherStation("Ottawa", 6.6),
                new WeatherStation("Ouagadougou", 28.3),
                new WeatherStation("Ouahigouya", 28.6),
                new WeatherStation("Ouarzazate", 18.9),
                new WeatherStation("Oulu", 2.7),
                new WeatherStation("Palembang", 27.3),
                new WeatherStation("Palermo", 18.5),
                new WeatherStation("Palm Springs", 24.5),
                new WeatherStation("Palmerston North", 13.2),
                new WeatherStation("Panama City", 28.0),
                new WeatherStation("Parakou", 26.8),
                new WeatherStation("Paris", 12.3),
                new WeatherStation("Perth", 18.7),
                new WeatherStation("Petropavlovsk-Kamchatsky", 1.9),
                new WeatherStation("Philadelphia", 13.2),
                new WeatherStation("Phnom Penh", 28.3),
                new WeatherStation("Phoenix", 23.9),
                new WeatherStation("Pittsburgh", 10.8),
                new WeatherStation("Podgorica", 15.3),
                new WeatherStation("Pointe-Noire", 26.1),
                new WeatherStation("Pontianak", 27.7),
                new WeatherStation("Port Moresby", 26.9),
                new WeatherStation("Port Sudan", 28.4),
                new WeatherStation("Port Vila", 24.3),
                new WeatherStation("Port-Gentil", 26.0),
                new WeatherStation("Portland (OR)", 12.4),
                new WeatherStation("Porto", 15.7),
                new WeatherStation("Prague", 8.4),
                new WeatherStation("Praia", 24.4),
                new WeatherStation("Pretoria", 18.2),
                new WeatherStation("Pyongyang", 10.8),
                new WeatherStation("Rabat", 17.2),
                new WeatherStation("Rangpur", 24.4),
                new WeatherStation("Reggane", 28.3),
                new WeatherStation("Reykjavík", 4.3),
                new WeatherStation("Riga", 6.2),
                new WeatherStation("Riyadh", 26.0),
                new WeatherStation("Rome", 15.2),
                new WeatherStation("Roseau", 26.2),
                new WeatherStation("Rostov-on-Don", 9.9),
                new WeatherStation("Sacramento", 16.3),
                new WeatherStation("Saint Petersburg", 5.8),
                new WeatherStation("Saint-Pierre", 5.7),
                new WeatherStation("Salt Lake City", 11.6),
                new WeatherStation("San Antonio", 20.8),
                new WeatherStation("San Diego", 17.8),
                new WeatherStation("San Francisco", 14.6),
                new WeatherStation("San Jose", 16.4),
                new WeatherStation("San José", 22.6),
                new WeatherStation("San Juan", 27.2),
                new WeatherStation("San Salvador", 23.1),
                new WeatherStation("Sana'a", 20.0),
                new WeatherStation("Santo Domingo", 25.9),
                new WeatherStation("Sapporo", 8.9),
                new WeatherStation("Sarajevo", 10.1),
                new WeatherStation("Saskatoon", 3.3),
                new WeatherStation("Seattle", 11.3),
                new WeatherStation("Ségou", 28.0),
                new WeatherStation("Seoul", 12.5),
                new WeatherStation("Seville", 19.2),
                new WeatherStation("Shanghai", 16.7),
                new WeatherStation("Singapore", 27.0),
                new WeatherStation("Skopje", 12.4),
                new WeatherStation("Sochi", 14.2),
                new WeatherStation("Sofia", 10.6),
                new WeatherStation("Sokoto", 28.0),
                new WeatherStation("Split", 16.1),
                new WeatherStation("St. John's", 5.0),
                new WeatherStation("St. Louis", 13.9),
                new WeatherStation("Stockholm", 6.6),
                new WeatherStation("Surabaya", 27.1),
                new WeatherStation("Suva", 25.6),
                new WeatherStation("Suwałki", 7.2),
                new WeatherStation("Sydney", 17.7),
                new WeatherStation("Tabora", 23.0),
                new WeatherStation("Tabriz", 12.6),
                new WeatherStation("Taipei", 23.0),
                new WeatherStation("Tallinn", 6.4),
                new WeatherStation("Tamale", 27.9),
                new WeatherStation("Tamanrasset", 21.7),
                new WeatherStation("Tampa", 22.9),
                new WeatherStation("Tashkent", 14.8),
                new WeatherStation("Tauranga", 14.8),
                new WeatherStation("Tbilisi", 12.9),
                new WeatherStation("Tegucigalpa", 21.7),
                new WeatherStation("Tehran", 17.0),
                new WeatherStation("Tel Aviv", 20.0),
                new WeatherStation("Thessaloniki", 16.0),
                new WeatherStation("Thiès", 24.0),
                new WeatherStation("Tijuana", 17.8),
                new WeatherStation("Timbuktu", 28.0),
                new WeatherStation("Tirana", 15.2),
                new WeatherStation("Toamasina", 23.4),
                new WeatherStation("Tokyo", 15.4),
                new WeatherStation("Toliara", 24.1),
                new WeatherStation("Toluca", 12.4),
                new WeatherStation("Toronto", 9.4),
                new WeatherStation("Tripoli", 20.0),
                new WeatherStation("Tromsø", 2.9),
                new WeatherStation("Tucson", 20.9),
                new WeatherStation("Tunis", 18.4),
                new WeatherStation("Ulaanbaatar", -0.4),
                new WeatherStation("Upington", 20.4),
                new WeatherStation("Ürümqi", 7.4),
                new WeatherStation("Vaduz", 10.1),
                new WeatherStation("Valencia", 18.3),
                new WeatherStation("Valletta", 18.8),
                new WeatherStation("Vancouver", 10.4),
                new WeatherStation("Veracruz", 25.4),
                new WeatherStation("Vienna", 10.4),
                new WeatherStation("Vientiane", 25.9),
                new WeatherStation("Villahermosa", 27.1),
                new WeatherStation("Vilnius", 6.0),
                new WeatherStation("Virginia Beach", 15.8),
                new WeatherStation("Vladivostok", 4.9),
                new WeatherStation("Warsaw", 8.5),
                new WeatherStation("Washington, D.C.", 14.6),
                new WeatherStation("Wau", 27.8),
                new WeatherStation("Wellington", 12.9),
                new WeatherStation("Whitehorse", -0.1),
                new WeatherStation("Wichita", 13.9),
                new WeatherStation("Willemstad", 28.0),
                new WeatherStation("Winnipeg", 3.0),
                new WeatherStation("Wrocław", 9.6),
                new WeatherStation("Xi'an", 14.1),
                new WeatherStation("Yakutsk", -8.8),
                new WeatherStation("Yangon", 27.5),
                new WeatherStation("Yaoundé", 23.8),
                new WeatherStation("Yellowknife", -4.3),
                new WeatherStation("Yerevan", 12.4),
                new WeatherStation("Yinchuan", 9.0),
                new WeatherStation("Zagreb", 10.7),
                new WeatherStation("Zanzibar City", 26.0),
                new WeatherStation("Zürich", 9.3));

        File file = new File(FILE);

        // break the loop and unroll it manually
        int strideSize = 50_000_000;
        int outer = size / strideSize;
        int remainder = size - (outer * strideSize);

        try (final BufferedWriter bw = new BufferedWriter(new FileWriter(file))) {
            for (int i = 0; i < outer; i++) {
                produce(bw, stations, strideSize);

                // we avoid a modulo if here and use the stride size to print and update
                System.out.println("Wrote %,d measurements in %s ms".formatted((i + 1) * strideSize, System.currentTimeMillis() - start));
            }
            // there might be a rest
            produce(bw, stations, remainder);

            // write fully before taking measurements
            bw.flush();
            System.out.println("Created file with %,d measurements in %s ms".formatted(size, System.currentTimeMillis() - start));
        }
    }

    private static void produce(BufferedWriter bw, List<WeatherStation> stations, int count) throws IOException {
        final int stationCount = stations.size();
        final int rest = count % 8;

        // use a fast ranodm impl without atomics to be able to utilize the cpu better
        // and avoid sideeffects, FastRandom is very fake random and does not have a state
        final FastRandom r1 = new FastRandom(ThreadLocalRandom.current().nextLong());
        final FastRandom r2 = new FastRandom(ThreadLocalRandom.current().nextLong());
        final FastRandom r3 = new FastRandom(ThreadLocalRandom.current().nextLong());
        final FastRandom r4 = new FastRandom(ThreadLocalRandom.current().nextLong());

        // write to a fix buffer first, don't create strings ever
        // reuse buffer
        final CheaperCharBuffer sb = new CheaperCharBuffer(200);

        // manual loop unroll for less jumps
        for (int i = 0; i < count; i = i + 8) {
            {
                // try to fill teh cpu pipeline as much as possible with
                // independent operations
                int s1 = r1.nextInt(stationCount);
                int s2 = r2.nextInt(stationCount);
                int s3 = r3.nextInt(stationCount);
                int s4 = r4.nextInt(stationCount);
                // get us the ojects one after the other to have the array
                // in our L1 cache and not push it out with other data
                var w1 = stations.get(s1);
                var w2 = stations.get(s2);
                var w3 = stations.get(s3);
                var w4 = stations.get(s4);
                // write our data to our buffer
                w1.measurement(sb);
                w2.measurement(sb);
                w3.measurement(sb);
                w4.measurement(sb);
            }
            {
                int s1 = r1.nextInt(stationCount);
                int s2 = r2.nextInt(stationCount);
                int s3 = r3.nextInt(stationCount);
                int s4 = r4.nextInt(stationCount);
                var w1 = stations.get(s1);
                var w2 = stations.get(s2);
                var w3 = stations.get(s3);
                var w4 = stations.get(s4);
                w1.measurement(sb);
                w2.measurement(sb);
                w3.measurement(sb);
                w4.measurement(sb);
            }
            // write the buffer directly, no intermediate string copy
            bw.write(sb.data_, 0, sb.length_);

            // reuse buffer, reset only, no cleaning
            sb.clear();
        }

        // there might be a rest to write
        for (int i = 0; i < rest; i++) {
            sb.clear();

            int s = r1.nextInt(stationCount);
            var w = stations.get(s);
            w.measurement(sb);

            bw.write(sb.data_, 0, sb.length_);
        }
    }
}
