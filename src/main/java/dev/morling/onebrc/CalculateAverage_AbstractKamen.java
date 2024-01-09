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
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class CalculateAverage_AbstractKamen {

    private static final String FILE = "./measurements.txt";
    private static final String SMALL_FILE = "./smallmeasurements.txt";

    private static class Measurement {
        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private double sum;
        private long count;

        public String toString() {
            return round(min) + "/" + round(sum / count) + "/" + round(max);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }

    public static void main(String[] args) throws IOException {
        try (final FileChannel fc = FileChannel.open(Paths.get(FILE), StandardOpenOption.READ);
             final RandomAccessFile raf = new RandomAccessFile(new File(FILE), "r")) {
            final Map<String, Measurement> res = getParallelBufferStream(raf, fc)
                .map(CalculateAverage_AbstractKamen::getMeasurements)
                .flatMap(m -> m.entrySet().stream())
                .collect(Collectors.collectingAndThen(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                                                                       CalculateAverage_AbstractKamen::aggregateMeasurements),
                                                      TreeMap::new));
            System.out.println(res);
            //System.out.println(res.toString().equals("{Abha=-36.3/18.0/68.6, Abidjan=-22.4/26.0/73.2, Abéché=-20.1/29.4/79.5, Accra=-22.2/26.4/78.8, Addis Ababa=-35.2/16.0/65.9, Adelaide=-32.9/17.3/67.8, Aden=-19.6/29.1/80.8, Ahvaz=-20.8/25.4/77.7, Albuquerque=-36.9/14.0/63.1, Alexandra=-37.2/11.0/62.1, Alexandria=-28.0/20.0/68.8, Algiers=-31.6/18.2/66.8, Alice Springs=-26.0/21.0/69.8, Almaty=-38.0/10.0/60.8, Amsterdam=-40.7/10.2/59.4, Anadyr=-61.5/-6.9/44.3, Anchorage=-47.7/2.8/53.5, Andorra la Vella=-38.5/9.8/61.0, Ankara=-36.9/12.0/67.7, Antananarivo=-29.5/17.9/68.8, Antsiranana=-23.4/25.2/76.6, Arkhangelsk=-47.7/1.3/53.9, Ashgabat=-30.4/17.1/63.7, Asmara=-34.3/15.6/69.3, Assab=-17.1/30.5/80.9, Astana=-50.7/3.5/53.4, Athens=-32.0/19.2/69.7, Atlanta=-32.3/17.0/72.5, Auckland=-34.3/15.2/63.0, Austin=-29.1/20.7/72.4, Baghdad=-31.5/22.8/73.0, Baguio=-34.1/19.5/68.5, Baku=-35.9/15.1/65.1, Baltimore=-35.8/13.1/61.5, Bamako=-20.0/27.8/75.6, Bangkok=-26.9/28.6/76.8, Bangui=-26.4/26.0/73.0, Banjul=-23.2/26.0/78.0, Barcelona=-29.2/18.2/69.6, Bata=-25.2/25.1/72.9, Batumi=-37.3/14.0/62.4, Beijing=-36.3/12.9/65.7, Beirut=-30.7/20.9/70.2, Belgrade=-40.2/12.5/64.5, Belize City=-24.3/26.7/81.0, Benghazi=-29.4/19.9/67.7, Bergen=-42.3/7.7/56.5, Berlin=-36.2/10.3/61.3, Bilbao=-31.9/14.7/64.3, Birao=-20.8/26.5/78.3, Bishkek=-37.9/11.3/60.9, Bissau=-22.5/27.0/76.2, Blantyre=-29.5/22.2/72.0, Bloemfontein=-34.1/15.6/64.5, Boise=-35.6/11.4/59.3, Bordeaux=-37.1/14.2/63.7, Bosaso=-17.7/30.0/79.3, Boston=-36.6/10.9/59.2, Bouaké=-22.3/26.0/78.6, Bratislava=-44.3/10.5/63.8, Brazzaville=-26.2/25.0/72.5, Bridgetown=-23.7/27.0/73.0, Brisbane=-31.1/21.4/70.7, Brussels=-40.5/10.5/58.3, Bucharest=-43.6/10.8/62.2, Budapest=-40.1/11.3/60.9, Bujumbura=-25.3/23.8/75.7, Bulawayo=-30.3/18.9/65.6, Burnie=-38.3/13.1/63.1, Busan=-34.7/15.0/63.7, Cabo San Lucas=-25.4/23.9/73.6, Cairns=-26.6/25.0/75.8, Cairo=-27.9/21.4/73.2, Calgary=-49.6/4.4/58.3, Canberra=-33.8/13.1/62.7, Cape Town=-34.2/16.2/70.4, Changsha=-29.5/17.4/65.7, Charlotte=-31.9/16.1/67.2, Chiang Mai=-31.0/25.8/79.3, Chicago=-40.0/9.8/64.9, Chihuahua=-29.5/18.6/69.7, Chittagong=-23.8/25.9/78.3, Chișinău=-40.4/10.2/58.5, Chongqing=-31.9/18.6/67.1, Christchurch=-34.1/12.2/65.0, City of San Marino=-37.1/11.8/65.7, Colombo=-22.4/27.4/78.5, Columbus=-36.2/11.7/60.5, Conakry=-21.9/26.4/77.2, Copenhagen=-42.0/9.1/57.8, Cotonou=-24.6/27.2/75.1, Cracow=-39.2/9.3/57.5, Da Lat=-34.2/17.9/66.1, Da Nang=-22.1/25.8/74.4, Dakar=-24.9/24.0/75.7, Dallas=-30.4/19.0/68.3, Damascus=-34.7/17.0/67.2, Dampier=-23.9/26.4/74.0, Dar es Salaam=-27.8/25.8/77.3, Darwin=-22.5/27.6/75.6, Denpasar=-26.7/23.7/73.3, Denver=-38.2/10.4/58.4, Detroit=-39.9/10.0/58.6, Dhaka=-20.4/25.9/74.2, Dikson=-58.8/-11.1/37.3, Dili=-24.0/26.6/77.1, Djibouti=-21.3/29.9/78.7, Dodoma=-25.9/22.7/75.7, Dolisie=-26.2/24.0/73.8, Douala=-22.8/26.7/76.2, Dubai=-25.6/26.9/76.9, Dublin=-42.3/9.8/55.5, Dunedin=-37.7/11.1/58.6, Durban=-31.8/20.6/68.9, Dushanbe=-37.7/14.7/66.5, Edinburgh=-40.4/9.3/61.6, Edmonton=-47.3/4.2/55.7, El Paso=-29.2/18.1/74.0, Entebbe=-27.1/21.0/68.6, Erbil=-29.6/19.5/69.9, Erzurum=-42.7/5.1/56.7, Fairbanks=-50.5/-2.3/47.2, Fianarantsoa=-29.3/17.9/69.5, Flores,  Petén=-23.7/26.4/77.8, Frankfurt=-38.4/10.6/61.7, Fresno=-28.2/17.9/65.8, Fukuoka=-31.6/17.0/67.2, Gaborone=-27.5/21.0/70.2, Gabès=-29.4/19.5/68.1, Gagnoa=-23.9/26.0/72.8, Gangtok=-32.5/15.2/65.3, Garissa=-18.2/29.3/80.2, Garoua=-23.5/28.3/80.3, George Town=-19.0/27.9/75.7, Ghanzi=-29.0/21.4/74.6, Gjoa Haven=-65.1/-14.4/35.2, Guadalajara=-27.8/20.9/71.7, Guangzhou=-30.0/22.4/77.9, Guatemala City=-31.5/20.4/73.3, Halifax=-43.3/7.5/60.7, Hamburg=-39.5/9.7/58.9, Hamilton=-34.0/13.8/64.0, Hanga Roa=-29.2/20.5/72.3, Hanoi=-26.3/23.6/74.6, Harare=-36.2/18.4/68.0, Harbin=-46.4/5.0/51.8, Hargeisa=-25.6/21.7/72.3, Hat Yai=-21.8/27.0/79.6, Havana=-21.4/25.2/79.6, Helsinki=-42.8/5.9/56.9, Heraklion=-36.0/18.9/65.6, Hiroshima=-32.3/16.3/68.2, Ho Chi Minh City=-27.6/27.4/83.4, Hobart=-36.1/12.7/65.1, Hong Kong=-27.8/23.3/75.1, Honiara=-26.8/26.5/75.9, Honolulu=-25.2/25.4/75.3, Houston=-29.2/20.8/69.1, Ifrane=-38.4/11.4/63.3, Indianapolis=-40.2/11.8/64.9, Iqaluit=-58.1/-9.3/43.4, Irkutsk=-51.5/1.0/52.4, Istanbul=-38.1/13.9/65.2, Jacksonville=-33.8/20.3/72.3, Jakarta=-23.5/26.7/75.2, Jayapura=-20.5/27.0/76.8, Jerusalem=-34.3/18.3/65.1, Johannesburg=-32.8/15.5/64.9, Jos=-31.6/22.8/75.3, Juba=-25.0/27.8/79.2, Kabul=-37.1/12.1/64.9, Kampala=-28.7/20.0/69.5, Kandi=-25.0/27.7/79.9, Kankan=-26.6/26.5/76.6, Kano=-24.1/26.4/75.3, Kansas City=-34.8/12.5/66.1, Karachi=-27.5/26.0/78.4, Karonga=-30.0/24.4/76.3, Kathmandu=-40.2/18.3/69.6, Khartoum=-19.4/29.9/82.0, Kingston=-22.5/27.4/79.9, Kinshasa=-28.7/25.3/76.0, Kolkata=-21.0/26.7/77.7, Kuala Lumpur=-24.1/27.3/79.5, Kumasi=-26.7/26.0/77.4, Kunming=-36.5/15.7/64.5, Kuopio=-46.6/3.4/50.7, Kuwait City=-25.3/25.7/79.6, Kyiv=-48.6/8.4/59.3, Kyoto=-32.2/15.8/64.2, La Ceiba=-25.7/26.2/73.8, La Paz=-28.0/23.7/72.6, Lagos=-22.0/26.8/79.4, Lahore=-30.3/24.3/75.5, Lake Havasu City=-24.0/23.7/81.4, Lake Tekapo=-42.8/8.7/58.9, Las Palmas de Gran Canaria=-25.7/21.2/70.6, Las Vegas=-32.0/20.3/70.0, Launceston=-36.1/13.1/59.5, Lhasa=-39.4/7.6/55.4, Libreville=-22.7/25.9/77.4, Lisbon=-32.0/17.5/65.5, Livingstone=-32.3/21.8/71.5, Ljubljana=-36.5/10.9/64.2, Lodwar=-23.2/29.3/84.4, Lomé=-22.8/26.9/76.2, London=-36.8/11.3/61.6, Los Angeles=-30.7/18.6/69.2, Louisville=-36.1/13.9/65.8, Luanda=-29.3/25.8/77.7, Lubumbashi=-31.7/20.8/70.7, Lusaka=-29.2/19.9/69.1, Luxembourg City=-43.3/9.3/59.0, Lviv=-38.9/7.8/58.4, Lyon=-35.8/12.5/64.8, Madrid=-31.9/15.0/65.1, Mahajanga=-26.5/26.3/73.8, Makassar=-20.6/26.7/74.3, Makurdi=-24.6/26.0/78.4, Malabo=-21.3/26.3/73.2, Malé=-23.0/28.0/82.0, Managua=-26.8/27.3/77.3, Manama=-22.7/26.5/76.0, Mandalay=-19.4/28.0/81.3, Mango=-19.9/28.1/82.3, Manila=-22.6/28.4/78.0, Maputo=-24.5/22.8/70.2, Marrakesh=-27.8/19.6/67.5, Marseille=-36.7/15.8/66.4, Maun=-28.5/22.4/69.5, Medan=-24.8/26.5/75.5, Mek'ele=-33.6/22.7/71.2, Melbourne=-32.9/15.1/65.7, Memphis=-34.6/17.2/65.3, Mexicali=-25.8/23.1/70.3, Mexico City=-35.6/17.5/69.9, Miami=-24.1/24.9/77.0, Milan=-33.8/13.0/63.8, Milwaukee=-38.2/8.9/60.3, Minneapolis=-39.7/7.8/56.4, Minsk=-40.0/6.7/61.4, Mogadishu=-22.4/27.1/77.9, Mombasa=-24.3/26.3/74.5, Monaco=-33.2/16.4/65.3, Moncton=-46.4/6.1/55.9, Monterrey=-31.0/22.3/72.3, Montreal=-40.9/6.8/63.1, Moscow=-44.6/5.8/57.0, Mumbai=-26.0/27.1/74.4, Murmansk=-52.1/0.6/50.0, Muscat=-21.7/28.0/77.2, Mzuzu=-32.5/17.7/71.9, N'Djamena=-23.1/28.3/82.5, Naha=-25.5/23.1/76.9, Nairobi=-32.0/17.8/70.4, Nakhon Ratchasima=-23.3/27.3/75.8, Napier=-39.6/14.6/69.2, Napoli=-34.5/15.9/63.9, Nashville=-33.4/15.4/63.9, Nassau=-23.5/24.6/76.4, Ndola=-32.7/20.3/70.1, New Delhi=-25.5/25.0/75.1, New Orleans=-30.9/20.7/68.2, New York City=-39.6/12.9/60.7, Ngaoundéré=-30.6/22.0/72.7, Niamey=-23.6/29.3/76.4, Nicosia=-34.2/19.7/72.5, Niigata=-37.3/13.9/65.5, Nouadhibou=-32.7/21.3/68.1, Nouakchott=-24.7/25.7/77.2, Novosibirsk=-47.2/1.7/59.3, Nuuk=-53.1/-1.4/50.6, Odesa=-37.3/10.7/65.1, Odienné=-24.0/26.0/76.0, Oklahoma City=-32.2/15.9/65.4, Omaha=-38.4/10.6/64.4, Oranjestad=-20.0/28.1/77.0, Oslo=-44.9/5.7/53.1, Ottawa=-47.2/6.6/54.8, Ouagadougou=-27.6/28.3/77.9, Ouahigouya=-25.6/28.6/80.3, Ouarzazate=-30.8/18.9/69.9, Oulu=-46.1/2.7/50.1, Palembang=-28.0/27.3/75.8, Palermo=-35.7/18.5/71.6, Palm Springs=-23.3/24.5/74.6, Palmerston North=-34.5/13.2/73.4, Panama City=-22.9/28.0/80.3, Parakou=-22.8/26.8/78.3, Paris=-42.1/12.3/59.9, Perth=-31.0/18.7/74.4, Petropavlovsk-Kamchatsky=-49.3/1.9/51.5, Philadelphia=-39.2/13.2/66.0, Phnom Penh=-20.6/28.3/78.5, Phoenix=-28.7/23.9/77.3, Pittsburgh=-40.4/10.8/60.2, Podgorica=-34.1/15.3/64.6, Pointe-Noire=-21.9/26.1/74.2, Pontianak=-25.3/27.7/78.5, Port Moresby=-22.8/26.9/77.2, Port Sudan=-26.3/28.4/75.4, Port Vila=-25.0/24.3/72.8, Port-Gentil=-24.9/26.0/73.3, Portland (OR)=-38.3/12.4/59.8, Porto=-33.2/15.7/62.9, Prague=-40.0/8.4/65.0, Praia=-30.2/24.4/78.3, Pretoria=-32.6/18.2/67.8, Pyongyang=-40.5/10.8/62.9, Rabat=-32.5/17.2/68.4, Rangpur=-22.5/24.4/75.1, Reggane=-20.8/28.3/75.5, Reykjavík=-51.0/4.3/53.4, Riga=-39.5/6.2/54.1, Riyadh=-21.6/26.0/76.0, Rome=-39.7/15.2/65.6, Roseau=-20.9/26.2/72.8, Rostov-on-Don=-38.2/9.9/59.2, Sacramento=-29.4/16.3/65.5, Saint Petersburg=-41.4/5.8/57.4, Saint-Pierre=-43.3/5.7/54.4, Salt Lake City=-36.1/11.6/61.8, San Antonio=-28.1/20.8/67.2, San Diego=-32.7/17.8/70.8, San Francisco=-32.3/14.6/61.2, San Jose=-32.4/16.4/69.1, San José=-27.4/22.6/72.6, San Juan=-27.6/27.2/78.6, San Salvador=-30.0/23.1/71.8, Sana'a=-30.8/20.0/67.5, Santo Domingo=-25.3/25.9/75.1, Sapporo=-38.8/8.9/60.6, Sarajevo=-46.3/10.1/62.9, Saskatoon=-49.8/3.3/55.1, Seattle=-38.2/11.3/63.8, Seoul=-38.7/12.5/63.6, Seville=-28.3/19.2/68.1, Shanghai=-33.9/16.7/66.6, Singapore=-21.9/27.0/79.5, Skopje=-39.8/12.4/64.3, Sochi=-34.8/14.2/63.9, Sofia=-41.3/10.6/59.8, Sokoto=-18.9/28.0/73.9, Split=-35.6/16.1/68.6, St. John's=-45.7/5.0/57.2, St. Louis=-36.4/13.9/61.5, Stockholm=-41.8/6.6/53.8, Surabaya=-23.0/27.1/78.1, Suva=-22.8/25.6/73.2, Suwałki=-45.0/7.2/58.0, Sydney=-34.1/17.7/66.4, Ségou=-21.6/28.0/77.1, Tabora=-26.7/23.0/73.1, Tabriz=-35.1/12.6/62.9, Taipei=-26.6/23.0/72.7, Tallinn=-41.5/6.4/53.6, Tamale=-22.7/27.9/79.5, Tamanrasset=-26.3/21.7/75.4, Tampa=-26.3/22.9/70.9, Tashkent=-33.2/14.8/66.3, Tauranga=-38.3/14.8/68.1, Tbilisi=-36.9/12.9/62.3, Tegucigalpa=-28.4/21.7/71.2, Tehran=-40.1/17.0/64.1, Tel Aviv=-35.5/20.0/73.5, Thessaloniki=-31.4/16.0/63.5, Thiès=-23.8/24.0/75.6, Tijuana=-30.1/17.8/68.5, Timbuktu=-25.1/28.0/77.5, Tirana=-39.3/15.2/68.7, Toamasina=-28.2/23.4/74.2, Tokyo=-34.6/15.4/68.7, Toliara=-25.2/24.1/75.7, Toluca=-37.8/12.4/59.5, Toronto=-42.2/9.4/59.8, Tripoli=-30.0/20.0/74.1, Tromsø=-47.7/2.9/58.8, Tucson=-27.8/20.9/74.4, Tunis=-34.9/18.4/67.7, Ulaanbaatar=-47.4/-0.4/47.7, Upington=-27.0/20.4/72.5, Vaduz=-39.8/10.1/60.6, Valencia=-32.0/18.3/68.9, Valletta=-31.5/18.8/69.0, Vancouver=-39.6/10.4/63.2, Veracruz=-28.6/25.4/76.3, Vienna=-41.3/10.4/61.9, Vientiane=-27.9/25.9/77.9, Villahermosa=-23.3/27.1/79.2, Vilnius=-42.3/6.0/58.4, Virginia Beach=-31.6/15.8/64.4, Vladivostok=-47.0/4.9/55.0, Warsaw=-45.2/8.5/67.0, Washington, D.C.=-35.9/14.6/67.4, Wau=-20.2/27.8/76.7, Wellington=-38.2/12.9/60.1, Whitehorse=-49.1/-0.1/48.1, Wichita=-41.7/13.9/63.8, Willemstad=-24.9/28.0/78.3, Winnipeg=-45.1/3.0/54.8, Wrocław=-41.8/9.6/64.1, Xi'an=-35.5/14.1/66.0, Yakutsk=-59.3/-8.8/39.1, Yangon=-23.7/27.5/78.2, Yaoundé=-23.2/23.8/72.6, Yellowknife=-55.3/-4.3/44.3, Yerevan=-37.4/12.4/61.2, Yinchuan=-41.5/9.0/60.4, Zagreb=-41.4/10.7/62.2, Zanzibar City=-23.8/26.0/75.4, Zürich=-48.1/9.3/58.0, Ürümqi=-46.5/7.4/58.7, İzmir=-34.1/17.9/68.3}"));

        }
    }

    private static Measurement aggregateMeasurements(Measurement src, Measurement target) {
        target.min = Math.min(src.min, target.min);
        target.max = Math.max(src.max, target.max);
        target.sum = src.sum + target.sum;
        target.count = src.count + target.count;
        return target;
    }

    private static Map<String, Measurement> getMeasurements(BufferSupplier getBuffer) {
        final Map<String, Measurement> map = new HashMap<>(10_000);
        final ByteBuffer byteBuffer = getBuffer.get();
        int start = byteBuffer.position();
        final int end = byteBuffer.limit();
        final byte[] bytes = new byte[69];
        for (int i = start; i < end; ++i) {
            int nameLen = 0;
            String name;
            byte b;
            while ((b = byteBuffer.get(i++)) != ';') {
                bytes[nameLen++] = b;
            }
            name = new String(bytes, 0, nameLen, StandardCharsets.UTF_8);
            int valueLen = 0;
            while (((b = byteBuffer.get(i++)) != '\r')) {
                bytes[valueLen++] = b;
            }
            takeMeasurement(bytes, valueLen, map, name);
        }
        return map;
    }

    private static void takeMeasurement(byte[] bytes, int valueLen, Map<String, Measurement> map, String name) {
        final double temperature = Double.parseDouble(new String(bytes, 0, valueLen, StandardCharsets.UTF_8));
        Measurement measurement = map.get(name);
        if (measurement != null) {
            measurement.min = Math.min(measurement.min, temperature);
            measurement.max = Math.max(measurement.max, temperature);
            measurement.sum += temperature;
            measurement.count++;
        } else {
            measurement = new Measurement();
            map.put(name, measurement);
            measurement.min = temperature;
            measurement.max = temperature;
            measurement.sum = temperature;
            measurement.count = 1;
        }
    }

    private static Stream<BufferSupplier> getParallelBufferStream(RandomAccessFile raf, FileChannel fc) throws IOException {
        final int availableProcessors = Runtime.getRuntime().availableProcessors();
        return StreamSupport.stream(
            StreamSupport.stream(
                    Spliterators.spliterator(
                        new BufferSupplierIterator(raf, fc, availableProcessors), availableProcessors,
                        Spliterator.IMMUTABLE | Spliterator.SIZED | Spliterator.SUBSIZED)
                    , false)
                .spliterator(), true);
    }

}

interface BufferSupplier extends Supplier<ByteBuffer> {
}

class BufferSupplierIterator implements Iterator<BufferSupplier> {
    private long start;
    private final RandomAccessFile raf;
    private final FileChannel fc;
    private final long fileLength;
    private final long chunkSize;

    public BufferSupplierIterator(RandomAccessFile raf, FileChannel fc, int numberOfParts) throws IOException {
        this.raf = raf;
        this.fc = fc;
        this.fileLength = fc.size();
        this.chunkSize = fileLength / numberOfParts;
    }

    @Override
    public boolean hasNext() {
        return start < fileLength;
    }

    @Override
    public BufferSupplier next() {
        try {
            if (hasNext()) {
                final long end = getEnd();
                long s = start;
                this.start = end;
                return getBufferSupplier(s, end);
            } else {
                throw new NoSuchElementException();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private long getEnd() throws IOException {
        long end = Math.min(start + chunkSize, fileLength);
        while (end < fileLength) {
            raf.seek(end++);
            if (raf.read() == '\n') break;
        }
        return end;
    }

    private BufferSupplier getBufferSupplier(long position, long end) {
        final long size = end - position;
        return new BufferSupplier() {

            private ByteBuffer bb;

            @Override
            public ByteBuffer get() {
                try {
                    if (bb == null) {
                        return (bb = fc.map(MapMode.READ_ONLY, position, size));
                    } else {
                        return bb;
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }
}