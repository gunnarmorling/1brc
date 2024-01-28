package dev.morling.onebrc;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;

public class HashTest {
    final static List<CreateMeasurements2.WeatherStation> stations = Arrays.asList(
            new CreateMeasurements2.WeatherStation("Abha", 18.0),
            new CreateMeasurements2.WeatherStation("Abidjan", 26.0),
            new CreateMeasurements2.WeatherStation("Abéché", 29.4),
            new CreateMeasurements2.WeatherStation("Accra", 26.4),
            new CreateMeasurements2.WeatherStation("Addis Ababa", 16.0),
            new CreateMeasurements2.WeatherStation("Adelaide", 17.3),
            new CreateMeasurements2.WeatherStation("Aden", 29.1),
            new CreateMeasurements2.WeatherStation("Ahvaz", 25.4),
            new CreateMeasurements2.WeatherStation("Albuquerque", 14.0),
            new CreateMeasurements2.WeatherStation("Alexandra", 11.0),
            new CreateMeasurements2.WeatherStation("Alexandria", 20.0),
            new CreateMeasurements2.WeatherStation("Algiers", 18.2),
            new CreateMeasurements2.WeatherStation("Alice Springs", 21.0),
            new CreateMeasurements2.WeatherStation("Almaty", 10.0),
            new CreateMeasurements2.WeatherStation("Amsterdam", 10.2),
            new CreateMeasurements2.WeatherStation("Anadyr", -6.9),
            new CreateMeasurements2.WeatherStation("Anchorage", 2.8),
            new CreateMeasurements2.WeatherStation("Andorra la Vella", 9.8),
            new CreateMeasurements2.WeatherStation("Ankara", 12.0),
            new CreateMeasurements2.WeatherStation("Antananarivo", 17.9),
            new CreateMeasurements2.WeatherStation("Antsiranana", 25.2),
            new CreateMeasurements2.WeatherStation("Arkhangelsk", 1.3),
            new CreateMeasurements2.WeatherStation("Ashgabat", 17.1),
            new CreateMeasurements2.WeatherStation("Asmara", 15.6),
            new CreateMeasurements2.WeatherStation("Assab", 30.5),
            new CreateMeasurements2.WeatherStation("Astana", 3.5),
            new CreateMeasurements2.WeatherStation("Athens", 19.2),
            new CreateMeasurements2.WeatherStation("Atlanta", 17.0),
            new CreateMeasurements2.WeatherStation("Auckland", 15.2),
            new CreateMeasurements2.WeatherStation("Austin", 20.7),
            new CreateMeasurements2.WeatherStation("Baghdad", 22.77),
            new CreateMeasurements2.WeatherStation("Baguio", 19.5),
            new CreateMeasurements2.WeatherStation("Baku", 15.1),
            new CreateMeasurements2.WeatherStation("Baltimore", 13.1),
            new CreateMeasurements2.WeatherStation("Bamako", 27.8),
            new CreateMeasurements2.WeatherStation("Bangkok", 28.6),
            new CreateMeasurements2.WeatherStation("Bangui", 26.0),
            new CreateMeasurements2.WeatherStation("Banjul", 26.0),
            new CreateMeasurements2.WeatherStation("Barcelona", 18.2),
            new CreateMeasurements2.WeatherStation("Bata", 25.1),
            new CreateMeasurements2.WeatherStation("Batumi", 14.0),
            new CreateMeasurements2.WeatherStation("Beijing", 12.9),
            new CreateMeasurements2.WeatherStation("Beirut", 20.9),
            new CreateMeasurements2.WeatherStation("Belgrade", 12.5),
            new CreateMeasurements2.WeatherStation("Belize City", 26.7),
            new CreateMeasurements2.WeatherStation("Benghazi", 19.9),
            new CreateMeasurements2.WeatherStation("Bergen", 7.7),
            new CreateMeasurements2.WeatherStation("Berlin", 10.3),
            new CreateMeasurements2.WeatherStation("Bilbao", 14.7),
            new CreateMeasurements2.WeatherStation("Birao", 26.5),
            new CreateMeasurements2.WeatherStation("Bishkek", 11.3),
            new CreateMeasurements2.WeatherStation("Bissau", 27.0),
            new CreateMeasurements2.WeatherStation("Blantyre", 22.2),
            new CreateMeasurements2.WeatherStation("Bloemfontein", 15.6),
            new CreateMeasurements2.WeatherStation("Boise", 11.4),
            new CreateMeasurements2.WeatherStation("Bordeaux", 14.2),
            new CreateMeasurements2.WeatherStation("Bosaso", 30.0),
            new CreateMeasurements2.WeatherStation("Boston", 10.9),
            new CreateMeasurements2.WeatherStation("Bouaké", 26.0),
            new CreateMeasurements2.WeatherStation("Bratislava", 10.5),
            new CreateMeasurements2.WeatherStation("Brazzaville", 25.0),
            new CreateMeasurements2.WeatherStation("Bridgetown", 27.0),
            new CreateMeasurements2.WeatherStation("Brisbane", 21.4),
            new CreateMeasurements2.WeatherStation("Brussels", 10.5),
            new CreateMeasurements2.WeatherStation("Bucharest", 10.8),
            new CreateMeasurements2.WeatherStation("Budapest", 11.3),
            new CreateMeasurements2.WeatherStation("Bujumbura", 23.8),
            new CreateMeasurements2.WeatherStation("Bulawayo", 18.9),
            new CreateMeasurements2.WeatherStation("Burnie", 13.1),
            new CreateMeasurements2.WeatherStation("Busan", 15.0),
            new CreateMeasurements2.WeatherStation("Cabo San Lucas", 23.9),
            new CreateMeasurements2.WeatherStation("Cairns", 25.0),
            new CreateMeasurements2.WeatherStation("Cairo", 21.4),
            new CreateMeasurements2.WeatherStation("Calgary", 4.4),
            new CreateMeasurements2.WeatherStation("Canberra", 13.1),
            new CreateMeasurements2.WeatherStation("Cape Town", 16.2),
            new CreateMeasurements2.WeatherStation("Changsha", 17.4),
            new CreateMeasurements2.WeatherStation("Charlotte", 16.1),
            new CreateMeasurements2.WeatherStation("Chiang Mai", 25.8),
            new CreateMeasurements2.WeatherStation("Chicago", 9.8),
            new CreateMeasurements2.WeatherStation("Chihuahua", 18.6),
            new CreateMeasurements2.WeatherStation("Chișinău", 10.2),
            new CreateMeasurements2.WeatherStation("Chittagong", 25.9),
            new CreateMeasurements2.WeatherStation("Chongqing", 18.6),
            new CreateMeasurements2.WeatherStation("Christchurch", 12.2),
            new CreateMeasurements2.WeatherStation("City of San Marino", 11.8),
            new CreateMeasurements2.WeatherStation("Colombo", 27.4),
            new CreateMeasurements2.WeatherStation("Columbus", 11.7),
            new CreateMeasurements2.WeatherStation("Conakry", 26.4),
            new CreateMeasurements2.WeatherStation("Copenhagen", 9.1),
            new CreateMeasurements2.WeatherStation("Cotonou", 27.2),
            new CreateMeasurements2.WeatherStation("Cracow", 9.3),
            new CreateMeasurements2.WeatherStation("Da Lat", 17.9),
            new CreateMeasurements2.WeatherStation("Da Nang", 25.8),
            new CreateMeasurements2.WeatherStation("Dakar", 24.0),
            new CreateMeasurements2.WeatherStation("Dallas", 19.0),
            new CreateMeasurements2.WeatherStation("Damascus", 17.0),
            new CreateMeasurements2.WeatherStation("Dampier", 26.4),
            new CreateMeasurements2.WeatherStation("Dar es Salaam", 25.8),
            new CreateMeasurements2.WeatherStation("Darwin", 27.6),
            new CreateMeasurements2.WeatherStation("Denpasar", 23.7),
            new CreateMeasurements2.WeatherStation("Denver", 10.4),
            new CreateMeasurements2.WeatherStation("Detroit", 10.0),
            new CreateMeasurements2.WeatherStation("Dhaka", 25.9),
            new CreateMeasurements2.WeatherStation("Dikson", -11.1),
            new CreateMeasurements2.WeatherStation("Dili", 26.6),
            new CreateMeasurements2.WeatherStation("Djibouti", 29.9),
            new CreateMeasurements2.WeatherStation("Dodoma", 22.7),
            new CreateMeasurements2.WeatherStation("Dolisie", 24.0),
            new CreateMeasurements2.WeatherStation("Douala", 26.7),
            new CreateMeasurements2.WeatherStation("Dubai", 26.9),
            new CreateMeasurements2.WeatherStation("Dublin", 9.8),
            new CreateMeasurements2.WeatherStation("Dunedin", 11.1),
            new CreateMeasurements2.WeatherStation("Durban", 20.6),
            new CreateMeasurements2.WeatherStation("Dushanbe", 14.7),
            new CreateMeasurements2.WeatherStation("Edinburgh", 9.3),
            new CreateMeasurements2.WeatherStation("Edmonton", 4.2),
            new CreateMeasurements2.WeatherStation("El Paso", 18.1),
            new CreateMeasurements2.WeatherStation("Entebbe", 21.0),
            new CreateMeasurements2.WeatherStation("Erbil", 19.5),
            new CreateMeasurements2.WeatherStation("Erzurum", 5.1),
            new CreateMeasurements2.WeatherStation("Fairbanks", -2.3),
            new CreateMeasurements2.WeatherStation("Fianarantsoa", 17.9),
            new CreateMeasurements2.WeatherStation("Flores,  Petén", 26.4),
            new CreateMeasurements2.WeatherStation("Frankfurt", 10.6),
            new CreateMeasurements2.WeatherStation("Fresno", 17.9),
            new CreateMeasurements2.WeatherStation("Fukuoka", 17.0),
            new CreateMeasurements2.WeatherStation("Gabès", 19.5),
            new CreateMeasurements2.WeatherStation("Gaborone", 21.0),
            new CreateMeasurements2.WeatherStation("Gagnoa", 26.0),
            new CreateMeasurements2.WeatherStation("Gangtok", 15.2),
            new CreateMeasurements2.WeatherStation("Garissa", 29.3),
            new CreateMeasurements2.WeatherStation("Garoua", 28.3),
            new CreateMeasurements2.WeatherStation("George Town", 27.9),
            new CreateMeasurements2.WeatherStation("Ghanzi", 21.4),
            new CreateMeasurements2.WeatherStation("Gjoa Haven", -14.4),
            new CreateMeasurements2.WeatherStation("Guadalajara", 20.9),
            new CreateMeasurements2.WeatherStation("Guangzhou", 22.4),
            new CreateMeasurements2.WeatherStation("Guatemala City", 20.4),
            new CreateMeasurements2.WeatherStation("Halifax", 7.5),
            new CreateMeasurements2.WeatherStation("Hamburg", 9.7),
            new CreateMeasurements2.WeatherStation("Hamilton", 13.8),
            new CreateMeasurements2.WeatherStation("Hanga Roa", 20.5),
            new CreateMeasurements2.WeatherStation("Hanoi", 23.6),
            new CreateMeasurements2.WeatherStation("Harare", 18.4),
            new CreateMeasurements2.WeatherStation("Harbin", 5.0),
            new CreateMeasurements2.WeatherStation("Hargeisa", 21.7),
            new CreateMeasurements2.WeatherStation("Hat Yai", 27.0),
            new CreateMeasurements2.WeatherStation("Havana", 25.2),
            new CreateMeasurements2.WeatherStation("Helsinki", 5.9),
            new CreateMeasurements2.WeatherStation("Heraklion", 18.9),
            new CreateMeasurements2.WeatherStation("Hiroshima", 16.3),
            new CreateMeasurements2.WeatherStation("Ho Chi Minh City", 27.4),
            new CreateMeasurements2.WeatherStation("Hobart", 12.7),
            new CreateMeasurements2.WeatherStation("Hong Kong", 23.3),
            new CreateMeasurements2.WeatherStation("Honiara", 26.5),
            new CreateMeasurements2.WeatherStation("Honolulu", 25.4),
            new CreateMeasurements2.WeatherStation("Houston", 20.8),
            new CreateMeasurements2.WeatherStation("Ifrane", 11.4),
            new CreateMeasurements2.WeatherStation("Indianapolis", 11.8),
            new CreateMeasurements2.WeatherStation("Iqaluit", -9.3),
            new CreateMeasurements2.WeatherStation("Irkutsk", 1.0),
            new CreateMeasurements2.WeatherStation("Istanbul", 13.9),
            new CreateMeasurements2.WeatherStation("İzmir", 17.9),
            new CreateMeasurements2.WeatherStation("Jacksonville", 20.3),
            new CreateMeasurements2.WeatherStation("Jakarta", 26.7),
            new CreateMeasurements2.WeatherStation("Jayapura", 27.0),
            new CreateMeasurements2.WeatherStation("Jerusalem", 18.3),
            new CreateMeasurements2.WeatherStation("Johannesburg", 15.5),
            new CreateMeasurements2.WeatherStation("Jos", 22.8),
            new CreateMeasurements2.WeatherStation("Juba", 27.8),
            new CreateMeasurements2.WeatherStation("Kabul", 12.1),
            new CreateMeasurements2.WeatherStation("Kampala", 20.0),
            new CreateMeasurements2.WeatherStation("Kandi", 27.7),
            new CreateMeasurements2.WeatherStation("Kankan", 26.5),
            new CreateMeasurements2.WeatherStation("Kano", 26.4),
            new CreateMeasurements2.WeatherStation("Kansas City", 12.5),
            new CreateMeasurements2.WeatherStation("Karachi", 26.0),
            new CreateMeasurements2.WeatherStation("Karonga", 24.4),
            new CreateMeasurements2.WeatherStation("Kathmandu", 18.3),
            new CreateMeasurements2.WeatherStation("Khartoum", 29.9),
            new CreateMeasurements2.WeatherStation("Kingston", 27.4),
            new CreateMeasurements2.WeatherStation("Kinshasa", 25.3),
            new CreateMeasurements2.WeatherStation("Kolkata", 26.7),
            new CreateMeasurements2.WeatherStation("Kuala Lumpur", 27.3),
            new CreateMeasurements2.WeatherStation("Kumasi", 26.0),
            new CreateMeasurements2.WeatherStation("Kunming", 15.7),
            new CreateMeasurements2.WeatherStation("Kuopio", 3.4),
            new CreateMeasurements2.WeatherStation("Kuwait City", 25.7),
            new CreateMeasurements2.WeatherStation("Kyiv", 8.4),
            new CreateMeasurements2.WeatherStation("Kyoto", 15.8),
            new CreateMeasurements2.WeatherStation("La Ceiba", 26.2),
            new CreateMeasurements2.WeatherStation("La Paz", 23.7),
            new CreateMeasurements2.WeatherStation("Lagos", 26.8),
            new CreateMeasurements2.WeatherStation("Lahore", 24.3),
            new CreateMeasurements2.WeatherStation("Lake Havasu City", 23.7),
            new CreateMeasurements2.WeatherStation("Lake Tekapo", 8.7),
            new CreateMeasurements2.WeatherStation("Las Palmas de Gran Canaria", 21.2),
            new CreateMeasurements2.WeatherStation("Las Vegas", 20.3),
            new CreateMeasurements2.WeatherStation("Launceston", 13.1),
            new CreateMeasurements2.WeatherStation("Lhasa", 7.6),
            new CreateMeasurements2.WeatherStation("Libreville", 25.9),
            new CreateMeasurements2.WeatherStation("Lisbon", 17.5),
            new CreateMeasurements2.WeatherStation("Livingstone", 21.8),
            new CreateMeasurements2.WeatherStation("Ljubljana", 10.9),
            new CreateMeasurements2.WeatherStation("Lodwar", 29.3),
            new CreateMeasurements2.WeatherStation("Lomé", 26.9),
            new CreateMeasurements2.WeatherStation("London", 11.3),
            new CreateMeasurements2.WeatherStation("Los Angeles", 18.6),
            new CreateMeasurements2.WeatherStation("Louisville", 13.9),
            new CreateMeasurements2.WeatherStation("Luanda", 25.8),
            new CreateMeasurements2.WeatherStation("Lubumbashi", 20.8),
            new CreateMeasurements2.WeatherStation("Lusaka", 19.9),
            new CreateMeasurements2.WeatherStation("Luxembourg City", 9.3),
            new CreateMeasurements2.WeatherStation("Lviv", 7.8),
            new CreateMeasurements2.WeatherStation("Lyon", 12.5),
            new CreateMeasurements2.WeatherStation("Madrid", 15.0),
            new CreateMeasurements2.WeatherStation("Mahajanga", 26.3),
            new CreateMeasurements2.WeatherStation("Makassar", 26.7),
            new CreateMeasurements2.WeatherStation("Makurdi", 26.0),
            new CreateMeasurements2.WeatherStation("Malabo", 26.3),
            new CreateMeasurements2.WeatherStation("Malé", 28.0),
            new CreateMeasurements2.WeatherStation("Managua", 27.3),
            new CreateMeasurements2.WeatherStation("Manama", 26.5),
            new CreateMeasurements2.WeatherStation("Mandalay", 28.0),
            new CreateMeasurements2.WeatherStation("Mango", 28.1),
            new CreateMeasurements2.WeatherStation("Manila", 28.4),
            new CreateMeasurements2.WeatherStation("Maputo", 22.8),
            new CreateMeasurements2.WeatherStation("Marrakesh", 19.6),
            new CreateMeasurements2.WeatherStation("Marseille", 15.8),
            new CreateMeasurements2.WeatherStation("Maun", 22.4),
            new CreateMeasurements2.WeatherStation("Medan", 26.5),
            new CreateMeasurements2.WeatherStation("Mek'ele", 22.7),
            new CreateMeasurements2.WeatherStation("Melbourne", 15.1),
            new CreateMeasurements2.WeatherStation("Memphis", 17.2),
            new CreateMeasurements2.WeatherStation("Mexicali", 23.1),
            new CreateMeasurements2.WeatherStation("Mexico City", 17.5),
            new CreateMeasurements2.WeatherStation("Miami", 24.9),
            new CreateMeasurements2.WeatherStation("Milan", 13.0),
            new CreateMeasurements2.WeatherStation("Milwaukee", 8.9),
            new CreateMeasurements2.WeatherStation("Minneapolis", 7.8),
            new CreateMeasurements2.WeatherStation("Minsk", 6.7),
            new CreateMeasurements2.WeatherStation("Mogadishu", 27.1),
            new CreateMeasurements2.WeatherStation("Mombasa", 26.3),
            new CreateMeasurements2.WeatherStation("Monaco", 16.4),
            new CreateMeasurements2.WeatherStation("Moncton", 6.1),
            new CreateMeasurements2.WeatherStation("Monterrey", 22.3),
            new CreateMeasurements2.WeatherStation("Montreal", 6.8),
            new CreateMeasurements2.WeatherStation("Moscow", 5.8),
            new CreateMeasurements2.WeatherStation("Mumbai", 27.1),
            new CreateMeasurements2.WeatherStation("Murmansk", 0.6),
            new CreateMeasurements2.WeatherStation("Muscat", 28.0),
            new CreateMeasurements2.WeatherStation("Mzuzu", 17.7),
            new CreateMeasurements2.WeatherStation("N'Djamena", 28.3),
            new CreateMeasurements2.WeatherStation("Naha", 23.1),
            new CreateMeasurements2.WeatherStation("Nairobi", 17.8),
            new CreateMeasurements2.WeatherStation("Nakhon Ratchasima", 27.3),
            new CreateMeasurements2.WeatherStation("Napier", 14.6),
            new CreateMeasurements2.WeatherStation("Napoli", 15.9),
            new CreateMeasurements2.WeatherStation("Nashville", 15.4),
            new CreateMeasurements2.WeatherStation("Nassau", 24.6),
            new CreateMeasurements2.WeatherStation("Ndola", 20.3),
            new CreateMeasurements2.WeatherStation("New Delhi", 25.0),
            new CreateMeasurements2.WeatherStation("New Orleans", 20.7),
            new CreateMeasurements2.WeatherStation("New York City", 12.9),
            new CreateMeasurements2.WeatherStation("Ngaoundéré", 22.0),
            new CreateMeasurements2.WeatherStation("Niamey", 29.3),
            new CreateMeasurements2.WeatherStation("Nicosia", 19.7),
            new CreateMeasurements2.WeatherStation("Niigata", 13.9),
            new CreateMeasurements2.WeatherStation("Nouadhibou", 21.3),
            new CreateMeasurements2.WeatherStation("Nouakchott", 25.7),
            new CreateMeasurements2.WeatherStation("Novosibirsk", 1.7),
            new CreateMeasurements2.WeatherStation("Nuuk", -1.4),
            new CreateMeasurements2.WeatherStation("Odesa", 10.7),
            new CreateMeasurements2.WeatherStation("Odienné", 26.0),
            new CreateMeasurements2.WeatherStation("Oklahoma City", 15.9),
            new CreateMeasurements2.WeatherStation("Omaha", 10.6),
            new CreateMeasurements2.WeatherStation("Oranjestad", 28.1),
            new CreateMeasurements2.WeatherStation("Oslo", 5.7),
            new CreateMeasurements2.WeatherStation("Ottawa", 6.6),
            new CreateMeasurements2.WeatherStation("Ouagadougou", 28.3),
            new CreateMeasurements2.WeatherStation("Ouahigouya", 28.6),
            new CreateMeasurements2.WeatherStation("Ouarzazate", 18.9),
            new CreateMeasurements2.WeatherStation("Oulu", 2.7),
            new CreateMeasurements2.WeatherStation("Palembang", 27.3),
            new CreateMeasurements2.WeatherStation("Palermo", 18.5),
            new CreateMeasurements2.WeatherStation("Palm Springs", 24.5),
            new CreateMeasurements2.WeatherStation("Palmerston North", 13.2),
            new CreateMeasurements2.WeatherStation("Panama City", 28.0),
            new CreateMeasurements2.WeatherStation("Parakou", 26.8),
            new CreateMeasurements2.WeatherStation("Paris", 12.3),
            new CreateMeasurements2.WeatherStation("Perth", 18.7),
            new CreateMeasurements2.WeatherStation("Petropavlovsk-Kamchatsky", 1.9),
            new CreateMeasurements2.WeatherStation("Philadelphia", 13.2),
            new CreateMeasurements2.WeatherStation("Phnom Penh", 28.3),
            new CreateMeasurements2.WeatherStation("Phoenix", 23.9),
            new CreateMeasurements2.WeatherStation("Pittsburgh", 10.8),
            new CreateMeasurements2.WeatherStation("Podgorica", 15.3),
            new CreateMeasurements2.WeatherStation("Pointe-Noire", 26.1),
            new CreateMeasurements2.WeatherStation("Pontianak", 27.7),
            new CreateMeasurements2.WeatherStation("Port Moresby", 26.9),
            new CreateMeasurements2.WeatherStation("Port Sudan", 28.4),
            new CreateMeasurements2.WeatherStation("Port Vila", 24.3),
            new CreateMeasurements2.WeatherStation("Port-Gentil", 26.0),
            new CreateMeasurements2.WeatherStation("Portland (OR)", 12.4),
            new CreateMeasurements2.WeatherStation("Porto", 15.7),
            new CreateMeasurements2.WeatherStation("Prague", 8.4),
            new CreateMeasurements2.WeatherStation("Praia", 24.4),
            new CreateMeasurements2.WeatherStation("Pretoria", 18.2),
            new CreateMeasurements2.WeatherStation("Pyongyang", 10.8),
            new CreateMeasurements2.WeatherStation("Rabat", 17.2),
            new CreateMeasurements2.WeatherStation("Rangpur", 24.4),
            new CreateMeasurements2.WeatherStation("Reggane", 28.3),
            new CreateMeasurements2.WeatherStation("Reykjavík", 4.3),
            new CreateMeasurements2.WeatherStation("Riga", 6.2),
            new CreateMeasurements2.WeatherStation("Riyadh", 26.0),
            new CreateMeasurements2.WeatherStation("Rome", 15.2),
            new CreateMeasurements2.WeatherStation("Roseau", 26.2),
            new CreateMeasurements2.WeatherStation("Rostov-on-Don", 9.9),
            new CreateMeasurements2.WeatherStation("Sacramento", 16.3),
            new CreateMeasurements2.WeatherStation("Saint Petersburg", 5.8),
            new CreateMeasurements2.WeatherStation("Saint-Pierre", 5.7),
            new CreateMeasurements2.WeatherStation("Salt Lake City", 11.6),
            new CreateMeasurements2.WeatherStation("San Antonio", 20.8),
            new CreateMeasurements2.WeatherStation("San Diego", 17.8),
            new CreateMeasurements2.WeatherStation("San Francisco", 14.6),
            new CreateMeasurements2.WeatherStation("San Jose", 16.4),
            new CreateMeasurements2.WeatherStation("San José", 22.6),
            new CreateMeasurements2.WeatherStation("San Juan", 27.2),
            new CreateMeasurements2.WeatherStation("San Salvador", 23.1),
            new CreateMeasurements2.WeatherStation("Sana'a", 20.0),
            new CreateMeasurements2.WeatherStation("Santo Domingo", 25.9),
            new CreateMeasurements2.WeatherStation("Sapporo", 8.9),
            new CreateMeasurements2.WeatherStation("Sarajevo", 10.1),
            new CreateMeasurements2.WeatherStation("Saskatoon", 3.3),
            new CreateMeasurements2.WeatherStation("Seattle", 11.3),
            new CreateMeasurements2.WeatherStation("Ségou", 28.0),
            new CreateMeasurements2.WeatherStation("Seoul", 12.5),
            new CreateMeasurements2.WeatherStation("Seville", 19.2),
            new CreateMeasurements2.WeatherStation("Shanghai", 16.7),
            new CreateMeasurements2.WeatherStation("Singapore", 27.0),
            new CreateMeasurements2.WeatherStation("Skopje", 12.4),
            new CreateMeasurements2.WeatherStation("Sochi", 14.2),
            new CreateMeasurements2.WeatherStation("Sofia", 10.6),
            new CreateMeasurements2.WeatherStation("Sokoto", 28.0),
            new CreateMeasurements2.WeatherStation("Split", 16.1),
            new CreateMeasurements2.WeatherStation("St. John's", 5.0),
            new CreateMeasurements2.WeatherStation("St. Louis", 13.9),
            new CreateMeasurements2.WeatherStation("Stockholm", 6.6),
            new CreateMeasurements2.WeatherStation("Surabaya", 27.1),
            new CreateMeasurements2.WeatherStation("Suva", 25.6),
            new CreateMeasurements2.WeatherStation("Suwałki", 7.2),
            new CreateMeasurements2.WeatherStation("Sydney", 17.7),
            new CreateMeasurements2.WeatherStation("Tabora", 23.0),
            new CreateMeasurements2.WeatherStation("Tabriz", 12.6),
            new CreateMeasurements2.WeatherStation("Taipei", 23.0),
            new CreateMeasurements2.WeatherStation("Tallinn", 6.4),
            new CreateMeasurements2.WeatherStation("Tamale", 27.9),
            new CreateMeasurements2.WeatherStation("Tamanrasset", 21.7),
            new CreateMeasurements2.WeatherStation("Tampa", 22.9),
            new CreateMeasurements2.WeatherStation("Tashkent", 14.8),
            new CreateMeasurements2.WeatherStation("Tauranga", 14.8),
            new CreateMeasurements2.WeatherStation("Tbilisi", 12.9),
            new CreateMeasurements2.WeatherStation("Tegucigalpa", 21.7),
            new CreateMeasurements2.WeatherStation("Tehran", 17.0),
            new CreateMeasurements2.WeatherStation("Tel Aviv", 20.0),
            new CreateMeasurements2.WeatherStation("Thessaloniki", 16.0),
            new CreateMeasurements2.WeatherStation("Thiès", 24.0),
            new CreateMeasurements2.WeatherStation("Tijuana", 17.8),
            new CreateMeasurements2.WeatherStation("Timbuktu", 28.0),
            new CreateMeasurements2.WeatherStation("Tirana", 15.2),
            new CreateMeasurements2.WeatherStation("Toamasina", 23.4),
            new CreateMeasurements2.WeatherStation("Tokyo", 15.4),
            new CreateMeasurements2.WeatherStation("Toliara", 24.1),
            new CreateMeasurements2.WeatherStation("Toluca", 12.4),
            new CreateMeasurements2.WeatherStation("Toronto", 9.4),
            new CreateMeasurements2.WeatherStation("Tripoli", 20.0),
            new CreateMeasurements2.WeatherStation("Tromsø", 2.9),
            new CreateMeasurements2.WeatherStation("Tucson", 20.9),
            new CreateMeasurements2.WeatherStation("Tunis", 18.4),
            new CreateMeasurements2.WeatherStation("Ulaanbaatar", -0.4),
            new CreateMeasurements2.WeatherStation("Upington", 20.4),
            new CreateMeasurements2.WeatherStation("Ürümqi", 7.4),
            new CreateMeasurements2.WeatherStation("Vaduz", 10.1),
            new CreateMeasurements2.WeatherStation("Valencia", 18.3),
            new CreateMeasurements2.WeatherStation("Valletta", 18.8),
            new CreateMeasurements2.WeatherStation("Vancouver", 10.4),
            new CreateMeasurements2.WeatherStation("Veracruz", 25.4),
            new CreateMeasurements2.WeatherStation("Vienna", 10.4),
            new CreateMeasurements2.WeatherStation("Vientiane", 25.9),
            new CreateMeasurements2.WeatherStation("Villahermosa", 27.1),
            new CreateMeasurements2.WeatherStation("Vilnius", 6.0),
            new CreateMeasurements2.WeatherStation("Virginia Beach", 15.8),
            new CreateMeasurements2.WeatherStation("Vladivostok", 4.9),
            new CreateMeasurements2.WeatherStation("Warsaw", 8.5),
            new CreateMeasurements2.WeatherStation("Washington, D.C.", 14.6),
            new CreateMeasurements2.WeatherStation("Wau", 27.8),
            new CreateMeasurements2.WeatherStation("Wellington", 12.9),
            new CreateMeasurements2.WeatherStation("Whitehorse", -0.1),
            new CreateMeasurements2.WeatherStation("Wichita", 13.9),
            new CreateMeasurements2.WeatherStation("Willemstad", 28.0),
            new CreateMeasurements2.WeatherStation("Winnipeg", 3.0),
            new CreateMeasurements2.WeatherStation("Wrocław", 9.6),
            new CreateMeasurements2.WeatherStation("Xi'an", 14.1),
            new CreateMeasurements2.WeatherStation("Yakutsk", -8.8),
            new CreateMeasurements2.WeatherStation("Yangon", 27.5),
            new CreateMeasurements2.WeatherStation("Yaoundé", 23.8),
            new CreateMeasurements2.WeatherStation("Yellowknife", -4.3),
            new CreateMeasurements2.WeatherStation("Yerevan", 12.4),
            new CreateMeasurements2.WeatherStation("Yinchuan", 9.0),
            new CreateMeasurements2.WeatherStation("Zagreb", 10.7),
            new CreateMeasurements2.WeatherStation("Zanzibar City", 26.0),
            new CreateMeasurements2.WeatherStation("Zürich", 9.3));

    public static void main(String[] args) {
        // int size = 10009;
        // FastHashMap2 map = new FastHashMap2(size);
        // for (CreateMeasurements2.WeatherStation w : stations) {
        // byte[] station = w.id.getBytes(StandardCharsets.UTF_8);
        // Key key = new Key(station, new int[]{hashcode1(station), hashcode2(station)});
        // if (map.get(key) != null) {
        // String first = "NA";
        // String second = w.id;
        // int h1 = key.hashCodes[0];
        // int h2 = key.hashCodes[1];
        // int h3 = modulo(h1, size);
        // int h4 = modulo(h2, size);
        // System.out.println(
        // STR."Found one collision: [key1 = \{first}, key2 = \{second}, h1 = \{h1}, h2 = \{h2}, h3 = \{h3}, h4 = \{h4}]");
        // } else {
        // map.compute(key, (k, v) -> {
        // if (v == null) {
        // return new CalculateAverage_godofwharf.MeasurementAggregator(1, 1, 1, 1);
        // }
        // return v;
        // });
        // }
        // }
        int h1 = hashcode1("Kansas City".getBytes(StandardCharsets.UTF_8));
        int h2 = hashcode2("Kansas City".getBytes(StandardCharsets.UTF_8));
        int h3 = hashcode1("Batumi".getBytes(StandardCharsets.UTF_8));
        int h4 = hashcode2("Batumi".getBytes(StandardCharsets.UTF_8));
        System.out.println("h1 = %d/%d, h2 = %d/%d".formatted(h1, h2, h3, h4));

    }

    public static int hashcode1(final byte[] b) {
        int result = 1;
        for (byte value : b) {
            result = result * 31 + value;
        }
        return result;
    }

    public static int hashcode2(final byte[] b) {
        int result = 1;
        for (byte value : b) {
            result = result * 127 + value;
        }
        return result;
    }

    public static int modulo(final int i,
                             final int m) {
        return (m + (i % m)) % m;
    }

    // public static class FastHashMap {
    // private TableEntry[] tableEntries;
    // private int size;
    //
    // public FastHashMap(final int capacity) {
    // this.size = capacity;
    // System.out.println(STR."Table size = \{this.size}");
    // this.tableEntries = new TableEntry[size];
    // }
    //
    // private int tableSizeFor(final int capacity) {
    // int n = -1 >>> Integer.numberOfLeadingZeros(capacity - 1);
    // return (n < 0) ? 1 : (n >= Integer.MAX_VALUE) ? Integer.MAX_VALUE : n + 1;
    // }
    //
    // public void put(final Key key,
    // final String value) {
    // tableEntries[modulo(key.hashCode, size)] = new TableEntry(key, value);
    // }
    //
    // public String get(final Key key) {
    // TableEntry entry = tableEntries[modulo(key.hashCode, size)];
    // if (entry != null) {
    // return entry.value;
    // }
    // return null;
    // }
    //
    // public void forEach(final BiConsumer<Key, String> action) {
    // for (int i = 0; i < size; i++) {
    // TableEntry entry = tableEntries[i];
    // if (entry != null) {
    // action.accept(entry.key, entry.value);
    // }
    // }
    // }
    //
    // record TableEntry(Key key, String value) {
    // }
    // }

    public static class Key {
        byte[] station;
        int[] hashCodes;

        public Key(final byte[] station,
                   final int[] hashCodes) {
            this.station = station;
            this.hashCodes = hashCodes;
        }

        @Override
        public int hashCode() {
            return hashCodes[0];
        }

        @Override
        public boolean equals(final Object o) {
            if (!(o instanceof Key)) {
                return false;
            }
            Key other = (Key) o;
            return station.length == other.station.length &&
                    station.equals(other.station);
        }
    }
}
