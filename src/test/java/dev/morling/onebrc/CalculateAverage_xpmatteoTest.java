package dev.morling.onebrc;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

@SuppressWarnings("all")
class CalculateAverage_xpmatteoTest {

    @Test
    void testFileName() {
        // assertEquals("./measurements.txt", CalculateAverage_xpmatteo.dataFileName(new String[0]));
        // assertEquals("foobar", CalculateAverage_xpmatteo.dataFileName(new String[]{ "foobar" }));
    }

    @Test
    void readAllBytes() throws IOException {
        // String fileName = "src/test/resources/samples/measurements-1.txt";
        // byte[] bytes = CalculateAverage_xpmatteo.readAllData(fileName);
        // assertEquals("Kunming;19.8\n", new String(bytes));
    }

    @Test
    void parseAllData_one() throws IOException {
        // CalculateAverage_xpmatteo.Results results = CalculateAverage_xpmatteo.parseData("Kunming;19.8\n".getBytes());
        //
        // assertThat(results).isEqualTo(Map.of("Kunming", new CalculateAverage_xpmatteo.CityData(198, 198, 198, 1)));
    }

    @Test
    void parseAllData_two() throws IOException {
        // byte[] bytes = "Kunming;1.0\nKunming;2.0\n".getBytes();
        //
        // CalculateAverage_xpmatteo.Results results = CalculateAverage_xpmatteo.parseData(bytes);
        //
        // assertThat(results).isEqualTo(Map.of("Kunming", new CalculateAverage_xpmatteo.CityData(10, 30, 20, 2)));
    }

    @Test
    void mergeDifferentEntries() {
        // CalculateAverage_xpmatteo.Results results0 = new CalculateAverage_xpmatteo.Results();
        // results0.put("Kunming", new CalculateAverage_xpmatteo.CityData(10, 30, 20, 2));
        // CalculateAverage_xpmatteo.Results results1 = new CalculateAverage_xpmatteo.Results();
        // results1.put("Koeln", new CalculateAverage_xpmatteo.CityData(1, 2, 3, 4));
        //
        // CalculateAverage_xpmatteo.Results merge = CalculateAverage_xpmatteo.merge(results0, results1);
        //
        // assertThat(merge).isEqualTo(Map.of(
        // "Kunming", new CalculateAverage_xpmatteo.CityData(10, 30, 20, 2),
        // "Koeln", new CalculateAverage_xpmatteo.CityData(1, 2, 3, 4)));
    }

    @Test
    void mergeSameEntry() {
        // CalculateAverage_xpmatteo.Results results0 = new CalculateAverage_xpmatteo.Results();
        // results0.put("Koeln", new CalculateAverage_xpmatteo.CityData(10, 30, 20, 2));
        // CalculateAverage_xpmatteo.Results results1 = new CalculateAverage_xpmatteo.Results();
        // results1.put("Koeln", new CalculateAverage_xpmatteo.CityData(1, 2, 3, 4));
        //
        // CalculateAverage_xpmatteo.Results merge = CalculateAverage_xpmatteo.merge(results0, results1);
        //
        // assertThat(merge).isEqualTo(Map.of(
        // "Koeln", new CalculateAverage_xpmatteo.CityData(1, 32, 20, 6)));
    }

    private static byte[] readAllData(String fileName1) throws IOException {
        String fileName = fileName1;
        byte[] bytes = CalculateAverage_xpmatteo.readAllData(fileName);
        return bytes;
    }
}
