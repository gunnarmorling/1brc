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

    private static byte[] readAllData(String fileName1) throws IOException {
        String fileName = fileName1;
        byte[] bytes = CalculateAverage_xpmatteo.readAllData(fileName);
        return bytes;
    }
}
