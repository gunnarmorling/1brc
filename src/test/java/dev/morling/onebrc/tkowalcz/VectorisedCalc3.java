package dev.morling.onebrc.tkowalcz;

import dev.morling.onebrc.CalculateAverage_tkowalcz;
import jdk.incubator.vector.*;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

@SuppressWarnings("unchecked")
public class VectorisedCalc3 {

    private static final String FILE = "measurements.txt";

    private static final VectorSpecies<Byte> SPECIES = ByteVector.SPECIES_256;
    private static final Vector<Byte> ZERO = ByteVector.zero(SPECIES);

    private static final VectorMask<Byte>[] CITY_LOOKUP_MASK = createMasks32();
    private static final VectorMask<Byte>[] MEASUREMENT_LOOKUP_MASK = createMasks32();

    public static final int TABLE_SIZE = 0x20000;
    public static final int TABLE_SIZE_MASK = 0x20000 - 1;

    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
        long startTime = System.nanoTime();

        try (Arena arena = Arena.ofShared()) {
            MemorySegment data = mmapDataFile(FILE, arena);

            int availableProcessors = Runtime.getRuntime().availableProcessors();
            ExecutorService executorService = Executors.newFixedThreadPool(availableProcessors);

            List<Future<CalculateAverage_tkowalcz.StatisticsAggregate[]>> await = new ArrayList<>();
            for (int i = 0; i < availableProcessors; i++) {
                long sliceSize = data.byteSize() / availableProcessors;
                MemorySegment slice = data.asSlice(sliceSize * i, sliceSize);
                Future<CalculateAverage_tkowalcz.StatisticsAggregate[]> future = executorService.submit(() -> execute(slice));

                await.add(future);
            }

            while (!await.isEmpty()) {
                await.removeFirst().get();
            }
            executorService.shutdown();
        } finally {
            long runtime = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startTime);
            System.out.println(STR."Runtime: \{runtime}s");
        }
    }

    private static CalculateAverage_tkowalcz.StatisticsAggregate[] execute(MemorySegment data) {
        CalculateAverage_tkowalcz.StatisticsAggregate[] dataTable = new CalculateAverage_tkowalcz.StatisticsAggregate[TABLE_SIZE];

        Vector<Byte> delimiterVector = SPECIES.broadcast(';');
        Vector<Byte> newlineVector = SPECIES.broadcast('\n');

        long offset = 0;
        do {
            Vector<Byte> byteVector = SPECIES.fromMemorySegment(data, offset, ByteOrder.nativeOrder());
            int firstDelimiter = byteVector.compare(VectorOperators.EQ, delimiterVector).firstTrue();

            VectorMask<Byte> hashMask = CITY_LOOKUP_MASK[firstDelimiter];

            Vector<Byte> hashInput = ZERO.blend(byteVector, hashMask);
            long perfectHash64 = hashInput.reinterpretAsLongs().reduceLanesToLong(VectorOperators.XOR);
            int perfectHash32 = (int) ((perfectHash64 >> 32) ^ perfectHash64);
            int index = perfectHash32 & TABLE_SIZE_MASK;
            CalculateAverage_tkowalcz.StatisticsAggregate statisticsAggregate = dataTable[index];
            if (statisticsAggregate == null) {
                byte[] city = new byte[SPECIES.length()];
                hashInput.reinterpretAsBytes().intoArray(city, 0, hashMask);

                statisticsAggregate = new CalculateAverage_tkowalcz.StatisticsAggregate(city, hashMask.trueCount());
                dataTable[index] = statisticsAggregate;
            }

            offset += firstDelimiter + 1;

            byteVector = SPECIES.fromMemorySegment(data, offset, ByteOrder.nativeOrder());
            VectorMask<Byte> newlines = byteVector.compare(VectorOperators.EQ, newlineVector);
            int newlinePosition = newlines.firstTrue();

            float v = parseFloat(byteVector, MEASUREMENT_LOOKUP_MASK[newlinePosition], newlinePosition);
            offset += newlinePosition + 1;

            statisticsAggregate.accept((long) v);
        } while (offset < data.byteSize());

        return dataTable;
    }

    private static final byte[] tmpArray = new byte[32];

    public static float parseFloat(Vector<Byte> input, VectorMask<Byte> mask, int newlinePosition) {
        input.compress(mask).reinterpretAsBytes().intoArray(tmpArray, 0);

        int integerPart = 0;
        int fractionalPart = 0;
        boolean negative = false;

        int i = 0;
        if (tmpArray[0] == '-') {
            negative = true;
            i = 1;
        }

        for (; i < newlinePosition; i++) {
            if (tmpArray[i] == '.') {
                break;
            }

            integerPart += tmpArray[i] - '0';
            integerPart *= 10;
        }

        for (; i < newlinePosition; i++) {
            fractionalPart += tmpArray[i] - '0';
            fractionalPart *= 10;
        }

        return 1.0f / fractionalPart + integerPart;
    }

    public static VectorMask<Byte>[] createMasks32() {
        VectorMask<Byte>[] result = new VectorMask[32];
        result[0] = SPECIES.maskAll(false);

        int maskSource = 0x1;
        for (int i = 1; i < 32; i++) {
            result[i] = VectorMask.fromLong(SPECIES, maskSource);
            maskSource <<= 1;
            maskSource += 1;
        }

        return result;
    }

    private static MemorySegment mmapDataFile(String fileName, Arena arena) throws IOException {
        try (RandomAccessFile file = new RandomAccessFile(fileName, "r");
                FileChannel channel = file.getChannel()) {
            return channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size(), arena);
        }
    }
}
