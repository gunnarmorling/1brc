package dev.morling.onebrc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;

//In some places I took from roy but this is mostly me figuring out stuff.
//ChatGPT 4 did help me a lot figuring out the bit mask stuff, and I gave my best shot at explaining it.
@SuppressWarnings("unchecked")
public class CalculateAverage_ricardopieper {

    private static final String FILE = "./measurements.txt";

    public static final class StationMeasurements {
        public long min;
        public long max;
        public long sum;
        public int count;

        public StationMeasurements(
                                   long min,
                                   long max,
                                   long sum,
                                   int count

        ) {
            this.min = min;
            this.max = max;
            this.sum = sum;
            this.count = count;
        }

        @Override
        public String toString() {
            var min = String.format("%.1f", (double)this.min / 10.0);
            var avg = String.format("%.1f", ((double)this.sum / (double) this.count) / 10.0);
            var max = String.format("%.1f", (double)this.max / 10.0);

            return STR."\{min}/\{avg}/\{max}";
        }
    }

    // This function only works for big files and numChunks > 1, otherwise some stuff
    // will break lol
    public static List<MappedByteBuffer> splitFile(FileChannel fileChannel, int numChunks) throws IOException {
        long size = fileChannel.size();
        long approxSizePerChunk = size / numChunks;
        var buffer = ByteBuffer.allocate(1024); // in practice records are max 20 chars long
        var chunks = new ArrayList<MappedByteBuffer>();
        long curOffset = 0;
        for (int i = 0; i < numChunks; i++) {
            long targetOffset = curOffset + approxSizePerChunk;
            // advance to the target offset
            fileChannel.position(targetOffset);
            // advance until we find a newline
            buffer.clear();
            fileChannel.read(buffer);
            int extraAdvance = 0;
            while (buffer.get(extraAdvance) != '\n') {
                extraAdvance++;
            }
            // +1 because we want to include the \n in the current chunk
            var end = Math.min(curOffset + approxSizePerChunk + extraAdvance + 1, size);
            long sizeOfChunk = end - curOffset;

            chunks.add(fileChannel.map(
                    FileChannel.MapMode.READ_ONLY, curOffset, sizeOfChunk));
            curOffset = end; // because we found the \n but we want to leave at the next character
            if (curOffset >= size) {
                break;
            }
        }

        return chunks;
    }

    public enum State {
        LookingForName,
        LookingForTemp,
        Measuring
    }

    // ignores the dots because we do a division in the end
    // and this avoids a MappedByteBuffer.get(byte[]) call.
    public static int longBytesToInt(long vector, int len) {
        byte b0 = (byte) (vector >> 64 - 8),
                b1 = (byte) (vector >> 64 - 16),
                b2 = (byte) (vector >> 64 - 24),
                b3 = (byte) (vector >> 64 - 32),
                b4 = (byte) (vector >> 64 - 40);

        // len can be 3 4 or 5
        // in this case we get bytes 0 and 2
        if (len == 3) {
            return (b0 - '0') * 10 + (b2 - '0');
        }
        boolean isNegative = b0 == '-';
        // a number like -1.0
        if (len == 4 && isNegative) {
            return -((b1 - '0') * 10 + (b3 - '0'));
        }
        // a number like 99.1
        if (len == 4) {
            return (b0 - '0') * 100 + (b1 - '0') * 10 + (b3 - '0');
        }
        // a number like -99.1
        if (len == 5 && isNegative) {
            return -((b1 - '0') * 100 + (b2 - '0') * 10 + (b4 - '0'));
        }

        throw new RuntimeException("Shouldn't reach here: non-negative number with >5 characters");
    }

    public static void main(String[] args) throws IOException, InterruptedException {

        var path = Paths.get(FILE);

        // 2* num cores, looks like helps with I/O.
        // More than this is useless.
        int numChunks = 2 * Runtime.getRuntime().availableProcessors();

        try (var fileChannel = FileChannel.open(path, StandardOpenOption.READ)) {
            var chunks = splitFile(fileChannel, numChunks);
            numChunks = chunks.size();
            MeasureMap[] maps = new MeasureMap[numChunks];
            Thread[] threads = new Thread[numChunks];

            for (int i = 0; i < numChunks; i++) {
                int index = i;
                var chunk = chunks.get(i);
                var t = new Thread(() -> {
                    maps[index] = processFileChunk(chunk);
                });
                threads[index] = t;

            }
            for (var t : threads) {
                t.start();
            }
            for (var t : threads) {
                t.join();
            }

            // merge maps
            var merged = maps[0].asMap();

            for (int i = 1; i < maps.length; i++) {
                var m = maps[i];
                for (var kv : m.asMap().entrySet()) {
                    var mergedEntry = merged.get(kv.getKey());
                    if (mergedEntry == null) {
                        merged.put(kv.getKey(), kv.getValue());
                    }
                    else {
                        mergedEntry.min = Math.min(mergedEntry.min, kv.getValue().min);
                        mergedEntry.max = Math.max(mergedEntry.max, kv.getValue().max);
                        mergedEntry.count += kv.getValue().count;
                        mergedEntry.sum += kv.getValue().sum;
                    }
                }
            }
            System.out.println(merged);
        }

    }

    private static final int findFirstOccurrence(long bytes, long pattern) {
        // This masked value will have a 00000000 at the byte where we have the separator,
        // and crucially, only at those places.
        long masked = bytes ^ pattern;

        // Now those 00000000 can be turned into a 11111111 (underflow). Why?
        // It will become apparent later.
        long underflowed = masked - 0x0101010101010101L;

        long clearHighBits = underflowed & ~masked;
        /*
         * A lot happened in that line of code above. Suppose this comment starts before the line executes.
         * To understand this, it's best if you take some actual values during runtime and plug them onto
         * bitwisecmd.com and see these things happening.
         *
         * Now that we have that full 0xFF byte at the position of the separators (or not,
         * maybe we didn't find a separator. Don't worry it still works the same), we can do a trick
         * where we look for the highest bit on each byte being set and isolating them. Unfortunately
         * it's possible that our masked value (and underflowed value) still have some unrelated
         * bytes with the high bit still set.
         *
         * When we negate the masked value, those high bits become 0. The AND will make those high bits in
         * underflowed switch to 0, because the negated mask high bit is zero. Notice that this is specific to our
         * mask value.
         *
         * How about our 0xFF? the ~masked turns the 0x00 at the byte position into 0xFF, so the AND
         * still leaves them 0xFF.
         *
         * Well, what if our mask has a high 1 though? like 0xBB? And the input had a mix of high bits
         * set and unset?
         *
         * In that case, masked would be 0x000000 at the byte position and would keep its high bits everywhere else,
         * and every input byte with high MSB the XOR would turn into a 0. In the next operations
         * it forwards a 0 into the calculations, and it doesn't matter what is in the other side of the AND,
         * it will result in zero.
         *
         * What if our mask is 0x3B as always but we have some bytes with MSB 1?
         * The XORed value will contain the high bits, but this same XORed value will be negated
         * later, making its high bits 0. When they get ANDed, the high bit 0 in the negated value
         * will clear that 1.
         *
         *
         * If the search finds nothing, the final value of clearHighBits will be zero at every MSB on every byte.
         */

        // this 0x808080 is just 0b100000000|100000000, it selects the highest bit of every byte.
        long highBitOfSeparator = clearHighBits & 0x8080808080808080L;

        // If nothing is found, highBitOfSeparator will be zero, and this call returns 64, /8 becomes 8 which means
        // we can advance 8 positions forward.

        return Long.numberOfLeadingZeros(highBitOfSeparator) / 8;
    }

    public static final long readNextLong(MappedByteBuffer chunk) {
        int remaining = chunk.remaining();
        if (remaining >= 8) {
            return chunk.getLong();
        }
        else {
            // get all remaining bytes
            byte[] bytes = new byte[8];
            chunk.get(bytes, 0, remaining);
            return ByteBuffer.wrap(bytes).getLong();
        }
    }

    private static MeasureMap processFileChunk(MappedByteBuffer chunk) {
        var map = new MeasureMap();

        // long size = fileChannel.size();
        long newlineMask = 0x0A0A0A0A0A0A0A0AL;
        long separatorMask = 0x3B3B3B3B3B3B3B3BL;

        // the state machine goes like this:
        // 1 - looking for the next ; to get name

        int nameStart = 0, nameEnd = 0;
        int tempStart = 0, tempEnd = 0;

        byte[] nameBytes = new byte[1000]; // ample space for weird unicode characters

        long vector = 0;
        while (chunk.hasRemaining()) {

            vector = readNextLong(chunk);

            int idx = findFirstOccurrence(vector, separatorMask);

            boolean foundSeparator = idx != 8;
            nameEnd += idx;
            if (foundSeparator) {
                // skip the separator
                tempStart = nameEnd + 1;
                tempEnd = tempStart;
            }
            else {
                continue;
            }

            chunk.position(tempStart);
            vector = readNextLong(chunk);

            // looking for temperature
            // this one is guaranteed to run in one go because
            // the temperature is max 5 characters, 6 with the newline,
            // and we start at the number already.

            int newlineIdx = findFirstOccurrence(vector, newlineMask);
            tempEnd += newlineIdx;

            chunk.position(nameStart);
            chunk.get(nameBytes, 0, nameEnd - nameStart);

            // var tempStr = new String(tempBytes, 0, tempEnd - tempStart);
            var temp = longBytesToInt(vector, tempEnd - tempStart);// Float.parseFloat(tempStr);

            var measurements = map.getOrAdd(nameBytes, nameEnd - nameStart);

            if (temp < measurements.min) {
                measurements.min = temp;
            }

            if (temp > measurements.max) {
                measurements.max = temp;
            }

            measurements.sum += temp;
            measurements.count += 1;

            // System.out.println(STR."\{name} -> \{tempStr}");

            nameStart = tempEnd + 1;
            nameEnd = nameStart;
            chunk.position(nameStart);
        }

        return map;
    }

    // I tried using a trie but it wasn't cache efficient enough.
    // THe problem with a HashMap is that it doesnt allow passing a byte and length,
    // I also tried using a wrapper object that, when added, I called a .consolidate() method
    // to actually copy the underlying buffer, but the profiler showed it allocated
    // way too much memory. Performance was kind of the same though...
    public static class MeasureMap {
        public record Entry(byte[] bytes, StationMeasurements measurements) {
        }

        ;

        ArrayList<Entry>[] buckets;

        public MeasureMap() {
            buckets = new ArrayList[32 * 1024];
            for (int i = 0; i < 32 * 1024; i++) {
                var bucket = new ArrayList<Entry>();
                buckets[i] = bucket;
            }
        }

        // I tried using a vectorized hashCode using ByteVector,
        // but I think this gets vectorized anyway because the performance is
        // either better or the same.
        public static int hashCode(byte[] array, int length) {

            int result = 1;
            for (int i = 0; i < length; i++) {
                result = 31 * result + array[i];
            }
            return result;
        }

        public static boolean equals(byte[] a1, int a1len, byte[] a2, int a2len) {
            if (a1len != a2len) {
                return false;
            }
            for (int i = 0; i < a1len; i++) {
                if (a1[i] != a2[i]) {
                    return false;
                }
            }
            return true;
        }

        public StationMeasurements getOrAdd(byte[] bytes, int length) {
            var hash = hashCode(bytes, length);

            // this is a modulo operation because:
            // buckets.lenght is a power of 2 (like 01000000, only 1 bit is set)
            // -1 makes all the bits right to that 1 become 1 (like 00111111)
            // the & will select only those 1s, and due to math, it is the remainder
            // this has also the benefit of not needing a positive hash
            int bucketIndex = (buckets.length - 1) & hash;

            var bucket = buckets[bucketIndex];
            for (int i = 0; i < bucket.size(); i++) {
                var item = bucket.get(i);
                boolean found = equals(bytes, length, item.bytes, item.bytes.length);
                if (found) {
                    return item.measurements;
                }
            }

            // new item, consolidate
            var newItemKey = Arrays.copyOfRange(bytes, 0, length);
            var measurements = new StationMeasurements(
                    Integer.MAX_VALUE,
                    Integer.MIN_VALUE,
                    0,
                    0);

            bucket.add(new Entry(
                    newItemKey, measurements));
            return measurements;
        }

        // Convenience method to merge maps later
        public TreeMap<String, StationMeasurements> asMap() {
            var result = new TreeMap<String, StationMeasurements>();
            for (var bucket : this.buckets) {
                for (var item : bucket) {
                    var str = new String(item.bytes);
                    result.put(str, item.measurements);
                }
            }
            return result;
        }

    }
}
