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

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

/*
 If I had to submit something without looking what others did, I am fairly certain I would
 be under 500MB/s on my machine on the dataset created by ./create_measurement.sh.

 Maybe it's cheating? But I learned stuff.

 Credits:
  - @royvanrijn for the idea of using bit twiddling without java.incubator.vector
  - @royvanrijn again for the parse int only and /10 in the very end
  - @flippingbits for the idea of not calling getLong on mmaped file directly, get a big chunk into memory instead
        - Maybe on linux it's actually faster?
        - Confirmed: mmap is faster at least on Windows.

  - OpenAI / ChatGPT for the actual bit twiddling idea for finding a character, it couldn't explain it so I did my best
    (Turns out @royvanrijn had the same idea)

 Note:
  - If someone needs maybe a new trick: in the hashmap, if you read a station name as a long and it
  fits under 8 bytes, use it directly on equality comparisons. Saves a lot of looping and gives a nice
  ~5% boost for me.


Perf tracking (on my Windows machine):

    1: ~11s
    2: 9.77s
    3: 8.0s

 Version 1:
   - Initial implementation

 Version 2:
   - Adding multiple ways to read longs from a file, either mmaped or preread.
   - Perf improvements due do mmap read.
     On my mac for some reason reading a big chunk is faster, on @royvanrijn's mac the mmap read is faster. Maybe
     due to M2 having more memory?

     On Linux AWS c6a-4xlarge graalvm 21.0.1:
       GraalVM:
         - Preread takes ~11.2 seconds
         - Mmap takes ~10.7 seconds
        So it's a bit faster but not much.

 Version 3:
  - The final aggregation of results was taking around 1 whole second to complete. Instead of adding the string
  to the TreeMap on every chunk, do it only once in the end when everything is aggregated.

 */
@SuppressWarnings("unchecked")
public class CalculateAverage_ricardopieper {
    private static final boolean FORCE_PREREAD = false;
    private static final boolean FORCE_MAPPED = false;
    private static final String FILE = "./measurements.txt";

    public static class ByteSlice {
        public ByteSlice(int start, int length, byte[] buffer) {
            this.start = start;
            this.length = length;
            this.buffer = buffer;
        }

        public int start;
        public int length;
        public byte[] buffer;

        public OwnedByteSlice copyBytes() {
            return new OwnedByteSlice(Arrays.copyOfRange(buffer, start, start + length));
        }

        @Override
        public int hashCode() {
            int result = 1;
            for (int i = start; i < start + length; i++) {
                result = 31 * result + buffer[i];
            }
            return result;
        }

        // I don't want to override the equals
        public boolean equalTo(byte[] a2) {
            if (length != a2.length) {
                return false;
            }
            // I tried all sorts of trickery here,
            // like comparing first and last before the loop.
            // It's useless. Maybe slower. Because when strings are the same length
            // it's a pretty high chance they are the same...
            for (int i = 0; i < length; i++) {
                if (buffer[start + i] != a2[i]) {
                    return false;
                }
            }
            return true;
        }

        public boolean equalTo(OwnedByteSlice a2) {
            if (length != a2.bytes.length) {
                return false;
            }
            // I tried all sorts of trickery here,
            // like comparing first and last before the loop.
            // It's useless. Maybe slower. Because when strings are the same length
            // it's a pretty high chance they are the same...
            for (int i = 0; i < length; i++) {
                if (buffer[start + i] != a2.bytes[i]) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public String toString() {
            return new String(buffer, start, length);
        }
    }

    public static class OwnedByteSlice {
        public byte[] bytes;

        public OwnedByteSlice(byte[] bytes) {
            this.bytes = bytes;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            OwnedByteSlice that = (OwnedByteSlice) o;
            return Arrays.equals(bytes, that.bytes);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(bytes);
        }

        @Override
        public String toString() {
            return new String(bytes);
        }
    }

    interface FileStreamer {
        long getLong();

        boolean hasRemaining();

        void position(int pos);

        int position();

        ByteSlice getSlice(int start, int length);

    }

    public static abstract class ByteBufferStreamer implements FileStreamer {
        protected ByteBuffer byteBuffer;

        ByteBufferStreamer() {
        }

        @Override
        public boolean hasRemaining() {
            return this.byteBuffer.hasRemaining();
        }

        @Override
        public void position(int pos) {
            this.byteBuffer.position(pos);
        }

        @Override
        public int position() {
            return this.byteBuffer.position();
        }

    }

    // Works well in my MacOS laptop
    static class ChunkPrereader extends ByteBufferStreamer {
        private final ByteSlice cachedByteSlice;

        public ChunkPrereader(FileChunk chunk) {
            // this +8 helps to remove a conditional check when calling getLong when there's less than 8 bytes remaining
            this.byteBuffer = ByteBuffer.allocate(chunk.size + 8);
            try (var channel = new RandomAccessFile(chunk.path.toString(), "r")) {
                channel.seek(chunk.start);
                channel.read(byteBuffer.array(), 0, chunk.size);
            }
            catch (Exception e) {
                // yes bad practice, but idc
                throw new RuntimeException(e);
            }
            byteBuffer.position(0);
            cachedByteSlice = new ByteSlice(0, 0, byteBuffer.array());
        }

        @Override
        public long getLong() {
            return this.byteBuffer.getLong();
        }

        @Override
        public ByteSlice getSlice(int start, int length) {
            cachedByteSlice.start = start;
            cachedByteSlice.length = length;
            return cachedByteSlice;
        }

    }

    // May be better in other computers
    static class ChunkMapped extends ByteBufferStreamer {
        private final ByteSlice cachedByteSlice;

        public ChunkMapped(FileChunk chunk) {
            try (var fileChannel = FileChannel.open(chunk.path, StandardOpenOption.READ)) {
                this.byteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, chunk.start, chunk.size);
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
            byteBuffer.position(0);
            cachedByteSlice = new ByteSlice(0, 0, new byte[200]);
        }

        public long getLong() {
            if (this.byteBuffer.remaining() >= 8) {
                return this.byteBuffer.getLong();
            }
            else {
                byte[] longBytes = new byte[8];
                this.byteBuffer.get(longBytes, 0, this.byteBuffer.remaining());
                return ByteBuffer.wrap(longBytes).getLong();
            }
        }

        @Override
        public ByteSlice getSlice(int start, int length) {
            var oldPos = this.byteBuffer.position();
            this.byteBuffer.position(start);
            this.byteBuffer.get(this.cachedByteSlice.buffer, 0, length);
            this.cachedByteSlice.start = 0;
            this.cachedByteSlice.length = length;
            this.byteBuffer.position(oldPos);
            return this.cachedByteSlice;
        }
    }

    public static final class StationMeasurements {
        public long min;
        public long max;
        public long sum;
        public int count;

        public StationMeasurements(
                                   long min,
                                   long max,
                                   long sum,
                                   int count) {
            this.min = min;
            this.max = max;
            this.sum = sum;
            this.count = count;
        }

        @Override
        public String toString() {
            var min = String.format("%.1f", (double) this.min / 10.0);
            var avg = String.format("%.1f", ((double) this.sum / (double) this.count) / 10.0);
            var max = String.format("%.1f", (double) this.max / 10.0);

            return STR."\{min}/\{avg}/\{max}";
        }
    }

    final static class FileStreamers {
        public static FileStreamer getStreamer(FileChunk chunk) {
            if (FORCE_PREREAD) {
                return new ChunkPrereader(chunk);
            }
            if (FORCE_MAPPED) {
                return new ChunkMapped(chunk);
            }
            String os = System.getProperty("os.name").toLowerCase();
            if (os.contains("mac") || os.contains("darwin")) {
                return new ChunkPrereader(chunk);
            }
            else {
                return new ChunkMapped(chunk);
            }
        }
    }

    public static record FileChunk(Path path, long start, int size) {
    }

    public static List<FileChunk> splitFile(Path path, int numChunks) throws IOException {
        try (var fileChannel = FileChannel.open(path, StandardOpenOption.READ)) {
            var chunks = new ArrayList<FileChunk>();
            long size = fileChannel.size();

            if (numChunks == 1 && size <= Integer.MAX_VALUE) {
                chunks.add(new FileChunk(path, 0, (int) size));
                return chunks;
            }
            long approxSizePerChunk = size / numChunks;
            var buffer = ByteBuffer.allocate(1024); // in practice records are max 20 chars long

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

                chunks.add(new FileChunk(path,
                        curOffset, (int) sizeOfChunk));
                curOffset = end; // because we found the \n but we want to leave at the next character
                if (curOffset >= size) {
                    break;
                }
            }

            return chunks;
        }
    }

    // Ignores the dots because we do a division in the end
    public static int longBytesToInt(long vector, int len) {
        byte b0 = (byte) (vector >> 64 - 8),
                b1 = (byte) (vector >> 64 - 16),
                b2 = (byte) (vector >> 64 - 24),
                b3 = (byte) (vector >> 64 - 32),
                b4 = (byte) (vector >> 64 - 40);

        // check for windows newline
        byte newlineByte = (byte) (vector >> (64 - (len * 8)));
        if (newlineByte == '\r') {
            len--;
        }

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

        var debugMode = System.getenv("DEBUG") != null && System.getenv("DEBUG").equals("true");
        var timeIt = System.getenv("TIME") != null && System.getenv("TIME").equals("true");

        Instant start = null, end = null;

        if (timeIt) {
            start = Instant.now();
        }

        var path = Paths.get(FILE);

        var averageSizePerChunk = 16 * 1024 * 1024;
        var fileSize = Files.size(path);
        var numChunks = Math.max(1, fileSize / averageSizePerChunk);
        var chunks = splitFile(path, (int) numChunks);
        HashMap<OwnedByteSlice, StationMeasurements> result = null;
        if (debugMode) {
            for (var chunk : chunks) {

                var r = processFileChunk(chunk);
                r.printCompactBucketStatistics();
                result = r.asMap();
                System.out.println("Processed chunk " + chunk.start);
            }
        }
        else {
            result = chunks.parallelStream()
                    .map(chunk -> {
                        try {
                            return processFileChunk(chunk);

                        }
                        catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    })
                    // .peek(x -> x.printStats())
                    .map(MeasureMap::asMap)
                    .reduce((map1, map2) -> {
                        for (var kv : map1.entrySet()) {
                            var mergedEntry = map2.get(kv.getKey());
                            if (mergedEntry == null) {
                                map2.put(kv.getKey(), kv.getValue());
                            }
                            else {
                                mergedEntry.min = Math.min(mergedEntry.min, kv.getValue().min);
                                mergedEntry.max = Math.max(mergedEntry.max, kv.getValue().max);
                                mergedEntry.count += kv.getValue().count;
                                mergedEntry.sum += kv.getValue().sum;
                            }
                        }
                        return map2;
                    }).get();
        }

        // now that we have the aggregated result, let's build a treemap with the string
        // so that we can have a sorted map alphabetically and easily printable
        TreeMap<String, StationMeasurements> finalResult = new TreeMap<>();

        assert result != null;
        for (var entry : result.entrySet()) {
            finalResult.put(entry.getKey().toString(), entry.getValue());
        }

        if (timeIt) {
            end = Instant.now();
        }
        System.out.println(finalResult);
        if (timeIt) {
            System.out.println(Duration.between(start, end));
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

    private static MeasureMap processFileChunk(FileChunk chunk) throws IOException {
        var map = new MeasureMap();

        long newlineMask = 0x0A0A0A0A0A0A0A0AL;
        long separatorMask = 0x3B3B3B3B3B3B3B3BL;

        // the state machine goes like this:
        // 1 - looking for the next ; to get name

        int nameStart = 0, nameEnd = 0;
        int tempStart = 0, tempEnd = 0;

        var streamer = FileStreamers.getStreamer(chunk);

        while (streamer.hasRemaining()) {

            long nameVector = streamer.getLong();

            int idx = findFirstOccurrence(nameVector, separatorMask);

            boolean foundSeparator = idx != 8;
            nameEnd += idx;
            if (foundSeparator) {
                // skip the separator
                tempStart = nameEnd + 1;
            }
            else {
                continue;
            }

            streamer.position(tempStart);
            var tempVector = streamer.getLong();

            // looking for temperature
            // this one is guaranteed to run in one go because
            // the temperature is max 5 characters, 6 with the newline,
            // and we start at the number already.

            int newlineIdx = findFirstOccurrence(tempVector, newlineMask);
            tempEnd = tempStart + newlineIdx;

            // var tempStr = new String(tempBytes, 0, tempEnd - tempStart);
            var temp = longBytesToInt(tempVector, tempEnd - tempStart);// Float.parseFloat(tempStr);

            var nameSlice = streamer.getSlice(nameStart, nameEnd - nameStart);

            var measurements = map.getOrAdd(nameSlice, nameVector);

            measurements.min = Math.min(measurements.min, temp);
            measurements.max = Math.max(measurements.max, temp);
            measurements.sum += temp;
            measurements.count += 1;

            // System.out.println(STR."\{name} -> \{tempStr}");

            nameStart = tempEnd + 1;
            nameEnd = nameStart;
            streamer.position(nameStart);
        }

        return map;
    }

    // I tried using a trie but it wasn't cache efficient enough.
    // THe problem with a HashMap is that it doesnt allow passing a byte and length,
    // I also tried using a wrapper object that, when added, I called a .consolidate() method
    // to actually copy the underlying buffer, but the profiler showed it allocated
    // way too much memory. Performance was kind of the same though...
    public static class MeasureMap {
        public record Entry(OwnedByteSlice nameBytes, long nameVector, StationMeasurements measurements) {
            @Override public String toString() {
                return STR."\{nameBytes.toString()} -> \{measurements.toString()}";
            }
        }

        ArrayList<Entry>[] buckets;

        public MeasureMap() {
            buckets = new ArrayList[32 * 1024];
        }

        public StationMeasurements getOrAdd(ByteSlice byteSlice, long nameVector) {
            long shift = (64 - (byteSlice.length * 8L));
            nameVector = nameVector >> shift << shift;

            var hash = byteSlice.hashCode();

            // This is a modulo operation because:
            // buckets.length is a power of 2 (like 01000000, only 1 bit is set)
            // -1 makes all the bits right to that 1 become 1 (like 00111111)
            // the & will select only those 1s, and due to math, it is the remainder
            // this has also the benefit of not needing a positive hash
            int bucketIndex = (buckets.length - 1) & hash;

            var bucket = buckets[bucketIndex];
            if (bucket == null) {
                buckets[bucketIndex] = new ArrayList<>();
                bucket = buckets[bucketIndex];
            }
            for (int i = 0; i < bucket.size(); i++) {
                var item = bucket.get(i);
                // <= 7 means we found the name and separator in one read, so it's
                // guaranteed the actualNameVector holds all the data
                if (byteSlice.length <= 7 && item.nameBytes.bytes.length == byteSlice.length && nameVector == item.nameVector) {
                    return item.measurements;
                }

                boolean found = byteSlice.equalTo(item.nameBytes.bytes);
                if (found) {
                    return item.measurements;
                }
            }

            // new item, consolidate
            var newItemKey = byteSlice.copyBytes();

            var measurements = new StationMeasurements(
                    Integer.MAX_VALUE,
                    Integer.MIN_VALUE,
                    0,
                    0);

            bucket.add(new Entry(
                    newItemKey, nameVector, measurements));

            return measurements;
        }

        // Convenience method to merge maps later
        public HashMap<OwnedByteSlice, StationMeasurements> asMap() {
            var result = new HashMap<OwnedByteSlice, StationMeasurements>();
            for (int i = 0; i < this.buckets.length; i++) {
                var bucket = this.buckets[i];
                if (bucket == null)
                    continue;
                for (int j = 0; j < bucket.size(); j++) {
                    var item = bucket.get(j);
                    result.put(item.nameBytes, item.measurements);
                }
            }
            return result;
        }

        public void printCompactBucketStatistics() {
            // Thanks ChatGPT! https://chat.openai.com/share/a8e4906b-a9b4-402e-9212-4bc468606f21
            int[] count = new int[6]; // For 0, 1, 2, 4, 8, 16 entries
            int over32 = 0;

            for (ArrayList<Entry> bucket : buckets) {
                if (bucket == null)
                    continue;
                int size = bucket.size();
                if (size >= 32)
                    over32++;
                else if (size >= 16)
                    count[5]++;
                else if (size >= 8)
                    count[4]++;
                else if (size >= 4)
                    count[3]++;
                else if (size >= 2)
                    count[2]++;
                else if (size == 1)
                    count[1]++;
                else
                    count[0]++;
            }

            System.out.println("Bucket Count Stats: ==0:" + count[0] + " >=1:" + count[1] + " >=2:" + count[2] +
                    " >=4:" + count[3] + " >=8:" + count[4] + " >=16:" + count[5] + " >=32:" + over32);
        }

    }

    // Useful for debugging
    public static byte[] longToBytes(long vector) {
        byte b0 = (byte) (vector >> 64 - 8),
                b1 = (byte) (vector >> 64 - 16),
                b2 = (byte) (vector >> 64 - 24),
                b3 = (byte) (vector >> 64 - 32),
                b4 = (byte) (vector >> 64 - 40),
                b5 = (byte) (vector >> 64 - 48),
                b6 = (byte) (vector >> 64 - 56),
                b7 = (byte) (vector);

        return new byte[]{
                b0, b1, b2, b3, b4, b5, b6, b7
        };
    }
}
