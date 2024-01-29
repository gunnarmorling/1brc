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
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.TreeMap;

public class CalculateAverage_gigiblender {
    private static final int AVAIL_CORES = Runtime.getRuntime().availableProcessors();
    private static final HashTable[] tables = new HashTable[AVAIL_CORES];

    private static Unsafe unsafe;
    static {
        Field theUnsafe = null;
        try {
            theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            unsafe = (Unsafe) theUnsafe.get(Unsafe.class);
        }
        catch (IllegalAccessException | NoSuchFieldException ignored) {
        }
    }

    private static final String FILE = "./measurements.txt";

    static class HashTable {

        // 10_000 unique hashes ->
        private static final int ENTRY_SIZE = 32;
        private static final int NUM_ENTRIES = 16384;
        private static final int DATA_SIZE = NUM_ENTRIES * ENTRY_SIZE;

        /*
         * data[i -> i + 7] = 8 bytes hash
         * data[i + 8 -> i + 15] = 7 bytes masked address of the string in the file. 1 byte for the length of the string
         * data[i + 16 -> i + 19] = 4 bytes count
         * data[i + 20 -> i + 21] = 2 bytes max
         * data[i + 22 -> i + 23] = 2 bytes min -- sign preserved
         * data[i + 24 -> i + 31] = 8 bytes sum
         */
        byte[] data;

        private static final int HASH_OFFSET = 0;

        private static final int ADDR_OFFSET = 8;
        private static final long ADDR_MASK = 0x00FFFFFFFFFFFFFFL;
        private static final int STRING_LENGTH_SHIFT = 56;

        private static final int COUNT_OFFSET = 16;

        private static final int SUM_OFFSET = 24;

        private int reprobe_count;

        public HashTable() {
            data = new byte[DATA_SIZE];
            // reprobe_count = 0;
        }

        private long string_addr_and_length(long hash) {
            return unsafe.getLong(data, Unsafe.ARRAY_BYTE_BASE_OFFSET + hash + ADDR_OFFSET);
        }

        private static long string_addr(long encoded_str_addr) {
            return (encoded_str_addr & ADDR_MASK);
        }

        private static long string_length(long encoded_str_addr) {
            return encoded_str_addr >>> STRING_LENGTH_SHIFT;
        }

        private long count_max_min(long hash) {
            return unsafe.getLong(data, Unsafe.ARRAY_BYTE_BASE_OFFSET + hash + COUNT_OFFSET);
        }

        private static short mask_min(long count_max_min) {
            // Preserve the sign
            return (short) (count_max_min >> 6 * Byte.SIZE);
        }

        private static short mask_max(long count_max_min) {
            return (short) (count_max_min >>> 4 * Byte.SIZE);
        }

        private static int mask_count(long count_max_min) {
            return (int) count_max_min;
        }

        private static long encode_count_max_min(int count, short max, short min) {
            return ((long) count) | ((((long) max) & 0xFFFF) << 4 * Byte.SIZE) | (((long) min) << 6 * Byte.SIZE);
        }

        private long sum(long hash) {
            return unsafe.getLong(data, Unsafe.ARRAY_BYTE_BASE_OFFSET + hash + SUM_OFFSET);
        }

        private static boolean string_equals(long string_addr, long entry_string_addr, int size_bytes) {
            int remaining_bytes = size_bytes % 8;
            int i = 0;
            for (; i < size_bytes - remaining_bytes; i += 8) {
                long entry_bytes = unsafe.getLong(entry_string_addr + i);
                long string_bytes = unsafe.getLong(string_addr + i);
                if (entry_bytes != string_bytes) {
                    return false;
                }
            }
            // The hash function is not great, so I end up in this case a lot, so I take some risks.
            // This never caused a SIGSEGV even though it might :) If it does, fall back to the commented version below.
            // I will try to improve on the hash function
            if (remaining_bytes != 0) {
                long entry_bytes = unsafe.getLong(entry_string_addr + i);
                long string_bytes = unsafe.getLong(string_addr + i);
                // mask the bytes we care about
                long mask = (1L << (remaining_bytes * Byte.SIZE)) - 1;
                entry_bytes &= mask;
                string_bytes &= mask;
                return entry_bytes == string_bytes;
            }
            // for (; i < size_bytes; i++) {
            // byte entry_byte = unsafe.getByte(entry_string_addr + i);
            // byte string_byte = unsafe.getByte(string_addr + i);
            // if (entry_byte != string_byte) {
            // return false;
            // }
            // }
            return true;
        }

        public void insert(long hash, long string_addr, byte string_size, long final_number) {
            assert string_addr >>> 56 == 0 : String.format("Expected final 8 bytes to be 0, got %s", Long.toBinaryString(string_addr));

            long encoded_string_addr_and_length = string_addr | ((long) string_size << STRING_LENGTH_SHIFT);
            assert string_addr(encoded_string_addr_and_length) == string_addr : String.format("Expected string addr to be %s, got %s", Long.toHexString(string_addr),
                    Long.toHexString(string_addr(encoded_string_addr_and_length)));
            assert string_length(encoded_string_addr_and_length) == string_size
                    : String.format("Expected string length to be %s, got %s", string_size, string_length(encoded_string_addr_and_length));

            long map_entry = apply_mask(hash * ENTRY_SIZE);
            while (true) {
                int entry_count0 = unsafe.getInt(data, Unsafe.ARRAY_BYTE_BASE_OFFSET + map_entry + COUNT_OFFSET);
                if (entry_count0 == 0) {
                    // dump_insert(map_entry, hash, string_addr, string_size, final_number);
                    // Found an empty slot. Insert the entry here
                    unsafe.putLong(data, Unsafe.ARRAY_BYTE_BASE_OFFSET + map_entry + HASH_OFFSET, hash);
                    unsafe.putLong(data, Unsafe.ARRAY_BYTE_BASE_OFFSET + map_entry + ADDR_OFFSET, encoded_string_addr_and_length);
                    unsafe.putLong(data, Unsafe.ARRAY_BYTE_BASE_OFFSET + map_entry + COUNT_OFFSET, encode_count_max_min(1, (short) final_number, (short) final_number));
                    unsafe.putLong(data, Unsafe.ARRAY_BYTE_BASE_OFFSET + map_entry + SUM_OFFSET, final_number);

                    assert mask_count(encode_count_max_min(1, (short) final_number, (short) final_number)) == 1 : String.format("Expected count to be 1, got %s",
                            Integer.toBinaryString(mask_count(encode_count_max_min(1, (short) final_number, (short) final_number))));
                    assert mask_max(encode_count_max_min(1, (short) final_number, (short) final_number)) == (short) final_number
                            : String.format("Expected max to be %s, got %s", final_number,
                                    Integer.toBinaryString(mask_max(encode_count_max_min(1, (short) final_number, (short) final_number))));
                    assert mask_min(encode_count_max_min(1, (short) final_number, (short) final_number)) == (short) final_number
                            : String.format("Expected min to be %s, got %s", final_number,
                                    Integer.toBinaryString(mask_min(encode_count_max_min(1, (short) final_number, (short) final_number))));
                    return;
                }
                else {
                    // Check if strings match. If yes, update. Otherwise, look for the next available slot
                    long entry_string_addr_and_length = string_addr_and_length(map_entry);
                    long entry_str_size = string_length(entry_string_addr_and_length);

                    if (string_size != entry_str_size) {
                        // Strings are not the same size. Continue looking for the next slot
                        map_entry = apply_mask(map_entry + ENTRY_SIZE);
                        // reprobe_count++;
                    }
                    else {
                        long entry_string_addr = string_addr(entry_string_addr_and_length);
                        if (string_equals(string_addr, entry_string_addr, string_size)) {
                            // Strings are the same. Update the entry
                            long entry_count_max_min = count_max_min(map_entry);
                            int entry_count = mask_count(entry_count_max_min);
                            short entry_max = mask_max(entry_count_max_min);
                            short entry_min = mask_min(entry_count_max_min);

                            entry_count++;
                            assert (int) final_number == final_number : String.format("Expected final number to be an int, got %s", final_number);
                            entry_max = (short) Math.max(entry_max, (int) final_number);
                            entry_min = (short) Math.min(entry_min, (int) final_number);

                            long entry_sum = sum(map_entry);
                            entry_sum += final_number;

                            unsafe.putLong(data, Unsafe.ARRAY_BYTE_BASE_OFFSET + map_entry + COUNT_OFFSET, encode_count_max_min(entry_count, entry_max, entry_min));
                            unsafe.putLong(data, Unsafe.ARRAY_BYTE_BASE_OFFSET + map_entry + SUM_OFFSET, entry_sum);
                            return;
                        }
                        else {
                            // Strings are not the same. Continue looking for the next slot
                            map_entry = apply_mask(map_entry + ENTRY_SIZE);
                            // reprobe_count++;
                        }
                    }
                }
            }
        }

        private static long apply_mask(long hash) {
            return hash & (DATA_SIZE - 1);
        }

        public void update_res(TreeMap<String, Result> result_map) {
            // System.err.println("Reprobe count: " + reprobe_count);
            Result r = new Result();

            for (int i = 0; i < NUM_ENTRIES; i++) {
                long entry_addr_offset = (long) i * ENTRY_SIZE;
                long entry_count_max_min = count_max_min(entry_addr_offset);
                int entry_count = mask_count(entry_count_max_min);
                if (entry_count == 0) {
                    continue;
                }
                long entry_string_addr_and_length = string_addr_and_length(entry_addr_offset);
                long entry_string_addr = string_addr(entry_string_addr_and_length);
                long entry_string_length = string_length(entry_string_addr_and_length);

                // no reason to copy the byte array twice here but what can you do...
                byte[] bytes = new byte[(int) entry_string_length];
                unsafe.copyMemory(null, entry_string_addr, bytes, Unsafe.ARRAY_BYTE_BASE_OFFSET, entry_string_length);
                String s = new String(bytes, StandardCharsets.UTF_8);

                short entry_max = mask_max(entry_count_max_min);
                short entry_min = mask_min(entry_count_max_min);

                long entry_sum = sum(entry_addr_offset);

                Result ret = result_map.putIfAbsent(s, r);
                if (ret == null) {
                    r.count = entry_count;
                    r.max = entry_max;
                    r.min = entry_min;
                    r.sum = entry_sum;
                    r = new Result();
                }
                else {
                    ret.count += entry_count;
                    ret.max = (short) Math.max(ret.max, entry_max);
                    ret.min = (short) Math.min(ret.min, entry_min);
                    ret.sum += entry_sum;
                }
            }
        }

        public void dump_insert(long map_entry, long hash, long string_addr, byte string_size, long final_number) {
            System.out.println("START dump_insert");
            System.out.println("Inserting " + final_number + " with hash " + hash);
            System.out.println("Map entry: " + map_entry);
            System.out.println("String addr: " + string_addr + " with length " + string_size);
            dump(string_addr, string_addr + string_size);
            System.out.println("END dump_insert");
        }
    }

    static class Result {
        public int count;
        public short max;
        public short min;
        public long sum;

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }

        @Override
        public String toString() {
            return round(min / 10.) + "/" + round(sum / (double) (10 * count)) + "/" + round(max / 10.);
        }
    }

    private static void compute_slice(final long base_addr, final long slice_size, final long file_size, final int thread_index) {
        HashTable my_table;
        if (!SINGLE_CORE) {
            my_table = new HashTable();
            tables[thread_index] = my_table;
        }
        else {
            if (tables[0] == null) {
                tables[0] = new HashTable();
            }
            my_table = tables[0];
        }

        long cur_addr = base_addr + (long) thread_index * slice_size;
        // Lookup the next newline. If thread_index == 0 then start right away
        if (thread_index != 0) {
            while (unsafe.getByte(cur_addr) != '\n') {
                cur_addr++;
            }
            cur_addr++;
        }

        long end_addr = base_addr + (long) (thread_index + 1) * slice_size;
        if (thread_index == (AVAIL_CORES - 1)) {
            // Last thread. We need to read until the end of the file
            end_addr = base_addr + file_size;
        }
        else {
            // look ahead for the next newline
            while (unsafe.getByte(end_addr) != '\n') {
                end_addr++;
            }
            end_addr++;
        }

        // We now have a well-defined interval [cur_addr, end_addr) to work on
        long hash = -2346162244362633811L;
        byte string_size = 0;
        long string_addr = cur_addr;
        while (cur_addr < end_addr) {
            long value_mem = unsafe.getLong(cur_addr);
            int semicolon_byte_index = get_semicolon_index(value_mem);

            string_size += (byte) semicolon_byte_index;

            // dump(cur_addr, cur_addr + semicolon_byte_index);

            if (semicolon_byte_index != 8) {
                long value_mem_up_to_semicolon = value_mem & ((1L << (semicolon_byte_index * Byte.SIZE)) - 1);

                // We have a semicolon, so the hash is complete now. We can construct the number
                // and insert it into the hash table
                long start_num_addr = cur_addr + semicolon_byte_index + 1;

                // Always read the next 8 bytes for the number. It seems that this is faster than
                // checking if the whole number is in the current 8 bytes and only reading if it is not
                long number_mem_value = unsafe.getLong(start_num_addr);
                long number_len_bytes = get_newline_index(number_mem_value);

                long final_number = extract_number(number_mem_value, number_len_bytes);

                // 0.2421196 % reprobe rate
                hash = compute_hash(hash ^ value_mem_up_to_semicolon);

                // We have the final number now. We can insert it into the hash table
                my_table.insert(hash, string_addr, string_size, final_number);
                // Now we can move on to the next line
                hash = -2346162244362633811L;
                string_size = 0;
                cur_addr = start_num_addr + number_len_bytes + 1;
                string_addr = cur_addr;
            }
            else {
                // No semicolon in the 8 bytes read. Continue reading
                hash = hash ^ value_mem;
                cur_addr += 8;
            }
        }
        assert cur_addr == end_addr : String.format("Expected cur_addr to be %s, got %s", end_addr, cur_addr);
    }

    private static long extract_number(long number_mem_value, long number_len_bytes) {
        // Pray for GVN/CSE and Sea of Nodes moving the mess below in the proper places because
        // I don't want to spend the time to do it properly :)
        long number_mem_dot_index = get_dot_index(number_mem_value);

        int fractional_part = get_fractional_part(number_mem_value, number_len_bytes);
        int sign = get_sign(number_mem_value);
        int skip_sign = skip_sign(number_mem_value);

        long number_mem_value_no_sign = number_mem_value >>> (skip_sign << 3);
        // Two cases: either there's a single digit before the dot, or there's two
        // Start from the dot index and go backwards
        long new_number_mem_dot_index = number_mem_dot_index - skip_sign;
        long read_byte_mask = 0xFFL << ((new_number_mem_dot_index - 1) * Byte.SIZE);
        long ones = ((number_mem_value_no_sign & read_byte_mask) >>> ((new_number_mem_dot_index - 1) * Byte.SIZE)) - 0x30;
        // Should be 0 due to the multiplication if there's only one digit before the dot
        long tens = ((number_mem_value_no_sign & 0xFFL) - 0x30) * (new_number_mem_dot_index - 1);

        long final_number = (tens * 100 + ones * 10 + fractional_part) * sign;
        return final_number;
    }

    private static int get_fractional_part(long number_mem_value, long number_len_bytes) {
        return (int) ((number_mem_value >>> ((number_len_bytes - 1) * Byte.SIZE)) & 0xFF) - 0x30;
    }

    private static int skip_sign(long number_mem_value) {
        // return 1 if char is '-', 0 if it is not
        long diff = (number_mem_value & 0xFF) - 0x2D;
        long sign = (diff | -diff) >>> 63;
        return (int) ((sign - 1) * -1);
    }

    private static int get_sign(long number_mem_value) {
        // return 1 if char is not '-', -1 if it is
        long diff = (number_mem_value & 0xFF) - 0x2D;
        long sign = (diff | -diff) >>> 63;
        return (int) (-2 * sign + 1) * -1;
    }

    private static long compute_hash(long x) { // Hash burrowed from artsiomkorzun and slightly changed
        long h = x * -7046029254386353131L;
        long h1 = h ^ (h >>> 32);
        h = h ^ (h << 32);
        return h1 ^ h;
    }

    private static void dump(long startAddr, long endAddr) {
        byte[] bytes = new byte[(int) (endAddr - startAddr)];
        unsafe.copyMemory(null, startAddr, bytes, Unsafe.ARRAY_BYTE_BASE_OFFSET, bytes.length);
        String s = new String(bytes, StandardCharsets.UTF_8);
        System.out.println(s);
        // Dump the bytes to binary form
        for (byte b : bytes) {
            System.out.print(Integer.toBinaryString(b & 0xFF));
            System.out.print(" ");
        }
        System.out.println();
        // Dump the bytes to hex form
        for (byte b : bytes) {
            System.out.print(Integer.toHexString(b & 0xFF));
            System.out.print(" ");
        }
        System.out.println();
    }

    private static int get_byte_0_index(long value) {
        long res = (value - 0x0101010101010101L) & (~value & 0x8080808080808080L);
        res = Long.numberOfTrailingZeros(res) >> 3;
        return (int) res;
    }

    private static int get_dot_index(long value) {
        long temp = value ^ 0x2E2E2E2E2E2E2E2EL;
        return get_byte_0_index(temp);
    }

    private static int get_newline_index(long value) {
        long temp = value ^ 0x0A0A0A0A0A0A0A0AL;
        return get_byte_0_index(temp);
    }

    private static int get_semicolon_index(long value) {
        long temp = value ^ 0x3B3B3B3B3B3B3B3BL;
        return get_byte_0_index(temp);
    }

    private static final boolean SINGLE_CORE = false;

    public static void main(String[] args) throws IOException, InterruptedException {
        FileChannel file_channel = FileChannel.open(Paths.get(FILE), StandardOpenOption.READ);
        long file_size = file_channel.size();
        long base_addr = file_channel.map(FileChannel.MapMode.READ_ONLY, 0, file_size, Arena.global()).address();

        if (!SINGLE_CORE) {
            int num_threads = AVAIL_CORES;
            Thread[] threads = new Thread[num_threads];
            for (int i = 0; i < num_threads; i++) {
                int finalI = i;
                threads[i] = new Thread(() -> {
                    long slice_size = file_size / AVAIL_CORES;
                    compute_slice(base_addr, slice_size, file_size, finalI);
                });
                threads[i].start();
            }

            TreeMap<String, Result> result_map = new TreeMap<>();
            for (int i = 0; i < num_threads; i++) {
                threads[i].join();
                tables[i].update_res(result_map);
            }

            System.out.println(result_map);
        }
        else {
            for (int i = 0; i < AVAIL_CORES; i++) {
                int finalI = i;
                long slice_size = file_size / AVAIL_CORES;
                compute_slice(base_addr, slice_size, file_size, finalI);
            }

            TreeMap<String, Result> result_map = new TreeMap<>();
            tables[0].update_res(result_map);

            System.out.println(result_map);
        }
    }
}
