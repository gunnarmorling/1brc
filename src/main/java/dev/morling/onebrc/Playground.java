package dev.morling.onebrc;

public class Playground {

    private record D(short min, short max, int count) {
    }

    public static D parse(long d1) {
        short currentMin = (short) (d1 & 0xFFFF);
        short currentMax = (short) ((d1 >> 16) & 0xFFFF);
        int currentCount = (int) ((d1 >> 32) & 0xFFFFFFFF);
        return new D(currentMin, currentMax, currentCount);
    }

    static long encode(short min, short max, int count) {
        // return min | (max << 16) | ((long) count << 32);
        return ((long) min & 0xFFFF) | (((long) max & 0xFFFF) << 16) | (((long) count & 0xFFFFFFFF) << 32);
    }

    public static void main(String[] args) {
        long d1 = ((Short.MAX_VALUE) | (Short.MIN_VALUE << 16)) & 0xFFFFFFFFL;
        long d2 = 0;

        D p = parse(d1);
        System.out.println(p);
        d1 = encode((short) 92, (short) 92, 1);
        p = parse(d1);
        System.out.println(p);
        d1 = encode((short) -68, (short) 92, 2);
        p = parse(d1);
        System.out.println(p);

        // System.out.println(encode(p.min(), p.max(), p.count()));

        // 33: [B] key=7449361081465794084, temp=92, min=32767, max=-32768, count=0, sum=0 10000000000000000111111111111111
        // 33: [A] key=7449361081465794084, temp=92, min=92, max=92, count=1, sum=92 100000000010111000000000001011100
        // 33: [B] key=7449361081465794084, temp=-68, min=92, max=92, count=1, sum=92 100000000010111000000000001011100
        // 33: [A] key=7449361081465794084, temp=-68, min=-68, max=92, count=2, sum=24 1111111111111111111111111111111111111111111111111111111110111100
    }
}
