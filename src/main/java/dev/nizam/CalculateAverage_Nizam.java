package dev.nizam;

import java.nio.*;
import java.nio.channels.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

public final class CalculateAverage_Nizam {

    /* ========= CONSTANTS (compile-time) ========= */

    static final int MAX_STATIONS = 10_000;
    static final int NAME_BUF = 128;
    static final int BUF_SIZE = 8 * 1024 * 1024;
    static final int MAX_THREADS = 8;

    /* ========= STATS ========= */

    static final class Stats {
        long c, s;
        int min = 1000, max = -1000;
    }

    /* ========= STATION TABLE ========= */

    static final class Dict {
        final byte[][] k = new byte[MAX_STATIONS][];
        final int[] l = new int[MAX_STATIONS];
        int sz = 0;

        int id(byte[] b, int n) {
            for (int i = 0; i < sz; i++) {
                if (l[i] == n) {
                    byte[] kb = k[i];
                    int j = 0;
                    while (j < n && kb[j] == b[j])
                        j++;
                    if (j == n)
                        return i;
                }
            }
            k[sz] = Arrays.copyOf(b, n);
            l[sz] = n;
            return sz++;
        }
    }

    /* ========= MAIN ========= */

    public static void main(String[] a) throws Exception {

        long currentTime = System.currentTimeMillis();

        final Path p = Path.of("measurements.txt");
        final long fs = Files.size(p);

        final int th = Math.min(Runtime.getRuntime().availableProcessors(), MAX_THREADS);
        final long ck = fs / th;

        final ExecutorService es = Executors.newFixedThreadPool(th);
        final Future<Stats[]>[] f = new Future[th];
        final Dict[] d = new Dict[th];

        for (int i = 0; i < th; i++) {
            final long st = i * ck;
            final long en = (i == th - 1) ? fs : st + ck;
            d[i] = new Dict();
            final Dict di = d[i];
            f[i] = es.submit(() -> run(p, st, en, di));
        }

        es.shutdown();

        final TreeMap<String, Stats> out = new TreeMap<>();

        for (int i = 0; i < th; i++) {
            final Stats[] s = f[i].get();
            final Dict di = d[i];
            for (int j = 0; j < di.sz; j++) {
                final String k = new String(di.k[j], 0, di.l[j]);
                Stats t = out.get(k);
                if (t == null) out.put(k, t = new Stats());
                Stats o = s[j];
                t.c += o.c;
                t.s += o.s;
                if (o.min < t.min) t.min = o.min;
                if (o.max > t.max) t.max = o.max;
            }
        }

        print(out);

        long timeTakenInSecs = System.currentTimeMillis() - currentTime;

        System.out.println(STR."Time taken : \{timeTakenInSecs}");
    }

    /* ========= WORKER ========= */

    static Stats[] run(Path p, long st, long en, Dict d) throws Exception {

        final Stats[] s = new Stats[MAX_STATIONS];
        for (int i = 0; i < MAX_STATIONS; i++)
            s[i] = new Stats();

        final ByteBuffer buf = ByteBuffer.allocateDirect(BUF_SIZE);
        final byte[] nb = new byte[NAME_BUF];

        try (FileChannel ch = FileChannel.open(p, StandardOpenOption.READ)) {

            ch.position(st);
            if (st != 0)
                skip(ch, buf);

            int nl = 0, v = 0;
            boolean neg = false;

            while (ch.position() < en) {
                buf.clear();
                if (ch.read(buf) <= 0)
                    break;
                buf.flip();

                while (buf.hasRemaining()) {
                    byte b = buf.get();
                    if (b == '\n') {
                        if (neg)
                            v = -v;
                        int id = d.id(nb, nl);
                        Stats x = s[id];
                        x.c++;
                        x.s += v;
                        if (v < x.min)
                            x.min = v;
                        if (v > x.max)
                            x.max = v;
                        nl = 0;
                        v = 0;
                        neg = false;
                    }
                    else if (b == '-')
                        neg = true;
                    else if (b == ';') {
                    }
                    else if (b != '.') {
                        if (b >= '0')
                            v = v * 10 + (b - '0');
                        else
                            nb[nl++] = b;
                    }
                }
            }
        }
        return s;
    }

    static void skip(FileChannel c, ByteBuffer b) throws Exception {
        b.clear();
        for (;;) {
            if (c.read(b) <= 0)
                return;
            b.flip();
            while (b.hasRemaining())
                if (b.get() == '\n')
                    return;
            b.clear();
        }
    }

    /* ========= OUTPUT ========= */

    static void print(TreeMap<String, Stats> m) {
        StringBuilder sb = new StringBuilder("{");
        boolean f = true;
        for (var e : m.entrySet()) {
            if (!f)
                sb.append(", ");
            f = false;
            Stats s = e.getValue();
            sb.append(e.getKey()).append('=').append(s.min / 10.0).append('/').append((s.s / s.c) / 10.0).append('/').append(s.max / 10.0);
        }
        sb.append('}');
        System.out.println(sb);
    }
}
