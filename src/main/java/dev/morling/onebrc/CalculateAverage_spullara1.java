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

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class CalculateAverage_spullara1 {
  private static final String FILE = "./measurements.txt";

  static final class Result {
    int min;
    int max;
    int sum;
    int count;

    Result(int min, int max, int sum, int count) {
      this.min = min;
      this.max = max;
      this.sum = sum;
      this.count = count;
    }

    @Override
    public String toString() {
      return min / 10.0 +
              "/" + (sum / count / 10.0) +
              "/" + max / 10.0;
    }
  }

  public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
    String filename = args.length == 0 ? FILE : args[0];
    File file = new File(filename);

    record FileSegment(long start, long end) {
    }

    int numberOfSegments = Runtime.getRuntime().availableProcessors();
    long fileSize = file.length();
    long segmentSize = fileSize / numberOfSegments;

    long start = System.currentTimeMillis();

    List<FileSegment> segments = new ArrayList<>();
    try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r")) {
      for (int i = 0; i < numberOfSegments; i++) {
        long segStart = i * segmentSize;
        long segEnd = (i == numberOfSegments - 1) ? fileSize : segStart + segmentSize;

        if (i != 0) {
          randomAccessFile.seek(segStart);
          while (segStart < segEnd) {
            segStart++;
            if (randomAccessFile.read() == '\n') break;
          }
        }

        if (i != numberOfSegments - 1) {
          randomAccessFile.seek(segEnd);
          while (segEnd < fileSize) {
            segEnd++;
            if (randomAccessFile.read() == '\n') break;
          }
        }

        segments.add(new FileSegment(segStart, segEnd));
      }

      try (ExecutorService es = Executors.newFixedThreadPool(numberOfSegments)) {
        List<Future<Map<String, Result>>> futures = new ArrayList<>();
        AtomicInteger totalLines = new AtomicInteger();
        for (FileSegment segment : segments) {
          futures.add(es.submit(() -> {
            Map<String, Result> resultMap = new HashMap<>();
            BoundedRandomAccessFileInputStream brafis;
            try {
              brafis = new BoundedRandomAccessFileInputStream(new RandomAccessFile(file, "r"), segment.start, segment.end);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
            InputStreamReader isr = new InputStreamReader(new BufferedInputStream(brafis, 128 * 1024), StandardCharsets.UTF_8);
            LocklessBufferedReader br = new LocklessBufferedReader(isr, 128 * 1024);
            StringBuilder s = new StringBuilder();
            int lines = 0;
            while (br.readUntil(s, ';')) {
              String city = s.toString();
              s.setLength(0);
              br.readUntil(s, '\n');
              int temp = 0;
              int negative = 1;
              int length = s.length();
              for (int i = 0; i < length; i++) {
                char c = s.charAt(i);
                if (c == '-') {
                  negative = -1;
                  continue;
                }
                if (c == '.') {
                  continue;
                }
                if (c == '\r') {
                  break;
                }
                temp = 10 * temp + (c - '0');
              }
              temp *= negative;
              s.setLength(0);
              int finalTemp = temp;
              resultMap.compute(city, (k, v) -> {
                if (v == null) {
                  return new Result(finalTemp, finalTemp, finalTemp, 1);
                } else {
                  v.min = Math.min(v.min, finalTemp);
                  v.max = Math.max(v.max, finalTemp);
                  v.sum += finalTemp;
                  v.count += 1;
                  return v;
                }
              });
              lines++;
            }
            totalLines.addAndGet(lines);
            return resultMap;
          }));
        }

        Map<String, Result> resultMap = new TreeMap<>();
        for (Future<Map<String, Result>> future : futures) {
          Map<String, Result> partition = future.get();
          for (Map.Entry<String, Result> entry : partition.entrySet()) {
            resultMap.compute(entry.getKey(), (k, v) -> {
              if (v == null) return entry.getValue();
              Result value = entry.getValue();
              v.min = Math.min(v.min, value.min);
              v.max = Math.max(v.max, value.max);
              v.sum += value.sum;
              v.count += value.count;
              return v;
            });
          }
        }

        System.out.println("Time: " + (System.currentTimeMillis() - start) + "ms");

        System.out.println("Lines processed: " + totalLines);
        System.out.println(resultMap);
      }
    }

  }

  static class BoundedRandomAccessFileInputStream extends InputStream {
    private final RandomAccessFile randomAccessFile;
    private final long end;
    private long currentPosition;

    public BoundedRandomAccessFileInputStream(RandomAccessFile randomAccessFile, long start, long end) throws IOException {
      this.randomAccessFile = randomAccessFile;
      this.end = end;
      this.currentPosition = start;
      randomAccessFile.seek(start);
    }

    @Override
    public int read() throws IOException {
      // Stop reading if the end of the segment is reached
      if (currentPosition >= end) {
        return -1;
      }
      int byteRead = randomAccessFile.read();
      if (byteRead != -1) {
        currentPosition++;
      }
      return byteRead;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      if (currentPosition >= end) {
        return -1;
      }
      len = (int) Math.min(end - currentPosition, len);
      int read = randomAccessFile.read(b, off, len);
      currentPosition += read;
      return read;
    }

    @Override
    public int available() throws IOException {
      long remaining = end - currentPosition;
      if (remaining > Integer.MAX_VALUE) {
        return Integer.MAX_VALUE;
      }
      return (int) remaining;
    }

    @Override
    public void close() throws IOException {
      // Don't close the underlying file
    }
  }
}

class LocklessBufferedReader extends Reader {
  private Reader in;

  private char[] cb;
  private int nChars, nextChar;

  private static final int INVALIDATED = -2;
  private static final int UNMARKED = -1;
  private int markedChar = UNMARKED;
  private int readAheadLimit = 0; /* Valid only when markedChar > 0 */

  private static final int DEFAULT_CHAR_BUFFER_SIZE = 8192;
  private static final int DEFAULT_EXPECTED_LINE_LENGTH = 80;

  /**
   * Creates a buffering character-input stream that uses an input buffer of
   * the specified size.
   *
   * @param in A Reader
   * @param sz Input-buffer size
   * @throws IllegalArgumentException If {@code sz <= 0}
   */
  public LocklessBufferedReader(Reader in, int sz) {
    super(in);
    if (sz <= 0)
      throw new IllegalArgumentException("Buffer size <= 0");
    this.in = in;
    cb = new char[sz];
    nextChar = nChars = 0;
  }

  /**
   * Fills the input buffer, taking the mark into account if it is valid.
   */
  private void fill() throws IOException {
    int dst;
    if (markedChar <= UNMARKED) {
      /* No mark */
      dst = 0;
    } else {
      /* Marked */
      int delta = nextChar - markedChar;
      if (delta >= readAheadLimit) {
        /* Gone past read-ahead limit: Invalidate mark */
        markedChar = INVALIDATED;
        readAheadLimit = 0;
        dst = 0;
      } else {
        if (readAheadLimit <= cb.length) {
          /* Shuffle in the current buffer */
          System.arraycopy(cb, markedChar, cb, 0, delta);
          markedChar = 0;
          dst = delta;
        } else {
          /* Reallocate buffer to accommodate read-ahead limit */
          char[] ncb = new char[readAheadLimit];
          System.arraycopy(cb, markedChar, ncb, 0, delta);
          cb = ncb;
          markedChar = 0;
          dst = delta;
        }
        nextChar = nChars = delta;
      }
    }

    int n;
    do {
      n = in.read(cb, dst, cb.length - dst);
    } while (n == 0);
    if (n > 0) {
      nChars = dst + n;
      nextChar = dst;
    }
  }

  /**
   * Reads a single character.
   *
   * @return The character read, as an integer in the range
   * 0 to 65535 ({@code 0x00-0xffff}), or -1 if the
   * end of the stream has been reached
   * @throws IOException If an I/O error occurs
   */
  public int read() throws IOException {
    throw new IllegalArgumentException();
  }

  public int read(char[] cbuf, int off, int len) throws IOException {
    throw new IllegalArgumentException();
  }

  public boolean readUntil(StringBuilder s, char delimiter) {
    int startChar;

    bufferLoop:
    for (; ; ) {

      if (nextChar >= nChars) {
        try {
          fill();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
      if (nextChar >= nChars) { /* EOF */
        return s != null && s.length() > 0;
      }
      boolean eol = false;
      char c = 0;
      int i;

      charLoop:
      for (i = nextChar; i < nChars; i++) {
        c = cb[i];
        if (c == delimiter) {
          eol = true;
          break charLoop;
        }
      }

      startChar = nextChar;
      nextChar = i;

      if (eol) {
        String str;
        if (s == null) {
          str = new String(cb, startChar, i - startChar);
        } else {
          s.append(cb, startChar, i - startChar);
          str = s.toString();
        }
        nextChar++;
        return true;
      }

      if (s == null)
        s = new StringBuilder(DEFAULT_EXPECTED_LINE_LENGTH);
      s.append(cb, startChar, i - startChar);
    }
  }

  /**
   * {@inheritDoc}
   */
  public long skip(long n) throws IOException {
    throw new IllegalArgumentException();
  }

  public boolean ready() throws IOException {
    throw new IllegalArgumentException();
  }

  /**
   * Tells whether this stream supports the mark() operation, which it does.
   */
  public boolean markSupported() {
    return true;
  }

  /**
   * Marks the present position in the stream.  Subsequent calls to reset()
   * will attempt to reposition the stream to this point.
   *
   * @param readAheadLimit Limit on the number of characters that may be
   *                       read while still preserving the mark. An attempt
   *                       to reset the stream after reading characters
   *                       up to this limit or beyond may fail.
   *                       A limit value larger than the size of the input
   *                       buffer will cause a new buffer to be allocated
   *                       whose size is no smaller than limit.
   *                       Therefore large values should be used with care.
   * @throws IllegalArgumentException If {@code readAheadLimit < 0}
   * @throws IOException              If an I/O error occurs
   */
  public void mark(int readAheadLimit) throws IOException {
    throw new IllegalArgumentException();
  }

  /**
   * Resets the stream to the most recent mark.
   *
   * @throws IOException If the stream has never been marked,
   *                     or if the mark has been invalidated
   */
  public void reset() throws IOException {
    throw new IllegalArgumentException();
  }

  public void close() throws IOException {
    implClose();
  }

  private void implClose() throws IOException {
    if (in == null)
      return;
    try {
      in.close();
    } finally {
      in = null;
      cb = null;
    }
  }
}
