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
package org.rschwietzke;

import java.util.Arrays;

/**
 * <p>This class is meant to replaces the old {@link CheaperCharBuffer} in all areas
 * where performance and memory-efficency is key. XMLString compatibility
 * remains in place in case one has used that in their own code.
 *
 * <p>This buffer is mutable and when you use it, make sure you work with
 * it responsibly. In many cases, we will reuse the buffer to avoid fresh
 * memory allocations, hence you have to pay attention to its usage pattern.
 * It is not meant to be a general String replacement.
 *
 * <p>This class avoids many of the standard runtime checks that will result
 * in a runtime or array exception anyway. Why check twice and raise the
 * same exception?
 *
 * @author René Schwietzke
 * @since 3.10.0
 */
public class CheaperCharBuffer implements CharSequence {
    // our data, can grow - that is not safe and has be altered from the original code
    // to allow speed
    public char[] data_;

    // the current size of the string data
    public int length_;

    // the current size of the string data
    private final int growBy_;

    // how much do we grow if needed, half a cache line
    public static final int CAPACITY_GROWTH = 64 / 2;

    // what is our start size?
    // a cache line is 64 byte mostly, the overhead is mostly 24 bytes
    // a char is two bytes, let's use one cache lines
    public static final int INITIAL_CAPACITY = (64 - 24) / 2;

    // static empty version; DON'T MODIFY IT
    public static final CheaperCharBuffer EMPTY = new CheaperCharBuffer(0);

    // the � character
    private static final char REPLACEMENT_CHARACTER = '\uFFFD';

    /**
     * Constructs an XMLCharBuffer with a default size.
     */
    public CheaperCharBuffer() {
        this.data_ = new char[INITIAL_CAPACITY];
        this.length_ = 0;
        this.growBy_ = CAPACITY_GROWTH;
    }

    /**
     * Constructs an XMLCharBuffer with a desired size.
     *
     * @param startSize the size of the buffer to start with
     */
    public CheaperCharBuffer(final int startSize) {
        this(startSize, CAPACITY_GROWTH);
    }

    /**
     * Constructs an XMLCharBuffer with a desired size.
     *
     * @param startSize the size of the buffer to start with
     * @param growBy by how much do we want to grow when needed
     */
    public CheaperCharBuffer(final int startSize, final int growBy) {
        this.data_ = new char[startSize];
        this.length_ = 0;
        this.growBy_ = Math.max(1, growBy);
    }

    /**
     * Constructs an XMLCharBuffer from another buffer. Copies the data
     * over. The new buffer capacity matches the length of the source.
     *
     * @param src the source buffer to copy from
     */
    public CheaperCharBuffer(final CheaperCharBuffer src) {
        this(src, 0);
    }

    /**
     * Constructs an XMLCharBuffer from another buffer. Copies the data
     * over. You can add more capacity on top of the source length. If
     * you specify 0, the capacity will match the src length.
     *
     * @param src the source buffer to copy from
     * @param addCapacity how much capacity to add to origin length
     */
    public CheaperCharBuffer(final CheaperCharBuffer src, final int addCapacity) {
        this.data_ = Arrays.copyOf(src.data_, src.length_ + Math.max(0, addCapacity));
        this.length_ = src.length();
        this.growBy_ = Math.max(1, CAPACITY_GROWTH);
    }

    /**
     * Constructs an XMLCharBuffer from a string. To avoid
     * too much allocation, we just take the string array as is and
     * don't allocate extra space in the first place.
     *
     * @param src the string to copy from
     */
    public CheaperCharBuffer(final String src) {
        this.data_ = src.toCharArray();
        this.length_ = src.length();
        this.growBy_ = CAPACITY_GROWTH;
    }

    /**
     * Constructs an XMLString structure preset with the specified values.
     * There will not be any room to grow, if you need that, construct an
     * empty one and append.
     *
     * <p>There are not range checks performed. Make sure your data is correct.
     *
     * @param ch     The character array, must not be null
     * @param offset The offset into the character array.
     * @param length The length of characters from the offset.
     */
    public CheaperCharBuffer(final char[] ch, final int offset, final int length) {
        // just as big as we need it
        this(length);
        append(ch, offset, length);
    }

    /**
     * Check capacity and grow if needed automatically
     *
     * @param minimumCapacity how much space do we need at least
     */
    private void ensureCapacity(final int minimumCapacity) {
        if (minimumCapacity > this.data_.length) {
            final int newSize = Math.max(minimumCapacity + this.growBy_, (this.data_.length << 1) + 2);
            this.data_ = Arrays.copyOf(this.data_, newSize);
        }
    }

    /**
     * Returns the current max capacity without growth. Does not
     * indicate how much capacity is already in use. Use {@link #length()}
     * for that.
     *
     * @return the current capacity, not taken any usage into account
     */
    public int capacity() {
        return this.data_.length;
    }

    /**
     * Appends a single character to the buffer.
     *
     * @param c the character to append
     * @return this instance
     */
    public CheaperCharBuffer append(final char c) {
        final int oldLength = this.length_++;

        // ensureCapacity is not inlined by the compiler, so put that here for the most
        // called method of all appends. Duplicate code, but for a reason.
        if (oldLength == this.data_.length) {
            final int newSize = Math.max(oldLength + this.growBy_, (this.data_.length << 1) + 2);
            this.data_ = Arrays.copyOf(this.data_, newSize);
        }

        this.data_[oldLength] = c;

        return this;
    }

    /**
     * Append a string to this buffer without copying the string first.
     *
     * @param src the string to append
     * @return this instance
     */
    public CheaperCharBuffer append(final String src) {
        final int start = this.length_;
        this.length_ = this.length_ + src.length();
        ensureCapacity(this.length_);

        // copy char by char because we don't get a copy for free
        // from a string yet, this might change when immutable arrays
        // make it into Java, but that will not be very soon
        for (int i = 0; i < src.length(); i++) {
            this.data_[start + i] = src.charAt(i);
        }

        return this;
    }

    /**
     * Add another buffer to this one.
     *
     * @param src the buffer to append
     * @return this instance
     */
    public CheaperCharBuffer append(final CheaperCharBuffer src) {
        final int start = this.length_;
        this.length_ = this.length_ + src.length();
        ensureCapacity(this.length_);

        System.arraycopy(src.data_, 0, this.data_, start, src.length_);

        return this;
    }

    /**
     * Add data from a char array to this buffer with the ability to specify
     * a range to copy from
     *
     * @param src the source char array
     * @param offset the pos to start to copy from
     * @param length the length of the data to copy
     *
     * @return this instance
     */
    public CheaperCharBuffer append(final char[] src, final int offset, final int length) {
        final int start = this.length_;
        this.length_ = start + length;

        ensureCapacity(this.length_);

        System.arraycopy(src, offset, this.data_, start, length);

        return this;
    }

    /**
     * Returns the current length
     *
     * @return the length of the charbuffer data
     */
    public int length() {
        return length_;
    }

    /**
     * Tell us how much the capacity grows if needed
     *
     * @return the value that determines how much we grow the backing
     *      array in case we have to
     */
    public int getGrowBy() {
        return this.growBy_;
    }

    /**
     * Resets the buffer to 0 length. It won't resize it to avoid memory
     * churn.
     *
     * @return this instance for fluid programming
     */
    public CheaperCharBuffer clear() {
        this.length_ = 0;

        return this;
    }

    /**
     * Resets the buffer to 0 length and sets the new data. This
     * is a little cheaper than clear().append(c) depending on
     * the where  and the inlining decisions.
     *
     * @param c the char to set
     * @return this instance for fluid programming
     */
    public CheaperCharBuffer clearAndAppend(final char c) {
        this.length_ = 0;

        if (this.data_.length > 0) {
            this.data_[this.length_] = c;
            this.length_++;
        }
        else {
            // the rare case when we don't have any buffer at hand
            append(c);
        }

        return this;
    }

    /**
     * Does this buffer end with this string? If we check for
     * the empty string, we get true. If we would support JDK 11, we could
     * use Arrays.mismatch and be way faster.
     *
     * @param s the string to check the end against
     * @return true of the end matches the buffer, false otherwise
     */
    public boolean endsWith(final String s) {
        // length does not match, cannot be the end
        if (this.length_ < s.length()) {
            return false;
        }

        // check the string by each char, avoids a copy of the string
        final int start = this.length_ - s.length();

        // change this to Arrays.mismatch when going JDK 11 or higher
        for (int i = 0; i < s.length(); i++) {
            if (this.data_[i + start] != s.charAt(i)) {
                return false;
            }
        }

        return true;
    }

    /**
     * Reduces the buffer to the content between start and end marker when
     * only whitespaces are found before the startMarker as well as after the end marker.
     * If both strings overlap due to identical characters such as "foo" and "oof"
     * and the buffer is " foof ", we don't do anything.
     *
     * <p>If a marker is empty, it behaves like {@link java.lang.String#trim()} on that side.
     *
     * @param startMarker the start string to find, must not be null
     * @param endMarker the end string to find, must not be null
     * @return this instance
     *
     * @deprecated Use the new method {@link #trimToContent(String, String)} instead.
     */
    public CheaperCharBuffer reduceToContent(final String startMarker, final String endMarker) {
        return trimToContent(startMarker, endMarker);
    }

    /**
     * Reduces the buffer to the content between start and end marker when
     * only whitespaces are found before the startMarker as well as after the end marker.
     * If both strings overlap due to identical characters such as "foo" and "oof"
     * and the buffer is " foof ", we don't do anything.
     *
     * <p>If a marker is empty, it behaves like {@link java.lang.String#trim()} on that side.
     *
     * @param startMarker the start string to find, must not be null
     * @param endMarker the end string to find, must not be null
     * @return this instance
     */
    public CheaperCharBuffer trimToContent(final String startMarker, final String endMarker) {
        // if both are longer or same length than content, don't do anything
        final int markerLength = startMarker.length() + endMarker.length();
        if (markerLength >= this.length_) {
            return this;
        }

        // run over starting whitespaces
        int sPos = 0;
        for (; sPos < this.length_ - markerLength; sPos++) {
            if (!Character.isWhitespace(this.data_[sPos])) {
                break;
            }
        }

        // run over ending whitespaces
        int ePos = this.length_ - 1;
        for (; ePos > sPos - markerLength; ePos--) {
            if (!Character.isWhitespace(this.data_[ePos])) {
                break;
            }
        }

        // if we have less content than marker length, give up
        // this also helps when markers overlap such as
        // <!-- and --> and the string is " <!---> "
        if (ePos - sPos + 1 < markerLength) {
            return this;
        }

        // check the start
        for (int i = 0; i < startMarker.length(); i++) {
            if (startMarker.charAt(i) != this.data_[i + sPos]) {
                // no start match, stop and don't do anything
                return this;
            }
        }

        // check the end, ePos is when the first good char
        // occurred
        final int endStartCheckPos = ePos - endMarker.length() + 1;
        for (int i = 0; i < endMarker.length(); i++) {
            if (endMarker.charAt(i) != this.data_[endStartCheckPos + i]) {
                // no start match, stop and don't do anything
                return this;
            }
        }

        // shift left and cut length
        final int newLength = ePos - sPos + 1 - markerLength;
        System.arraycopy(this.data_,
                sPos + startMarker.length(),
                this.data_,
                0, newLength);
        this.length_ = newLength;

        return this;
    }

    /**
     * Check if we have only whitespaces
     *
     * @return true if we have only whitespace, false otherwise
     */
    public boolean isWhitespace() {
        for (int i = 0; i < this.length_; i++) {
            if (!Character.isWhitespace(this.data_[i])) {
                return false;
            }
        }
        return true;
    }

    /**
     * Trims the string similar to {@link java.lang.String#trim()}
     *
     * @return a string with removed whitespace at the beginning and the end
     */
    public CheaperCharBuffer trim() {
        // clean the end first, because it is cheap
        return trimTrailing().trimLeading();
    }

    /**
     * Removes all whitespace before the first non-whitespace char.
     * If all are whitespaces, we get an empty buffer
     *
     * @return this instance
     */
    public CheaperCharBuffer trimLeading() {
        // run over starting whitespace
        int sPos = 0;
        for (; sPos < this.length_; sPos++) {
            if (!Character.isWhitespace(this.data_[sPos])) {
                break;
            }
        }

        if (sPos == 0) {
            // nothing to do
            return this;
        }
        else if (sPos == this.length_) {
            // only whitespace
            this.length_ = 0;
            return this;
        }

        // shift left
        final int newLength = this.length_ - sPos;
        System.arraycopy(this.data_,
                sPos,
                this.data_,
                0, newLength);
        this.length_ = newLength;

        return this;
    }

    /**
     * Removes all whitespace at the end.
     * If all are whitespace, we get an empty buffer
     *
     * @return this instance
     *
     * @deprecated Use {@link #trimTrailing()} instead.
     */
    public CheaperCharBuffer trimWhitespaceAtEnd() {
        return trimTrailing();
    }

    /**
     * Removes all whitespace at the end.
     * If all are whitespace, we get an empty buffer
     *
     * @return this instance
     */
    public CheaperCharBuffer trimTrailing() {
        // run over ending whitespaces
        int ePos = this.length_ - 1;
        for (; ePos >= 0; ePos--) {
            if (!Character.isWhitespace(this.data_[ePos])) {
                break;
            }
        }

        this.length_ = ePos + 1;

        return this;
    }

    /**
     * Shortens the buffer by that many positions. If the count is
     * larger than the length, we get just an empty buffer. If you pass in negative
     * values, we are failing, likely often silently. It is all about performance and
     * not a general all-purpose API.
     *
     * @param count a positive number, no runtime checks, if count is larger than
     *      length, we get length = 0
     * @return this instance
     */
    public CheaperCharBuffer shortenBy(final int count) {
        final int newLength = this.length_ - count;
        this.length_ = newLength < 0 ? 0 : newLength;

        return this;
    }

    /**
     * Get the characters as char array, this will be a copy!
     *
     * @return a copy of the underlying char darta
     */
    public char[] getChars() {
        return Arrays.copyOf(this.data_, this.length_);
    }

    /**
     * Returns a string representation of this buffer. This will be a copy
     * operation. If the buffer is emoty, we get a constant empty String back
     * to avoid any overhead.
     *
     * @return a string of the content of this buffer
     */
    @Override
    public String toString() {
        if (this.length_ > 0) {
            return new String(this.data_, 0, this.length_);
        }
        else {
            return "";
        }
    }

    /**
     * Returns the char a the given position. Will complain if
     * we try to read outside the range. We do a range check here
     * because we might not notice when we are within the buffer
     * but outside the current length.
     *
     * @param index the position to read from
     * @return the char at the position
     * @throws IndexOutOfBoundsException
     *      in case one tries to read outside of valid buffer range
     */
    @Override
    public char charAt(final int index) {
        if (index > this.length_ - 1 || index < 0) {
            throw new IndexOutOfBoundsException(
                    "Tried to read outside of the valid buffer data");
        }

        return this.data_[index];
    }

    /**
     * Returns the char at the given position. No checks are
     * performed. It is up to the caller to make sure we
     * read correctly. Reading outside of the array will
     * cause an {@link IndexOutOfBoundsException} but using an
     * incorrect position in the array (such as beyond length)
     * might stay unnoticed! This is a performance method,
     * use at your own risk.
     *
     * @param index the position to read from
     * @return the char at the position
     */
    public char unsafeCharAt(final int index) {
        return this.data_[index];
    }

    /**
     * Returns a content copy of this buffer
     *
     * @return a copy of this buffer, the capacity might differ
     */
    @Override
    public CheaperCharBuffer clone() {
        return new CheaperCharBuffer(this);
    }

    /**
     * Returns a <code>CharSequence</code> that is a subsequence of this sequence.
     * The subsequence starts with the <code>char</code> value at the specified index and
     * ends with the <code>char</code> value at index <tt>end - 1</tt>.  The length
     * (in <code>char</code>s) of the
     * returned sequence is <tt>end - start</tt>, so if <tt>start == end</tt>
     * then an empty sequence is returned.
     *
     * @param   start   the start index, inclusive
     * @param   end     the end index, exclusive
     *
     * @return  the specified subsequence
     *
     * @throws  IndexOutOfBoundsException
     *          if <tt>start</tt> or <tt>end</tt> are negative,
     *          if <tt>end</tt> is greater than <tt>length()</tt>,
     *          or if <tt>start</tt> is greater than <tt>end</tt>
     *
     * @return a charsequence of this buffer
     */
    @Override
    public CharSequence subSequence(final int start, final int end) {
        if (start < 0) {
            throw new StringIndexOutOfBoundsException(start);
        }
        if (end > this.length_) {
            throw new StringIndexOutOfBoundsException(end);
        }

        final int l = end - start;
        if (l < 0) {
            throw new StringIndexOutOfBoundsException(l);
        }

        return new String(this.data_, start, l);
    }

    /**
     * Two buffers are identical when the length and
     * the content of the backing array (only for the
     * data in view) are identical.
     *
     * @param o the object to compare with
     * @return true if length and array content match, false otherwise
     */
    @Override
    public boolean equals(final Object o) {
        if (o instanceof CharSequence) {
            final CharSequence ob = (CharSequence) o;

            if (ob.length() != this.length_) {
                return false;
            }

            // ok, in JDK 11 or up, we could use an
            // Arrays.mismatch, but we cannot do that
            // due to JDK 8 compatibility
            for (int i = 0; i < this.length_; i++) {
                if (ob.charAt(i) != this.data_[i]) {
                    return false;
                }
            }

            // length and content match, be happy
            return true;
        }

        return false;
    }

    /**
     * We don't cache the hashcode because we mutate often. Don't use this in
     * hashmaps as key. But you can use that to look up in a hashmap against
     * a string using the CharSequence interface.
     *
     * @return the hashcode, similar to what a normal string would deliver
     */
    @Override
    public int hashCode() {
        int h = 0;

        for (int i = 0; i < this.length_; i++) {
            h = ((h << 5) - h) + this.data_[i];
        }

        return h;
    }

    /**
     * Append a character to an XMLCharBuffer. The character is an int value, and
     * can either be a single UTF-16 character or a supplementary character
     * represented by two UTF-16 code points.
     *
     * @param value The character value.
     * @return this instance for fluid programming
     *
     * @throws IllegalArgumentException if the specified
     *          {@code codePoint} is not a valid Unicode code point.
     */
    public CheaperCharBuffer appendCodePoint(final int value) {
        if (value <= Character.MAX_VALUE) {
            return this.append((char) value);
        }
        else {
            try {
                final char[] chars = Character.toChars(value);
                return this.append(chars, 0, chars.length);
            }
            catch (final IllegalArgumentException e) {
                // when value is not valid as UTF-16
                this.append(REPLACEMENT_CHARACTER);
                throw e;
            }
        }
    }
}
