/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2001-2002 Kana Software, Inc.
// Copyright (C) 2001-2007 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 30 August, 2001
*/

package mondrian.rolap;

import java.util.BitSet;
import java.util.Iterator;

/**
 * Represents a set of bits.
 *
 * <p>Unlike {@link java.util.BitSet}, the number of bits cannot be changed
 * after the BitKey is created. This allows us to optimize.
 *
 * <p>If you have a collection of immutable objects, each of which has a unique
 * positive number and you wish to do comparisons between subsets of those
 * objects testing for equality, then encoding the subsets as BitKeys is very
 * efficient.
 *
 * <p>There are two implementations that target groups of objects with maximum
 * number less than 64 and less than 128; and there is one implements that is
 * general for any positive number.
 *
 * <p>One caution: if the maximum number assigned to one of the
 * objects is large, then this representation might be sparse and therefore
 * not efficient.
 *
 * @author Richard M. Emberson
 * @version $Id$
 */
public interface BitKey extends Comparable<BitKey>, Iterable<Integer> {

    /**
     * Sets the bit at the specified index to the specified value.
     */
    void set(int bitIndex, boolean value);

    /**
     * Sets the bit at the specified index to <code>true</code>.
     */
    void set(int bitIndex);

    /**
     * Returns the value of the bit with the specified index. The value
     * is <code>true</code> if the bit with the index <code>bitIndex</code>
     * is currently set in this <code>BitKey</code>; otherwise, the result
     * is <code>false</code>.
     */
    boolean get(int bitIndex);

    /**
     * Sets the bit specified by the index to <code>false</code>.
     */
    void clear(int bitIndex);

    /**
     * Sets all of the bits in this BitKey to <code>false</code>.
     */
    void clear();

    /**
     * Is every bit set in the parameter <code>bitKey</code> also set in
     * <code>this</code>.
     * If one switches <code>this</code> with the parameter <code>bitKey</code>
     * one gets the equivalent of isSubSetOf.
     *
     * @param bitKey
     */
    boolean isSuperSetOf(BitKey bitKey);

    /**
     * Or the parameter <code>BitKey</code> with <code>this</code>.
     *
     * @param bitKey
     */
    BitKey or(BitKey bitKey);

    /**
     * Returns the boolean AND of this bitkey and the given bitkey.
     */
    BitKey and(BitKey bitKey);

    /**
     * Returns a <code>BitKey</code> containing all of the bits in this
     * <code>BitSet</code> whose corresponding
     * bit is NOT set in the specified <code>BitSet</code>.
     */
    BitKey andNot(BitKey bitKey);

    /**
     * Returns a copy of this BitKey.
     *
     * @return copy of BitKey
     */
    BitKey copy();

    /**
     * Returns an empty BitKey of the same type. This is the same
     * as calling {@link #copy} followed by {@link #clear()}.
     *
     * @return BitKey of same type
     */
    BitKey emptyCopy();

    /**
     * Returns true if this <code>BitKey</code> contains no bits that are set
     * to <code>true</code>.
     */
    boolean isEmpty();

    /**
     * Returns whether this BitKey has any bits in common with a given BitKey.
     */
    boolean intersects(BitKey bitKey);

    /**
     * Returns a {@link BitSet} with the same contents as this BitKey.
     */
    BitSet toBitSet();

    /**
     * An Iterator over the bit positions.
     * For example, if the BitKey had positions 3 and 4 set, then
     * the Iterator would return the values 3 and then 4. The bit
     * positions returned by the iterator are in the order, from
     * smallest to largest, as they are set in the BitKey.
     */
    Iterator<Integer> iterator();

    public abstract class Factory {

        /**
         * Creates a {@link BitKey} with a capacity for a given number of bits.
         * @param size Number of bits in key
         */
        public static BitKey makeBitKey(int size) {
            if (size < 0) {
                String msg = "Negative size \"" + size + "\" not allowed";
                throw new IllegalArgumentException(msg);
            }
            if (size < 64) {
                return new BitKey.Small();
            } else if (size < 128) {
                return new BitKey.Mid128();
            } else {
                return new BitKey.Big(size);
            }
/*
            switch (AbstractBitKey.chunkCount(size)) {
            case 0:
            case 1:
                return new BitKey.Small();
            case 2:
                return new BitKey.Mid128();
            default:
                return new BitKey.Big(size);
            }
*/
        }

        /**
         * Creates a {@link BitKey} with the same contents as a {@link BitSet}.
         */
        public static BitKey makeBitKey(BitSet bitSet) {
            BitKey bitKey = makeBitKey(bitSet.length());
            for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1)) {
                bitKey.set(i);
            }
            return bitKey;
        }
    }

    /**
     * Abstract implementation of {@link BitKey}.
     */
    abstract class AbstractBitKey implements BitKey {
        protected AbstractBitKey() {
        }

        // chunk is a long, which has 64 bits
        protected static final int ChunkBitCount = 6;
        protected static final int Mask = 63;

        /**
         * Creates a chunk containing a single bit.
         */
        protected static long bit(int pos) {
            return (1L << (pos & Mask));
        }

        /**
         * Returns which chunk a given bit falls into.
         * Bits 0 to 63 fall in chunk 0, bits 64 to 127 fall into chunk 1.
         */
        protected static int chunkPos(int size) {
            return (size >> ChunkBitCount);
        }

        /**
         * Returns the number of chunks required for a given number of bits.
         *
         * <p>0 bits requires 0 chunks; 1 - 64 bits requires 1 chunk; etc.
         */
        protected static int chunkCount(int size) {
            return (size + 63) >> ChunkBitCount;
        }

        public final void set(int pos, boolean value) {
            if (value) {
                set(pos);
            } else {
                clear(pos);
            }
        }

        protected static void copyFromByte(BitSet bitSet, int pos, byte x)
        {
            if (x == 0) {
                return;
            }
            if ((x & 0x01) != 0) {
                bitSet.set(pos, true);
            }
            ++pos;
            if ((x & 0x02) != 0) {
                bitSet.set(pos, true);
            }
            ++pos;
            if ((x & 0x04) != 0) {
                bitSet.set(pos, true);
            }
            ++pos;
            if ((x & 0x08) != 0) {
                bitSet.set(pos, true);
            }
            ++pos;
            if ((x & 0x10) != 0) {
                bitSet.set(pos, true);
            }
            ++pos;
            if ((x & 0x20) != 0) {
                bitSet.set(pos, true);
            }
            ++pos;
            if ((x & 0x40) != 0) {
                bitSet.set(pos, true);
            }
            ++pos;
            if ((x & 0x80) != 0) {
                bitSet.set(pos, true);
            }
        }

        protected static void copyFromLong(
                final BitSet bitSet,
                int pos,
                long x) {
            while (x != 0) {
                copyFromByte(bitSet, pos, (byte) (x & 0xff));
                x >>>= 8;
                pos += 8;
            }
        }

        protected IllegalArgumentException createException(BitKey bitKey) {
            final String msg = (bitKey == null)
                ? "Null BitKey"
                : "Bad BitKey type: " +bitKey.getClass().getName();
            return new IllegalArgumentException(msg);
        }
    }

    /**
     * Implementation of {@link BitKey} for bit counts less than 64.
     */
    public class Small extends AbstractBitKey {
        private long bits;

        private Small() {
        }
        private Small(long bits) {
            this.bits = bits;
        }
        public void set(int pos) {
            if (pos < 64) {
                bits |= bit(pos);
            } else {
                throw new IllegalArgumentException("pos " + pos + " exceeds capacity 64");
            }
        }
        public boolean get(int pos) {
            return pos < 64 && ((bits & bit(pos)) != 0);
        }
        public void clear(int pos) {
            bits &= ~bit(pos);
        }
        public void clear() {
            bits = 0;
        }
        private void or(long bits) {
            this.bits |= bits;
        }

        private void and(long bits) {
            this.bits &= bits;
        }

        public BitKey or(BitKey bitKey) {
            if (bitKey instanceof BitKey.Small) {
                final BitKey.Small other = (BitKey.Small) bitKey;
                final BitKey.Small bk = (BitKey.Small) copy();
                bk.or(other.bits);
                return bk;

            } else if (bitKey instanceof BitKey.Mid128) {
                final BitKey.Mid128 other = (BitKey.Mid128) bitKey;
                final BitKey.Mid128 bk = (BitKey.Mid128) other.copy();
                bk.or(this.bits, 0);
                return bk;

            } else if (bitKey instanceof BitKey.Big) {
                final BitKey.Big other = (BitKey.Big) bitKey;
                final BitKey.Big bk = (BitKey.Big) other.copy();
                bk.or(this.bits);
                return bk;
            }

            throw createException(bitKey);
        }

        public BitKey and(BitKey bitKey) {
            if (bitKey instanceof BitKey.Small) {
                final BitKey.Small other = (BitKey.Small) bitKey;
                final BitKey.Small bk = (BitKey.Small) copy();
                bk.and(other.bits);
                return bk;

            } else if (bitKey instanceof BitKey.Mid128) {
                final BitKey.Mid128 other = (BitKey.Mid128) bitKey;
                final BitKey.Small bk = (BitKey.Small) copy();
                bk.and(other.bits0);
                return bk;

            } else if (bitKey instanceof BitKey.Big) {
                final BitKey.Big other = (BitKey.Big) bitKey;
                final BitKey.Small bk = (BitKey.Small) copy();
                bk.and(other.bits[0]);
                return bk;
            }

            throw createException(bitKey);
        }

        public BitKey andNot(BitKey bitKey) {
            if (bitKey instanceof BitKey.Small) {
                final BitKey.Small other = (BitKey.Small) bitKey;
                final BitKey.Small bk = (BitKey.Small) copy();
                bk.andNot(other.bits);
                return bk;

            } else if (bitKey instanceof BitKey.Mid128) {
                final BitKey.Mid128 other = (BitKey.Mid128) bitKey;
                final BitKey.Small bk = (BitKey.Small) copy();
                bk.andNot(other.bits0);
                return bk;

            } else if (bitKey instanceof BitKey.Big) {
                final BitKey.Big other = (BitKey.Big) bitKey;
                final BitKey.Small bk = (BitKey.Small) copy();
                bk.andNot(other.bits[0]);
                return bk;
            }

            throw createException(bitKey);
        }

        private void andNot(long bits) {
            this.bits &= ~bits;
        }

        public boolean isSuperSetOf(BitKey bitKey) {
            if (bitKey instanceof BitKey.Small) {
                BitKey.Small other = (BitKey.Small) bitKey;
                return ((this.bits | other.bits) == this.bits);

            } else if (bitKey instanceof BitKey.Mid128) {
                BitKey.Mid128 other = (BitKey.Mid128) bitKey;
                return ((this.bits | other.bits0) == this.bits) &&
                    (other.bits1 == 0);

            } else if (bitKey instanceof BitKey.Big) {
                BitKey.Big other = (BitKey.Big) bitKey;
                if ((this.bits | other.bits[0]) != this.bits) {
                    return false;
                } else {
                    for (int i = 1; i < other.bits.length; i++) {
                        if (other.bits[i] != 0) {
                            return false;
                        }
                    }
                    return true;
                }
            }
            return false;
        }

        public boolean intersects(BitKey bitKey) {
            if (bitKey instanceof BitKey.Small) {
                BitKey.Small other = (BitKey.Small) bitKey;
                return (this.bits & other.bits) != 0;

            } else if (bitKey instanceof BitKey.Mid128) {
                BitKey.Mid128 other = (BitKey.Mid128) bitKey;
                return (this.bits & other.bits0) != 0;

            } else if (bitKey instanceof BitKey.Big) {
                BitKey.Big other = (BitKey.Big) bitKey;
                return (this.bits & other.bits[0]) != 0;
            }
            return false;
        }

        public BitSet toBitSet() {
            final BitSet bitSet = new BitSet(64);
            long x = bits;
            int pos = 0;
            while (x != 0) {
                copyFromByte(bitSet, pos, (byte) (x & 0xff));
                x >>>= 8;
                pos += 8;
            }
            return bitSet;
        }

        /**
         * To say that I am happy about this algorithm (or the variations
         * of the algorithm used for the Mid128 and Big BitKey implementations)
         * would be a stretch. It works but there has to be a more
         * elegant and faster one but this is the best I could come up
         * with in a couple of hours.
         *
         */
        public Iterator<Integer> iterator() {
            return new Iterator<Integer>() {
                int pos = -1;
                long bits = Small.this.bits;
                public boolean hasNext() {
                    if (bits == 0) {
                        return false;
                    }
                    // This is a special case
                    // Long.MIN_VALUE == -9223372036854775808
                    if (bits == Long.MIN_VALUE) {
                        pos = 63;
                        bits = 0;
                        return true;
                    }
                    long b = (bits&-bits);
                    if (b == 0) {
                        // should never happen
                        return false;
                    }
                    int delta = 0;
                    while (b >= 256) {
                        b = (b >> 8);
                        delta += 8;
                    }
                    int p = bitPositionTable[(int) b];
                    if (p >= 0) {
                        p += delta;
                    } else {
                        p = delta;
                    }
                    if (pos < 0) {
                        // first time
                        pos = p;
                    } else if (p == 0) {
                        pos++;
                    } else {
                        pos += (p+1);
                    }
                    bits = bits >>> (p+1);
                    return true;
                }
                public Integer next() {
                    return Integer.valueOf(pos);
                }
                public void remove() {
                    throw new UnsupportedOperationException("remove");
                }
            };
        }

        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o instanceof BitKey.Small) {
                BitKey.Small other = (BitKey.Small) o;
                return (this.bits == other.bits);

            } else if (o instanceof BitKey.Mid128) {
                BitKey.Mid128 other = (BitKey.Mid128) o;
                return (this.bits == other.bits0) && (other.bits1 == 0);

            } else if (o instanceof BitKey.Big) {
                BitKey.Big other = (BitKey.Big) o;
                if (this.bits != other.bits[0]) {
                    return false;
                } else {
                    for (int i = 1; i < other.bits.length; i++) {
                        if (other.bits[i] != 0) {
                            return false;
                        }
                    }
                    return true;
                }
            }
            return false;
        }
        public int hashCode() {
            return (int)(bits ^ (bits >>> 32));
        }

        // implement Comparable (in lazy, expensive fashion)
        public int compareTo(BitKey bitKey) {
            return toString().compareTo(bitKey.toString());
        }

        public String toString() {
            StringBuilder buf = new StringBuilder(64);
            buf.append("0x");
            for (int i = 63; i >= 0; i--) {
                buf.append((get(i)) ? '1' : '0');
            }
            return buf.toString();
        }
        public BitKey copy() {
            return new Small(this.bits);
        }
        public BitKey emptyCopy() {
            return new Small();
        }

        public boolean isEmpty() {
            return bits == 0;
        }
    }

    /**
     * Implementation of {@link BitKey} good for sizes less than 128.
     */
    public class Mid128 extends AbstractBitKey {
        private long bits0;
        private long bits1;

        private Mid128() {
        }
        private Mid128(Mid128 mid) {
            this.bits0 = mid.bits0;
            this.bits1 = mid.bits1;
        }

        public void set(int pos) {
            if (pos < 64) {
                bits0 |= bit(pos);
            } else if (pos < 128) {
                bits1 |= bit(pos);
            } else {
                throw new IllegalArgumentException("pos " + pos + " exceeds capacity 128");
            }
        }

        public boolean get(int pos) {
            if (pos < 64) {
                return (bits0 & bit(pos)) != 0;
            } else if (pos < 128) {
                return (bits1 & bit(pos)) != 0;
            } else {
                return false;
            }
        }

        public void clear(int pos) {
            if (pos < 64) {
                bits0 &= ~bit(pos);
            } else if (pos < 128) {
                bits1 &= ~bit(pos);
            } else {
                throw new IndexOutOfBoundsException(
                        "pos " + pos + " exceeds size " + 128);
            }
        }

        public void clear() {
            bits0 = 0;
            bits1 = 0;
        }

        private void or(long bits0, long bits1) {
            this.bits0 |= bits0;
            this.bits1 |= bits1;
        }

        private void and(long bits0, long bits1) {
            this.bits0 &= bits0;
            this.bits1 &= bits1;
        }

        public BitKey or(BitKey bitKey) {
            if (bitKey instanceof BitKey.Small) {
                final BitKey.Small other = (BitKey.Small) bitKey;
                final BitKey.Mid128 bk = (BitKey.Mid128) copy();
                bk.or(other.bits, 0);
                return bk;

            } else if (bitKey instanceof BitKey.Mid128) {
                final BitKey.Mid128 other = (BitKey.Mid128) bitKey;
                final BitKey.Mid128 bk = (BitKey.Mid128) copy();
                bk.or(other.bits0, other.bits1);
                return bk;

            } else if (bitKey instanceof BitKey.Big) {
                final BitKey.Big other = (BitKey.Big) bitKey;
                final BitKey.Big bk = (BitKey.Big) other.copy();
                bk.or(this.bits0, this.bits1);
                return bk;
            }

            throw createException(bitKey);
        }

        public BitKey and(BitKey bitKey) {
            if (bitKey instanceof BitKey.Small) {
                final BitKey.Small other = (BitKey.Small) bitKey;
                final BitKey.Mid128 bk = (BitKey.Mid128) copy();
                bk.and(other.bits, 0);
                return bk;

            } else if (bitKey instanceof BitKey.Mid128) {
                final BitKey.Mid128 other = (BitKey.Mid128) bitKey;
                final BitKey.Mid128 bk = (BitKey.Mid128) copy();
                bk.and(other.bits0, other.bits1);
                return bk;

            } else if (bitKey instanceof BitKey.Big) {
                final BitKey.Big other = (BitKey.Big) bitKey;
                final BitKey.Mid128 bk = (BitKey.Mid128) copy();
                bk.and(other.bits[0], other.bits[1]);
                return bk;
            }

            throw createException(bitKey);
        }

        public BitKey andNot(BitKey bitKey) {
            if (bitKey instanceof BitKey.Small) {
                final BitKey.Small other = (BitKey.Small) bitKey;
                final BitKey.Mid128 bk = (BitKey.Mid128) copy();
                bk.andNot(other.bits, 0);
                return bk;

            } else if (bitKey instanceof BitKey.Mid128) {
                final BitKey.Mid128 other = (BitKey.Mid128) bitKey;
                final BitKey.Mid128 bk = (BitKey.Mid128) copy();
                bk.andNot(other.bits0, other.bits1);
                return bk;

            } else if (bitKey instanceof BitKey.Big) {
                final BitKey.Big other = (BitKey.Big) bitKey;
                final BitKey.Mid128 bk = (BitKey.Mid128) copy();
                bk.andNot(other.bits[0], other.bits[1]);
                return bk;
            }

            throw createException(bitKey);
        }

        private void andNot(long bits0, long bits1) {
            this.bits0 &= ~bits0;
            this.bits1 &= ~bits1;
        }

        public boolean isSuperSetOf(BitKey bitKey) {
            if (bitKey instanceof BitKey.Small) {
                BitKey.Small other = (BitKey.Small) bitKey;
                return ((this.bits0 | other.bits) == this.bits0);

            } else if (bitKey instanceof BitKey.Mid128) {
                BitKey.Mid128 other = (BitKey.Mid128) bitKey;
                return ((this.bits0 | other.bits0) == this.bits0) &&
                    ((this.bits1 | other.bits1) == this.bits1);

            } else if (bitKey instanceof BitKey.Big) {
                BitKey.Big other = (BitKey.Big) bitKey;
                if ((this.bits0 | other.bits[0]) != this.bits0) {
                    return false;
                } else if ((this.bits1 | other.bits[1]) != this.bits1) {
                    return false;
                } else {
                    for (int i = 2; i < other.bits.length; i++) {
                        if (other.bits[i] != 0) {
                            return false;
                        }
                    }
                    return true;
                }
            }
            return false;
        }

        public boolean intersects(BitKey bitKey) {
            if (bitKey instanceof BitKey.Small) {
                BitKey.Small other = (BitKey.Small) bitKey;
                return (this.bits0 & other.bits) != 0;

            } else if (bitKey instanceof BitKey.Mid128) {
                BitKey.Mid128 other = (BitKey.Mid128) bitKey;
                return (this.bits0 & other.bits0) != 0 ||
                    (this.bits1 & other.bits1) != 0;

            } else if (bitKey instanceof BitKey.Big) {
                BitKey.Big other = (BitKey.Big) bitKey;
                if ((this.bits0 & other.bits[0]) != 0) {
                    return true;
                } else if ((this.bits1 & other.bits[1]) != 0) {
                    return true;
                } else {
                    return false;
                }
            }
            return false;
        }

        public BitSet toBitSet() {
            final BitSet bitSet = new BitSet(128);
            copyFromLong(bitSet, 0, bits0);
            copyFromLong(bitSet, 64, bits1);
            return bitSet;
        }
        public Iterator<Integer> iterator() {
            return new Iterator<Integer>() {
                long bits0 = Mid128.this.bits0;
                long bits1 = Mid128.this.bits1;
                int pos = -1;
                public boolean hasNext() {
                    if (bits0 != 0) {
                        if (bits0 == Long.MIN_VALUE) {
                            pos = 63;
                            bits0 = 0;
                            return true;
                        }
                        long b = (bits0&-bits0);
                        int delta = 0;
                        while (b >= 256) {
                            b = (b >> 8);
                            delta += 8;
                        }
                        int p = bitPositionTable[(int) b];
                        if (p >= 0) {
                            p += delta;
                        } else {
                            p = delta;
                        }
                        if (pos < 0) {
                            pos = p;
                        } else if (p == 0) {
                            pos++;
                        } else {
                            pos += (p+1);
                        }
                        bits0 = bits0 >>> (p+1);
                        return true;
                    } else {
                        if (pos < 63) {
                            pos = 63;
                        }
                        if (bits1 == Long.MIN_VALUE) {
                            pos = 127;
                            bits1 = 0;
                            return true;
                        }
                        long b = (bits1&-bits1);
                        if (b == 0) {
                            return false;
                        }
                        int delta = 0;
                        while (b >= 256) {
                            b = (b >> 8);
                            delta += 8;
                        }
                        int p = bitPositionTable[(int) b];
                        if (p >= 0) {
                            p += delta;
                        } else {
                            p = delta;
                        }
                        if (pos < 0) {
                            pos = p;
                        } else if (p == 63) {
                            pos++;
                        } else {
                            pos += (p+1);
                        }
                        bits1 = bits1 >>> (p+1);
                        return true;
                    }
                }
                public Integer next() {
                    return Integer.valueOf(pos);
                }
                public void remove() {
                    throw new UnsupportedOperationException("remove");
                }
            };
        }

        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o instanceof BitKey.Small) {
                BitKey.Small other = (BitKey.Small) o;
                return (this.bits0 == other.bits) && (this.bits1 == 0);

            } else if (o instanceof BitKey.Mid128) {
                BitKey.Mid128 other = (BitKey.Mid128) o;
                return (this.bits0 == other.bits0) &&
                    (this.bits1 == other.bits1);

            } else if (o instanceof BitKey.Big) {
                BitKey.Big other = (BitKey.Big) o;
                if (this.bits0 != other.bits[0]) {
                    return false;
                } else if (this.bits1 != other.bits[1]) {
                    return false;
                } else {
                    for (int i = 2; i < other.bits.length; i++) {
                        if (other.bits[i] != 0) {
                            return false;
                        }
                    }
                    return true;
                }
            }
            return false;
        }
        public int hashCode() {
            long h = 1234;
            h ^= bits0;
            h ^= bits1 * 2;
            return (int)((h >> 32) ^ h);
        }
        public String toString() {
            StringBuilder buf = new StringBuilder(64);
            buf.append("0x");
            for (int i = 127; i >= 0; i--) {
                buf.append((get(i)) ? '1' : '0');
            }
            return buf.toString();
        }
        public BitKey copy() {
            return new Mid128(this);
        }
        public BitKey emptyCopy() {
            return new Mid128();
        }

        public boolean isEmpty() {
            return bits0 == 0 &&
                    bits1 == 0;
        }

        // implement Comparable (in lazy, expensive fashion)
        public int compareTo(BitKey bitKey) {
            return toString().compareTo(bitKey.toString());
        }
    }

    /**
     * Implementation of {@link BitKey} with more than 64 bits. Similar to
     * {@link java.util.BitSet}, but does not require dynamic resizing.
     */
    public class Big extends AbstractBitKey {
        private long[] bits;

        private Big(int size) {
            bits = new long[chunkCount(size+1)];
        }
        private Big(Big big) {
            bits = (long[]) big.bits.clone();
        }
        private int size() {
            return bits.length;
        }
        public void set(int pos) {
            bits[chunkPos(pos)] |= bit(pos);
        }

        public boolean get(int pos) {
            return (bits[chunkPos(pos)] & bit(pos)) != 0;
        }
        public void clear(int pos) {
            bits[chunkPos(pos)] &= ~bit(pos);
        }
        public void clear() {
            for (int i = 0; i < bits.length; i++) {
                bits[i] = 0;
            }
        }
        private void or(long bits0) {
            this.bits[0] |= bits0;
        }
        private void or(long bits0, long bits1) {
            this.bits[0] |= bits0;
            this.bits[1] |= bits1;
        }
        private void or(long[] bits) {
            for (int i = 0; i < bits.length; i++) {
                this.bits[i] |= bits[i];
            }
        }
        private void and(long[] bits) {
            int length = Math.min(bits.length, this.bits.length);
            for (int i = 0; i < length; i++) {
                this.bits[i] &= bits[i];
            }
            for (int i = bits.length; i < this.bits.length; i++) {
                this.bits[i] = 0;
            }
        }

        public BitKey or(BitKey bitKey) {
            if (bitKey instanceof BitKey.Small) {
                final BitKey.Small other = (BitKey.Small) bitKey;
                final BitKey.Big bk = (BitKey.Big) copy();
                bk.or(other.bits);
                return bk;

            } else if (bitKey instanceof BitKey.Mid128) {
                final BitKey.Mid128 other = (BitKey.Mid128) bitKey;
                final BitKey.Big bk = (BitKey.Big) copy();
                bk.or(other.bits0, other.bits1);
                return bk;

            } else if (bitKey instanceof BitKey.Big) {
                final BitKey.Big other = (BitKey.Big) bitKey;
                if (other.size() > size()) {
                    final BitKey.Big bk = (BitKey.Big) other.copy();
                    bk.or(bits);
                    return bk;
                } else {
                    final BitKey.Big bk = (BitKey.Big) copy();
                    bk.or(other.bits);
                    return bk;
                }
            }

            throw createException(bitKey);
        }

        public BitKey and(BitKey bitKey) {
            if (bitKey instanceof BitKey.Small) {
                final BitKey.Small bk = (BitKey.Small) bitKey.copy();
                bk.and(bits[0]);
                return bk;

            } else if (bitKey instanceof BitKey.Mid128) {
                final BitKey.Mid128 bk = (BitKey.Mid128) bitKey.copy();
                bk.and(bits[0], bits[1]);
                return bk;

            } else if (bitKey instanceof BitKey.Big) {
                final BitKey.Big other = (BitKey.Big) bitKey;
                if (other.size() < size()) {
                    final BitKey.Big bk = (BitKey.Big) other.copy();
                    bk.and(bits);
                    return bk;
                } else {
                    final BitKey.Big bk = (BitKey.Big) copy();
                    bk.and(other.bits);
                    return bk;
                }
            }

            throw createException(bitKey);
        }

        public BitKey andNot(BitKey bitKey) {
            if (bitKey instanceof BitKey.Small) {
                final BitKey.Small other = (BitKey.Small) bitKey;
                final BitKey.Big bk = (BitKey.Big) copy();
                bk.andNot(other.bits);
                return bk;

            } else if (bitKey instanceof BitKey.Mid128) {
                final BitKey.Mid128 other = (Mid128) bitKey;
                final BitKey.Big bk = (BitKey.Big) copy();
                bk.andNot(other.bits0, other.bits1);
                return bk;

            } else if (bitKey instanceof BitKey.Big) {
                final BitKey.Big other = (BitKey.Big) bitKey;
                final BitKey.Big bk = (BitKey.Big) copy();
                bk.andNot(other.bits);
                return bk;
            }

            throw createException(bitKey);
        }

        private void andNot(long[] bits) {
            for (int i = 0; i < bits.length; i++) {
                this.bits[i] &= ~bits[i];

            }
        }

        private void andNot(long bits0, long bits1) {
            this.bits[0] &= ~bits0;
            this.bits[1] &= ~bits1;
        }

        private void andNot(long bits) {
            this.bits[0] &= ~bits;
        }

        public boolean isSuperSetOf(BitKey bitKey) {
            if (bitKey instanceof BitKey.Small) {
                BitKey.Small other = (BitKey.Small) bitKey;
                return ((this.bits[0] | other.bits) == this.bits[0]);

            } else if (bitKey instanceof BitKey.Mid128) {
                BitKey.Mid128 other = (BitKey.Mid128) bitKey;
                return ((this.bits[0] | other.bits0) == this.bits[0]) &&
                    ((this.bits[1] | other.bits1) == this.bits[1]);

            } else if (bitKey instanceof BitKey.Big) {
                BitKey.Big other = (BitKey.Big) bitKey;

                int len = Math.min(bits.length, other.bits.length);
                for (int i = 0; i < len; i++) {
                    if ((this.bits[i] | other.bits[i]) != this.bits[i]) {
                        return false;
                    }
                }
                if (other.bits.length > this.bits.length) {
                    for (int i = len; i < other.bits.length; i++) {
                        if (other.bits[i] != 0) {
                            return false;
                        }
                    }
                }
                return true;
            }
            return false;
        }

        public boolean intersects(BitKey bitKey) {
            if (bitKey instanceof BitKey.Small) {
                BitKey.Small other = (BitKey.Small) bitKey;
                return (this.bits[0] & other.bits) != 0;

            } else if (bitKey instanceof BitKey.Mid128) {
                BitKey.Mid128 other = (BitKey.Mid128) bitKey;
                return (this.bits[0] & other.bits0) != 0 ||
                    (this.bits[1] & other.bits1) != 0;

            } else if (bitKey instanceof BitKey.Big) {
                BitKey.Big other = (BitKey.Big) bitKey;

                int len = Math.min(bits.length, other.bits.length);
                for (int i = 0; i < len; i++) {
                    if ((this.bits[i] & other.bits[i]) != 0) {
                        return true;
                    }
                }
                return false;
            }
            return false;
        }

        public BitSet toBitSet() {
            final BitSet bitSet = new BitSet(64);
            int pos = 0;
            for (int i = 0; i < bits.length; i++) {
                copyFromLong(bitSet, pos, bits[i]);
                pos += 64;
            }
            return bitSet;
        }
        public Iterator<Integer> iterator() {
            return new Iterator<Integer>() {
                long[] bits = Big.this.bits.clone();
                int pos = -1;
                int index = 0;
                public boolean hasNext() {
                    if (index >= bits.length) {
                        return false;
                    }
                    if (pos < 0) {
                        while (bits[index] == 0) {
                            index++;
                            if (index >= bits.length) {
                                return false;
                            }
                        }
                        pos = (64 * index) - 1;
                    }
                    long bs = bits[index];
                    if (bs == 0) {
                        while (bits[index] == 0) {
                            index++;
                            if (index >= bits.length) {
                                return false;
                            }
                        }
                        pos = (64 * index) - 1;
                        bs = bits[index];
                    }
                    if (bs != 0) {
                        if (bs == Long.MIN_VALUE) {
                            pos = (64 * index) + 63;
                            bits[index] = 0;
                            return true;
                        }
                        long b = (bs&-bs);
                        int delta = 0;
                        while (b >= 256) {
                            b = (b >> 8);
                            delta += 8;
                        }
                        int p = bitPositionTable[(int) b];
                        if (p >= 0) {
                            p += delta;
                        } else {
                            p = delta;
                        }
                        if (pos < 0) {
                            pos = p;
                        } else if (p == 0) {
                            pos++;
                        } else {
                            pos += (p+1);
                        }
                        bits[index] = bits[index] >>> (p+1);
                        return true;
                    }
                    return false;
                }
                public Integer next() {
                    return Integer.valueOf(pos);
                }
                public void remove() {
                    throw new UnsupportedOperationException("remove");
                }
            };
        }
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o instanceof BitKey.Small) {
                BitKey.Small other = (BitKey.Small) o;
                if (this.bits[0] != other.bits) {
                    return false;
                } else {
                    for (int i = 1; i < this.bits.length; i++) {
                        if (this.bits[i] != 0) {
                            return false;
                        }
                    }
                    return true;
                }

            } else if (o instanceof BitKey.Mid128) {
                BitKey.Mid128 other = (BitKey.Mid128) o;
                if (this.bits[0] != other.bits0) {
                    return false;
                } else if (this.bits[1] != other.bits1) {
                    return false;
                } else {
                    for (int i = 2; i < this.bits.length; i++) {
                        if (this.bits[i] != 0) {
                            return false;
                        }
                    }
                    return true;
                }

            } else if (o instanceof BitKey.Big) {
                BitKey.Big other = (BitKey.Big) o;

                int len = Math.min(bits.length, other.bits.length);
                for (int i = 0; i < len; i++) {
                    if (this.bits[i] != other.bits[i]) {
                        return false;
                    }
                }
                if (this.bits.length > other.bits.length) {
                    for (int i = len; i < this.bits.length; i++) {
                        if (this.bits[i] != 0) {
                            return false;
                        }
                    }
                } else if (other.bits.length > this.bits.length) {
                    for (int i = len; i < other.bits.length; i++) {
                        if (other.bits[i] != 0) {
                            return false;
                        }
                    }
                }
                return true;
            }
            return false;
        }
        public int hashCode() {
            long h = 1234;
            for (int i = bits.length; --i >= 0; ) {
                h ^= bits[i] * (i + 1);
            }
            return (int)((h >> 32) ^ h);
        }
        public String toString() {
            StringBuilder buf = new StringBuilder(64);
            buf.append("0x");
            int start = bits.length*64 -1;
            for (int i = start; i >= 0; i--) {
                buf.append((get(i)) ? '1' : '0');
            }
            return buf.toString();
        }

        public BitKey copy() {
            return new Big(this);
        }

        public BitKey emptyCopy() {
            return new Big(bits.length << ChunkBitCount);
        }

        public boolean isEmpty() {
            for (long bit : bits) {
                if (bit != 0) {
                    return false;
                }
            }
            return true;
        }

        // implement Comparable (in lazy, expensive fashion)
        public int compareTo(BitKey bitKey) {
            return toString().compareTo(bitKey.toString());
        }
    }

    final static byte bitPositionTable[] = {
       -1, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
        4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
        5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
        4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
        6, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
        4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
        5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
        4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
        7, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
        4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
        5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
        4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
        6, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
        4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
        5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
        4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0};


}

// End BitKey.java
