/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 30 August, 2001
*/

package mondrian.rolap;

/** 
 * If you have a collection of immutable objects, each of which has a unique
 * positive number and you wish to do comparisons between subsets of those
 * objects testing for equality, then encoding the subsets as BitKeys is very
 * efficient. 
 *
 * There are two implements that target groups of objects with maximum number
 * less than 64 and less than 128; and there is one implements that is
 * general for any positive number.
 *
 * One caution: if the maximum number assigned to one of the
 * objects is large, then this representation might be sparse and therefore
 * not efficient.
 * 
 * @author <a>Richard M. Emberson</a>
 * @version 
 */
public interface BitKey {
    public abstract class Factory implements BitKey {
        // this is 64
        private final static int ADDRESS_BITS_PER_UNIT = 6;
        private final static int BITS_PER_UNIT = 1 << ADDRESS_BITS_PER_UNIT;
        private final static int BIT_INDEX_MASK = BITS_PER_UNIT - 1;

        public static BitKey makeBitKey(int size) {
            if (size < 0) {
                String msg = "Negative size \"" + size + "\" not allowed";
                throw new IllegalArgumentException(msg);
            }
            switch (unitIndex(size)) {
            case 0 :
                return new BitKey.Small();
            case 1 :
                return new BitKey.Mid128();
            default :
                return new BitKey.Big(size);
            }
        }
        
        /** 
         * Translate a number into an bit position, i.e., 
         *      0 becomes 0x000001
         *      1 becomes 0x000010
         *      2 becomes 0x000100 
         *      3 becomes 0x001000 
         * etc.
         * 
         * @param pos 
         * @return 
         */
        protected static long bit(int pos) {
            return (1L << (pos & BIT_INDEX_MASK));
        }
        
        /** 
         * Convert size into which 64 bit segment it is in, i.e.,
         *      0-63    becomes 0
         *      64-127  becomes 1
         *      128-191 becomes 2
         * etc.
         * 
         * @param size 
         * @return 
         */
        protected static int unitIndex(int size) {
            return (size >> ADDRESS_BITS_PER_UNIT);
        }

        protected Factory() {}
        protected Factory(Factory factory) {}

        public void setByPos(int pos, boolean value) {
            if (value) {
                setByPos(pos);
            } else {
                clearByPos(pos);
            }
        }
        public abstract void setByPos(int pos);
        public abstract boolean isSetByPos(int pos);
        public abstract void clearByPos(int pos);
        public abstract void clear();
        public abstract boolean isSuperSetOf(BitKey bitKey);

        public abstract BitKey copy();
    }
    
    /** 
     * Good for sizes less than 64.  
     * 
     */
    public class Small extends Factory {
        private long bits;

        private Small() {
            super();
        }
        private Small(Small small) {
            super(small);
            this.bits = small.bits;
        }
        public void setByPos(int pos) {
            bits |= bit(pos);
        }
        public boolean isSetByPos(int pos) {
            return ((bits & bit(pos)) != 0);
        }
        public void clearByPos(int pos) {
            bits &= ~bit(pos);
        }
        public void clear() {
            bits = 0;
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

        public String toString() {
            StringBuffer buf = new StringBuffer(64);
            buf.append("0x");
            for (int i = 63; i >= 0; i--) {
                buf.append((isSetByPos(i)) ? '1' : '0');
            }
            return buf.toString();
        }
        public BitKey copy() {
            return new Small(this);
        }
    }
    /** 
     * Good for sizes less than 128.  
     * 
     */
    public class Mid128 extends Factory {
        private long bits0;
        private long bits1;

        private Mid128() {
            super();
        }
        private Mid128(Mid128 mid) {
            super(mid);
            this.bits0 = mid.bits0;
            this.bits1 = mid.bits1;
        }
        public void setByPos(int pos) {
            if (pos < 64) {
                bits0 |= bit(pos);
            } else {
                bits1 |= bit(pos);
            }
        }
        public boolean isSetByPos(int pos) {
            if (pos < 64) {
                return ((bits0 & bit(pos)) != 0);
            } else {
                return ((bits1 & bit(pos)) != 0);
            }
        }
        public void clearByPos(int pos) {
            if (pos < 64) {
                bits0 &= ~bit(pos);
            } else {
                bits1 &= ~bit(pos);
            }
        }
        public void clear() {
            bits0 = 0;
            bits1 = 0;
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
            StringBuffer buf = new StringBuffer(64);
            buf.append("0x");
            for (int i = 127; i >= 0; i--) {
                buf.append((isSetByPos(i)) ? '1' : '0');
            }
            return buf.toString();
        }
        public BitKey copy() {
            return new Mid128(this);
        }
    }
    
    /** 
     * Bits of this (so to speak) are taken from java.util.BitSet. Since we
     * know the maximum size right from the start, we do not need the dynamics
     * resizing found in BitSet.
     */
    public class Big extends Factory {
        private long[] bits;

        private Big(int size) {
            super();
            this.bits = new long[(unitIndex(size-1) + 1)];
        }
        private Big(Big big) {
            super(big);
            this.bits = new long[big.bits.length];
            System.arraycopy(big.bits, 0, this.bits, 0, big.bits.length);
        }
        public void setByPos(int pos) {
            int unitIndex = unitIndex(pos);
            bits[unitIndex] |= bit(pos);
        }
        public boolean isSetByPos(int pos) {
            int unitIndex = unitIndex(pos);
            return ((bits[unitIndex] & bit(pos)) != 0);
        }
        public void clearByPos(int pos) {
            int unitIndex = unitIndex(pos);
            bits[unitIndex] &= ~bit(pos);
        }
        public void clear() {
            for (int i = 0; i < this.bits.length; i++) {
                this.bits[i] = 0;
            }
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
            StringBuffer buf = new StringBuffer(64);
            buf.append("0x");
            int start = bits.length*64 -1;
            for (int i = start; i >= 0; i--) {
                buf.append((isSetByPos(i)) ? '1' : '0');
            }
            return buf.toString();
        }
        public BitKey copy() {
            return new Big(this);
        }
    }
    void setByPos(int pos, boolean value);
    void setByPos(int pos);
    boolean isSetByPos(int pos);
    void clearByPos(int pos);
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

    public BitKey copy();
}
