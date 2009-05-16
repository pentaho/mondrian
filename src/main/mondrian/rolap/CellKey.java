/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2001-2002 Kana Software, Inc.
// Copyright (C) 2001-2007 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 10 August, 2001
*/

package mondrian.rolap;

import java.util.Arrays;

/**
 * A <code>CellKey<code> is used as a key in maps which access cells by their
 * position.
 *
 * <p>CellKey is also used within
 * {@link mondrian.rolap.agg.SparseSegmentDataset} to store values within
 * aggregations.
 *
 * <p>It is important that CellKey is memory-efficient, and that the
 * {@link Object#hashCode} and {@link Object#equals} methods are extremely
 * efficient. There are particular implementations for the
 * most likely cases where the number of axes is 1, 2 and 3 as well as a general
 * implementation.
 *
 * <p>To create a key, call the
 * {@link mondrian.rolap.CellKey.Generator#newCellKey(int[])} method.
 *
 * @author jhyde
 * @since 10 August, 2001
 * @version $Id$
 */
public interface CellKey {
    /**
     * Returns the number of axes.
     *
     * @return number of axes
     */
    int size();

    /**
     * Returns the axis keys as an array.
     *
     * <p>Note: caller should treat the array as immutable. If the contents of
     * the array are modified, behavior is unspecified.
     *
     * @return Array of axis keys
     */
    int[] getOrdinals();

    /**
     * This method make a copy of the int array parameter.
     * Throws a RuntimeException if the int array size is not the
     * size of the CellKey.
     *
     * @param pos Array of axis keys
     */
    void setOrdinals(int[] pos);

    /**
     * Returns the <code>axis</code>th axis value.
     *
     * @param axis Axis ordinal
     * @return Value of the <code>axis</code>th axis
     * @throws ArrayIndexOutOfBoundsException if axis is out of range
     */
    int getAxis(int axis);

    /**
     * Sets a given axis.
     *
     * @param axis Axis ordinal
     * @param value Value
     * @throws RuntimeException if axis parameter is larger than {@link #size()}
     */
    void setAxis(int axis, int value);

    /**
     * Returns a mutable copy of this CellKey.
     *
     * @return Mutable copy
     */
    CellKey copy();

    public class Generator {
        /**
         * Creates a CellKey with a given number of axes.
         *
         * @param size Number of axes
         * @return new CellKey
         */
        public static CellKey newCellKey(int size) {
            switch (size) {
            case 0:
                return Zero.INSTANCE;
            case 1:
                return new One(0);
            case 2:
                return new Two(0, 0);
            case 3:
                return new Three(0, 0, 0);
            default:
                return new Many(new int[size]);
            }
        }

        /**
         * Creates a CellKey populated with the given coordinates.
         *
         * @param pos Coordinate array
         * @return CellKey
         */
        public static CellKey newCellKey(int[] pos) {
            switch (pos.length) {
            case 0:
                return Zero.INSTANCE;
            case 1:
                return new One(pos[0]);
            case 2:
                return new Two(pos[0], pos[1]);
            case 3:
                return new Three(pos[0], pos[1], pos[2]);
            default:
                return new Many(pos.clone());
            }
        }

        /**
         * Creates a CellKey based on a reference to the given coordinate
         * array. Whenever the contents of the coordinate array change, the
         * CellKey will also.
         *
         * @param pos Coordinate array
         * @return CellKey
         */
        public static CellKey newRefCellKey(int[] pos) {
            // don't clone pos!
            return new Many(pos);
        }

        /**
         * Creates a CellKey implemented by an array to hold the coordinates,
         * regardless of the size. This is used for testing only.
         *
         * @param size Number of coordinates
         * @return CallKey
         */
        static CellKey newManyCellKey(int size) {
            return new Many(new int[size]);
        }
    }

    public class Zero implements CellKey {
        private static final int[] EMPTY_INT_ARRAY = new int[0];
        public static final Zero INSTANCE = new Zero();

        /**
         * Use singleton {@link #INSTANCE}.
         */
        private Zero() {
        }

        public Zero copy() {
            // no need to make copy since there is no state
            return this;
        }

        public boolean equals(Object o) {
            if (o instanceof Zero) {
                return true;
            } else if (o instanceof Many) {
                Many many = (Many) o;
                return many.ordinals.length == 0;
            } else {
                return false;
            }
        }

        public int hashCode() {
            return 11;
        }

        public int size() {
            return 0;
        }

        public int[] getOrdinals() {
            return EMPTY_INT_ARRAY;
        }

        public void setOrdinals(int[] pos) {
            if (pos.length != 0) {
                throw new IllegalArgumentException();
            }
        }

        public int getAxis(int axis) {
            throw new ArrayIndexOutOfBoundsException(axis);
        }

        public void setAxis(int axis, int value) {
            throw new ArrayIndexOutOfBoundsException(axis);
        }
    }

    public class One implements CellKey {
        private int ordinal0;

        /**
         * Creates a One.
         *
         * @param ordinal0 Ordinate #0
         */
        private One(int ordinal0) {
            this.ordinal0 = ordinal0;
        }

        public int size() {
            return 1;
        }

        public int[] getOrdinals() {
            return new int[] {ordinal0};
        }

        public void setOrdinals(int[] pos) {
            if (pos.length != 1) {
                throw new IllegalArgumentException();
            }
            ordinal0 = pos[0];
        }

        public int getAxis(int axis) {
            switch (axis) {
            case 0:
                return ordinal0;
            default:
                throw new ArrayIndexOutOfBoundsException(axis);
            }
        }

        public void setAxis(int axis, int value) {
            switch (axis) {
            case 0:
                ordinal0 = value;
                break;
            default:
                throw new ArrayIndexOutOfBoundsException(axis);
            }
        }

        public One copy() {
            return new One(ordinal0);
        }

        public boolean equals(Object o) {
            // here we cheat, we know that all CellKey's will be the same size
            if (o instanceof One) {
                One other = (One) o;
                return (this.ordinal0 == other.ordinal0);
            } else if (o instanceof Many) {
                Many many = (Many) o;
                return many.ordinals.length == 1 &&
                    many.ordinals[0] == this.ordinal0;
            } else {
                return false;
            }
        }

        public String toString() {
            return "(" + ordinal0 + ")";
        }

        public int hashCode() {
            return 17 + ordinal0;
        }
    }

    public class Two implements CellKey {
        private int ordinal0;
        private int ordinal1;

        /**
         * Creates a Two.
         *
         * @param ordinal0 Ordinate #0
         * @param ordinal1 Ordinate #1
         */
        private Two(int ordinal0, int ordinal1) {
            this.ordinal0 = ordinal0;
            this.ordinal1 = ordinal1;
        }

        public String toString() {
            return "(" + ordinal0 + ", " + ordinal1 + ")";
        }

        public Two copy() {
            return new Two(ordinal0, ordinal1);
        }

        public boolean equals(Object o) {
            if (o instanceof Two) {
                Two other = (Two) o;
                return (other.ordinal0 == this.ordinal0) &&
                       (other.ordinal1 == this.ordinal1);
            } else if (o instanceof Many) {
                Many many = (Many) o;
                return many.ordinals.length == 2 &&
                    many.ordinals[0] == this.ordinal0 &&
                    many.ordinals[1] == this.ordinal1;
            } else {
                return false;
            }
        }

        public int hashCode() {
            int h0 = 17 + ordinal0;
            return h0 * 37 + ordinal1;
        }

        public int size() {
            return 2;
        }

        public int[] getOrdinals() {
            return new int[] {ordinal0, ordinal1};
        }

        public void setOrdinals(int[] pos) {
            if (pos.length != 2) {
                throw new IllegalArgumentException();
            }
            ordinal0 = pos[0];
            ordinal1 = pos[1];
        }

        public int getAxis(int axis) {
            switch (axis) {
            case 0:
                return ordinal0;
            case 1:
                return ordinal1;
            default:
                throw new ArrayIndexOutOfBoundsException(axis);
            }
        }

        public void setAxis(int axis, int value) {
            switch (axis) {
            case 0:
                ordinal0 = value;
                break;
            case 1:
                ordinal1 = value;
                break;
            default:
                throw new ArrayIndexOutOfBoundsException(axis);
            }
        }
    }

    class Three implements CellKey {
        private int ordinal0;
        private int ordinal1;
        private int ordinal2;

        /**
         * Creates a Three.
         *
         * @param ordinal0 Ordinate #0
         * @param ordinal1 Ordinate #1
         * @param ordinal2 Ordinate #2
         */
        private Three(int ordinal0, int ordinal1, int ordinal2) {
            this.ordinal0 = ordinal0;
            this.ordinal1 = ordinal1;
            this.ordinal2 = ordinal2;
        }

        public String toString() {
            return "(" + ordinal0 + ", " + ordinal1 + ", " + ordinal2 + ")";
        }

        public Three copy() {
            return new Three(ordinal0, ordinal1, ordinal2);
        }

        public boolean equals(Object o) {
            // here we cheat, we know that all CellKey's will be the same size
            if (o instanceof Three) {
                Three other = (Three) o;
                return (other.ordinal0 == this.ordinal0) &&
                       (other.ordinal1 == this.ordinal1) &&
                       (other.ordinal2 == this.ordinal2);
            } else if (o instanceof Many) {
                Many many = (Many) o;
                return many.ordinals.length == 3 &&
                    many.ordinals[0] == this.ordinal0 &&
                    many.ordinals[1] == this.ordinal1 &&
                    many.ordinals[2] == this.ordinal2;
            } else {
                return false;
            }
        }

        public int hashCode() {
            int h0 = 17 + ordinal0;
            int h1 = h0 * 37 + ordinal1;
            return h1 * 37 + ordinal2;
        }

        public int getAxis(int axis) {
            switch (axis) {
            case 0:
                return ordinal0;
            case 1:
                return ordinal1;
            case 2:
                return ordinal2;
            default:
                throw new ArrayIndexOutOfBoundsException(axis);
            }
        }

        public void setAxis(int axis, int value) {
            switch (axis) {
            case 0:
                ordinal0 = value;
                break;
            case 1:
                ordinal1 = value;
                break;
            case 2:
                ordinal2 = value;
                break;
            default:
                throw new ArrayIndexOutOfBoundsException(axis);
            }
        }

        public int size() {
            return 3;
        }

        public int[] getOrdinals() {
            return new int[] {ordinal0, ordinal1, ordinal2};
        }

        public void setOrdinals(int[] pos) {
            if (pos.length != 3) {
                throw new IllegalArgumentException();
            }
            ordinal0 = pos[0];
            ordinal1 = pos[1];
            ordinal2 = pos[2];
        }
    }

    public class Many implements CellKey {
        private final int[] ordinals;

        /**
         * Creates a Many.
         * @param ordinals Ordinates
         */
        protected Many(int[] ordinals) {
            this.ordinals = ordinals;
        }

        public final int size() {
            return this.ordinals.length;
        }

        public final void setOrdinals(int[] pos) {
            if (ordinals.length != pos.length) {
                throw new IllegalArgumentException();
            }
            System.arraycopy(pos, 0, this.ordinals, 0, ordinals.length);
        }
        public final int[] getOrdinals() {
            return this.ordinals;
        }

        public void setAxis(int axis, int value) {
            this.ordinals[axis] = value;
        }

        public int getAxis(int axis) {
            return this.ordinals[axis];
        }

        public String toString() {
            StringBuilder buf = new StringBuilder();
            buf.append('(');
            for (int i = 0; i < ordinals.length; i++) {
                if (i > 0) {
                    buf.append(',');
                }
                buf.append(ordinals[i]);
            }
            buf.append(')');
            return buf.toString();
        }

        public Many copy() {
            return new Many(this.ordinals.clone());
        }

        public int hashCode() {
            int h = 17;
            for (int ordinal : ordinals) {
                h = (h * 37) + ordinal;
            }
            return h;
        }

        public boolean equals(Object o) {
            if (o instanceof Many) {
                Many that = (Many) o;
                return Arrays.equals(this.ordinals, that.ordinals);
            } else {
                // Use symmetric logic in One, Two, Three.
                return o instanceof CellKey && o.equals(this);
            }
        }
    }
}

// End CellKey.java
