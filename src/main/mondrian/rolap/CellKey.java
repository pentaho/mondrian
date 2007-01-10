/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2001-2002 Kana Software, Inc.
// Copyright (C) 2001-2005 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 10 August, 2001
*/

package mondrian.rolap;

import mondrian.olap.Util;

/**
 * CellKey's are used as keys in Map's mapping from the key (a cell) to 
 * a value. As such, both the hashCode and equals method are called a lot.
 * There are particular implementations for the most likely cases where 
 * the number of axes is 1, 2 and 3 as well as a general implementation.
 *
 * @author jhyde
 * @since 10 August, 2001
 * @version $Id$
 */
public interface CellKey {
    public class Generator {
        public static CellKey create(int size) {
            switch (size) {
            case 0:
                return new CellKey.Zero();
            case 1:
                return new CellKey.One();
            case 2:
                return new CellKey.Two();
            case 3:
                return new CellKey.Three();
            default:
                return new CellKey.Many(size);
            }
        }
        public static CellKey create(int[] pos) {
            CellKey key = create(pos.length);
            return key.makeCopy(pos);
        }
        // used for testing only
        public static CellKey createMany(int size) {
            return new CellKey.Many(size);
        }
    }
    public abstract class Base implements CellKey {
        protected final int[] ordinals;
        protected Base(int size) {
            this.ordinals = new int[size];
        }
        protected Base(int[] ordinals) {
            this.ordinals = ordinals;
        }
        public final int size() {
            return this.ordinals.length;
        }
        public final void setOrdinals(int[] pos) {
            check(pos);
            for (int i = 0; i < size(); i++) {
                this.ordinals[i] = pos[i];
            }
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
        public abstract CellKey copy();

        public boolean equals(Object o) {
            if (o instanceof CellKey) {
                CellKey other = (CellKey) o;
                if (other.size() != size()) {
                    return false;
                }
                for (int i = 0; i < size(); i++) {
                    if (other.getAxis(i) != getAxis(i)) {
                        return false;
                    }
                }
                return true;
            } else {
                return false;
            }
        }

        public final void check(int[] pos) {
            if (pos.length != ordinals.length) {
                throw Util.newError(
                        "coordinates should have dimension " + ordinals.length);
            }
        }
        public final CellKey makeCopy(int[] pos) {
            return make((int[]) pos.clone());
        }
        public String toString() {
            StringBuffer buf = new StringBuffer();
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
    }
    public class Zero extends Base {
        private Zero() {
            super(0);
        }
        public CellKey copy() {
            // no need to make copy since there is no state
            return this;
        }
        public CellKey make(int[] pos) {
            check(pos);
            // no need to make copy since there is no state
            return this;
        }
    }
    public class One extends Base {
        private One() {
            super(1);
        }
        private One(int[] ordinals) {
            super(ordinals);
        }
        public CellKey copy() {
            // no need to make copy since there is no state
            return new CellKey.One((int[]) this.ordinals.clone());
        }
        public CellKey make(int[] pos) {
            check(pos);
            return new CellKey.One(pos);
        }
        public boolean equals(Object o) {
            // here we cheat, we know that all CellKey's will be the same size
            if (o instanceof CellKey.One) {
                CellKey.One other = (CellKey.One) o;
                return (other.ordinals[0] == this.ordinals[0]);
            } else {
                return super.equals(o);
            }
        }
        public int hashCode() {
            int h = 17 + ordinals[0];
            return h;
        }
    }
    public class Two extends Base {
        private Two() {
            super(2);
        }
        private Two(int[] ordinals) {
            super(ordinals);
        }
        public CellKey copy() {
            // no need to make copy since there is no state
            return new CellKey.Two((int[]) this.ordinals.clone());
        }
        public CellKey make(int[] pos) {
            check(pos);
            return new CellKey.Two(pos);
        }
        public boolean equals(Object o) {
            if (o instanceof CellKey.Two) {
                CellKey.Two other = (CellKey.Two) o;
                return (other.ordinals[0] == this.ordinals[0]) &&
                       (other.ordinals[1] == this.ordinals[1]);
            } else {
                return super.equals(o);
            }
        }
        public int hashCode() {
            return ((17 + ordinals[0]) * 37) + ordinals[1];
        }
    }
    public class Three extends Base {
        private Three() {
            super(3);
        }
        private Three(int[] ordinals) {
            super(ordinals);
        }
        public CellKey copy() {
            // no need to make copy since there is no state
            return new CellKey.Three((int[]) this.ordinals.clone());
        }
        public CellKey make(int[] pos) {
            check(pos);
            return new CellKey.Three(pos);
        }
        public boolean equals(Object o) {
            // here we cheat, we know that all CellKey's will be the same size
            if (o instanceof CellKey.Three) {
                CellKey.Three other = (CellKey.Three) o;
                return (other.ordinals[0] == this.ordinals[0]) &&
                       (other.ordinals[1] == this.ordinals[1]) &&
                       (other.ordinals[2] == this.ordinals[2]);
            } else {
                return super.equals(o);
            }
        }
        public int hashCode() {
            return (((17 + ordinals[0]) * 37) + ordinals[1]) * 37 + ordinals[2];
        }
    }
    public class Many extends Base {
        private Many(int size) {
            super(size);
        }
        private Many(int[] ordinals) {
            super(ordinals);
        }
        public CellKey copy() {
            // no need to make copy since there is no state
            return new CellKey.Many((int[]) this.ordinals.clone());
        }
        public CellKey make(int[] pos) {
            check(pos);
            return new CellKey.Many(pos);
        }
        public int hashCode() {
            int h = 17;
            for (int i = 0; i < ordinals.length; i++) {
                h = (h * 37) + ordinals[i];
            }
            return h;
        }
    }

    int size();
    int[] getOrdinals();
    
    /** 
     * This method make a copy of the int array parameter. 
     * Throws a RuntimeException if the int array size is not the
     * size of the CellKey.
     * 
     * @param pos 
     */
    void setOrdinals(int[] pos);

    int getAxis(int axis);

    /** 
     *  
     * Throws a RuntimeException if axis parameter is larger than the size 
     * of the CellKey.
     * 
     * @param axis 
     * @param value 
     */
    void setAxis(int axis, int value);
    
    /** 
     * This basically clones the current CellKey. 
     * 
     * @return 
     */
    CellKey copy();
    
    /** 
     * The int array parameter 'pos' is NOT copied. If you simply want to create
     * a new CellKey and you are sure that during its lifetime the int array
     * will not be modified, then use this method, otherwise use the makeCopy
     * method.
     * 
     * @param pos 
     * @return 
     */
    CellKey make(int[] pos);
    
    /** 
     * The int array parameter 'pos' is copied.
     * 
     * @param pos 
     * @return 
     */
    CellKey makeCopy(int[] pos);
}



// End CellKey.java
