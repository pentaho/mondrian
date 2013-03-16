/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2002-2005 Julian Hyde
// Copyright (C) 2005-2013 Pentaho and others
// All Rights Reserved.
//
// jhyde, 21 March, 2002
*/
package mondrian.rolap.agg;

import mondrian.rolap.CellKey;

import java.util.Iterator;
import java.util.Map;

/**
 * A <code>DenseSegmentDataset</code> is a means of storing segment values
 * which is suitable when most of the combinations of keys have a value
 * present.
 *
 * <p>The storage requirements are as follows. Table requires 1 word per
 * cell.</p>
 *
 * @author jhyde
 * @since 21 March, 2002
 */
abstract class DenseSegmentDataset implements SegmentDataset {
    private final SegmentAxis[] axes;
    protected final int[] axisMultipliers;

    /**
     * Creates a DenseSegmentDataset.
     *
     * @param axes Segment axes, containing actual column values
     */
    DenseSegmentDataset(SegmentAxis[] axes) {
        this.axes = axes;
        this.axisMultipliers = computeAxisMultipliers();
    }

    private int[] computeAxisMultipliers() {
        final int[] axisMultipliers = new int[axes.length];
        int multiplier = 1;
        for (int i = axes.length - 1; i >= 0; --i) {
            final SegmentAxis axis = axes[i];
            axisMultipliers[i] = multiplier;
            multiplier *= axis.getKeys().length;
        }
        return axisMultipliers;
    }

    public final double getBytes() {
        // assume a slot, key, and value are each 4 bytes
        return getSize() * 12;
    }

    public Iterator<Map.Entry<CellKey, Object>> iterator() {
        return new DenseSegmentDatasetIterator();
    }

    protected abstract Object getObject(int i);

    protected final int getOffset(int[] keys) {
        return CellKey.Generator.getOffset(keys, axisMultipliers);
    }

    protected final int getOffset(Object[] keys) {
        int offset = 0;
outer:
        for (int i = 0; i < keys.length; i++) {
            SegmentAxis axis = axes[i];
            Object[] ks = axis.getKeys();
            final int axisLength = ks.length;
            offset *= axisLength;
            Object value = keys[i];
            for (int j = 0; j < axisLength; j++) {
                if (ks[j].equals(value)) {
                    offset += j;
                    continue outer;
                }
            }
            return -1; // not found
        }
        return offset;
    }

    public Object getObject(CellKey pos) {
        throw new UnsupportedOperationException();
    }

    public int getInt(CellKey pos) {
        throw new UnsupportedOperationException();
    }

    public double getDouble(CellKey pos) {
        throw new UnsupportedOperationException();
    }

    protected abstract int getSize();

    /**
     * Iterator over a DenseSegmentDataset.
     *
     * <p>This is a 'cheap' implementation
     * which doesn't allocate a new Entry every step: it just returns itself.
     * The Entry must therefore be used immediately, before calling
     * {@link #next()} again.
     */
    private class DenseSegmentDatasetIterator implements
        Iterator<Map.Entry<CellKey, Object>>,
        Map.Entry<CellKey, Object>
    {
        private final int last = getSize() - 1;
        private int i = -1;
        private final int[] ordinals;

        DenseSegmentDatasetIterator() {
            ordinals = new int[axes.length];
            ordinals[ordinals.length - 1] = -1;
        }

        public boolean hasNext() {
            return i < last;
        }

        public Map.Entry<CellKey, Object> next() {
            ++i;
            int k = ordinals.length - 1;
            while (k >= 0) {
                if (ordinals[k] < axes[k].getKeys().length - 1) {
                    ++ordinals[k];
                    break;
                } else {
                    ordinals[k] = 0;
                    --k;
                }
            }
            return this;
        }

        // implement Iterator
        public void remove() {
            throw new UnsupportedOperationException();
        }

        // implement Entry
        public CellKey getKey() {
            return CellKey.Generator.newCellKey(ordinals);
        }

        // implement Entry
        public Object getValue() {
            return getObject(i);
        }

        // implement Entry
        public Object setValue(Object value) {
            throw new UnsupportedOperationException();
        }
    }
}

// End DenseSegmentDataset.java
