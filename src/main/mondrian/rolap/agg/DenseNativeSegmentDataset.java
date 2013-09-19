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

import java.util.BitSet;

/**
 * Implementation of {@link DenseSegmentDataset} that stores
 * values of type {@code double}.
 *
 * @author jhyde
 */
abstract class DenseNativeSegmentDataset extends DenseSegmentDataset {
    protected final BitSet nullValues;

    /**
     * Creates a DenseNativeSegmentDataset.
     *
     * @param axes Segment axes, containing actual column values
     * @param nullValues A bit-set indicating whether values are null. Each
     *                   position in the bit-set corresponds to an offset in the
     *                   value array. If position is null, the corresponding
     *                   entry in the value array will also be 0.
     */
    DenseNativeSegmentDataset(
        SegmentAxis[] axes,
        BitSet nullValues)
    {
        super(axes);
        this.nullValues = nullValues;
    }

    public boolean isNull(CellKey key) {
        int offset = key.getOffset(axisMultipliers);
        return isNull(offset);
    }

    /**
     * Returns whether the value at the given offset is null.
     *
     * <p>The native value at this offset will also be 0. You only need to
     * call this method if the {@link #getInt getXxx} method has returned 0.
     *
     * @param offset Cell offset
     * @return Whether the cell at this offset is null
     */
    protected final boolean isNull(int offset) {
        return nullValues.get(offset);
    }
}

// End DenseNativeSegmentDataset.java
