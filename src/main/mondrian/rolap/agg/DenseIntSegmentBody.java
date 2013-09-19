/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.rolap.agg;

import mondrian.util.Pair;

import java.util.*;

/**
 * Implementation of a segment body which stores the data inside
 * a dense primitive array of integers.
 *
 * @author LBoudreau
 */
class DenseIntSegmentBody extends AbstractSegmentBody {
    private static final long serialVersionUID = 5391233622968115488L;

    private final int[] values;
    private final BitSet nullValues;

    /**
     * Creates a DenseIntSegmentBody.
     *
     * <p>Stores the given array of cell values and null indicators; caller must
     * not modify them afterwards.</p>
     *
     * @param nullValues A bit-set indicating whether values are null. Each
     *                   position in the bit-set corresponds to an offset in the
     *                   value array. If position is null, the corresponding
     *                   entry in the value array will also be 0.
     * @param values Cell values
     * @param axes Axes
     */
    DenseIntSegmentBody(
        BitSet nullValues,
        int[] values,
        List<Pair<SortedSet<Comparable>, Boolean>> axes)
    {
        super(axes);
        this.values = values;
        this.nullValues = nullValues;
    }

    @Override
    public Object getValueArray() {
        return values;
    }

    @Override
    public BitSet getNullValueIndicators() {
        return nullValues;
    }

    protected int getSize() {
        return values.length - nullValues.cardinality();
    }

    protected Object getObject(int i) {
        int value = values[i];
        if (value == 0 && nullValues.get(i)) {
            return null;
        }
        return value;
    }
}

// End DenseIntSegmentBody.java
