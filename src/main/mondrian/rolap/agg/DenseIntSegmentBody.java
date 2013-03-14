/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2011-2013 Pentaho and others
// All Rights Reserved.
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
    private final BitSet notNullZeroValues;

    /**
     * Creates a DenseIntSegmentBody.
     *
     * <p>Stores the given array of cell values and null indicators; caller must
     * not modify them afterwards.</p>
     *
     * @param notNullZeroValues a bitset indicating whether values of "0" should
     * be considered as true "0" values instead of nulls.  Each position in the
     * bitset corresponds to an offset in the value array
     * @param values Cell values
     * @param axes Axes
     */
    DenseIntSegmentBody(
        BitSet notNullZeroValues,
        int[] values,
        List<Pair<SortedSet<Comparable>, Boolean>> axes)
    {
        super(axes);
        this.values = values;
        this.notNullZeroValues = notNullZeroValues;
    }

    @Override
    public Object getValueArray() {
        return values;
    }

    @Override
    public BitSet getIndicators() {
        return notNullZeroValues;
    }

    protected int getSize() {
        return values.length;
    }

    protected Object getObject(int i) {
        int value = values[i];
        if (value == 0 && !notNullZeroValues.get(i)) {
            return null;
        }
        return value;
    }
}

// End DenseIntSegmentBody.java
