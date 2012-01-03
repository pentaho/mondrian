/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2011-2011 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap.agg;

import mondrian.util.Pair;

import java.util.*;

/**
 * Implementation of a segment body which stores the data inside
 * a dense primitive array of double precision numbers.
 *
 * @author LBoudreau
 * @version $Id$
 */
class DenseDoubleSegmentBody extends AbstractSegmentBody {
    private static final long serialVersionUID = 5775717165497921144L;

    private final double[] values;
    private final BitSet nullIndicators;

    /**
     * Creates a DenseDoubleSegmentBody.
     *
     * <p>Stores the given array of cell values and null indicators; caller must
     * not modify them afterwards.</p>
     *
     * @param nullIndicators Null indicators
     * @param values Cell values
     * @param axes Axes
     */
    DenseDoubleSegmentBody(
        BitSet nullIndicators,
        double[] values,
        List<Pair<SortedSet<Comparable<?>>, Boolean>> axes)
    {
        super(axes);
        this.values = values;
        this.nullIndicators = nullIndicators;
    }

    @Override
    public Object getValueArray() {
        return values;
    }

    @Override
    public BitSet getIndicators() {
        return nullIndicators;
    }

    @Override
    protected int getSize() {
        return values.length - nullIndicators.cardinality();
    }

    @Override
    protected Object getObject(int i) {
        double value = values[i];
        if (value == 0d && nullIndicators.get(i)) {
            return null;
        }
        return value;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("DenseDoubleSegmentBody(size=");
        sb.append(values.length);
        sb.append(", data=");
        sb.append(Arrays.toString(values));
        sb.append(", nullIndicators=").append(nullIndicators);
        sb.append(", axisValueSets=");
        sb.append(Arrays.toString(getAxisValueSets()));
        sb.append(", nullAxisFlags=");
        sb.append(Arrays.toString(getNullAxisFlags()));
        if (getAxisValueSets().length > 0) {
            if (getAxisValueSets()[0].iterator().hasNext()) {
                sb.append(", aVS[0]=");
                sb.append(getAxisValueSets()[0].getClass());
                sb.append(", aVS[0][0]=");
                sb.append(getAxisValueSets()[0].iterator().next().getClass());
            }
        }
        sb.append(")");
        return sb.toString();
    }
}

// End DenseDoubleSegmentBody.java
