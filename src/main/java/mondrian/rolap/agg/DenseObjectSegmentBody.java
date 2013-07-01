/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2011-2012 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap.agg;

import mondrian.util.Pair;

import java.util.*;

/**
 * Implementation of a segment body which stores the data inside
 * a dense array of Java objects.
 *
 * @author LBoudreau
 */
class DenseObjectSegmentBody extends AbstractSegmentBody {
    private static final long serialVersionUID = -3558427982849392173L;

    private final Object[] values;

    /**
     * Creates a DenseObjectSegmentBody.
     *
     * <p>Stores the given array of cell values; caller must not modify it
     * afterwards.</p>
     *
     * @param values Cell values
     * @param axes Axes
     */
    DenseObjectSegmentBody(
        Object[] values,
        List<Pair<SortedSet<Comparable>, Boolean>> axes)
    {
        super(axes);
        this.values = values;
    }

    @Override
    public Object getValueArray() {
        return values;
    }

    @Override
    protected Object getObject(int i) {
        return values[i];
    }

    @Override
    protected int getSize() {
        return values.length; // TODO: subtract number of nulls?
    }
}

// End DenseObjectSegmentBody.java
