/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2010-2012 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap.agg;

import mondrian.rolap.CellKey;
import mondrian.rolap.SqlStatement;
import mondrian.spi.SegmentBody;
import mondrian.util.Pair;

import java.util.List;
import java.util.SortedSet;

/**
 * Implementation of {@link mondrian.rolap.agg.DenseSegmentDataset} that stores
 * values of type {@link Object}.
 *
 * <p>The storage requirements are as follows. Table requires 1 word per
 * cell.</p>
 *
 * @author jhyde
 * @since 21 March, 2002
 */
class DenseObjectSegmentDataset extends DenseSegmentDataset {
    final Object[] values; // length == m[0] * ... * m[axes.length-1]

    /**
     * Creates a DenseSegmentDataset.
     *
     * @param axes Segment axes, containing actual column values
     * @param size Number of coordinates
     */
    DenseObjectSegmentDataset(SegmentAxis[] axes, int size) {
        this(axes, new Object[size]);
    }

    /**
     * Creates and populates a DenseSegmentDataset. The data set is not copied.
     *
     * @param axes Axes
     * @param values Data set
     */
    DenseObjectSegmentDataset(SegmentAxis[] axes, Object[] values) {
        super(axes);
        this.values = values;
    }

    public Object getObject(CellKey key) {
        int offset = key.getOffset(axisMultipliers);
        return values[offset];
    }

    public boolean isNull(CellKey pos) {
        return getObject(pos) != null;
    }

    public boolean exists(CellKey pos) {
        return getObject(pos) != null;
    }

    public void populateFrom(int[] pos, SegmentDataset data, CellKey key) {
        values[getOffset(pos)] = data.getObject(key);
    }

    public void populateFrom(
        int[] pos, SegmentLoader.RowList rowList, int column)
    {
        int offset = getOffset(pos);
        values[offset] = rowList.getObject(column);
    }

    public SqlStatement.Type getType() {
        return SqlStatement.Type.OBJECT;
    }

    public void put(CellKey key, Object value) {
        int offset = key.getOffset(axisMultipliers);
        values[offset] = value;
    }

    protected Object getObject(int i) {
        return values[i];
    }

    protected int getSize() {
        return values.length;
    }

    public SegmentBody createSegmentBody(
        List<Pair<SortedSet<Comparable>, Boolean>> axes)
    {
        return new DenseObjectSegmentBody(
            values,
            axes);
    }
}

// End DenseObjectSegmentDataset.java
