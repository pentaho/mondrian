/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2002-2002 Kana Software, Inc.
// Copyright (C) 2002-2010 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 21 March, 2002
*/
package mondrian.rolap.agg;

import mondrian.rolap.CellKey;
import mondrian.rolap.SqlStatement;

/**
 * Implementation of {@link mondrian.rolap.agg.DenseSegmentDataset} that stores
 * values of type {@link Object}.
 *
 * <p>The storage requirements are as follows. Table requires 1 word per
 * cell.</p>
 *
 * @author jhyde
 * @since 21 March, 2002
 * @version $Id$
 */
class DenseObjectSegmentDataset extends DenseSegmentDataset {
    private final Object[] values; // length == m[0] * ... * m[axes.length-1]

    /**
     * Creates a DenseSegmentDataset.
     *
     * @param segment Segment
     * @param size Number of coordinates
     */
    DenseObjectSegmentDataset(Segment segment, int size) {
        super(segment);
        this.values = new Object[size];
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

    protected int size() {
        return values.length;
    }

    protected Object getObject(int i) {
        return values[i];
    }
}

// End DenseObjectSegmentDataset.java
