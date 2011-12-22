/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2011 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap.agg;

import java.util.Map;
import java.util.SortedSet;

import mondrian.rolap.CellKey;

/**
 * Implementation of a segment body which stores the data of a
 * sparse segment data set into a dense array of java objects.
 * @author LBoudreau
 * @version $Id$
 */
class SparseSegmentBody extends AbstractSegmentBody {
    private static final long serialVersionUID = -6684830985364895836L;
    final CellKey[] keys;
    final Object[] data;

    SparseSegmentBody(
        Map<CellKey, Object> dataToSave,
        SortedSet<Comparable<?>>[] axisValueSets,
        boolean[] nullAxisFlags)
    {
        super(axisValueSets, nullAxisFlags);

        this.keys = new CellKey[dataToSave.size()];
        System.arraycopy(
            dataToSave.keySet().toArray(),
            0,
            this.keys,
            0,
            dataToSave.size());

        this.data = new Object[dataToSave.size()];
        System.arraycopy(
            dataToSave.values().toArray(),
            0,
            this.data,
            0,
            dataToSave.size());
    }
    public SegmentDataset createSegmentDataset(Segment segment) {
        SparseSegmentDataset ds =
            new SparseSegmentDataset(segment);
        for (int i = 0; i < keys.length; i++) {
            ds.put(this.keys[i], this.data[i]);
        }
        return ds;
    }
}
// End SparseSegmentBody.java
