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

import java.util.SortedSet;

/**
 * Implementation of a segment body which stores the data inside
 * a dense array of Java objects.
 * @author LBoudreau
 * @version $Id$
 */
class DenseObjectSegmentBody extends AbstractSegmentBody {
    private static final long serialVersionUID = -3558427982849392173L;
    final Object[] data;
    private final int size;
    DenseObjectSegmentBody(
            Object[] dataToSave,
            int size,
            SortedSet<Comparable<?>>[] axisValueSets,
            boolean[] nullAxisFlags)
    {
        super(axisValueSets, nullAxisFlags);
        this.size = size;
        this.data = new Object[size];
        System.arraycopy(dataToSave, 0, data, 0, size);
    }
    public SegmentDataset createSegmentDataset(Segment segment) {
        DenseObjectSegmentDataset ds =
            new DenseObjectSegmentDataset(segment, this.size);
        System.arraycopy(data, 0, ds.values, 0, this.size);
        return ds;
    }
}
//End DenseObjectSegmentBody.java