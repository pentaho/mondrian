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

import java.util.BitSet;
import java.util.SortedSet;

/**
 * Implementation of a segment body which stores the data inside
 * a dense primitive array of integers.
 * @author LBoudreau
 * @version $Id$
 */
class DenseIntSegmentBody extends AbstractSegmentBody {
    private static final long serialVersionUID = 5391233622968115488L;
    final int[] data;
    private final int size;
    private final BitSet nullIndicators;
    DenseIntSegmentBody(
            BitSet nullIndicators,
            int[] dataToSave,
            int size,
            SortedSet<Comparable<?>>[] axisValueSets,
            boolean[] nullAxisFlags)
    {
        super(axisValueSets, nullAxisFlags);
        this.size = size;
        this.data = new int[size];
        System.arraycopy(dataToSave, 0, data, 0, size);
        this.nullIndicators = new BitSet(nullIndicators.length());
        this.nullIndicators.or(nullIndicators);
    }
    public SegmentDataset createSegmentDataset(Segment segment) {
        DenseIntSegmentDataset ds =
            new DenseIntSegmentDataset(segment, this.size);
        System.arraycopy(data, 0, ds.values, 0, this.size);
        ds.nullIndicators.clear();
        ds.nullIndicators.or(nullIndicators);
        return ds;
    }
}
// End DenseIntSegmentBody.java