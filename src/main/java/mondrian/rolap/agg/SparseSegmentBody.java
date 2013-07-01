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

import mondrian.rolap.CellKey;
import mondrian.util.Pair;

import java.util.*;

/**
 * Implementation of a segment body which stores the data of a
 * sparse segment data set into a dense array of java objects.
 *
 * @author LBoudreau
 */
class SparseSegmentBody extends AbstractSegmentBody {
    private static final long serialVersionUID = -6684830985364895836L;
    final CellKey[] keys;
    final Object[] data;

    SparseSegmentBody(
        Map<CellKey, Object> dataToSave,
        List<Pair<SortedSet<Comparable>, Boolean>> axes)
    {
        super(axes);

        this.keys = new CellKey[dataToSave.size()];
        this.data = new Object[dataToSave.size()];
        int i = 0;
        for (Map.Entry<CellKey, Object> entry : dataToSave.entrySet()) {
            keys[i] = entry.getKey();
            data[i] = entry.getValue();
            ++i;
        }
    }

    @Override
    protected int getSize() {
        return keys.length;
    }

    @Override
    protected Object getObject(int i) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<CellKey, Object> getValueMap() {
        final Map<CellKey, Object> map =
            new HashMap<CellKey, Object>(keys.length * 3 / 2);
        for (int i = 0; i < keys.length; i++) {
            map.put(keys[i], data[i]);
        }
        return map;
    }
}

// End SparseSegmentBody.java
