/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/

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
