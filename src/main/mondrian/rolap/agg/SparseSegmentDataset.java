/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2002-2002 Kana Software, Inc.
// Copyright (C) 2002-2005 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 21 March, 2002
*/
package mondrian.rolap.agg;

import mondrian.olap.Util;
import mondrian.rolap.CellKey;

import java.util.Map;
import java.util.HashMap;

/**
 * A <code>SparseSegmentDataset</code> is a means of storing segment values
 * which is suitable when few of the combinations of keys have a value present.
 *
 * <p>The storage requirements are as follows. Key is 1 word for each
 * dimension. Hashtable entry is 3 words. Value is 1 word. Total space is (4 +
 * d) * v. (May also need hash table to ensure that values are only stored
 * once.)</p>
 *
 * <p>NOTE: This class is not synchronized.</p>
 *
 * @author jhyde
 * @since 21 March, 2002
 * @version $Id$
 */
class SparseSegmentDataset implements SegmentDataset {
    private final Map values = new HashMap();

    SparseSegmentDataset(Segment segment) {
        Util.discard(segment);
    }
    public Object get(CellKey pos) {
        return values.get(pos);
    }
    void put(CellKey key, Object value) {
        values.put(key, value);
    }
    public double getBytes() {
        // assume a slot, key, and value are each 4 bytes
        return values.size() * 12;
    }
}

// End SparseSegmentDataset.java
