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

import mondrian.rolap.CellKey;

/**
 * A <code>SegmentDataset</code> holds the values in a segment.
 *
 * @author jhyde
 * @since 21 March, 2002
 * @version $Id$
 */
interface SegmentDataset {
    Object get(CellKey pos);
    double getBytes();
}

// End SegmentDataset.java
