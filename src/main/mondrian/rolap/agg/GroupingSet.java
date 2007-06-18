/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2004-2007 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap.agg;

import mondrian.rolap.BitKey;
import mondrian.rolap.RolapStar;

/**
 * <p>The <code>GroupingSet</code> stores the information about an
 * {@link mondrian.rolap.agg.Aggregation} which can be represented
 * as a GROUP BY GROUPING SET in a sql. </p>
 * <p>
 *
 * @author Thiyagu
 * @version $Id$
 * @since 05-Jun-2007
 */

public class GroupingSet {
    private final Segment[] segments;
    private final BitKey levelBitKey;
    private final BitKey measureBitKey;
    private final Aggregation.Axis[] axes;
    private final RolapStar.Column[] columns;

    public GroupingSet(
        Segment[] segments, BitKey levelBitKey, BitKey measureBitKey,
        Aggregation.Axis[] axes, RolapStar.Column[] columns)
    {
        this.segments = segments;
        this.levelBitKey = levelBitKey;
        this.measureBitKey = measureBitKey;
        this.axes = axes;
        this.columns = columns;
    }


    public Segment[] getSegments() {
        return segments;
    }

    public BitKey getLevelBitKey() {
        return levelBitKey;
    }

    public BitKey getMeasureBitKey() {
        return measureBitKey;
    }

    public Aggregation.Axis[] getAxes() {
        return axes;
    }

    public RolapStar.Column[] getColumns() {
        return columns;
    }
}