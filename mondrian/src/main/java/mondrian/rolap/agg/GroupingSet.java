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

import mondrian.rolap.*;

import java.util.List;

/**
 * <p>A collection
 * of {@link mondrian.rolap.agg.Segment}s that can be represented
 * as a GROUP BY GROUPING SET in a SQL query.</p>
 *
 * @author Thiyagu
 * @since 05-Jun-2007
 */
public class GroupingSet {
    private final List<Segment> segments;
    final Segment segment0;
    private final BitKey levelBitKey;
    private final BitKey measureBitKey;
    private final StarColumnPredicate[] predicates;
    private final SegmentAxis[] axes;
    private final RolapStar.Column[] columns;

    /**
     * Creates a GroupingSet.
     *
     * @param segments Constituent segments
     * @param levelBitKey Levels
     * @param measureBitKey Measures
     * @param predicates Predicates
     * @param columns Columns
     */
    public GroupingSet(
        List<Segment> segments,
        BitKey levelBitKey,
        BitKey measureBitKey,
        StarColumnPredicate[] predicates,
        RolapStar.Column[] columns)
    {
        this.segments = segments;
        this.segment0 = segments.get(0);
        this.levelBitKey = levelBitKey;
        this.measureBitKey = measureBitKey;
        this.predicates = predicates;
        this.axes = new SegmentAxis[predicates.length];
        this.columns = columns;
    }


    public List<Segment> getSegments() {
        return segments;
    }

    public BitKey getLevelBitKey() {
        return levelBitKey;
    }

    public BitKey getMeasureBitKey() {
        return measureBitKey;
    }

    public SegmentAxis[] getAxes() {
        return axes;
    }

    public StarColumnPredicate[] getPredicates() {
        return predicates;
    }

    public RolapStar.Column[] getColumns() {
        return columns;
    }

    /**
     * Sets all the segments which are in loading state as failed
     */
    public void setSegmentsFailed() {
        for (Segment segment : segments) {
            // TODO: segment.setFailIfStillLoading();
        }
    }
}

// End GroupingSet.java
