/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2001-2002 Kana Software, Inc.
// Copyright (C) 2001-2005 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 30 August, 2001
*/

package mondrian.rolap.agg;

import mondrian.olap.MondrianProperties;
import mondrian.olap.Util;
import mondrian.rolap.*;
import mondrian.rolap.aggmatcher.AggStar;

import org.apache.log4j.Logger;

import java.util.*;

/**
 * <code>RolapAggregationManager</code> manages all {@link Aggregation}s
 * in the system. It is a singleton class.
 *
 * @author jhyde
 * @since 30 August, 2001
 * @version $Id$
 */
public class AggregationManager extends RolapAggregationManager {
    private static final Logger LOGGER =
            Logger.getLogger(AggregationManager.class);

    private static AggregationManager instance;

    /** Returns or creates the singleton. */
    public static synchronized AggregationManager instance() {
        if (instance == null) {
            instance = new AggregationManager();
        }
        return instance;
    }

    AggregationManager() {
        super();
    }

    public Logger getLogger() {
        return LOGGER;
    }

    /** 
     * Called by FastBatchingCellReader.loadAggregation where the
     * RolapStar creates an Aggregation if needed.
     * 
     * @param measures 
     * @param columns 
     * @param bitKey 
     * @param constraintses 
     * @param pinnedSegments 
     */
    public void loadAggregation(
            RolapStar.Measure[] measures,
            RolapStar.Column[] columns,
            BitKey bitKey,
            ColumnConstraint[][] constraintses,
            Collection pinnedSegments) {
        RolapStar star = measures[0].getStar();
        Aggregation aggregation = star.lookupOrCreateAggregation(bitKey);

        // synchronized access
        synchronized (aggregation) {
            // try to eliminate unneccessary constraints
            // for Oracle: prevent an IN-clause with more than 1000 elements
            constraintses =
                aggregation.optimizeConstraints(columns, constraintses);

            aggregation.load(columns, measures, constraintses, pinnedSegments);
        }
    }



    public Object getCellFromCache(CellRequest request) {
        RolapStar.Measure measure = request.getMeasure();
        Aggregation aggregation = measure.getStar().lookupAggregation(
            request.getBatchKey());

        if (aggregation == null) {
            // cell is not in any aggregation
            return null;
        }
        Object o = aggregation.get(
                measure, request.getSingleValues(), null);
        if (o != null) {
            return o;
        }
        throw Util.newInternal("not found");
    }

    public Object getCellFromCache(CellRequest request, Set pinSet) {
        Util.assertPrecondition(pinSet != null);

        RolapStar.Measure measure = request.getMeasure();
        Aggregation aggregation = measure.getStar().lookupAggregation(
            request.getBatchKey());

        if (aggregation == null) {
            // cell is not in any aggregation
            return null;
        } else {
            return aggregation.get(measure, request.getSingleValues(), pinSet);
        }
    }

    public String getDrillThroughSQL(final CellRequest request) {
        DrillThroughQuerySpec spec = new DrillThroughQuerySpec(request);
        String sql = spec.generateSqlQuery();

        if (getLogger().isDebugEnabled()) {
            StringBuffer buf = new StringBuffer(256);
            buf.append("DrillThroughSQL: ");
            buf.append(sql);
            buf.append(Util.nl);
            getLogger().debug(buf.toString());
        }

        return sql;
    }

    /**
     * Generates the query to retrieve the cells for a list of segments.
     * Called by Segment.load
     */
    public String generateSql(
            final Segment[] segments,
            final BitKey levelBitKey,
            final BitKey measureBitKey) {
        // Check if using aggregates is enabled.
        if (MondrianProperties.instance().UseAggregates.get()) {
            RolapStar star = segments[0].aggregation.getStar();

            final boolean[] rollup = {false};
            AggStar aggStar = findAgg(star, levelBitKey, measureBitKey, rollup);

            if (aggStar != null) {
                // Got a match, hot damn

                if (getLogger().isDebugEnabled()) {
                    StringBuffer buf = new StringBuffer(256);
                    buf.append("MATCH: ");
                    buf.append(star.getFactTable().getAlias());
                    buf.append("   foreign=");
                    buf.append(levelBitKey);
                    buf.append(Util.nl);
                    buf.append("   measure=");
                    buf.append(measureBitKey);
                    buf.append(Util.nl);
                    buf.append("   aggstar=");
                    buf.append(aggStar.getBitKey());
                    buf.append(Util.nl);
                    buf.append("AggStar=");
                    buf.append(aggStar.getFactTable().getName());
                    buf.append(Util.nl);
                    for (Iterator columnIter =
                            aggStar.getFactTable().getColumns().iterator();
                         columnIter.hasNext(); ) {
                        AggStar.Table.Column column =
                                (AggStar.Table.Column) columnIter.next();
                        buf.append("   ");
                        buf.append(column);
                        buf.append(Util.nl);
                    }
                    getLogger().debug(buf.toString());
                }

                AggQuerySpec aggQuerySpec =
                        new AggQuerySpec(aggStar, segments, rollup[0]);
                String sql = aggQuerySpec.generateSqlQuery();
                return sql;
            }

            // No match, fall through and use fact table.
        }

        if (getLogger().isDebugEnabled()) {
            RolapStar star = segments[0].aggregation.getStar();

            StringBuffer buf = new StringBuffer(256);
            buf.append("NO MATCH: ");
            buf.append(star.getFactTable().getAlias());
            buf.append("   foreign=");
            buf.append(levelBitKey);
            buf.append(Util.nl);
            buf.append("   measure=");
            buf.append(measureBitKey);
            buf.append(Util.nl);

            getLogger().debug(buf.toString());
        }


        // Fact table query
        SegmentArrayQuerySpec spec =
            new SegmentArrayQuerySpec(segments);
        String sql = spec.generateSqlQuery();
        return sql;
    }

    /**
     * Finds an aggregate table in the given star which has the desired levels
     * and measures. Returns null if no aggregate table is suitable.
     *
     * <p>If there no aggregate is an exact match, returns a more
     * granular aggregate which can be rolled up, and sets rollup to true.
     * If one or more of the measures are distinct-count measures
     * rollup is possible only in limited circumstances.
     *
     * @param star Star
     * @param levelBitKey Set of levels
     * @param measureBitKey Set of measures
     * @param rollup Out parameter, is set to true if the aggregate is not
     *   an exact match
     * @return An aggregate, or null if none is suitable.
     */
    public AggStar findAgg(
            RolapStar star,
            final BitKey levelBitKey,
            final BitKey measureBitKey,
            boolean[] rollup) {
        // If there is no distinct count measure, isDistinct == false,
        // then all we want is an AggStar whose BitKey is a superset
        // of the combined measure BitKey and foreign-key/level BitKey.
        //
        // On the other hand, if there is at least one distinct count
        // measure, isDistinct == true, then want is wanted is an AggStar
        // whose measure BitKey is a superset of the measure BitKey,
        // whose level BitKey is an exact match and the aggregate table
        // can NOT have any foreign keys.
        assert rollup != null;
        BitKey fullBitKey = levelBitKey.or(measureBitKey);
        // superset match
        for (Iterator it = star.getAggStars().iterator(); it.hasNext(); ) {
            AggStar aggStar = (AggStar) it.next();
            if (!aggStar.superSetMatch(fullBitKey)) {
                continue;
            }

            boolean isDistinct = measureBitKey.intersects(
                    aggStar.getDistinctMeasureBitKey());

            if (!isDistinct) {
                rollup[0] = !aggStar.getLevelBitKey().equals(levelBitKey);
                return aggStar;
            }

            // If there are distinct measures, we can only rollup in limited
            // circumstances.

            // no foreign keys
            // level key exact match
            // measure superset match

            // Compute the core levels -- those which can be safely
            // rolled up to. For example,
            // if the measure is 'distinct customer count',
            // and the agg table has levels customer_id,
            // then gender is a core level.
            final BitKey distinctMeasuresBitKey =
                    measureBitKey.and(aggStar.getDistinctMeasureBitKey());
            final BitSet distinctMeasures = distinctMeasuresBitKey.toBitSet();
            BitKey combinedLevelBitKey = null;
            for (int k = distinctMeasures.nextSetBit(0); k >= 0;
                 k = distinctMeasures.nextSetBit(k + 1)) {
                final AggStar.FactTable.Measure distinctMeasure =
                        aggStar.lookupMeasure(k);
                BitKey rollableLevelBitKey =
                        distinctMeasure.getRollableLevelBitKey();
                if (combinedLevelBitKey == null) {
                    combinedLevelBitKey = rollableLevelBitKey;
                } else {
                    // TODO use '&=' to remove unnecessary copy
                    combinedLevelBitKey =
                            combinedLevelBitKey.and(rollableLevelBitKey);
                }
            }

            if (aggStar.hasForeignKeys()) {
                // TODO: This is a little pessimistic. If the measure is
                // 'count(distinct customer_id)' and one of the foreign keys is
                // 'customer_id' then it is OK to roll up.
                continue;
            }

            if (!aggStar.select(
                    levelBitKey, combinedLevelBitKey, measureBitKey)) {
                continue;
            }

            rollup[0] = !aggStar.getLevelBitKey().equals(levelBitKey);
            return aggStar;
        }
        return null;
    }
}

// End AggregationManager.java
