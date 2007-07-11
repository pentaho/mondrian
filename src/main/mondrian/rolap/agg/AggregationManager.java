/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2001-2002 Kana Software, Inc.
// Copyright (C) 2001-2007 Julian Hyde and others
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
     * @param measures Measures to load
     * @param columns this is the CellRequest's constrained columns
     * @param constrainedColumnsBitKey this is the CellRequest's constrained column BitKey
     * @param predicates Array of constraints on each column
     * @param pinnedSegments Set of pinned segments
     * @param groupingSetsCollector grouping sets collector
     */
    public void loadAggregation(
            RolapStar.Measure[] measures,
            RolapStar.Column[] columns,
            BitKey constrainedColumnsBitKey,
            StarColumnPredicate[] predicates,
            PinSet pinnedSegments,
            GroupingSetsCollector groupingSetsCollector) {
        RolapStar star = measures[0].getStar();
        Aggregation aggregation =
                star.lookupOrCreateAggregation(constrainedColumnsBitKey);

            // try to eliminate unneccessary constraints
            // for Oracle: prevent an IN-clause with more than 1000 elements
            predicates = aggregation.optimizePredicates(columns, predicates);
            aggregation.load(columns, measures, predicates, pinnedSegments, groupingSetsCollector);
    }

    public Object getCellFromCache(CellRequest request) {
        RolapStar.Measure measure = request.getMeasure();
        Aggregation aggregation = measure.getStar().lookupAggregation(
            request.getConstrainedColumnsBitKey());

        if (aggregation == null) {
            // cell is not in any aggregation
            return null;
        }
            Object o = aggregation.getCellValue(
                    measure, request.getSingleValues(), null);
            if (o != null) {
                return o;
        }
        throw Util.newInternal("not found");
    }

    public Object getCellFromCache(CellRequest request, PinSet pinSet) {
        Util.assertPrecondition(pinSet != null);

        RolapStar.Measure measure = request.getMeasure();
        Aggregation aggregation = measure.getStar().lookupAggregation(
            request.getConstrainedColumnsBitKey());

        if (aggregation == null) {
            // cell is not in any aggregation
            return null;
        } else {
                return aggregation.getCellValue(measure,
                            request.getSingleValues(), pinSet);
        }
    }

    public String getDrillThroughSql(
        final CellRequest request,
        boolean countOnly)
    {
        DrillThroughQuerySpec spec =
            new DrillThroughQuerySpec(
                request,
                countOnly);
        String sql = spec.generateSqlQuery();

        if (getLogger().isDebugEnabled()) {
            StringBuilder buf = new StringBuilder(256);
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
    public String generateSql(GroupByGroupingSets groupByGroupingSets) {

        BitKey levelBitKey = groupByGroupingSets.getDefaultLevelBitKey();
        BitKey measureBitKey = groupByGroupingSets.getDefaultMeasureBitKey();


        // Check if using aggregates is enabled.
        if (MondrianProperties.instance().UseAggregates.get()) {
            RolapStar star = groupByGroupingSets.getStar();

            final boolean[] rollup = {false};
            AggStar aggStar = findAgg(star, levelBitKey, measureBitKey, rollup);

            if (aggStar != null) {
                // Got a match, hot damn

                if (getLogger().isDebugEnabled()) {
                    StringBuilder buf = new StringBuilder(256);
                    buf.append("MATCH: ");
                    buf.append(star.getFactTable().getAlias());
                    buf.append(Util.nl);
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
                    for (AggStar.Table.Column column : aggStar.getFactTable()
                        .getColumns()) {
                        buf.append("   ");
                        buf.append(column);
                        buf.append(Util.nl);
                    }
                    getLogger().debug(buf.toString());
                }

                AggQuerySpec aggQuerySpec =
                    new AggQuerySpec(aggStar, rollup[0],
                        groupByGroupingSets);
                String sql = aggQuerySpec.generateSqlQuery();

                if (getLogger().isDebugEnabled()) {
                    StringBuilder buf = new StringBuilder(256);
                    buf.append("generateSqlQuery: sql=");
                    buf.append(sql);
                    getLogger().debug(buf.toString());
                }

                return sql;
            }

            // No match, fall through and use fact table.
        }

        if (getLogger().isDebugEnabled()) {
            RolapStar star = groupByGroupingSets.getStar();

            StringBuilder buf = new StringBuilder(256);
            buf.append("NO MATCH: ");
            buf.append(star.getFactTable().getAlias());
            buf.append(Util.nl);
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
            new SegmentArrayQuerySpec(groupByGroupingSets);
        String sql = spec.generateSqlQuery();

        if (getLogger().isDebugEnabled()) {
            StringBuilder buf = new StringBuilder(256);
            buf.append("generateSqlQuery: sql=");
            buf.append(sql);
            getLogger().debug(buf.toString());
        }

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
        // measure, isDistinct == true, then what is wanted is an AggStar
        // whose measure BitKey is a superset of the measure BitKey,
        // whose level BitKey is an exact match and the aggregate table
        // can NOT have any foreign keys.
        assert rollup != null;
        BitKey fullBitKey = levelBitKey.or(measureBitKey);

        // The AggStars are already ordered from smallest to largest so
        // we need only find the first one and return it.
        for (AggStar aggStar : star.getAggStars()) {
            // superset match
            if (!aggStar.superSetMatch(fullBitKey)) {
                continue;
            }

            boolean isDistinct = measureBitKey.intersects(
                aggStar.getDistinctMeasureBitKey());

            // The AggStar has no "distinct count" measures so
            // we can use it without looking any further.
            if (!isDistinct) {
                rollup[0] = !aggStar.getLevelBitKey().equals(levelBitKey);
                return aggStar;
            }

            // If there are distinct measures, we can only rollup in limited
            // circumstances.

            // No foreign keys (except when its used as a distinct count
            //   measure).
            // Level key exact match.
            // Measure superset match.

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
/*
                    StringBuilder buf = new StringBuilder(256);
                    buf.append("");
                    buf.append(star.getFactTable().getAlias());
                    buf.append(Util.nl);
                    buf.append("foreign =");
                    buf.append(levelBitKey);
                    buf.append(Util.nl);
                    buf.append("measure =");
                    buf.append(measureBitKey);
                    buf.append(Util.nl);
                    buf.append("aggstar =");
                    buf.append(aggStar.getBitKey());
                    buf.append(Util.nl);
                    buf.append("distinct=");
                    buf.append(aggStar.getDistinctMeasureBitKey());
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
System.out.println(buf.toString());
*/
                // This is a little pessimistic. If the measure is
                // 'count(distinct customer_id)' and one of the foreign keys is
                // 'customer_id' then it is OK to roll up.

                // Some of the measures in this query are distinct count.
                // Get all of the foreign key columns.
                // For each such measure, is it based upon a foreign key.
                // Are there any foreign keys left over. No, can use AggStar.
                BitKey fkBitKey = aggStar.getForeignKeyBitKey().copy();
                Iterator mit = aggStar.getFactTable().getMeasures().iterator();
                for (AggStar.FactTable.Measure measure : aggStar.getFactTable()
                    .getMeasures()) {
                    if (measure.isDistinct()) {
                        if (measureBitKey.get(measure.getBitPosition())) {
                            fkBitKey.clear(measure.getBitPosition());
                        }
                    }
                }
                if (!fkBitKey.isEmpty()) {
                    // there are foreign keys left so we can not use this
                    // AggStar.
                    continue;
                }
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

    public PinSet createPinSet() {
        return new PinSetImpl();
    }

    /**
     * Implementation of {@link mondrian.rolap.RolapAggregationManager.PinSet}
     * using a {@link HashSet}.
     */
    public static class PinSetImpl extends HashSet<Segment>
                implements RolapAggregationManager.PinSet {
    }
}

// End AggregationManager.java
