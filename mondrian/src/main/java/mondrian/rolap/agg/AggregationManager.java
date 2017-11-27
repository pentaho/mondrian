/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2001-2005 Julian Hyde
// Copyright (C) 2005-2015 Pentaho and others
// All Rights Reserved.
//
// jhyde, 30 August, 2001
*/
package mondrian.rolap.agg;

import mondrian.olap.*;
import mondrian.rolap.*;
import mondrian.rolap.SqlStatement.Type;
import mondrian.rolap.aggmatcher.AggStar;
import mondrian.server.Locus;
import mondrian.util.Pair;

import org.apache.log4j.Logger;

import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.*;

/**
 * <code>RolapAggregationManager</code> manages all {@link Aggregation}s
 * in the system. It is a singleton class.
 *
 * @author jhyde
 * @since 30 August, 2001
 */
public class AggregationManager extends RolapAggregationManager {

    private static final MondrianProperties properties =
        MondrianProperties.instance();

    private static final Logger LOGGER =
        Logger.getLogger(AggregationManager.class);

    public final SegmentCacheManager cacheMgr;

    /**
     * Creates the AggregationManager.
     */
    public AggregationManager(MondrianServer server) {
        if (properties.EnableCacheHitCounters.get()) {
            LOGGER.error(
                "Property " + properties.EnableCacheHitCounters.getPath()
                + " is obsolete; ignored.");
        }
        this.cacheMgr = new SegmentCacheManager(server);
    }

    /**
     * Returns the log4j logger.
     *
     * @return Logger
     */
    public final Logger getLogger() {
        return LOGGER;
    }

    /**
     * Returns or creates the singleton.
     *
     * @deprecated No longer a singleton, and will be removed in mondrian-4.
     *   Use {@link mondrian.olap.MondrianServer#getAggregationManager()}.
     *   To get a server, call
     *   {@link mondrian.olap.MondrianServer#forConnection(mondrian.olap.Connection)},
     *   passing in a null connection if you absolutely must.
     */
    public static synchronized AggregationManager instance() {
        return
            MondrianServer.forId(null).getAggregationManager();
    }

    /**
     * Called by FastBatchingCellReader.load where the
     * RolapStar creates an Aggregation if needed.
     *
     * @param cacheMgr Cache manager
     * @param cellRequestCount Number of missed cells that led to this request
     * @param measures Measures to load
     * @param columns this is the CellRequest's constrained columns
     * @param aggregationKey this is the CellRequest's constraint key
     * @param predicates Array of constraints on each column
     * @param groupingSetsCollector grouping sets collector
     * @param segmentFutures List of futures into which each statement will
     *     place a list of the segments it has loaded, when it completes
     */
    public static void loadAggregation(
        SegmentCacheManager cacheMgr,
        int cellRequestCount,
        List<RolapStar.Measure> measures,
        RolapStar.Column[] columns,
        AggregationKey aggregationKey,
        StarColumnPredicate[] predicates,
        GroupingSetsCollector groupingSetsCollector,
        List<Future<Map<Segment, SegmentWithData>>> segmentFutures)
    {
        RolapStar star = measures.get(0).getStar();
        Aggregation aggregation =
            star.lookupOrCreateAggregation(aggregationKey);

        // try to eliminate unnecessary constraints
        // for Oracle: prevent an IN-clause with more than 1000 elements
        predicates = aggregation.optimizePredicates(columns, predicates);
        aggregation.load(
            cacheMgr, cellRequestCount, columns, measures, predicates,
            groupingSetsCollector, segmentFutures);
    }

    /**
     * Returns an API with which to explicitly manage the contents of the cache.
     *
     * @param connection Server whose cache to control
     * @param pw Print writer, for tracing
     * @return CacheControl API
     */
    public CacheControl getCacheControl(
        RolapConnection connection,
        final PrintWriter pw)
    {
        return new CacheControlImpl(connection) {
            protected void flushNonUnion(final CellRegion region) {
                final SegmentCacheManager.FlushResult result =
                    cacheMgr.execute(
                        new SegmentCacheManager.FlushCommand(
                            Locus.peek(),
                            cacheMgr,
                            region,
                            this));
                final List<Future<Boolean>> futures =
                    new ArrayList<Future<Boolean>>();
                for (Callable<Boolean> task : result.tasks) {
                    futures.add(cacheMgr.cacheExecutor.submit(task));
                }
                for (Future<Boolean> future : futures) {
                    Util.discard(Util.safeGet(future, "Flush cache"));
                }
            }

            public void flush(final CellRegion region) {
                if (pw != null) {
                    pw.println("Cache state before flush:");
                    printCacheState(pw, region);
                    pw.println();
                }
                super.flush(region);
                if (pw != null) {
                    pw.println("Cache state after flush:");
                    printCacheState(pw, region);
                    pw.println();
                }
            }

            public void trace(final String message) {
                if (pw != null) {
                    pw.println(message);
                }
            }

            public boolean isTraceEnabled() {
                return pw != null;
            }
        };
    }

    public Object getCellFromCache(CellRequest request) {
        return getCellFromCache(request, null);
    }

    public Object getCellFromCache(CellRequest request, PinSet pinSet) {
        // NOTE: This method used to check both local (thread/statement) cache
        // and global cache (segments in JVM, shared between statements). Now it
        // only looks in local cache. This can be done without acquiring any
        // locks, because the local cache is thread-local. If a segment that
        // matches this cell-request in global cache, a call to
        // SegmentCacheManager will copy it into local cache.
        final RolapStar.Measure measure = request.getMeasure();
        return measure.getStar().getCellFromCache(request, pinSet);
    }

    public Object getCellFromAllCaches(CellRequest request) {
        final RolapStar.Measure measure = request.getMeasure();
        return measure.getStar().getCellFromAllCaches(request);
    }

    public String getDrillThroughSql(
        final DrillThroughCellRequest request,
        final StarPredicate starPredicateSlicer,
        List<OlapElement> fields,
        final boolean countOnly)
    {
        DrillThroughQuerySpec spec =
            new DrillThroughQuerySpec(
                request,
                starPredicateSlicer,
                fields,
                countOnly);
        Pair<String, List<SqlStatement.Type>> pair = spec.generateSqlQuery();

        if (getLogger().isDebugEnabled()) {
            getLogger().debug(
                "DrillThroughSQL: "
                + pair.left
                + Util.nl);
        }

        return pair.left;
    }

    /**
     * Generates the query to retrieve the cells for a list of segments.
     * Called by Segment.load.
     *
     * @return A pair consisting of a SQL statement and a list of suggested
     *     types of columns
     */
    public static Pair<String, List<SqlStatement.Type>> generateSql(
        GroupingSetsList groupingSetsList,
        List<StarPredicate> compoundPredicateList)
    {
        final RolapStar star = groupingSetsList.getStar();
        BitKey levelBitKey = groupingSetsList.getDefaultLevelBitKey();
        BitKey measureBitKey = groupingSetsList.getDefaultMeasureBitKey();

        // Check if using aggregates is enabled.
        boolean hasCompoundPredicates = false;
        if (compoundPredicateList != null && compoundPredicateList.size() > 0) {
            // Do not use Aggregate tables if compound predicates are present.
            hasCompoundPredicates = true;
        }
        if (MondrianProperties.instance().UseAggregates.get()
             && !hasCompoundPredicates)
        {
            final boolean[] rollup = {false};
            AggStar aggStar = findAgg(star, levelBitKey, measureBitKey, rollup);

            if (aggStar != null) {
                // Got a match, hot damn

                if (LOGGER.isDebugEnabled()) {
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
                    for (AggStar.Table.Column column
                        : aggStar.getFactTable().getColumns())
                    {
                        buf.append("   ");
                        buf.append(column);
                        buf.append(Util.nl);
                    }
                    LOGGER.debug(buf.toString());
                }

                AggQuerySpec aggQuerySpec =
                    new AggQuerySpec(
                        aggStar, rollup[0], groupingSetsList);
                Pair<String, List<Type>> sql = aggQuerySpec.generateSqlQuery();

                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(
                        "generateSqlQuery: sql="
                        + sql.left);
                }

                return sql;
            }

            // No match, fall through and use fact table.
        }

        if (LOGGER.isDebugEnabled()) {
            StringBuilder sb = new StringBuilder();
            sb.append("NO MATCH : ");
            sb.append(star.getFactTable().getAlias());
            sb.append(Util.nl);
            sb.append("Foreign columns bit key=");
            sb.append(levelBitKey);
            sb.append(Util.nl);
            sb.append("Measure bit key=        ");
            sb.append(measureBitKey);
            sb.append(Util.nl);
            sb.append("Agg Stars=[");
            sb.append(Util.nl);
            for (AggStar aggStar : star.getAggStars()) {
                sb.append(aggStar.toString());
            }
            sb.append(Util.nl);
            sb.append("]");
            LOGGER.debug(sb.toString());
        }


        // Fact table query
        SegmentArrayQuerySpec spec =
            new SegmentArrayQuerySpec(groupingSetsList, compoundPredicateList);

        Pair<String, List<SqlStatement.Type>> pair = spec.generateSqlQuery();

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                "generateSqlQuery: sql=" + pair.left);
        }

        return pair;
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
    public static AggStar findAgg(
        RolapStar star,
        final BitKey levelBitKey,
        final BitKey measureBitKey,
        boolean[] rollup)
    {
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

        // a levelBitKey with all parent bits set.
        final BitKey expandedLevelBitKey = expandLevelBitKey(
            star, levelBitKey.copy());

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
                // Need to use SUM if the query levels don't match
                // the agg stars levels, or if the agg star is not
                // fully collapsed.
                rollup[0] = !aggStar.isFullyCollapsed()
                    || aggStar.hasIgnoredColumns()
                    || (levelBitKey.isEmpty()
                    || !aggStar.getLevelBitKey().equals(levelBitKey));
                return aggStar;
            } else if (aggStar.hasIgnoredColumns()) {
                // we cannot safely pull a distinct count from an agg
                // table if ignored columns are present since granularity
                // may not be at the level of the dc measure
                LOGGER.info(
                    aggStar.getFactTable().getName()
                    + " cannot be used for distinct-count measures since it has"
                    + " unused or ignored columns.");
                continue;
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
                k = distinctMeasures.nextSetBit(k + 1))
            {
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
                         columnIter.hasNext();) {
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
                for (AggStar.FactTable.Measure measure
                    : aggStar.getFactTable().getMeasures())
                {
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

            // We can use the expandedLevelBitKey here because
            // presence of parent level columns won't effect granularity,
            // so will still be an allowable agg match
            if (!aggStar.select(
                    expandedLevelBitKey, combinedLevelBitKey, measureBitKey))
            {
                continue;
            }

            if (expandedLevelBitKey.isEmpty()) {
                // We won't be able to resolve a distinct count measure like
                // this. We need to resolve the distinct values but we don't
                // have any levels for which we constraint on. This would
                // result in either a bloated value (non-distinct) or
                // only the first (non-rolled-up) to be returned.
                continue;
            }
            rollup[0] = !aggStar.getLevelBitKey().equals(expandedLevelBitKey);
            return aggStar;
        }
        return null;
    }

    /**
     * Sets the bits for parent columns.
     */
    private static BitKey expandLevelBitKey(
        RolapStar star, BitKey levelBitKey)
    {
        int bitPos = levelBitKey.nextSetBit(0);
        while (bitPos >= 0) {
            levelBitKey = setParentsBitKey(star, levelBitKey, bitPos);
            bitPos = levelBitKey.nextSetBit(bitPos + 1);
        }
        return levelBitKey;
    }

    private static BitKey setParentsBitKey(
        RolapStar star, BitKey levelBitKey, int bitPos)
    {
        RolapStar.Column parent = star.getColumn(bitPos).getParentColumn();
        if (parent == null) {
            return levelBitKey;
        }
        levelBitKey.set(parent.getBitPosition());
        return setParentsBitKey(star, levelBitKey, parent.getBitPosition());
    }

    public PinSet createPinSet() {
        return new PinSetImpl();
    }

    public void shutdown() {
        // Send a shutdown command and wait for it to return.
        cacheMgr.shutdown();
        // Now we can cleanup.
        for (SegmentCacheWorker worker : cacheMgr.segmentCacheWorkers) {
            worker.shutdown();
        }
    }

    /**
     * Implementation of {@link mondrian.rolap.RolapAggregationManager.PinSet}
     * using a {@link HashSet}.
     */
    public static class PinSetImpl
        extends HashSet<Segment>
        implements RolapAggregationManager.PinSet
    {
    }

    public SegmentCacheManager getCacheMgr() {
      return cacheMgr;
    }

}

// End AggregationManager.java
