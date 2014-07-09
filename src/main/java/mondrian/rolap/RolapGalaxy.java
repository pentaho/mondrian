/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2001-2005 Julian Hyde
// Copyright (C) 2005-2013 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.olap.*;
import mondrian.util.Pair;

import org.apache.log4j.Logger;

import java.util.*;

/**
 * The collection of {@link RolapMeasureGroup}s and {@link RolapStar}s that make
 * up a {@link RolapCube}.
 *
 * <p>Contains mappings between the column ordinals of the various stars, and
 * an index to find the fact or aggregate table that contains a given set of
 * measures at a given granularity.</p>
 *
 * <p>Not to be confused with {@link RolapStarSet}.</p>
 *
 * @author jhyde
 */
public class RolapGalaxy {
    private final RolapCube cube;

    private final Map<RolapStar, StarInfo> starMap =
        new HashMap<RolapStar, StarInfo>();

    private final StarInfo[] sortedStarInfos;

    // TODO: also add path to the key
    private final Map<Object, Integer> columnMap =
        new HashMap<Object, Integer>();

    final Map<RolapSchema.PhysExpr, RolapStar.Measure> starMeasureRefs =
        new HashMap<RolapSchema.PhysExpr, RolapStar.Measure>();

    /**
     * Prototype for bit keys. Set when the number of distinct columns is known,
     * thereafter used as a factory.
     */
    private final BitKey prototypeBitKey;

    private final BitKey nonAdditiveMeasuresBitKey;

    /**
     * For each non-additive measure, the global ordinals of columns to which
     * the measure can safely be rolled up.
     *
     * <p>The bit-key is defined as the columns that are functionally dependent
     * on the argument of the measure. For example, if the measure is
     * {@code count(distinct customer_id)} then {@code customer.gender} would be
     * one of the columns.</p>
     */
    private Map<RolapStar.Measure, BitKey> nonAdditiveMeasureSafeToRollup =
        new HashMap<RolapStar.Measure, BitKey>();

    private static final Logger LOGGER = Logger.getLogger(RolapStar.class);

    RolapGalaxy(RolapCube cube) {
        this.cube = cube;

        // Build map of columns and measures.

        final Map<RolapStar, List<RolapMeasureGroup>> stars =
            new LinkedHashMap<RolapStar, List<RolapMeasureGroup>>();
        for (RolapMeasureGroup measureGroup : cube.getMeasureGroups()) {
            for (RolapMeasureGroup.RolapMeasureRef measureRef
                : measureGroup.measureRefList)
            {
                starMeasureRefs.put(
                    measureRef.aggColumn,
                    measureRef.measure.getStarMeasure());
            }
            Util.putMulti(stars, measureGroup.getStar(), measureGroup);
        }

        for (Map.Entry<RolapStar, List<RolapMeasureGroup>> entry
            : stars.entrySet())
        {
            final RolapStar star = entry.getKey();
            final List<RolapMeasureGroup> measureGroups = entry.getValue();
            starMap.put(star, new StarInfo(this, star, measureGroups));
        }
        sortedStarInfos =
            starMap.values().toArray(
                new StarInfo[starMap.size()]);
        Arrays.sort(
            sortedStarInfos,
            new Comparator<StarInfo>() {
                public int compare(StarInfo o1, StarInfo o2) {
                    int compare = Util.compare(o1.cost, o2.cost);
                    if (compare != 0) {
                        return compare;
                    }
                    return o1.star.getFactTable().getRelation().getAlias()
                        .compareTo(
                            o2.star.getFactTable().getRelation().getAlias());
                }
            });

        // Create "alias" columns due to CopyLinks.
        for (;;) {
            int n = 0;
            for (StarInfo starInfo : starMap.values()) {
                n += starInfo.copyColumns(starMap.values());
            }
            if (n == 0) {
                break;
            }
        }

        // Add columns reachable from copied columns.
        for (StarInfo starInfo : starMap.values()) {
            starInfo.addReachableColumns();
        }

        // Initialize each StarInfo's bit keys.
        final int columnCount = columnMap.size();
        prototypeBitKey = BitKey.Factory.makeBitKey(columnCount);
        nonAdditiveMeasuresBitKey = prototypeBitKey.emptyCopy();
        for (StarInfo starInfo : starMap.values()) {
            starInfo.init(this);
        }

        for (StarInfo starInfo : starMap.values()) {
            starInfo.init2(this);
        }

        assert columnCount == columnMap.size();
        assert prototypeBitKey.isEmpty();
    }

    /**
     * Finds a fact or aggregate table in the given cube that has the desired
     * levels and measures. Returns null if no fact or aggregate table is
     * suitable; if more than one is suitable, returns the smallest (per the
     * {@link MondrianProperties#ChooseAggregateByVolume} property).
     *
     * <p>If there no aggregate is an exact match, returns a more
     * granular aggregate which can be rolled up, and sets rollup to true.
     * If one or more of the measures are distinct-count measures
     * rollup is possible only in limited circumstances.</p>
     *
     * @param star Star
     * @param levelBitKey Set of levels
     * @param measureBitKey Set of measures
     * @param rollupOut Out parameter, is set to true if the aggregate is not
     *   an exact match
     * @return An aggregate, or null if none is suitable.
     */
    public RolapMeasureGroup findAgg(
        RolapStar star,
        final BitKey levelBitKey,
        final BitKey measureBitKey,
        boolean[] rollupOut)
    {
        if (!MondrianProperties.instance().ReadAggregates.get()
            || !MondrianProperties.instance().UseAggregates.get())
        {
            // Can't do anything here.
            return null;
        }

        final StarInfo starInfo = starMap.get(star);

        // For each of the measures, find equivalent measures.
        //
        // REVIEW: Maybe union into one bitKey?
        final BitKey measuresGlobalBitKey = toGlobal(measureBitKey, starInfo);
        final BitKey levelsGlobalBitKey = toGlobal(levelBitKey, starInfo);

        // Find the smallest agg/fact table that contains all required measures
        // at sufficient granularity. Since the array is sorted, it will be the
        // first one found.
        StarInfo betterStarInfo = null;
        for (StarInfo starInfo1 : sortedStarInfos) {
            if (starInfo1.measuresGlobalBitKey.isSuperSetOf(
                    measuresGlobalBitKey)
                && starInfo1.levelsGlobalBitKey.isSuperSetOf(
                    levelsGlobalBitKey))
            {
                betterStarInfo = starInfo1;
                break;
            }
        }

        if (betterStarInfo == null
            || betterStarInfo.star == star
            || betterStarInfo.cost >= star.getCost())
        {
            return null;
        }

        boolean rollup =
            rollupOut[0] =
            !betterStarInfo.star.areRowsUnique()
            || !betterStarInfo.levelsGlobalBitKey.equals(levelsGlobalBitKey);

        if (rollup
            && measuresGlobalBitKey.intersects(nonAdditiveMeasuresBitKey))
        {
            BitKey safeRollupBitKey = null;
            for (int i : measuresGlobalBitKey.and(nonAdditiveMeasuresBitKey)) {
                RolapStar.Measure measure =
                    (RolapStar.Measure) betterStarInfo.localColumns.get(i);
                final RolapStar.Measure baseStarMeasure =
                    starMeasureRefs.get(measure.getExpression());

                final BitKey measureSafeRollupBitKey =
                    nonAdditiveMeasureSafeToRollup.get(baseStarMeasure);
                if (safeRollupBitKey == null) {
                    safeRollupBitKey = measureSafeRollupBitKey;
                } else {
                    safeRollupBitKey =
                        safeRollupBitKey.and(measureSafeRollupBitKey);
                }
            }

            assert safeRollupBitKey != null;

            // Columns that we are aggregating away.
            BitKey rollupBitKey =
                betterStarInfo.levelsGlobalBitKey
                    .andNot(levelsGlobalBitKey);

            // If some of the columns that we're aggregating away are not safe
            // to aggregate away, this aggregate is not usable.
            if (!safeRollupBitKey.isSuperSetOf(rollupBitKey)) {
                return null;
            }
        }

        return betterStarInfo.measureGroups.get(0);
    }

    private int globalOrdinal(RolapStar.Column column, boolean add) {
        final Object key;
        if (column instanceof RolapStar.Measure) {
            RolapStar.Measure measure = (RolapStar.Measure) column;
            LOGGER.debug("testing " + column);
            final RolapStar.Measure baseMeasure =
                starMeasureRefs.get(measure.getExpression());
            if (baseMeasure != null) {
                measure = baseMeasure;
            }
            key = Arrays.asList(
                measure.getTable(),
                measure.getAggregator(),
                measure.getExpression());
        } else {
            key = column.getExpression();
        }
        Integer integer = columnMap.get(key);
        if (integer == null) {
            if (!add) {
                return -1;
            }
            integer = columnMap.size();
            columnMap.put(key, integer);
            assert integer == columnMap.size() - 1;
            LOGGER.debug("adding " + column + " as " + integer);
        }
        return integer;
    }

    private BitKey toGlobal(BitKey measureBitKey, StarInfo starInfo) {
        BitKey globalMeasureBitKey = prototypeBitKey.emptyCopy();
        for (int measureOrdinal : measureBitKey) {
            int measureGlobalOrdinal =
                starInfo.globalOrdinals.get(measureOrdinal);
            globalMeasureBitKey.set(measureGlobalOrdinal);
        }
        return globalMeasureBitKey;
    }

    /**
     * Returns the equivalent column, in another star.
     *
     * @param column Star column
     * @param toStar Other star in this galaxy
     * @return Column in {@code toStar} that is equivalent to {@code column};
     *   or null
     */
    public RolapStar.Column getEquivalentColumn(
        RolapStar.Column column,
        RolapStar toStar)
    {
        final int fromBit = column.getBitPosition();
        return getEquivalentColumn(fromBit, column.getStar(), toStar);
    }

    public RolapStar.Column getEquivalentColumn(
        int fromBit,
        RolapStar fromStar,
        RolapStar toStar)
    {
        final StarInfo fromStarInfo = starMap.get(fromStar);
        final StarInfo toStarInfo = starMap.get(toStar);
        final Integer globalOrdinal =
            fromStarInfo.globalOrdinals.get(fromBit);
        return toStarInfo.localColumns.get(globalOrdinal);
    }

    public int getEquivalentBit(
        int fromBit,
        RolapStar fromStar,
        RolapStar toStar)
    {
        return getEquivalentColumn(fromBit, fromStar, toStar).getBitPosition();
    }

    static List<RolapStar.Table> starTables(RolapStar star) {
        final List<RolapStar.Table> tables =
            new ArrayList<RolapStar.Table>();
        collectTables(star.getFactTable(), tables);
        return tables;
    }

    static void collectTables(
        RolapStar.Table table,
        List<RolapStar.Table> tables)
    {
        tables.add(table);
        for (RolapStar.Table childTable : table.getChildren()) {
            collectTables(childTable, tables);
        }
    }

    private static class StarInfo {
        /**
         * Mapping from local ordinals (bit positions within a given star)
         * to global ordinals (bit positions unique within the galaxy).
         */
        private final Map<Integer, Integer> globalOrdinals =
            new HashMap<Integer, Integer>();

        /**
         * Mapping from global ordinals to columns within this star.
         *
         * <p>It is approximately the inverse of {@code globalOrdinals}. The
         * following illustrates the relationship. For all values of
         * {@code globalOrdinal}, either
         *
         * <pre>localColumns.get(globalOrdinal) == null</pre>
         *
         * or
         *
         * <pre>
         * globalOrdinals.get(localColumns.get(globalOrdinal).getBitPosition())
         *  == globalOrdinal</pre>
         */
        private final Map<Integer, RolapStar.Column> localColumns =
            new HashMap<Integer, RolapStar.Column>();

        private final RolapStar star;
        private final List<RolapMeasureGroup> measureGroups;
        private final int cost;
        private BitKey measuresGlobalBitKey;
        private BitKey levelsGlobalBitKey;

        StarInfo(
            RolapGalaxy galaxy,
            RolapStar star,
            List<RolapMeasureGroup> measureGroups)
        {
            this.star = star;
            this.measureGroups = measureGroups;
            this.cost = star.getCost();

            final Set<RolapStar.Column> set = Util.newIdentityHashSet();
            for (RolapMeasureGroup group : measureGroups) {
                for (RolapStar.Column c : Pair.leftIter(group.copyColumnList)) {
                    set.add(c);
                }
            }

            LOGGER.debug("galaxy " + galaxy + ": initializing star " + star);
            for (RolapStar.Table table : starTables(star)) {
                for (RolapStar.Column column : table.getColumns()) {
                    // Don't assign CopyLink columns a global ordinal at this
                    // point. We can assign an ordinal when we resolve them.
                    final boolean add = !set.contains(column);

                    int globalOrdinal = galaxy.globalOrdinal(column, add);
                    if (globalOrdinal < 0) {
                        continue;
                    }
                    globalOrdinals.put(column.getBitPosition(), globalOrdinal);
                    localColumns.put(globalOrdinal, column);
                }
            }
        }

        void init(RolapGalaxy galaxy) {
            measuresGlobalBitKey = galaxy.prototypeBitKey.emptyCopy();
            levelsGlobalBitKey = galaxy.prototypeBitKey.emptyCopy();
        }

        void init2(RolapGalaxy galaxy) {
            for (Map.Entry<Integer, RolapStar.Column> entry
                : localColumns.entrySet())
            {
                final RolapStar.Column column = entry.getValue();
                final int globalOrdinal = entry.getKey();
                if (column instanceof RolapStar.Measure) {
                    measuresGlobalBitKey.set(globalOrdinal);
                    final RolapStar.Measure measure =
                        (RolapStar.Measure) column;
                    if (measure.getAggregator().isDistinct()) {
                        galaxy.nonAdditiveMeasuresBitKey.set(globalOrdinal);

                        // Compute what columns are reachable from the arg of
                        // that measure. They will determine what rollups are
                        // valid.
                        galaxy.nonAdditiveMeasureSafeToRollup.put(
                            measure, dependentGlobalBitKey(galaxy, measure));
                    }
                } else {
                    levelsGlobalBitKey.set(globalOrdinal);
                }
            }
        }

        /**
         * Returns a bit-key of the global ordinals of star columns that are
         * functionally dependent on the argument of a measure.
         *
         * @param galaxy Galaxy
         * @param measure Measure
         * @return Bit-key of functionally dependent columns
         */
        private BitKey dependentGlobalBitKey(
            RolapGalaxy galaxy,
            RolapStar.Measure measure)
        {
            BitKey bitKey = galaxy.prototypeBitKey.emptyCopy();
            List<RolapSchema.PhysColumn> key =
                Collections.singletonList(measure.getExpression());
            for (RolapStar.Table table : star.getFactTable().getChildren()) {
                if (table.getPath().getLinks().get(0).columnList.equals(key)) {
                    setTransitive(table, bitKey);
                }
            }
            return bitKey;
        }

        private void setTransitive(RolapStar.Table table, BitKey bitKey) {
            for (RolapStar.Column column : table.getColumns()) {
                bitKey.set(globalOrdinals.get(column.getBitPosition()));
            }
            for (RolapStar.Table childTable : table.getChildren()) {
                setTransitive(childTable, bitKey);
            }
        }

        int copyColumns(Collection<StarInfo> starInfos) {
            int n = 0;
            for (RolapMeasureGroup measureGroup : measureGroups) {
                for (Pair<RolapStar.Column, RolapSchema.PhysColumn> pair
                    : measureGroup.copyColumnList)
                {
                    final RolapSchema.PhysColumn physColumn = pair.right;
                    final RolapStar.Column starColumn = pair.left;
                    for (StarInfo starInfo : starInfos) {
                        if (starInfo == this) {
                            continue;
                        }
                        for (Map.Entry<Integer, RolapStar.Column> entry
                            : starInfo.localColumns.entrySet())
                        {
                            RolapStar.Column column = entry.getValue();
                            final Integer globalOrdinal = entry.getKey();
                            if (!(column instanceof RolapStar.Measure)
                                && column.getExpression().equals(physColumn)
                                && !localColumns.containsKey(globalOrdinal))
                            {
                                LOGGER.debug(
                                    "copy: globalOrdinal=" + globalOrdinal
                                    + ", starColumn=" + starColumn
                                    + ", physColumn=" + physColumn
                                    + ", column=" + column);
                                localColumns.put(globalOrdinal, starColumn);
                                globalOrdinals.put(
                                    starColumn.getBitPosition(), globalOrdinal);
                                ++n;
                            }
                        }
                    }
                }
            }
            return n;
        }

        public void addReachableColumns() {
            for (;;) {
                final Set<RolapSchema.PhysColumn> physColumns =
                    new LinkedHashSet<RolapSchema.PhysColumn>();
                final Set<RolapSchema.PhysRelation> physTables =
                    new LinkedHashSet<RolapSchema.PhysRelation>();
                for (RolapMeasureGroup measureGroup : measureGroups) {
                    for (Pair<RolapStar.Column, RolapSchema.PhysColumn> pair
                        : measureGroup.copyColumnList)
                    {
                        physColumns.add(pair.right);
                        physTables.add(pair.right.relation);
                    }
                }
                // TODO:
                Util.discard(physColumns);
                Util.discard(physTables);
                if (true) {
                    return;
                }
            }
        }
    }
}

// End RolapGalaxy.java
