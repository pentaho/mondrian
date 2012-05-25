/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2001-2005 Julian Hyde
// Copyright (C) 2005-2012 Pentaho others
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
            putMulti(stars, measureGroup.getStar(), measureGroup);
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
                    return Util.compare(o1.cost, o2.cost);
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

        // Populate each StarInfo's bit keys.
        prototypeBitKey = BitKey.Factory.makeBitKey(columnMap.size());
        nonAdditiveMeasuresBitKey = prototypeBitKey.emptyCopy();
        for (StarInfo starInfo : starMap.values()) {
            starInfo.init(prototypeBitKey, nonAdditiveMeasuresBitKey);
        }
    }

    private static <K, V> void putMulti(Map<K, List<V>> map, K k, V v) {
        List<V> list = map.put(k, Collections.singletonList(v));
        if (list != null) {
            if (list.size() == 1) {
                list = new ArrayList<V>(list);
            }
            list.add(v);
            map.put(k, list);
        }
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
     * @param rollup Out parameter, is set to true if the aggregate is not
     *   an exact match
     * @return An aggregate, or null if none is suitable.
     */
    public RolapStar findAgg(
        RolapStar star,
        final BitKey levelBitKey,
        final BitKey measureBitKey,
        boolean[] rollup)
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

        rollup[0] =
            !betterStarInfo.star.areRowsUnique()
            || !betterStarInfo.measuresGlobalBitKey.equals(
                measuresGlobalBitKey);

        if (rollup[0]
            && measuresGlobalBitKey.intersects(nonAdditiveMeasuresBitKey))
        {
            for (int i : measuresGlobalBitKey.and(nonAdditiveMeasuresBitKey)) {
                BitKey combinedLevelBitKey = null;
                RolapStar.Measure measure =
                    (RolapStar.Measure) betterStarInfo.localColumns.get(i);
                final RolapStar.Measure baseStarMeasure =
                    starMeasureRefs.get(measure.getExpression());

                BitKey rollableLevelBitKey = null;
                if (combinedLevelBitKey == null) {
                        combinedLevelBitKey = rollableLevelBitKey;
                    } else {
                        // TODO use '&=' to remove unnecessary copy
                        combinedLevelBitKey =
                            combinedLevelBitKey.and(rollableLevelBitKey);
                    }
            }
            return null;
        }

        return betterStarInfo.star;
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

            LOGGER.debug("galaxy " + galaxy + ": initializing star " + star);
            final List<RolapStar.Table> tables =
                new ArrayList<RolapStar.Table>();
            collectTables(star.getFactTable(), tables);
            for (RolapStar.Table table : tables) {
                for (RolapStar.Column column : table.getColumns()) {
                    int globalOrdinal = galaxy.globalOrdinal(column, true);
                    globalOrdinals.put(column.getBitPosition(), globalOrdinal);
                    localColumns.put(globalOrdinal, column);
                }
            }
        }

        void init(BitKey prototypeBitKey, BitKey nonAdditiveMeasuresBitKey) {
            measuresGlobalBitKey = prototypeBitKey.emptyCopy();
            levelsGlobalBitKey = prototypeBitKey.emptyCopy();
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
                        nonAdditiveMeasuresBitKey.set(globalOrdinal);
                    }
                } else {
                    levelsGlobalBitKey.set(globalOrdinal);
                }
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

        private void collectTables(
            RolapStar.Table table,
            List<RolapStar.Table> tables)
        {
            tables.add(table);
            for (RolapStar.Table childTable : table.getChildren()) {
                collectTables(childTable, tables);
            }
        }
    }
}

// End RolapGalaxy.java
