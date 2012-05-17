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

    private final Map<RolapMeasureGroup, MeasureGroupInfo> measureGroupMap =
        new HashMap<RolapMeasureGroup, MeasureGroupInfo>();

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

    RolapGalaxy(RolapCube cube) {
        this.cube = cube;

        // Build map of columns and measures.

        final Set<RolapStar> stars = new LinkedHashSet<RolapStar>();
        for (RolapMeasureGroup measureGroup : cube.getMeasureGroups()) {
            measureGroupMap.put(
                measureGroup, new MeasureGroupInfo(measureGroup));
            for (RolapMeasureGroup.RolapMeasureRef measureRef
                : measureGroup.measureRefList)
            {
                starMeasureRefs.put(
                    measureRef.aggColumn,
                    measureRef.measure.getStarMeasure());
            }
            stars.add(measureGroup.getStar());
        }

        for (RolapStar star : stars) {
            starMap.put(star, new StarInfo(this, star));
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

        prototypeBitKey = BitKey.Factory.makeBitKey(columnMap.size());
        for (StarInfo starInfo : starMap.values()) {
            starInfo.init(prototypeBitKey);
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

        return betterStarInfo.star;
    }

    private int globalOrdinal(RolapStar.Column column, boolean add) {
        final Object key;
        if (column instanceof RolapStar.Measure) {
            RolapStar.Measure measure = (RolapStar.Measure) column;
//            System.out.println("testing " + column);
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
//            System.out.println("adding " + column + " as " + integer);
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
        private final int cost;
        private BitKey measuresGlobalBitKey;
        private BitKey levelsGlobalBitKey;

        StarInfo(
            RolapGalaxy galaxy,
            RolapStar star)
        {
            this.star = star;
            this.cost = star.getCost();

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

        void init(BitKey prototypeBitKey) {
            measuresGlobalBitKey = prototypeBitKey.emptyCopy();
            levelsGlobalBitKey = prototypeBitKey.emptyCopy();
            for (Map.Entry<Integer, RolapStar.Column> entry
                : localColumns.entrySet())
            {
                final RolapStar.Column column = entry.getValue();
                final int globalOrdinal = entry.getKey();
                (column instanceof RolapStar.Measure
                    ? measuresGlobalBitKey
                    : levelsGlobalBitKey).set(globalOrdinal);
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

    // ??used?
    private static class MeasureGroupInfo {
        private final RolapMeasureGroup measureGroup;

        MeasureGroupInfo(RolapMeasureGroup measureGroup) {
            this.measureGroup = measureGroup;
        }
    }
}

// End RolapGalaxy.java
