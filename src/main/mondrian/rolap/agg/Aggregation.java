/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2001-2005 Julian Hyde
// Copyright (C) 2005-2012 Pentaho and others
// All Rights Reserved.
//
// jhyde, 28 August, 2001
*/
package mondrian.rolap.agg;

import mondrian.olap.*;
import mondrian.rolap.*;

import java.util.*;
import java.util.concurrent.Future;

/**
 * A <code>Aggregation</code> is a pre-computed aggregation over a set of
 * columns.
 *
 * <p>Rollup operations:<ul>
 * <li>drop an unrestricted column (e.g. state=*)</li>
 * <li>tighten any restriction (e.g. year={1997,1998} becomes
 * year={1997})</li>
 * <li>restrict an unrestricted column (e.g. year=* becomes
 * year={1997})</li>
 * </ul>
 *
 * <p>Representation of aggregations. Sparse and dense representations are
 * necessary for different data sets. Should adapt automatically. Use an
 * interface to hold the data set, so the segment doesn't care.</p>
 *
 * Suppose we have a segment {year=1997, quarter={1,2,3},
 * state={CA,WA}}. We want to roll up to a segment for {year=1997,
 * state={CA,WA}}.  We need to know that we have all quarters.  We don't.
 * Because year and quarter are independent, we know that we have all of
 * the ...</p>
 *
 * <p>Suppose we have a segment specified by {region=West, state=*,
 * year=*}, which materializes to ({West}, {CA,WA,OR}, {1997,1998}).
 * Because state=*, we can rollup to {region=West, year=*} or {region=West,
 * year=1997}.</p>
 *
 * <p>The space required for a segment depends upon the dimensionality (d),
 * cell count (c) and the value count (v). We don't count the space
 * required for the actual values, which is the same in any scheme.</p>
 *
 * @author jhyde
 * @since 28 August, 2001
 */
public class Aggregation {

    private final List<StarPredicate> compoundPredicateList;
    private final RolapStar star;
    private final BitKey constrainedColumnsBitKey;

    /**
     * Setting for optimizing SQL predicates.
     */
    private final int maxConstraints;

    /**
     * Timestamp of when the aggregation was created. (We use
     * {@link java.util.Date} rather than {@link java.sql.Timestamp} because it
     * has less baggage.)
     */
    private final Date creationTimestamp;

    /**
     * Creates an Aggregation.
     *
     * @param aggregationKey the key specifying the axes, the context and
     *                       the RolapStar for this Aggregation
     */
    public Aggregation(
        AggregationKey aggregationKey)
    {
        this.compoundPredicateList = aggregationKey.getCompoundPredicateList();
        this.star = aggregationKey.getStar();
        this.constrainedColumnsBitKey =
            aggregationKey.getConstrainedColumnsBitKey();
        this.maxConstraints =
            MondrianProperties.instance().MaxConstraints.get();
        this.creationTimestamp = new Date();
    }

    /**
     * @return Returns the timestamp when the aggregation was created
     */
    public Date getCreationTimestamp() {
        return creationTimestamp;
    }

    /**
     * Loads a set of segments into this aggregation, one per measure,
     * each constrained by the same set of column values, and each pinned
     * once.
     *
     * <p>A Column and its constraints are accessed at the same level in their
     * respective arrays.
     *
     * <p>For example,
     * <blockquote><pre>
     * measures = {unit_sales, store_sales},
     * state = {CA, OR},
     * gender = unconstrained</pre></blockquote>
     *
     * @param segmentFutures List of futures wherein each statement will place
     *                       a list of the segments it has loaded, when it
     *                       completes
     */
    public void load(
        SegmentCacheManager cacheMgr,
        int cellRequestCount,
        RolapStar.Column[] columns,
        List<RolapStar.Measure> measures,
        StarColumnPredicate[] predicates,
        GroupingSetsCollector groupingSetsCollector,
        List<Future<Map<Segment, SegmentWithData>>> segmentFutures)
    {
        BitKey measureBitKey = getConstrainedColumnsBitKey().emptyCopy();
        int axisCount = columns.length;
        Util.assertTrue(predicates.length == axisCount);

        List<Segment> segments =
            createSegments(
                columns, measures, measureBitKey, predicates);

        // The constrained columns are simply the level and foreign columns
        BitKey levelBitKey = getConstrainedColumnsBitKey();
        GroupingSet groupingSet =
            new GroupingSet(
                segments, levelBitKey, measureBitKey, predicates, columns);
        if (groupingSetsCollector.useGroupingSets()) {
            groupingSetsCollector.add(groupingSet);
        } else {
            final SegmentLoader segmentLoader = new SegmentLoader(cacheMgr);
            segmentLoader.load(
                cellRequestCount,
                new ArrayList<GroupingSet>(
                    Collections.singletonList(groupingSet)),
                compoundPredicateList,
                segmentFutures);
        }
    }

    private List<Segment> createSegments(
        RolapStar.Column[] columns,
        List<RolapStar.Measure> measures,
        BitKey measureBitKey,
        StarColumnPredicate[] predicates)
    {
        List<Segment> segments = new ArrayList<Segment>(measures.size());
        for (RolapStar.Measure measure : measures) {
            measureBitKey.set(measure.getBitPosition());
            Segment segment =
                new Segment(
                    star,
                    constrainedColumnsBitKey,
                    columns,
                    measure,
                    predicates,
                    Collections.<Segment.ExcludedRegion>emptyList(),
                    compoundPredicateList);
            segments.add(segment);
        }
        // It is important to sort the segments per measure bitkey.
        // The order in which the measures come in is not deterministic.
        // It actually depends on the order of the CellRequests.
        // See: mondrian.rolap.BatchLoader.Batch.add(CellRequest request).
        // Failure to sort them will give out wrong results (uses the wrong
        // column) if we have more than one column in the grouping set.
        Collections.sort(
            segments, new Comparator<Segment>() {
                public int compare(Segment o1, Segment o2) {
                    return Integer.valueOf(
                        o1.measure.getBitPosition())
                            .compareTo(o2.measure.getBitPosition());
                }
            });
        return segments;
    }

    /**
     * Drops predicates, where the list of values is close to the values which
     * would be returned anyway.
     */
    public StarColumnPredicate[] optimizePredicates(
        RolapStar.Column[] columns,
        StarColumnPredicate[] predicates)
    {
        RolapStar star = getStar();
        Util.assertTrue(predicates.length == columns.length);
        StarColumnPredicate[] newPredicates = predicates.clone();
        double[] bloats = new double[columns.length];

        // We want to handle the special case "drilldown" which occurs pretty
        // often. Here, the parent is here as a constraint with a single member
        // and the list of children as well.
        List<Member> potentialParents = new ArrayList<Member>();
        for (final StarColumnPredicate predicate : predicates) {
            Member m;
            if (predicate instanceof MemberColumnPredicate) {
                m = ((MemberColumnPredicate) predicate).getMember();
                potentialParents.add(m);
            }
        }

        for (int i = 0; i < newPredicates.length; i++) {
            // A set of constraints with only one entry will not be optimized
            // away
            if (!(newPredicates[i] instanceof ListColumnPredicate)) {
                bloats[i] = 0.0;
                continue;
            }

            final ListColumnPredicate newPredicate =
                (ListColumnPredicate) newPredicates[i];
            final List<StarColumnPredicate> predicateList =
                newPredicate.getPredicates();
            final int valueCount = predicateList.size();
            if (valueCount < 2) {
                bloats[i] = 0.0;
                continue;
            }

            if (valueCount > maxConstraints) {
                // Some databases can handle only a limited number of elements
                // in 'WHERE IN (...)'. This set is greater than this database
                // can handle, so we drop this constraint. Hopefully there are
                // other constraints that will limit the result.
                bloats[i] = 1.0; // will be optimized away
                continue;
            }

            // more than one - check for children of same parent
            double constraintLength = (double) valueCount;
            Member parent = null;
            Level level = null;
            for (int j = 0; j < valueCount; j++) {
                Object value = predicateList.get(j);
                if (value instanceof MemberColumnPredicate) {
                    MemberColumnPredicate memberColumnPredicate =
                        (MemberColumnPredicate) value;
                    Member m = memberColumnPredicate.getMember();
                    if (j == 0) {
                        parent = m.getParentMember();
                        level = m.getLevel();
                    } else {
                        if (parent != null
                            && !parent.equals(m.getParentMember()))
                        {
                            parent = null; // no common parent
                        }
                        if (level != null
                            && !level.equals(m.getLevel()))
                        {
                            // should never occur, constraints are of same level
                            level = null;
                        }
                    }
                } else {
                    // Value constraint with no associated member.
                    // Compute bloat by #constraints / column cardinality.
                    parent = null;
                    level = null;
                    bloats[i] = constraintLength / columns[i].getCardinality();
                    break;
                }
            }
            boolean done = false;
            if (parent != null) {
                // common parent exists
                if (parent.isAll() || potentialParents.contains(parent)) {
                    // common parent is there as constraint
                    //  if the children are complete, this constraint set is
                    //  unneccessary try to get the children directly from
                    //  cache for the drilldown case, the children will be
                    //  in the cache
                    // - if not, forget this optimization.
                    SchemaReader scr = star.getSchema().getSchemaReader();
                    int childCount = scr.getChildrenCountFromCache(parent);
                    if (childCount == -1) {
                        // nothing gotten from cache
                        if (!parent.isAll()) {
                            // parent is in constraints
                            // no information about children cardinality
                            //  constraints must not be optimized away
                            bloats[i] = 0.0;
                            done = true;
                        }
                    } else {
                        bloats[i] = constraintLength / childCount;
                        done = true;
                    }
                }
            }

            if (!done && level != null) {
                // if the level members are cached, we do not need "count *"
                SchemaReader scr = star.getSchema().getSchemaReader();
                int memberCount = scr.getLevelCardinality(level, true, false);
                if (memberCount > 0) {
                    bloats[i] = constraintLength / memberCount;
                    done = true;
                }
            }

            if (!done) {
                bloats[i] = constraintLength / columns[i].getCardinality();
            }
        }

        // build a list of constraints sorted by 'bloat factor'
        ConstraintComparator comparator = new ConstraintComparator(bloats);
        Integer[] indexes = new Integer[columns.length];
        for (int i = 0; i < columns.length; i++) {
            indexes[i] = i;
        }

        // sort indexes by bloat descending
        Arrays.sort(indexes, comparator);

        // Eliminate constraints one by one, until the constrained cell count
        // became half of the unconstrained cell count. We can not have an
        // absolute value here, because its
        // very different if we fetch data for 2 years or 10 years (5 times
        // more means 5 times slower). So a relative comparison is ok here
        // but not an absolute one.

        double abloat = 1.0;
        final double aBloatLimit = .5;

        for (Integer j : indexes) {
            abloat = abloat * bloats[j];
            if (abloat <= aBloatLimit) {
                break;
            }
            // eliminate this constraint
            if (MondrianProperties.instance().OptimizePredicates.get()
                || bloats[j] == 1)
            {
                newPredicates[j] = new LiteralStarPredicate(columns[j], true);
            }
        }

        // Now do simple structural optimizations, e.g. convert a list predicate
        // with one element to a value predicate.
        for (int i = 0; i < newPredicates.length; i++) {
            newPredicates[i] = StarPredicates.optimize(newPredicates[i]);
        }

        return newPredicates;
    }

    /**
     * This is called during SQL generation.
     */
    public RolapStar getStar() {
        return star;
    }

    /**
     * Returns the BitKey for ALL columns (Measures and Levels) involved in the
     * query.
     */
    public BitKey getConstrainedColumnsBitKey() {
        return constrainedColumnsBitKey;
    }

    // -- classes -------------------------------------------------------------

    /**
     * Helper class to figure out which axis values evaluate to true at least
     * once by a given predicate.
     *
     * <p>Consider, for example, the flush predicate<blockquote><code>
     *
     * member between [Time].[1997].[Q3] and [Time].[1999].[Q1]
     *
     * </code></blockquote>applied to the segment <blockquote><code>
     *
     * year in (1996, 1997, 1998, 1999)<br/>
     * quarter in (Q1, Q2, Q3, Q4)
     *
     * </code></blockquote> The predicate evaluates to true for the pairs
     * <blockquote><code>
     *
     * {(1997, Q3), (1997, Q4),
     * (1998, Q1), (1998, Q2), (1998, Q3), (1998, Q4), (1999, Q1)}
     *
     * </code></blockquote> and therefore we wish to eliminate these pairs from
     * the segment. But we can eliminate a value only if <em>all</em> of its
     * values are eliminated.
     *
     * <p>In this case, year=1998 is the only value which can be eliminated from
     * the segment.
     */
    private static class ValuePruner {
        /**
         * Multi-column predicate. If the predicate evaluates to true, a cell
         * will be removed from the segment. But we can only eliminate a value
         * if all of its cells are eliminated.
         */
        private final StarPredicate flushPredicate;
        /**
         * Number of columns predicate depends on.
         */
        private final int arity;
        /**
         * For each column, the segment axis which the column corresponds to, or
         * null.
         */
        private final SegmentAxis[] axes;
        /**
         * For each column, a bitmap of values for which the predicate is
         * sometimes false. These values cannot be eliminated from the axis.
         */
        private final BitSet[] keepBitSets;
        /**
         * For each segment axis, the predicate column which depends on the
         * axis, or -1.
         */
        private final int[] axisInverseOrdinals;
        /**
         * Workspace which contains the current key value for each column.
         */
        private final Object[] values;
        /**
         * View onto {@link #values} as a list.
         */
        private final List<Object> valueList;
        /**
         * Workspace which contains the ordinal of the current value of each
         * column on its axis.
         */
        private final int[] ordinals;

        private final SegmentDataset data;

        private final CellKey cellKey;

        /**
         * Creates a ValuePruner.
         *
         * @param flushPredicate Multi-column predicate to test
         * @param segmentAxes    Axes of the segment. (The columns that the
         *                       predicate may not be present, or may
         *                       be in a different order.)
         * @param data           Segment dataset, which allows pruner
         *                       to determine whether a particular
         *                       cell is currently empty
         */
        ValuePruner(
            StarPredicate flushPredicate,
            SegmentAxis[] segmentAxes,
            SegmentDataset data)
        {
            this.flushPredicate = flushPredicate;
            this.arity = flushPredicate.getConstrainedColumnList().size();
            this.axes = new SegmentAxis[arity];
            this.keepBitSets = new BitSet[arity];
            this.axisInverseOrdinals = new int[segmentAxes.length];
            Arrays.fill(axisInverseOrdinals, -1);
            this.values = new Object[arity];
            this.valueList = Arrays.asList(values);
            this.ordinals = new int[arity];
            assert data != null;
            this.data = data;
            this.cellKey = CellKey.Generator.newCellKey(segmentAxes.length);

            // Pair up constraint columns with axes. If one of the constraint's
            // columns is not in this segment, it gets the null axis. The
            // constraint will have to evaluate to true for all possible values
            // of that column.
            for (int i = 0; i < arity; i++) {
                RolapStar.Column column =
                    flushPredicate.getConstrainedColumnList().get(i);
                int axisOrdinal =
                    findAxis(segmentAxes, column.getBitPosition());
                if (axisOrdinal < 0) {
                    this.axes[i] = null;
                    values[i] = StarPredicate.WILDCARD;
                    keepBitSets[i] = new BitSet(1); // dummy
                } else {
                    axes[i] = segmentAxes[axisOrdinal];
                    axisInverseOrdinals[axisOrdinal] = i;
                    final int keyCount = axes[i].getKeys().length;
                    keepBitSets[i] = new BitSet(keyCount);
                }
            }
        }

        private int findAxis(SegmentAxis[] axes, int bitPosition) {
            for (int i = 0; i < axes.length; i++) {
                SegmentAxis axis = axes[i];
                if (axis.getPredicate().getConstrainedColumn().getBitPosition()
                    == bitPosition)
                {
                    return i;
                }
            }
            return -1;
        }

        /**
         * Applies this ValuePruner's predicate and sets bits in axisBitSets
         * to indicate extra values which can be removed.
         *
         * @param axisKeepBitSets Array containing, for each axis, a bitset
         *                        of values to keep (not flush)
         */
        void go(BitSet[] axisKeepBitSets) {
            evaluatePredicate(0);

            // Clear bits in the axis bit sets (indicating that a value is never
            // used) if this predicate evaluates to true for every combination
            // of values which this axis value appears in.
            for (int i = 0; i < axisKeepBitSets.length; i++) {
                if (axisInverseOrdinals[i] < 0) {
                    continue;
                }
                BitSet axisKeepBitSet = axisKeepBitSets[axisInverseOrdinals[i]];
                final BitSet keepBitSet = keepBitSets[i];
                axisKeepBitSet.and(keepBitSet);
            }
        }

        /**
         * Evaluates the predicate for axes <code>i</code> and higher, and marks
         * {@link #keepBitSets} if the predicate ever evaluates to false.
         * The result is that discardBitSets[i] will be false for column #i if
         * the predicate evaluates to true for all cells in the segment which
         * have that column value.
         *
         * @param axisOrdinal Axis ordinal
         */
        private void evaluatePredicate(int axisOrdinal) {
            if (axisOrdinal == arity) {
                // If the flush predicate evaluates to false for this cell,
                // and this cell currently has some data (*),
                // then none of the values which are the coordinates of this
                // cell can be discarded.
                //
                // * Important when there is sparsity. Consider the cell
                // {year=1997, quarter=Q1, month=12}. This cell would never have
                // data, so there's no point keeping it.
                if (!flushPredicate.evaluate(valueList)) {
                    // REVIEW: getObject forces an int or double dataset to
                    // create a boxed object; use exists() instead?
                    if (data.getObject(cellKey) != null) {
                        for (int k = 0; k < arity; k++) {
                            keepBitSets[k].set(ordinals[k]);
                        }
                    }
                }
            } else {
                final SegmentAxis axis = axes[axisOrdinal];
                if (axis == null) {
                    evaluatePredicate(axisOrdinal + 1);
                } else {
                    final Comparable[] keys = axis.getKeys();
                    for (int keyOrdinal = 0;
                        keyOrdinal < keys.length;
                        keyOrdinal++)
                    {
                        Object key = keys[keyOrdinal];
                        values[axisOrdinal] = key;
                        ordinals[axisOrdinal] = keyOrdinal;
                        cellKey.setAxis(
                            axisInverseOrdinals[axisOrdinal],
                            keyOrdinal);
                        evaluatePredicate(axisOrdinal + 1);
                    }
                }
            }
        }
    }

    private static class ConstraintComparator implements Comparator<Integer> {
        private final double[] bloats;

        ConstraintComparator(double[] bloats) {
            this.bloats = bloats;
        }

        // implement Comparator
        // order by bloat descending
        public int compare(Integer o0, Integer o1) {
            double bloat0 = bloats[o0];
            double bloat1 = bloats[o1];
            return (bloat0 == bloat1)
                ? 0
                : (bloat0 < bloat1)
                    ? 1
                    : -1;
        }
    }


}

// End Aggregation.java
