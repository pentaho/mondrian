/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2001-2010 Julian Hyde and others
// Copyright (C) 2001-2002 Kana Software, Inc.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 28 August, 2001
 */

package mondrian.rolap.agg;

import mondrian.olap.*;
import mondrian.rolap.*;

import java.io.PrintWriter;
import java.lang.ref.SoftReference;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

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
 * @version $Id$
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
     * List of soft references to segments.  This List implementation should be
     * thread-safe on all mutative operations (add, set, and so on). Access to
     * this list is not synchronized in the code.  This is the only mutable
     * field in the class.
     */
    private final List<SoftReference<Segment>> segmentRefs;

    /**
     * Timestamp of when the aggregation was created. (We use
     * {@link java.util.Date} rather than {@link java.sql.Timestamp} because it
     * has less baggage.)
     */
    private final Date creationTimestamp;

    /**
     * This is set in the load method and is used during
     * the processing of a particular aggregate load.
     */
    private RolapStar.Column[] columns;

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
        this.segmentRefs = getThreadSafeListImplementation();
        this.maxConstraints =
            MondrianProperties.instance().MaxConstraints.get();
        this.creationTimestamp = new Date();
    }

    private CopyOnWriteArrayList<SoftReference<Segment>>
    getThreadSafeListImplementation()
    {
        return new CopyOnWriteArrayList<SoftReference<Segment>>();
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
     */
    public void load(
        RolapStar.Column[] columns,
        RolapStar.Measure[] measures,
        StarColumnPredicate[] predicates,
        RolapAggregationManager.PinSet pinnedSegments,
        GroupingSetsCollector groupingSetsCollector)
    {
        // all constrained columns
        if (this.columns == null) {
            this.columns = columns;
        }

        BitKey measureBitKey = getConstrainedColumnsBitKey().emptyCopy();
        int axisCount = columns.length;
        Util.assertTrue(predicates.length == axisCount);

        // This array of Aggregation.Axis is shared by all Segments for
        // this set of measures and constraints
        Aggregation.Axis[] axes = new Aggregation.Axis[axisCount];
        for (int i = 0; i < axisCount; i++) {
            axes[i] = new Aggregation.Axis(predicates[i]);
        }
        List<Segment> segments =
            addSegmentsToAggregation(
                measures, measureBitKey, axes, pinnedSegments);
        // The constrained columns are simply the level and foreign columns
        BitKey levelBitKey = getConstrainedColumnsBitKey();
        GroupingSet groupingSet =
            new GroupingSet(
                segments, levelBitKey, measureBitKey, axes, columns);
        if (groupingSetsCollector.useGroupingSets()) {
            groupingSetsCollector.add(groupingSet);
            // Segments are loaded using group by grouping sets
            // by CompositeBatch.loadAggregation
        } else {
            new SegmentLoader().load(
                Collections.singletonList(groupingSet),
                pinnedSegments,
                compoundPredicateList);
        }
    }

    private List<Segment> addSegmentsToAggregation(
        RolapStar.Measure[] measures,
        BitKey measureBitKey,
        Axis[] axes,
        RolapAggregationManager.PinSet pinnedSegments)
    {
        List<Segment> segments = new ArrayList<Segment>(measures.length);
        for (RolapStar.Measure measure : measures) {
            measureBitKey.set(measure.getBitPosition());
            Segment segment = new Segment(
                this, measure, axes, Collections.<Segment.Region>emptyList());
            segments.add(segment);
            SoftReference<Segment> ref = new SoftReference<Segment>(segment);
            segmentRefs.add(ref);
            ((AggregationManager.PinSetImpl) pinnedSegments).add(segment);
        }
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
        return newPredicates;
    }

    public String toString() {
        java.io.StringWriter sw = new java.io.StringWriter(256);
        PrintWriter pw = new PrintWriter(sw);
        print(pw);
        pw.flush();
        return sw.toString();
    }

    /**
     * Prints the state of this <code>Aggregation</code> to a writer.
     *
     * @param pw Writer
     */
    public void print(PrintWriter pw) {
        List<Segment> segmentList = new ArrayList<Segment>();
        for (SoftReference<Segment> ref : segmentRefs) {
            Segment segment = ref.get();
            if (segment == null) {
                continue;
            }
            segmentList.add(segment);
        }

        // Sort segments, to make order deterministic.
        Collections.sort(
            segmentList,
            new Comparator<Segment>() {
                public int compare(Segment o1, Segment o2) {
                    return o1.id - o2.id;
                }
            });

        for (Segment segment : segmentList) {
            segment.print(pw);
        }
    }

    public void flush(
        CacheControl cacheControl,
        RolapCacheRegion cacheRegion)
    {
        // Compare the bitmaps.
        //
        // Case 1: aggregate bitmap contains request bitmap.
        // E.g. agg = (year={1997, 1998}, quarter=*, nation=USA),
        //      request = (year=1997, nation={USA, Canada}).
        // Assuming descendants (which we do, for now) flush the segment
        // based on the {Year, Nation} values:
        //      flush = (year=1997, quarter=*, nation=USA)
        //
        // Case 2: aggregate bitmap is strict subset of request bitmap
        // E.g. agg = (year={1997, 1998}, nation=*)
        //      request = (year={1997}, nation=*, gender="F")
        // This segment isn't constrained on gender, therefore all cells could
        // contain gender="F" values:
        //      flush = (year=1997, nation=*)
        //
        // Case 3: no overlap
        // E.g. agg = (product, gender),
        //      request = (year=1997)
        // This segment isn't constrained on year, therefore all cells could
        // contain 1997 values. Flush the whole segment.
        //
        // The rule is:
        //  - Column in flush request and in segment. Apply constraints.
        //  - Column in flush request, not in segment. Ignore it.
        //  - Column not in flush request, in segment. Ignore it.
        final boolean bitmapsIntersect =
            cacheRegion.getConstrainedColumnsBitKey().intersects(
                getConstrainedColumnsBitKey());

        // New list of segments - will replace segmentRefs when we're done.
        List<SoftReference<Segment>> newSegmentRefs =
            new ArrayList<SoftReference<Segment>>();
        segmentLoop:
        for (SoftReference<Segment> segmentRef : segmentRefs) {
            Segment segment = segmentRef.get();
            if (segment == null) {
                // Segment has been garbage collected. Flush it.
                cacheControl.trace("discarding garbage collected segment");
                continue;
            }
            if (!bitmapsIntersect) {
                // No intersection between the columns constraining the flush
                // and the columns defining the segment. Therefore, the segment
                // is definitely affected. Flush it.
                cacheControl.trace(
                    "discard segment - it has no columns in common: "
                    + segment);
                continue;
            }

            // For each axis, indicates which values will be retained when
            // constraints have been applied.
            BitSet[] axisKeepBitSets = new BitSet[columns.length];
            for (int i = 0; i < columns.length; i++) {
                final Axis axis = segment.axes[i];
                int keyCount = axis.getKeys().length;
                final BitSet axisKeepBitSet =
                    axisKeepBitSets[i] =
                    new BitSet(keyCount);
                final StarColumnPredicate predicate = axis.predicate;
                assert predicate != null;

                RolapStar.Column column = columns[i];
                if (!cacheRegion.getConstrainedColumnsBitKey().get(
                    column.getBitPosition()))
                {
                    axisKeepBitSet.set(0, keyCount);
                    continue;
                }
                StarColumnPredicate flushPredicate =
                    cacheRegion.getPredicate(column.getBitPosition());

                // If the flush request is not constrained on this column, move
                // on to the next column.
                if (flushPredicate == null) {
                    axisKeepBitSet.set(0, keyCount);
                    continue;
                }

                // If the segment is constrained on this column,
                // and the flush request is constrained on this column,
                // and the constraints do not intersect,
                // then this flush request does not affect this segment.
                // Keep it.
                if (!flushPredicate.mightIntersect(predicate)) {
                    newSegmentRefs.add(segmentRef);
                    continue segmentLoop;
                }

                // The flush constraints overlap. We need to create a new
                // constraint which captures what is actually in this segment.
                //
                // After the flush, values explicitly flushed must be outside
                // the constraints of the axis. In particular, if the axis is
                // initially unconstrained, contains the values {X, Y, Z}, and
                // value Z is flushed, the new constraint of the axis will be
                // {X, Y}. This will force the reader to look to another segment
                // for the Z value, rather than assuming that it does not exist.
                //
                // Example #1. Column constraint is {A, B, C},
                // actual values are {A, B},
                // flush is {A, D}. New constraint could be
                // either {B, C} (constraint minus flush)
                // or {B} (actual minus flush).
                //
                // Example #2. Column constraint is * (unconstrained),
                // actual values are {A, B},
                // flush is {A, D}. New constraint must be
                // {B} (actual minus flush) because mondrian cannot model
                // negative constraints on segments.
                final Object[] axisKeys = axis.getKeys();
                for (int k = 0; k < axisKeys.length; k++) {
                    Object key = axisKeys[k];
                    if (!flushPredicate.evaluate(key)) {
                        axisKeepBitSet.set(k);
                    }
                }
            }

            // Now go through the multi-column constraints, and eliminate any
            // values which are always blocked by a given predicate.
            for (StarPredicate predicate : cacheRegion.getPredicates()) {
                ValuePruner pruner =
                    new ValuePruner(
                        predicate,
                        segment.axes,
                        segment.getData());
                pruner.go(axisKeepBitSets);
            }

            // Figure out which of the axes retains most of its values.
            float bestRetention = 0f;
            int bestColumn = -1;
            for (int i = 0; i < columns.length; i++) {
                // What proportion of the values on this axis survived the flush
                // constraint? 1.0 means they all survived. This means that none
                // of the cells in the segment will be discarded.
                // But we still need to tighten the constraints on the
                // segment, in case new axis values have appeared.
                RolapStar.Column column = columns[i];
                final int bitPosition = column.getBitPosition();
                if (!cacheRegion.getConstrainedColumnsBitKey().get(
                    bitPosition))
                {
                    continue;
                }

                final BitSet axisBitSet = axisKeepBitSets[i];
                final Axis axis = segment.axes[i];
                final Object[] axisKeys = axis.getKeys();

                if (axisBitSet.cardinality() == 0) {
                    // If one axis is empty, the entire segment is empty.
                    // Discard it.
                    continue segmentLoop;
                }

                float retention =
                    (float) axisBitSet.cardinality()
                    / (float) axisKeys.length;

                if (bestColumn == -1 || retention > bestRetention) {
                    // If there are multiple partially-satisfied
                    // constraints ANDed together, keep the constraint
                    // which is least selective.
                    bestRetention = retention;
                    bestColumn = i;
                }
            }

            // Come up with an estimate of how many cells this region contains.
            List<StarColumnPredicate> regionPredicates =
                new ArrayList<StarColumnPredicate>();
            int cellCount = 1;
            for (int i = 0; i < this.columns.length; i++) {
                RolapStar.Column column = this.columns[i];
                Axis axis = segment.axes[i];
                final int pos = column.getBitPosition();
                StarColumnPredicate flushPredicate =
                    cacheRegion.getPredicate(pos);
                int keysMatched;
                if (flushPredicate == null) {
                    flushPredicate = LiteralStarPredicate.TRUE;
                    keysMatched = axis.getKeys().length;
                } else {
                    keysMatched = axis.getMatchCount(flushPredicate);
                }
                cellCount *= keysMatched;
                regionPredicates.add(flushPredicate);
            }

            // We don't know the selectivity of multi-column predicates
            // (typically member predicates such as '>= [Time].[1997].[Q2]') so
            // we guess 50% selectivity.
            for (StarPredicate p : cacheRegion.getPredicates()) {
                cellCount *= .5;
            }
            Segment.Region region =
                new Segment.Region(
                    regionPredicates,
                    new ArrayList<StarPredicate>(cacheRegion.getPredicates()),
                    cellCount);

            // How many cells left after we exclude this region? If there are
            // none left, throw away the segment. It doesn't matter if we
            // over-estimate how many cells are in the region, and therefore
            // throw away a segment which has a few cells left.
            int remainingCellCount = segment.getCellCount();
            if (remainingCellCount - cellCount <= 0) {
                continue;
            }

            // Add the flush region to the list of excluded regions.
            //
            // TODO: If the region has been fully accounted for in changes to
            // the predicates on the axes, then don't add it to the exclusion
            // list.
            final List<Segment.Region> excludedRegions =
                new ArrayList<Segment.Region>(segment.getExcludedRegions());
            if (!excludedRegions.contains(region)) {
                excludedRegions.add(region);
            }

            StarColumnPredicate bestColumnPredicate;
            if (bestColumn >= 0) {
                // Instantiate the axis with the best retention.
                RolapStar.Column column = columns[bestColumn];
                final int bitPosition = column.getBitPosition();
                StarColumnPredicate flushPredicate =
                    cacheRegion.getPredicate(bitPosition);
                final Axis axis = segment.axes[bestColumn];
                bestColumnPredicate = axis.predicate;
                if (flushPredicate != null) {
                    bestColumnPredicate =
                        bestColumnPredicate.minus(flushPredicate);
                }
            } else {
                bestColumnPredicate = null;
            }

            final Segment newSegment =
                segment.createSubSegment(
                    axisKeepBitSets,
                    bestColumn,
                    bestColumnPredicate,
                    excludedRegions);

            newSegmentRefs.add(new SoftReference<Segment>(newSegment));
        }

        // Replace list of segments.
        // FIXME: Synchronize.
        // TODO: Replace segmentRefs, don't copy.
        segmentRefs.clear();
        segmentRefs.addAll(newSegmentRefs);
    }

    /**
     * Retrieves the value identified by <code>keys</code>.
     * If the requested cell is found in the loading segment, current Thread
     * will be blocked until segment is loaded.
     *
     * <p>If <code>pinSet</code> is not null, pins the
     * segment which holds it. <code>pinSet</code> ensures that a segment is
     * only pinned once.
     *
     * <p>Returns <code>null</code> if no segment contains the cell.
     *
     * <p>Returns {@link Util#nullValue} if a segment contains the cell and the
     * cell's value is null.
     */
    public Object getCellValue(
        RolapStar.Measure measure,
        Object[] keys,
        RolapAggregationManager.PinSet pinSet)
    {
        for (SoftReference<Segment> segmentref : segmentRefs) {
            Segment segment = segmentref.get();
            if (segment == null) {
                segmentRefs.remove(segmentref);
                continue; // it's being garbage-collected
            }
            if (segment.measure != measure) {
                continue;
            }
            if (segment.isReady()) {
                Object o = segment.getCellValue(keys);
                if (o != null) {
                    if (pinSet != null) {
                        ((AggregationManager.PinSetImpl) pinSet).add(segment);
                    }
                    return o;
                }
            } else {
                // avoid to call wouldContain - its slow
                if (pinSet != null
                    && !((AggregationManager.PinSetImpl) pinSet).contains(
                        segment)
                    && segment.wouldContain(keys))
                {
                    segment.waitUntilLoaded(); // waiting on Segment state
                    if (segment.isReady()) {
                        ((AggregationManager.PinSetImpl) pinSet).add(segment);
                        return segment.getCellValue(keys);
                    }
                }
            }
        }
        // No segment contains the requested cell.
        return null;
    }

    /**
     * This is called during Sql generation.
     */
    public RolapStar.Column[] getColumns() {
        return columns;
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

    static class Axis {

        /**
         * Constraint on the keys in this Axis. Never null.
         */
        private final StarColumnPredicate predicate;

        /**
         * Map holding the position of each key value.
         *
         * <p>TODO: Hold keys in a sorted array, then deduce ordinal by doing
         * binary search.
         */
        private final Map<Comparable<?>, Integer> mapKeyToOffset =
            new HashMap<Comparable<?>, Integer>();

        /**
         * Actual key values retrieved.
         */
        private Comparable<?>[] keys;

        private static final Integer ZERO = Integer.valueOf(0);
        private static final Integer ONE = Integer.valueOf(1);

        /**
         * Creates an empty Axis.
         *
         * @param predicate Predicate defining which keys should appear on
         *                  axis. (If a key passes the predicate but
         *                  is not in the list, every cell with that
         *                  key is assumed to have a null value.)
         */
        Axis(StarColumnPredicate predicate) {
            this.predicate = predicate;
            assert predicate != null;
        }

        /**
         * Creates an axis populated with a set of keys.
         *
         * @param predicate Predicate defining which keys should appear on
         *                  axis. (If a key passes the predicate but
         *                  is not in the list, every cell with that
         *                  key is assumed to have a null value.)
         * @param keys      Keys
         */
        Axis(StarColumnPredicate predicate, Comparable[] keys) {
            this(predicate);
            this.keys = keys;
            for (int i = 0; i < keys.length; i++) {
                Comparable<?> key = keys[i];
                mapKeyToOffset.put(key, i);
                //noinspection unchecked
                assert i == 0
                       || keys[i - 1].compareTo(keys[i]) < 0;
            }
        }

        StarColumnPredicate getPredicate() {
            return predicate;
        }

        Comparable<?>[] getKeys() {
            return this.keys;
        }

        /**
         * Loads keys into the axis.
         *
         * @param valueSet Set of distinct key values, sorted
         * @param hasNull  Whether the axis contains the null value, in addition
         *                 to the values in <code>valueSet</code>
         * @return Number of keys on axis
         */
        int loadKeys(SortedSet<Comparable<?>> valueSet, boolean hasNull) {
            int size = valueSet.size();

            if (hasNull) {
                size++;
            }
            keys = new Comparable<?>[size];

            valueSet.toArray(keys);
            if (hasNull) {
                keys[size - 1] = RolapUtil.sqlNullValue;
            }

            for (int i = 0; i < size; i++) {
                mapKeyToOffset.put(keys[i], i);
            }

            return size;
        }

        static Comparable wrap(Object o) {
            // Before JDK 1.5, Boolean did not implement Comparable
            if (Util.PreJdk15 && o instanceof Boolean) {
                return (Boolean) o ? ONE : ZERO;
            } else {
                return (Comparable) o;
            }
        }

        final int getOffset(Object o) {
            return getOffset(wrap(o));
        }

        final int getOffset(Comparable key) {
            Integer ordinal = mapKeyToOffset.get(key);
            if (ordinal == null) {
                return -1;
            }
            return ordinal.intValue();
        }

        /**
         * Returns whether this axis contains a given key, or would contain it
         * if it existed.
         *
         * <p>For example, if this axis is unconstrained, then this method
         * returns <code>true</code> for any value.
         *
         * @param key Key
         * @return Whether this axis would contain <code>key</code>
         */
        boolean contains(Object key) {
            return predicate.evaluate(key);
        }

        /**
         * Returns how many of this Axis' keys match a given constraint.
         *
         * @param predicate Predicate
         * @return How many keys match constraint
         */
        public int getMatchCount(StarColumnPredicate predicate) {
            int matchCount = 0;
            for (Object key : keys) {
                if (predicate.evaluate(key)) {
                    ++matchCount;
                }
            }
            return matchCount;
        }
    }

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
        private final Axis[] axes;
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
            Axis[] segmentAxes,
            SegmentDataset data)
        {
            this.flushPredicate = flushPredicate;
            this.arity = flushPredicate.getConstrainedColumnList().size();
            this.axes = new Axis[arity];
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

        private int findAxis(Axis[] axes, int bitPosition) {
            for (int i = 0; i < axes.length; i++) {
                Axis axis = axes[i];
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
                final Axis axis = axes[axisOrdinal];
                if (axis == null) {
                    evaluatePredicate(axisOrdinal + 1);
                } else {
                    for (int keyOrdinal = 0;
                        keyOrdinal < axis.keys.length;
                        keyOrdinal++)
                    {
                        Object key = axis.keys[keyOrdinal];
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
