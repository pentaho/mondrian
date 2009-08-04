/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2002-2002 Kana Software, Inc.
// Copyright (C) 2002-2009 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 21 March, 2002
*/
package mondrian.rolap.agg;

import mondrian.olap.*;
import mondrian.rolap.*;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.io.PrintWriter;

import org.apache.log4j.Logger;

/**
 * A <code>Segment</code> is a collection of cell values parameterized by
 * a measure, and a set of (column, value) pairs. An example of a segment is</p>
 *
 * <blockquote>
 *   <p>(Unit sales, Gender = 'F', State in {'CA','OR'}, Marital Status = <i>
 *   anything</i>)</p>
 * </blockquote>
 *
 * <p>All segments over the same set of columns belong to an Aggregation, in
 * this case:</p>
 *
 * <blockquote>
 *   <p>('Sales' Star, Gender, State, Marital Status)</p>
 * </blockquote>
 *
 * <p>Note that different measures (in the same Star) occupy the same
 * Aggregation.  Aggregations belong to the AggregationManager, a singleton.</p>
 *
 * <p>Segments are pinned during the evaluation of a single MDX query. The query
 * evaluates the expressions twice. The first pass, it finds which cell values
 * it needs, pins the segments containing the ones which are already present
 * (one pin-count for each cell value used), and builds a {@link CellRequest
 * cell request} for those which are not present. It executes the cell request
 * to bring the required cell values into the cache, again, pinned. Then it
 * evalutes the query a second time, knowing that all cell values are
 * available. Finally, it releases the pins.</p>
 *
 * <p>A Segment may have a list of excluded {@link Region} objects. These are
 * caused by cache flushing. Usually a segment is a hypercube: it is defined by
 * a set of values on each of its axes. But after a cache flush request, a
 * segment may have a rectangular 'hole', and therefore not be a hypercube
 * anymore.
 *
 * <p>For example, the segment defined by {CA, OR, WA} * {F, M} is a
 * 2-dimensional hyper-rectangle with 6 cells. After flushing {CA, OR, TX} *
 * {F}, the result is 4 cells:
 *
 * <pre>
 *     F     M
 * CA  out   in
 * OR  out   in
 * WA  in    in
 * </pre>
 *
 * defined by the original segment minus the region ({CA, OR} * {F}).
 *
 * @author jhyde
 * @since 21 March, 2002
 * @version $Id$
 */
class Segment {
    private static int nextId = 0; // generator for "id"

    final int id; // for debug
    private String desc;
    final Aggregation aggregation;
    final RolapStar.Measure measure;

    final Aggregation.Axis[] axes;

    /**
     * <p><code>data</code> holds a reference to the <code>SegmentDataset</code>
     * that contains the underlying cell values.</p>
     *
     * <p>Since the <code>SegmentDataset</code> is loaded and assigned after
     * <code>Segment</code> is constructed, threadsafe access to it is only
     * guaranteed if the access is guarded.<p/>
     *
     * <p>Access which does not depend on <code>data</code> already having been
     * loaded should be guarded by obtaining either a read or write lock on
     * <code>stateLock</code>, as appropriate.</p>
     *
     * <p>Access that should not proceed until the <code>data</code> reference
     * has been loaded should be guarded using the <code>dataGate</code> latch.
     * This is typically accomplished by calling <code>waitUntilLoaded()</code>,
     * which will block until the latch is released and throw an error if
     * <code>data</code> failed to load.</p>
     *
     * <p>Once set, the value of <code>data</code> is presumed to be invariant
     * and should never be reset, nor should the contents be modified.  Thus,
     * for a given thread, any read access to data which comes after
     * <code>dataGate.await()</code> (or, by extension,
     * <code>waitUntilLoaded</code> will be threadsafe.</p>
     */
    private SegmentDataset data;
    private final CountDownLatch dataGate = new CountDownLatch(1);


    /**
     * <p><code>state</code> == state of the segment (loading, ready, etc).
     * Since it correlates to the value of the <code>data</code> reference
     * and may be accessed from multiple threads, access to it
     * is guarded by stateLock.</p>
     *
     * <p>The initial value of state is Loading.  It may then be set to
     * either Ready or Failed.  Ready and Failed are both terminal
     * states; once set to either, state may not be reset.</p>
     */
    private State state = State.Loading;
    private final ReentrantReadWriteLock stateLock =
            new ReentrantReadWriteLock();

    /**
     * List of regions to ignore when reading this segment. This list is
     * populated when a region is flushed. The cells for these regions may be
     * physically in the segment, because trimming segments can be expensive,
     * but should still be ignored.
     */
    private final List<Region> excludedRegions;
    private static final Logger LOGGER =
            Logger.getLogger(Segment.class);

    /**
     * Creates a <code>Segment</code>; it's not loaded yet.
     *
     * @param aggregation The aggregation this <code>Segment</code> belongs to
     * @param measure Measure whose values this Segment contains
     * @param axes List of axes; each is a constraint plus a list of values
     * @param excludedRegions List of regions which are not in this segment.
     */
    Segment(
        Aggregation aggregation,
        RolapStar.Measure measure,
        Aggregation.Axis[] axes,
        List<Region> excludedRegions)
    {
        this.id = nextId++;
        this.aggregation = aggregation;
        this.measure = measure;
        this.axes = axes;
        this.excludedRegions = excludedRegions;
        for (Region region : excludedRegions) {
            assert region.getPredicates().size() == axes.length;
        }
    }

    /**
     * Sets the data, and notifies any threads which are blocked in
     * {@link #waitUntilLoaded}.
     */
     void setData(
        SegmentDataset data,
        RolapAggregationManager.PinSet pinnedSegments)
    {
        stateLock.writeLock().lock(); // need exclusive write access to state
        try {
            Util.assertTrue(this.data == null);
            Util.assertTrue(this.state == State.Loading);

            this.data = data;
            this.state = State.Ready;
        } finally {
            stateLock.writeLock().unlock(); // always release state lock
        }

        dataGate.countDown(); // allow data reader threads to proceed
    }

    /**
     * If this segment is still loading, signals that it failed to load, and
     * notifies any threads which are blocked in {@link #waitUntilLoaded}.
     */
    void setFailIfStillLoading() {
        stateLock.writeLock().lock(); // need exclusive write access to state
        try {
            switch (state) {
            case Loading:
                Util.assertTrue(this.data == null);
                this.state = State.Failed;
                break;
            case Ready:
                // The segment loaded just fine.
                break;
            default:
                throw Util.badValue(state);
            }
        } finally {
            stateLock.writeLock().unlock(); // always release state lock
            if (this.state == State.Failed) {
                dataGate.countDown(); // allow data reader threads to proceed
            }
        }
    }

    /**
     * Compares internal <code>state</code> variable to a passed-in value
     * in a threadsafe way using the <code>stateLock</code> read lock.
     *
     * @param value The State value to which <code>state</code> should be
     * compared.
     * @return True if states match, false otherwise
     */
    private boolean compareState(State value) {
        boolean retval = false;
        stateLock.readLock().lock();
        try {
            retval = (state == value);
        } finally {
            stateLock.readLock().unlock();
        }
        return (retval);
    }

    public boolean isReady() {
        return (compareState(State.Ready));
    }

    boolean isFailed() {
        return (compareState(State.Failed));
    }

    private void makeDescription(StringBuilder buf, boolean values) {
        final String sep = Util.nl + "    ";
        buf.append(printSegmentHeaderInfo(sep));

        RolapStar.Column[] columns = aggregation.getColumns();
        for (int i = 0; i < columns.length; i++) {
            buf.append(sep);
            buf.append(columns[i].getExpression().getGenericExpression());
            final Aggregation.Axis axis = axes[i];
            axis.getPredicate().describe(buf);
            if (values && isReady()) {
                Object[] keys = axis.getKeys();
                buf.append(", values={");
                for (int j = 0; j < keys.length; j++) {
                    if (j > 0) {
                        buf.append(", ");
                    }
                    Object key = keys[j];
                    buf.append(key);
                }
                buf.append("}");
            }
        }
        if (!excludedRegions.isEmpty()) {
            buf.append(sep);
            buf.append("excluded={");
            int k = 0;
            for (Region excludedRegion : excludedRegions) {
                if (k++ > 0) {
                    buf.append(", ");
                }
                excludedRegion.describe(buf);
            }
            buf.append('}');
        }
        buf.append('}');
    }

    private String printSegmentHeaderInfo(String sep) {
        StringBuilder buf = new StringBuilder();
        buf.append("Segment #");
        buf.append(id);
        buf.append(" {");
        buf.append(sep);
        buf.append("measure=");
        buf.append(measure.getAggregator().getExpression(
                        measure.getExpression().getGenericExpression()));
        return buf.toString();
    }

    public String toString() {
        if (this.desc == null) {
            StringBuilder buf = new StringBuilder(64);
            makeDescription(buf, false);
            this.desc = buf.toString();
        }
        return this.desc;
    }

    /**
     * Retrieves the value at the location identified by
     * <code>keys</code>.
     *
     * <p>Returns<ul>
     *
     * <li>{@link Util#nullValue} if the cell value
     * is null (because no fact table rows met those criteria);</li>
     *
     * <li><code>null</code> if the value is not supposed to be in this segment
     * (because one or more of the keys do not pass the axis criteria);</li>
     *
     * <li>the data value otherwise</li>
     *
     * </ul></p>
     *
     */
    Object getCellValue(Object[] keys) {
        assert keys.length == axes.length;
        int missed = 0;
        CellKey cellKey = CellKey.Generator.newCellKey(axes.length);
        for (int i = 0; i < keys.length; i++) {
            Object key = keys[i];
            int offset = axes[i].getOffset(key);
            if (offset < 0) {
                if (axes[i].getPredicate().evaluate(key)) {
                    // see whether this segment should contain this value
                    missed++;
                    continue;
                } else {
                    // this value should not appear in this segment; we
                    // should be looking in a different segment
                    return null;
                }
            }
            cellKey.setAxis(i, offset);
        }
        if (isExcluded(keys)) {
            // this value should not appear in this segment; we
            // should be looking in a different segment
            return null;
        }
        if (missed > 0) {
            // the value should be in this segment, but isn't, because one
            // or more of its keys does have any values
            return Util.nullValue;
        } else {
            // waitUntilLoaded() ensures data exists, and makes
            // following read threadsafe
            waitUntilLoaded();
            Object o = data.get(cellKey);
            if (o == null) {
                o = Util.nullValue;
            }
            return o;
        }
    }

    /**
     * Returns whether the given set of key values will be in this segment
     * when it finishes loading.
     */
    boolean wouldContain(Object[] keys) {
        Util.assertTrue(keys.length == axes.length);
        for (int i = 0; i < keys.length; i++) {
            Object key = keys[i];
            if (!axes[i].getPredicate().evaluate(key)) {
                return false;
            }
        }
        return !isExcluded(keys);
    }

    /**
     * Returns whether a cell value is excluded from this segment.
     */
    private boolean isExcluded(Object[] keys) {
        for (Region excludedRegion : excludedRegions) {
            if (excludedRegion.wouldContain(keys)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Blocks until this segment has finished loading; if this segment has
     * already loaded, returns immediately.
     */
    public void waitUntilLoaded() {
        if (isLoading()) {
            try {
                LOGGER.debug("Waiting on " + printSegmentHeaderInfo(","));
                dataGate.await();

                stateLock.readLock().lock();
                switch (state) {
                case Ready:
                    return; // excellent!
                case Failed:
                    throw Util.newError(
                        "Pending segment failed to load: "
                        + toString());
                default:
                    throw Util.badValue(state);
                }
            } catch (InterruptedException e) {
                //ignore
            } finally {
                stateLock.readLock().unlock();
            }
        }
    }

    private boolean isLoading() {
        return (compareState(State.Loading));
    }

    /**
     * Prints the state of this <code>Segment</code>, including constraints
     * and values. Blocks the current thread until the segment is loaded.
     *
     * @param pw Writer
     */
    public void print(PrintWriter pw) {
        waitUntilLoaded();
        final StringBuilder buf = new StringBuilder();
        makeDescription(buf, true);
        pw.print(buf.toString());
        pw.println();
    }

    public List<Region> getExcludedRegions() {
        return excludedRegions;
    }

    /**
     * Returns the number of cells in this Segment, deducting cells in
     * excluded regions.
     *
     * <p>This method may return a value which is slightly too low, or
     * occasionally even negative. This occurs when a Segment has more than one
     * excluded region, and those regions overlap. Cells which are in both
     * regions will be counted twice.
     *
     * @return Number of cells in this Segment
     */
    public int getCellCount() {
        int cellCount = 1;
        for (Aggregation.Axis axis : axes) {
            cellCount *= axis.getKeys().length;
        }
        for (Region excludedRegion : excludedRegions) {
            cellCount -= excludedRegion.cellCount;
        }
        return cellCount;
    }

    /**
     * Creates a Segment which has the same dimensionality as this Segment and a
     * subset of the values.
     *
     * <p>If <code>bestColumn</code> is not -1, the <code>bestColumn</code>th
     * column's predicate should be replaced by <code>bestPredicate</code>.
     *
     * @param axisKeepBitSets For each axis, a bitmap of the axis values to
     *   keep; each axis must have at least one bit set
     * @param bestColumn
     * @param bestPredicate
     * @param excludedRegions List of regions to exclude from segment
     * @return Segment containing a subset of the values
     */
    Segment createSubSegment(
        BitSet[] axisKeepBitSets,
        int bestColumn,
        StarColumnPredicate bestPredicate,
        List<Segment.Region> excludedRegions)
    {
        assert axisKeepBitSets.length == axes.length;

        // Create a new segment with a subset of the values. If only one
        // of the axes is restricted, restrict just that axis. If more than
        // one of the axis is restricted, add a negation to the segment.
        final Aggregation.Axis[] newAxes = axes.clone();

        // For each axis, map from old position to new position.
        final Map<Integer, Integer>[] axisPosMaps = new Map[axes.length];

        int valueCount = 1;
        for (int j = 0; j < axes.length; j++) {
            Aggregation.Axis axis = axes[j];
            StarColumnPredicate newPredicate = axis.getPredicate();
            if (j == bestColumn) {
                newPredicate = bestPredicate;
            }
            final Comparable<?>[] axisKeys = axis.getKeys();
            BitSet keepBitSet = axisKeepBitSets[j];
            int firstClearBit = keepBitSet.nextClearBit(0);
            Comparable<?>[] newAxisKeys;
            if (firstClearBit >= axisKeys.length) {
                // Keep everything
                newAxisKeys = axisKeys;
                axisPosMaps[j] = null; // identity map
            } else {
                List<Object> newAxisKeyList = new ArrayList<Object>();
                Map<Integer, Integer> map
                    = axisPosMaps[j]
                    = new HashMap<Integer, Integer>();
                for (int bit = keepBitSet.nextSetBit(0);
                    bit >= 0;
                    bit = keepBitSet.nextSetBit(bit + 1))
                {
                    map.put(bit, newAxisKeyList.size());
                    newAxisKeyList.add(axisKeys[bit]);
                }
                newAxisKeys =
                    newAxisKeyList.toArray(
                        new Comparable<?>[newAxisKeyList.size()]);
                assert newAxisKeys.length > 0;
            }
            final Aggregation.Axis newAxis =
                new Aggregation.Axis(newPredicate, newAxisKeys);
            newAxes[j] = newAxis;
            valueCount *= newAxisKeys.length;
        }

        // Create a new segment.
        final Segment newSegment =
            new Segment(aggregation, measure, newAxes, excludedRegions);

        // Create a dataset containing a subset of the current dataset.
        // Keep the same representation as the current dataset.
        // (We could be smarter - sometimes a subset of a sparse dataset will
        // be dense and VERY occasionally a subset of a relatively dense dataset
        // will be sparse.)
        SegmentDataset newData;

        // isReady() is guarded and ensures visibility of data
        Util.assertTrue(isReady());
        if (data instanceof SparseSegmentDataset) {
            newData =
                new SparseSegmentDataset(
                    newSegment);
        } else {
            Object[] newValues = new Object[valueCount];
            newData =
                new DenseSegmentDataset(
                    newSegment,
                    newValues);
        }

        // If the source is sparse, it is more efficient to iterate over the
        // values we need. If it's dense, it doesn't matter too much.
        int[] pos = new int[axes.length];
        CellKey newKey = CellKey.Generator.newRefCellKey(pos);
        data:
        for (Map.Entry<CellKey, Object> entry : data) {
            CellKey key = entry.getKey();

            // Map each of the source coordinates to the target coordinate.
            // If any of the coordinates maps to null, it means that the
            // cell falls outside the subset.
            for (int i = 0; i < pos.length; i++) {
                int ordinal = key.getAxis(i);

                Map<Integer, Integer> axisPosMap = axisPosMaps[i];
                if (axisPosMap == null) {
                    pos[i] = ordinal;
                } else {
                    Integer integer = axisPosMap.get(ordinal);
                    if (integer == null) {
                        continue data;
                    }
                    pos[i] = integer;
                }
            }
            newData.put(newKey, entry.getValue());
        }
        newSegment.setData(newData, null);

        return newSegment;
    }

    /**
     * <p>Returns this Segment's dataset, or null if the data has not yet been
     * loaded.</p>
     *
     * <p>WARNING: the returned SegmentDataset reference should not be modified;
     * it is assumed to be invariant.</p>
     *
     * @return The <code>data</code> reference if it has been loaded,
     * null otherwise.
     */
    SegmentDataset getData() {
        //Review: letting a non-threadsafe object reference escape
        //is inherently unsafe.  Consider returning a copy.
        if (isReady()) {
            // isReady() is guarded, and ensures visibility of data
            return data;
        } else {
            return null;
        }
    }

    /**
     * <code>State</code> enumerates the allowable values of a segment's
     * state.
     */
    private static enum State {
        Initial, Loading, Ready, Failed
    }

    /**
     * Definition of a region of values which are not in a segment.
     *
     * <p>A region is defined by a set of constraints, one for each column
     * in the segment. A constraint may be
     * {@link mondrian.rolap.agg.LiteralStarPredicate}(true), meaning that
     * the column is unconstrained.
     *
     * <p>For example,
     * <pre>
     * segment (State={CA, OR, WA}, Gender=*)
     * actual values {1997, 1998} * {CA, OR, WA} * {M, F} = 12 cells
     * excluded region (Year=*, State={CA, OR}, Gender={F})
     * excluded values {1997, 1998} * {CA, OR} * {F} = 4 cells
     *
     * Values:
     *
     *     F     M
     * CA  out   in
     * OR  out   in
     * WA  in    in
     * </pre>
     *
     * <p>Note that the resulting segment is not a hypercube: it has a 'hole'.
     * This is why regions are required.
     */
    static class Region {
        private final StarColumnPredicate[] predicates;
        private final StarPredicate[] multiColumnPredicates;
        private final int cellCount;

        Region(
            List<StarColumnPredicate> predicateList,
            List<StarPredicate> multiColumnPredicateList,
            int cellCount)
        {
            this.predicates =
                predicateList.toArray(
                    new StarColumnPredicate[predicateList.size()]);
            this.multiColumnPredicates =
                multiColumnPredicateList.toArray(
                    new StarPredicate[multiColumnPredicateList.size()]);
            this.cellCount = cellCount;
        }

        public List<StarColumnPredicate> getPredicates() {
            return Collections.unmodifiableList(Arrays.asList(predicates));
        }

        public List<StarPredicate> getMultiColumnPredicates() {
            return Collections.unmodifiableList(
                Arrays.asList(multiColumnPredicates));
        }

        public int getCellCount() {
            return cellCount;
        }

        public boolean wouldContain(Object[] keys) {
            assert keys.length == predicates.length;
            for (int i = 0; i < keys.length; i++) {
                final Object key = keys[i];
                final StarColumnPredicate predicate = predicates[i];
                if (!predicate.evaluate(key)) {
                    return false;
                }
            }
            return true;
        }

        public boolean equals(Object obj) {
            if (obj instanceof Region) {
                Region that = (Region) obj;
                return Arrays.equals(
                        this.predicates, that.predicates)
                    && Arrays.equals(
                        this.multiColumnPredicates,
                        that.multiColumnPredicates);
            } else {
                return false;
            }
        }

        public int hashCode() {
            return Arrays.hashCode(multiColumnPredicates) ^
                Arrays.hashCode(predicates);
        }

        /**
         * Describes this Segment.
         * @param buf Buffer to write to.
         */
        public void describe(StringBuilder buf) {
            int k = 0;
            for (StarColumnPredicate predicate : predicates) {
                if (predicate instanceof LiteralStarPredicate
                    && ((LiteralStarPredicate) predicate).getValue())
                {
                    continue;
                }
                if (k++ > 0) {
                    buf.append(" AND ");
                }
                predicate.describe(buf);
            }
            for (StarPredicate predicate : multiColumnPredicates) {
                if (predicate instanceof LiteralStarPredicate
                    && ((LiteralStarPredicate) predicate).getValue())
                {
                    continue;
                }
                if (k++ > 0) {
                    buf.append(" AND ");
                }
                predicate.describe(buf);
            }
        }
    }
}

// End Segment.java
