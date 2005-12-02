/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2002-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 21 March, 2002
*/
package mondrian.rolap.agg;

import mondrian.olap.*;
import mondrian.rolap.*;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * A <code>Segment</code> is a collection of cell values parameterized by
 * a measure, and a set of (column, value) pairs. An example of a segment is</p>
 *
 * <blockquote>
 *   <p>(Unit sales, Gender = 'F', State in {'CA','OR'}, Marital Status = <i>
 *   anything</i>)</p>
 * </blockquote>
 *
 * <p>All segments over the same set of columns belong to an Aggregation, in this
 * case</p>
 *
 * <blockquote>
 *   <p>('Sales' Star, Gender, State, Marital Status)</p>
 * </blockquote>
 *
 * <p>Note that different measures (in the same Star) occupy the same Aggregation.
 * Aggregations belong to the AggregationManager, a singleton.</p>
 * <p>Segments are pinned during the evaluation of a single MDX query. The query
 * evaluates the expressions twice. The first pass, it finds which cell values it
 * needs, pins the segments containing the ones which are already present (one
 * pin-count for each cell value used), and builds a {@link CellRequest
 * cell request} for those which are not present. It executes
 * the cell request to bring the required cell values into the cache, again,
 * pinned. Then it evalutes the query a second time, knowing that all cell values
 * are available. Finally, it releases the pins.</p>
 *
 * @author jhyde
 * @since 21 March, 2002
 * @version $Id$
 **/
class Segment {

    /**
     * <code>State</code> enumerates the allowable values of a segment's
     * state.
     */
    private static class State extends EnumeratedValues {
        public static final State instance = new State();
        private State() {
            super(new String[] {"init","loading","ready","failed"});
        }
        public static final int Initial = 0;
        public static final int Loading = 1;
        public static final int Ready = 2;
        public static final int Failed = 3;
    }

    private static int nextId = 0; // generator for "id"

    private final int id; // for debug
    private String desc;
    final Aggregation aggregation;
    final RolapStar.Measure measure;

    final Aggregation.Axis[] axes;
    private SegmentDataset data;
    private final CellKey cellKey; // workspace
    /** State of the segment, values are described by {@link State}. */
    private int state;

    /**
     * Creates a <code>Segment</code>; it's not loaded yet.
     *
     * @param aggregation The aggregation that this <code>Segment</code>
     *    belongs to
     * @param constraintses For each column, either an array of values
     *    to fetch or null, indicating that the column is unconstrained
     **/
    Segment(Aggregation aggregation,
            RolapStar.Measure measure,
            ColumnConstraint[][] constraintses,
            Aggregation.Axis[] axes) {
        this.id = nextId++;
        this.aggregation = aggregation;
        this.measure = measure;
        this.axes = axes;
        this.cellKey = new CellKey(new int[axes.length]);
        this.state = State.Loading;
    }

    /**
     * Sets the data, and notifies any threads which are blocked in
     * {@link #waitUntilLoaded}.
     */
    synchronized void setData(SegmentDataset data,
                              Collection pinnedSegments) {
        Util.assertTrue(this.data == null);
        Util.assertTrue(this.state == State.Loading);

        this.data = data;
        this.state = State.Ready;
        notifyAll();
    }

    /**
     * If this segment is still loading, signals that it failed to load, and
     * notifies any threads which are blocked in {@link #waitUntilLoaded}.
     */
    synchronized void setFailed() {
        switch (state) {
        case State.Loading:
            Util.assertTrue(this.data == null);
            this.state = State.Failed;
            notifyAll();
            break;
        case State.Ready:
            // The segment loaded just fine.
            break;
        default:
            throw State.instance.badValue(state);
        }
    }

    public boolean isReady() {
        return (state == State.Ready);
    }

    private String makeDescription() {
        StringBuffer buf = new StringBuffer(64);
        buf.append("Segment #");
        buf.append(id);
        buf.append(" {measure=");
        buf.append(measure.getAggregator().getExpression(
                        measure.getExpression().getGenericExpression()));

        RolapStar.Column[] columns = aggregation.getColumns();
        for (int i = 0; i < columns.length; i++) {
            buf.append(", ");
            buf.append(columns[i].getExpression().getGenericExpression());
            ColumnConstraint[] constraints = axes[i].getConstraints();
            if (constraints == null) {
                buf.append("=any");
            } else {
                buf.append("={");
                for (int j = 0; j < constraints.length; j++) {
                    if (j > 0) {
                        buf.append(", ");
                    }
                    buf.append(constraints[j].getValue().toString());
                }
                buf.append('}');
            }
        }
        buf.append('}');
        return buf.toString();
    }

    public String toString() {
        if (this.desc == null) {
            this.desc = makeDescription();
        }
        return this.desc;
    }

    /**
     * Retrieves the value at the location identified by
     * <code>keys</code>. Returns {@link Util#nullValue} if the cell value
     * is null (because no fact table rows met those criteria), and
     * <code>null</code> if the value is not supposed to be in this segment
     * (because one or more of the keys do not pass the axis criteria).
     *
     * <p>Note: Must be called from a synchronized context, because uses the
     * <code>cellKey[]</code> as workspace.</p>
     **/
    Object get(Object[] keys) {
        Util.assertTrue(keys.length == axes.length);
        int missed = 0;
        for (int i = 0; i < keys.length; i++) {
            Object key = keys[i];
            Integer integer = axes[i].getOffset(key);
            if (integer == null) {
                if (axes[i].contains(key)) {
                    // see whether this segment should contain this value
                    missed++;
                    continue;
                } else {
                    // this value should not appear in this segment; we
                    // should be looking in a different segment
                    return null;
                }
            }
            cellKey.ordinals[i] = integer.intValue();
        }
        if (missed > 0) {
            // the value should be in this segment, but isn't, because one
            // or more of its keys does have any values
            return Util.nullValue;
        } else {
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
     **/
    boolean wouldContain(Object[] keys) {
        Util.assertTrue(keys.length == axes.length);
        for (int i = 0; i < keys.length; i++) {
            Object key = keys[i];
            if (!axes[i].contains(key)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Reads a segment of <code>measure</code>, where <code>columns</code> are
     * constrained to <code>values</code>.  Each entry in <code>values</code>
     * can be null, meaning don't constrain, or can have several values. For
     * example, <code>getSegment({Unit_sales}, {Region, State, Year}, {"West"},
     * {"CA", "OR", "WA"}, null})</code> returns sales in states CA, OR and WA
     * in the Western region, for all years.
     *
     * @pre segments[i].aggregation == aggregation
     **/
    static void load(final Segment[] segments,
                     final BitKey fkBK,
                     final BitKey measureBK,
                     final boolean isDistinct,
                     final Collection pinnedSegments,
                     final Aggregation.Axis[] axes) {
        String sql = AggregationManager.instance().generateSQL(segments,
                                                               fkBK,
                                                               measureBK,
                                                               isDistinct);
        Segment segment0 = segments[0];
        RolapStar star = segment0.aggregation.getStar();
        RolapStar.Column[] columns = segment0.aggregation.getColumns();
        int arity = columns.length;

        // execute
        ResultSet resultSet = null;
        final int measureCount = segments.length;
        java.sql.Connection jdbcConnection = star.getJdbcConnection();
        try {
            resultSet = RolapUtil.executeQuery(
                    jdbcConnection, sql, "Segment.load");
            List rows = new ArrayList();
            while (resultSet.next()) {
                Object[] row = new Object[arity + measureCount];
                // get the columns
                int k = 1;
                for (int i = 0; i < arity; i++) {
                    Object o = resultSet.getObject(k++);
                    if (o == null) {
                        o = RolapUtil.sqlNullValue;
                    }
                    Integer offsetInteger = axes[i].getOffset(o);
                    if (offsetInteger == null) {
                        axes[i].addNextOffset(o);
                    }
                    row[i] = o;
                }
                // get the measure
                for (int i = 0; i < measureCount; i++) {
                    Object o = resultSet.getObject(k++);
                    if (o == null) {
                        o = Util.nullValue; // convert to placeholder
                    }
                    row[arity + i] = o;
                }
                rows.add(row);
            }

            // figure out size of dense array, and allocate it (todo: use
            // sparse array sometimes)
            boolean sparse = false;
            int n = 1;
            for (int i = 0; i < arity; i++) {
                Aggregation.Axis axis = axes[i];
                int size = axis.loadKeys();

                int previous = n;
                n *= size;
                if ((n < previous) || (n < size)) {
                    // Overflow has occurred.
                    n = Integer.MAX_VALUE;
                    sparse = true;
                }
            }
            SegmentDataset[] datas = new SegmentDataset[segments.length];
            sparse = sparse || useSparse((double) n, (double) rows.size());
            for (int i = 0; i < segments.length; i++) {
                datas[i] = sparse
                    ? (SegmentDataset) new SparseSegmentDataset(segments[i])
                    : new DenseSegmentDataset(segments[i], new Object[n]);
            }
            // now convert the rows into a sparse array
            int[] pos = new int[arity];
            for (int i = 0, count = rows.size(); i < count; i++) {
                Object[] row = (Object[]) rows.get(i);
                int k = 0;
                for (int j = 0; j < arity; j++) {
                    k *= axes[j].getKeys().length;
                    Object o = row[j];
                    Aggregation.Axis axis = axes[j];
                    Integer offsetInteger = axis.getOffset(o);
                    int offset = offsetInteger.intValue();
                    pos[j] = offset;
                    k += offset;
                }
                CellKey key = null;
                if (sparse) {
                    key = new CellKey((int[]) pos.clone());
                }
                for (int j = 0; j < segments.length; j++) {
                    final Object o = row[arity + j];
                    if (sparse) {
                        ((SparseSegmentDataset) datas[j]).put(key, o);
                    } else {
                        ((DenseSegmentDataset) datas[j]).set(k, o);
                    }
                }
            }

            for (int i = 0; i < segments.length; i++) {
                segments[i].setData(datas[i], pinnedSegments);
            }

        } catch (SQLException e) {
            throw Util.newInternal(e,
                    "Error while loading segment; sql=[" + sql + "]");
        } finally {
            try {
                if (resultSet != null) {
                    resultSet.getStatement().close();
                    resultSet.close();
                }
            } catch (SQLException e) {
                // ignore
            }
            try {
                jdbcConnection.close();
            } catch (SQLException e) {
                //ignore
            }
            // Any segments which are still loading have failed.
            for (int i = 0; i < segments.length; i++) {
                segments[i].setFailed();
            }
        }
    }

    /**
     * Decides whether to use a sparse representation for this segment, using
     * the formula described
     * {@link MondrianProperties#SparseSegmentCountThreshold here}.
     *
     * @param possibleCount Number of values in the space.
     * @param actualCount Actual number of values.
     * @return Whether to use a sparse representation.
     */
    private static boolean useSparse(
            final double possibleCount,
            final double actualCount) {
        final MondrianProperties properties = MondrianProperties.instance();
        double densityThreshold = properties.SparseSegmentDensityThreshold.get();
        if (densityThreshold < 0) {
            densityThreshold = 0;
        }
        if (densityThreshold > 1) {
            densityThreshold = 1;
        }
        int countThreshold = properties.SparseSegmentCountThreshold.get();
        if (countThreshold < 0) {
            countThreshold = 0;
        }
        boolean sparse =
            (possibleCount - countThreshold) * densityThreshold > actualCount;
        if (possibleCount < countThreshold) {
            assert !sparse :
                    "Should never use sparse if count is less " +
                    "than threshold, possibleCount=" + possibleCount +
                    ", actualCount=" + actualCount +
                    ", countThreshold=" + countThreshold +
                    ", densityThreshold=" + densityThreshold;
        }
        if (possibleCount == actualCount) {
            assert !sparse :
                    "Should never use sparse if result is 100% dense: " +
                    "possibleCount=" + possibleCount +
                    ", actualCount=" + actualCount +
                    ", countThreshold=" + countThreshold +
                    ", densityThreshold=" + densityThreshold;
        }
        return sparse;
    }

    /**
     * Blocks until this segment has finished loading; if this segment has
     * already loaded, returns immediately.
     */
    public synchronized void waitUntilLoaded() {
        if (!isReady()) {
            try {
                wait();
            } catch (InterruptedException e) {
            }
            switch (state) {
            case State.Ready:
                return; // excellent!
            case State.Failed:
                throw Util.newError("Pending segment failed to load: "
                    + toString());
            default:
                throw State.instance.badValue(state);
            }
        }
    }
}

// End Segment.java
