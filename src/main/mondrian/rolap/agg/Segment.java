/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2002-2003 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 21 March, 2002
*/
package mondrian.rolap.agg;

import mondrian.olap.EnumeratedValues;
import mondrian.olap.MondrianProperties;
import mondrian.olap.Util;
import mondrian.rolap.CachePool;
import mondrian.rolap.CellKey;
import mondrian.rolap.RolapStar;
import mondrian.rolap.RolapUtil;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;

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
 * <p><b>Note to developers</b>: this class must obey the contract for
 * objects which implement {@link CachePool.Cacheable}.
 *
 * @author jhyde
 * @since 21 March, 2002
 * @version $Id$
 **/
public class Segment implements CachePool.Cacheable
{
	private int id; // for debug
	private static int nextId = 0; // generator for "id"
	private String desc;
	Aggregation aggregation;
	RolapStar.Measure measure;

	Aggregation.Axis[] axes;
	private SegmentDataset data;
	private CellKey cellKey; // workspace
	private double recency; // when was this segment last used?
	private int pinCount;
	private double cost;
	/** State of the segment, values are described by {@link State}. */
	private int state;

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

	/**
	 * Creates a <code>Segment</code>; it's not loaded yet.
	 *
	 * @param aggregation The aggregation that this <code>Segment</code>
	 *    belongs to
	 * @param constraintses For each column, either an array of values
	 *    to fetch or null, indicating that the column is unconstrained
	 **/
	Segment(Aggregation aggregation, RolapStar.Measure measure,
			Object[][] constraintses) {
		this.id = nextId++;
		this.aggregation = aggregation;
		this.measure = measure;
		int axisCount = aggregation.columns.length;
		Util.assertTrue(constraintses.length == axisCount);
		this.axes = new Aggregation.Axis[axisCount];
		for (int i = 0; i < axisCount; i++) {
			Aggregation.Axis axis =
				this.axes[i] = new Aggregation.Axis();
			axis.column = aggregation.columns[i];
			axis.constraints = constraintses[i];
			axis.mapKeyToOffset = new HashMap();
		}
		this.cellKey = new CellKey(new int[axisCount]);
		this.desc = makeDescription();
		this.state = State.Loading;
	}

	protected void finalize() {
		CachePool.instance().deregister(this, true); // per Cacheable contract
	}

	/**
	 * Sets the data, and notifies any threads which are blocked in
	 * {@link #waitUntilLoaded}.
	 */
	synchronized void setData(
			SegmentDataset data, Aggregation.Axis[] axes,
			Collection pinnedSegments) {
		Util.assertTrue(this.data == null);
		Util.assertTrue(this.state == State.Loading);
		this.axes = axes;
		this.data = data;
		this.state = State.Ready;
		adjustCost();
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
			adjustCost();
			notifyAll();
			break;
		case State.Ready:
			// The segment loaded just fine.
			break;
		default:
			throw State.instance.badValue(state);
		}
	}

	/**
	 * Must be called from synchronized context.
	 */
	private synchronized void adjustCost() {
		double newCost = 0;
		if (axes != null) {
			for (int i = 0; i < axes.length; i++) {
				newCost += axes[i].getBytes();
			}
		}
		if (data != null) {
			newCost += data.getBytes();
		}
		if (state == State.Failed) {
			newCost += 1000000; // large value, will cause it to fall out of cache
		}
		double previousCost = cost;
		cost = newCost;
		CachePool.instance().notify(this, previousCost);
	}

	public boolean isReady() {
		return state == State.Ready;
	}

	private String makeDescription()
	{
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		pw.print("Segment #" + id + " {measure=" +
                measure.aggregator.getExpression(
                        measure.expression.getGenericExpression()));
		for (int i = 0; i < aggregation.columns.length; i++) {
			pw.print(", ");
			pw.print(aggregation.columns[i].expression.getGenericExpression());
			Object[] constraints = axes[i].constraints;
			if (constraints == null) {
				pw.print("=any");
			} else {
				pw.print("={");
				for (int j = 0; j < constraints.length; j++) {
					if (j > 0) {
						pw.print(", ");
					}
					Object o = constraints[j];
					pw.print(o);
				}
				pw.print("}");
			}
		}
		pw.print("}");
		pw.flush();
		return sw.toString();
	}

	public String toString()
	{
		return desc;
	}

    public void removeFromCache() {
        aggregation.removeSegment(this);
    }

    // implement CachePool.Cacheable
	public Object getKey()
	{
		return this;
	}
	// implement CachePool.Cacheable
	public void markAccessed(double recency)
	{
		this.recency = recency;
	}
	// implement CachePool.Cacheable
	public void setPinCount(int pinCount)
	{
//		System.out.println("Segment: pinCount=" + pinCount + " (was " + this.pinCount + ")");
		this.pinCount = pinCount;
	}
	// implement CachePool.Cacheable
	public int getPinCount()
	{
		return pinCount;
	}
	// implement CachePool.Cacheable
	public double getScore()
	{
		double benefit = getBenefit(),
			cost = getCost();
		return benefit / cost * recency;
	}
	// implement CachePool.Cacheable
	public double getCost()
	{
		return cost;
	}
	private double getBenefit()
	{
//			throw new UnsupportedOperationException();
		return 16;
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
	Object get(Object[] keys)
	{
		Util.assertTrue(keys.length == axes.length);
		int missed = 0;
		for (int i = 0; i < keys.length; i++) {
			Object key = keys[i];
			Integer integer = (Integer) axes[i].mapKeyToOffset.get(key);
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
	boolean wouldContain(Object[] keys)
	{
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
	static void load(Segment[] segments, Collection pinnedSegments) {
		String sql = AggregationManager.generateSQL(segments);
		Segment segment0 = segments[0];
		RolapStar star = segment0.aggregation.star;
		RolapStar.Column[] columns = segment0.aggregation.columns;
		int arity = columns.length;
		// execute
		ResultSet resultSet = null;
		final int measureCount = segments.length;
		java.sql.Connection jdbcConnection = star.getJdbcConnection();
		try {
			resultSet = RolapUtil.executeQuery(
					jdbcConnection, sql, "Segment.load");
			ArrayList rows = new ArrayList();
			while (resultSet.next()) {
				Object[] row = new Object[arity + measureCount];
				// get the columns
				int k = 1;
				for (int i = 0; i < arity; i++) {
					Object o = resultSet.getObject(k++);
					HashMap h = segment0.axes[i].mapKeyToOffset;
					Integer offsetInteger = (Integer) h.get(o);
					if (offsetInteger == null) {
						h.put(o, new Integer(h.size()));
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
				Aggregation.Axis axis = segment0.axes[i];
				int size = axis.mapKeyToOffset.size();
				axis.keys = new Object[size];
				for (Iterator keys = axis.mapKeyToOffset.keySet().iterator();
					 keys.hasNext();) {
					Object key = keys.next();
					Integer offsetInteger = (Integer) axis.mapKeyToOffset.get(key);
					int offset = offsetInteger.intValue();
					Util.assertTrue(axis.keys[offset] == null);
					axis.keys[offset] = key;
				}
				int previous = n;
				n *= size;
				if (n < previous || n < size) {
					// Overflow has occurred.
					n = Integer.MAX_VALUE;
					sparse = true;
				}
			}
			SegmentDataset[] datas = new SegmentDataset[segments.length];
			sparse = sparse || useSparse((double) n, (double) rows.size());
			for (int i = 0; i < segments.length; i++) {
				datas[i] = sparse ?
						(SegmentDataset) new SparseSegmentDataset(segments[i]) :
						new DenseSegmentDataset(segments[i], new Object[n]);
			}
			// now convert the rows into a sparse array
			int[] pos = new int[arity];
			for (int i = 0, count = rows.size(); i < count; i++) {
				Object[] row = (Object[]) rows.get(i);
				int k = 0;
				for (int j = 0; j < arity; j++) {
					k *= segment0.axes[j].keys.length;
					Object o = row[j];
					Aggregation.Axis axis = segment0.axes[j];
					Integer offsetInteger = (Integer) axis.mapKeyToOffset.get(o);
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
				segments[i].setData(datas[i], segments[0].axes, pinnedSegments);
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
	 * {@link MondrianProperties#getSparseSegmentCountThreshold here}.
	 *
	 * @param possibleCount Number of values in the space.
	 * @param actualCount Actual number of values.
	 * @return Whether to use a sparse representation.
	 */
	private static boolean useSparse(final double possibleCount,
									 final double actualCount) {
		final MondrianProperties properties = MondrianProperties.instance();
		double densityThreshold = properties.getSparseSegmentDensityThreshold();
		if (densityThreshold < 0) {
			densityThreshold = 0;
		}
		if (densityThreshold > 1) {
			densityThreshold = 1;
		}
		int countThreshold = properties.getSparseSegmentCountThreshold();
		if (countThreshold < 0) {
			countThreshold = 0;
		}
		boolean sparse =
			(possibleCount - countThreshold) * densityThreshold > actualCount;
		if (possibleCount < countThreshold) {
			Util.assertTrue(!sparse);
		}
		if (possibleCount == actualCount) {
			Util.assertTrue(!sparse);
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
				throw Util.newError("Pending segment failed to load: " + desc);
			default:
				throw State.instance.badValue(state);
			}
		}
	}
}

// End Segment.java
