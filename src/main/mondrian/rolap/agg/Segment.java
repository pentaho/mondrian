/*
// $Id$
// (C) Copyright 2002 Kana Software, Inc.
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 21 March, 2002
*/
package mondrian.rolap.agg;

import mondrian.olap.Util;
import mondrian.rolap.CachePool;
import mondrian.rolap.RolapStar;
import mondrian.rolap.RolapUtil;
import mondrian.rolap.sql.SqlQuery;

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
	private int[] pos; // workspace
	private double recency; // when was this segment last used?
	private int pinCount;
	private double cost;

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
		this.pos = new int[axisCount];
		this.desc = makeDescription();
	}

	protected void finalize() {
		CachePool.instance().deregister(this, true); // per Cacheable contract
	}

	/**
	 * Sets the data, and notifies any threads which are blocked in
	 * {@link #waitUntilLoaded}.
	 */
	synchronized void setData(
			DenseSegmentDataset data, Aggregation.Axis[] axes,
			Collection pinnedSegments) {
		Util.assertTrue(this.data == null);
		this.axes = axes;
		this.data = data;
		adjustCost();
		notifyAll();
	}

	/**
	 * Must be called from synchronized context.
	 */
	private synchronized void adjustCost() {
		double previousCost = cost;
		cost = 0;
		if (axes != null) {
			for (int i = 0; i < axes.length; i++) {
				cost += axes[i].getBytes();
			}
		}
		if (data != null) {
			cost += data.getBytes();
		}
		CachePool.instance().notify(this, previousCost);
	}

	public boolean isLoaded() {
		return data != null;
	}

	private String makeDescription()
	{
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		pw.print("Segment #" + id + " {measure=" + measure.aggregator +
			"("	+ measure.expression.getGenericExpression() + ")");
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

	// implement CachePool.Cacheable
	public void removeFromCache()
	{
		boolean existed = aggregation.segmentRefs.remove(
				new CachePool.SoftCacheableReference(this));
		Util.assertTrue(
				existed,
				"removeFromCache: Segment is not registered with its Aggregator");
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
			pos[i] = integer.intValue();
		}
		if (missed > 0) {
			// the value should be in this segment, but isn't, because one
			// or more of its keys does have any values
			return Util.nullValue;
		} else {
			Object o = data.get(pos);
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
	 * example, <code>getSegment({Unit_sales}, {Region, State, Year}, {"West",
	 * {"CA", "OR", "WA"}, null})</code> returns sales in states CA, OR and WA
	 * in the Western region, for all years.
	 *
	 * @pre segments[i].aggregation == aggregation
	 **/
	static void load(Segment[] segments, Collection pinnedSegments) {
		Segment segment0 = segments[0];
		RolapStar star = segment0.aggregation.star;
		RolapStar.Column[] columns = segment0.aggregation.columns;
		int arity = columns.length;
		SqlQuery sqlQuery;
		try {
			sqlQuery = new SqlQuery(
				star.getJdbcConnection().getMetaData());
		} catch (SQLException e) {
			throw Util.getRes().newInternal("while loading segment", e);
		}
		// add constraining dimensions
		for (int i = 0; i < arity; i++) {
			Object[] constraints = segments[0].axes[i].constraints;
			RolapStar.Column column = columns[i];
			RolapStar.Table table = column.table;
			if (table.isFunky()) {
				// this is a funky dimension -- ignore for now
				continue;
			}
			table.addToFrom(sqlQuery, false, true);
			String expr = column.getExpression(sqlQuery);
			if (constraints != null) {
				sqlQuery.addWhere(
					expr + " in " + column.quoteValues(constraints));
			}
			sqlQuery.addSelect(expr);
			sqlQuery.addGroupBy(expr);
		}
		// add measures
		for (int i = 0; i < segments.length; i++) {
			Segment segment = segments[i];
			RolapStar.Measure measure = segment.measure;
			Util.assertTrue(measure.table == star.factTable);
			if (i > 0) {
				Util.assertTrue(segment.aggregation == segment0.aggregation);
				int n = segment.axes.length;
				Util.assertTrue(n == segment0.axes.length);
				for (int j = 0; j < segment.axes.length; j++) {
					// We only require that the two arrays have the same
					// contents, we but happen to know they are the same array,
					// because we constructed them at the same time.
					Util.assertTrue(
							segment.axes[j].constraints ==
							segment0.axes[j].constraints);
				}
			}
			star.factTable.addToFrom(sqlQuery, false, true);
			sqlQuery.addSelect(
				measure.aggregator + "(" + measure.getExpression(sqlQuery) + ")");
		}
		// execute
		String sql = sqlQuery.toString();
		ResultSet resultSet = null;
		final int measureCount = segments.length;
		try {
			resultSet = RolapUtil.executeQuery(
					star.getJdbcConnection(), sql, "Segment.load");
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
					Segment segment = segments[i];
					Object o = resultSet.getObject(k++);
					if (o == null) {
						o = Util.nullValue; // convert to placeholder
					}
					row[arity] = o;
				}
				rows.add(row);
			}
			// figure out size of dense array, and allocate it (todo: use
			// sparse array sometimes)
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
				n *= size;
			}
			DenseSegmentDataset[] datas = new DenseSegmentDataset[segments.length];
			for (int i = 0; i < segments.length; i++) {
				DenseSegmentDataset data = new DenseSegmentDataset();
				datas[i] = data;
				data.segment = segments[i];
				data.values = new Object[n];
			}
			// now convert the rows into a dense array
			for (int i = 0, count = rows.size(); i < count; i++) {
				Object[] row = (Object[]) rows.get(i);
				int k = 0;
				for (int j = 0; j < arity; j++) {
					k *= segment0.axes[j].keys.length;
					Object o = row[j];
					Aggregation.Axis axis = segment0.axes[j];
					Integer offsetInteger = (Integer)
						axis.mapKeyToOffset.get(o);
					int offset = offsetInteger.intValue();
					k += offset;
				}
				for (int j = 0; j < segments.length; j++) {
					datas[j].values[k] = row[arity + j];
				}
			}
			for (int i = 0; i < segments.length; i++) {
				Segment segment = segments[i];
				segment.setData(datas[i], segments[0].axes, pinnedSegments);
			}
		} catch (SQLException e) {
			throw Util.getRes().newInternal(
					"while loading segment; sql=[" + sql + "]", e);
		} finally {
			try {
				if (resultSet != null) {
					resultSet.getStatement().close();
					resultSet.close();
				}
			} catch (SQLException e) {
				// ignore
			}
		}
	}

	/**
	 * Blocks until this segment has finished loading; if this segment has
	 * already loaded, returns immediately.
	 */
	public synchronized void waitUntilLoaded() {
		if (!isLoaded()) {
			try {
				wait();
			} catch (InterruptedException e) {
			}
			Util.assertTrue(isLoaded());
		}
	}
}

// End Segment.java
