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

import mondrian.rolap.CachePool;
import mondrian.rolap.RolapStar;
import mondrian.rolap.RolapUtil;
import mondrian.rolap.sql.SqlQuery;
import mondrian.olap.Util;

import java.util.Hashtable;
import java.util.Vector;
import java.util.Enumeration;
import java.io.StringWriter;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;

/**
 * <code>Segment</code> todo:
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

	Aggregation.Axis[] axes;
	private SegmentDataset data;
	private int[] pos; // workspace
	private double recency; // when was this segment last used?
	private int pinCount;

	/**
	 * Creates a <code>Segment</code>.
	 * @param aggregation The aggregation that this <code>Segment</code>
	 *    belongs to
	 * @param constraintses For each column, either an array of values
	 *    to fetch or null, indicating that the column is unconstrained
	 **/
	Segment(Aggregation aggregation, Object[][] constraintses)
	{
		this.id = nextId++;
		this.aggregation = aggregation;
		int axisCount = aggregation.columns.length;
		Util.assertTrue(constraintses.length == axisCount);
		this.axes = new Aggregation.Axis[axisCount];
		for (int i = 0; i < axisCount; i++) {
			Aggregation.Axis axis =
				this.axes[i] = new Aggregation.Axis();
			axis.column = aggregation.columns[i];
			axis.constraints = constraintses[i];
			axis.mapKeyToOffset = new Hashtable();
		}
		this.pos = new int[axisCount];
		this.data = load();
		this.desc = makeDescription();
	}

	private String makeDescription()
	{
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		pw.print("Segment #" + id + " {measure=" + aggregation.measure.aggregator +
			"("	+ aggregation.measure.name + ")");
		for (int i = 0; i < aggregation.columns.length; i++) {
			pw.print(", ");
			pw.print(aggregation.columns[i].name);
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
		boolean existed = aggregation.segments.remove(this);
		Util.assertTrue(existed);
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
		double bytes = 0;
		for (int i = 0; i < axes.length; i++) {
			bytes += axes[i].getBytes();
		}
		bytes += data.getBytes();
		return bytes;
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
	 * Reads a segment of <code>measure</code>, where <code>columns</code> are
	 * constrained to <code>values</code>.  Each entry in <code>values</code>
	 * can be null, meaning don't constrain, or can have several values. For
	 * example, <code>getSegment({Unit_sales}, {Region, State, Year}, {"West",
	 * {"CA", "OR", "WA"}, null})</code> returns sales in states CA, OR and WA
	 * in the Western region, for all years.
	 **/
	SegmentDataset load()
	{
		RolapStar star = aggregation.star;
		RolapStar.Measure measure = aggregation.measure;
		RolapStar.Column[] columns = aggregation.columns;
		int arity = columns.length;
		SqlQuery sqlQuery;
		try {
			sqlQuery = new SqlQuery(
				star.getJdbcConnection().getMetaData());
		} catch (SQLException e) {
			throw Util.getRes().newInternal(e, "while loading segment");
		}
		// add constraining dimensions
		for (int i = 0; i < arity; i++) {
			Object[] constraints = axes[i].constraints;
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
		// add measure
		Util.assertTrue(measure.table == star.factTable);
		star.factTable.addToFrom(sqlQuery, false, true);
		sqlQuery.addSelect(
			measure.aggregator + "(" + measure.getExpression(sqlQuery) + ")");
		// execute
		String sql = sqlQuery.toString();
		ResultSet resultSet = null;
		try {
			resultSet = RolapUtil.executeQuery(
					star.getJdbcConnection(), sql, "Segment.load");
			Vector rows = new Vector();
			while (resultSet.next()) {
				Object[] row = new Object[arity + 1];
				// get the columns
				for (int i = 0; i < arity; i++) {
					Object o = resultSet.getObject(i + 1);
					Hashtable h = this.axes[i].mapKeyToOffset;
					Integer offsetInteger = (Integer) h.get(o);
					if (offsetInteger == null) {
						h.put(o, new Integer(h.size()));
					}
					row[i] = o;
				}
				// get the measure
				Object o = resultSet.getObject(arity + 1);
				if (o == null) {
					o = Util.nullValue; // convert to placeholder
				}
				row[arity] = o;
				rows.addElement(row);
			}
			// figure out size of dense array, and allocate it (todo: use
			// sparse array sometimes)
			int n = 1;
			for (int i = 0; i < arity; i++) {
				Aggregation.Axis axis = this.axes[i];
				int size = axis.mapKeyToOffset.size();
				axis.keys = new Object[size];
				Enumeration keys = axis.mapKeyToOffset.keys();
				while (keys.hasMoreElements()) {
					Object o = keys.nextElement();
					Integer offsetInteger = (Integer)
						axis.mapKeyToOffset.get(o);
					int offset = offsetInteger.intValue();
					Util.assertTrue(axis.keys[offset] == null);
					axis.keys[offset] = o;
				}
				n *= size;
			}
			Object[] values = new Object[n];
			// now convert the rows into a dense array
			for (int i = 0, count = rows.size(); i < count; i++) {
				Object[] row = (Object[]) rows.elementAt(i);
				int k = 0;
				for (int j = 0; j < arity; j++) {
					k *= this.axes[j].keys.length;
					Object o = row[j];
					Aggregation.Axis axis = this.axes[j];
					Integer offsetInteger = (Integer)
						axis.mapKeyToOffset.get(o);
					int offset = offsetInteger.intValue();
					k += offset;
				}
				values[k] = row[arity];
			}
			DenseSegmentDataset data = new DenseSegmentDataset();
			data.segment = this;
			data.values = values;
			return data;
		} catch (SQLException e) {
			throw Util.getRes().newInternal(
				e, "while loading segment; sql=[" + sql + "]");
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
}

// End Segment.java
