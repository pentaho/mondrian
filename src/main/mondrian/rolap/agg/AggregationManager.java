/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 30 August, 2001
*/

package mondrian.rolap.agg;
import mondrian.olap.Util;
import mondrian.rolap.RolapAggregationManager;
import mondrian.rolap.RolapStar;
import mondrian.rolap.sql.SqlQuery;

import java.util.*;
import java.sql.SQLException;

/**
 * <code>RolapAggregationManager</code> manages all {@link Aggregation}s
 * in the system. It is a singleton class.
 *
 * @author jhyde
 * @since 30 August, 2001
 * @version $Id$
 **/
public class AggregationManager extends RolapAggregationManager {
	private static AggregationManager instance;
	ArrayList aggregations;

	AggregationManager()
	{
		this.aggregations = new ArrayList();
	}

 	/** Returns or creates the singleton. **/
	public static synchronized RolapAggregationManager instance()
	{
		if (instance == null) {
			instance = new AggregationManager();
		}
		return instance;
	}

	public void loadAggregations(ArrayList batches, Collection pinnedSegments) {
		for (Iterator batchIter = batches.iterator(); batchIter.hasNext();) {
			Batch batch = (Batch) batchIter.next();
			ArrayList requests = batch.requests;
			CellRequest firstRequest = (CellRequest) requests.get(0);
			RolapStar.Column[] columns = firstRequest.getColumns();
			ArrayList measuresList = new ArrayList();
			HashSet[] valueSets = new HashSet[columns.length];
			for (int i = 0; i < valueSets.length; i++) {
				valueSets[i] = new HashSet();
			}
			for (int i = 0, count = requests.size(); i < count; i++) {
				CellRequest request = (CellRequest) requests.get(i);
				for (int j = 0; j < columns.length; j++) {
					Object value = request.getValueList().get(j);
					Util.assertTrue(
						!(value instanceof Object[]),
						"multi-valued key not valid in this cell request");
                    valueSets[j].add(value);
				}
				RolapStar.Measure measure = request.getMeasure();
				if (!measuresList.contains(measure)) {
					if (measuresList.size() > 0) {
						Util.assertTrue(
								measure.table.star ==
								((RolapStar.Measure) measuresList.get(0)).table.star);
					}
					measuresList.add(measure);
				}
			}
			Object[][] constraintses = new Object[columns.length][];
			for (int j = 0; j < columns.length; j++) {
				Object[] constraints;
				HashSet valueSet = valueSets[j];
				if (valueSet == null) {
					constraints = null;
				} else {
					constraints = valueSet.toArray();
				}
				constraintses[j] = constraints;
			}
			// todo: optimize key sets; drop a constraint if more than x% of
			// the members are requested; whether we should get just the cells
			// requested or expand to a n-cube
			RolapStar.Measure[] measures = (RolapStar.Measure[])
					measuresList.toArray(
							new RolapStar.Measure[measuresList.size()]);
			loadAggregation(measures, columns, constraintses, pinnedSegments);
		}
	}

	private synchronized void loadAggregation(
		RolapStar.Measure[] measures, RolapStar.Column[] columns,
		Object[][] constraintses, Collection pinnedSegments)
	{
		RolapStar star = measures[0].table.star;
		Aggregation aggregation = lookupAggregation(star, columns);
		if (aggregation == null) {
			aggregation = new Aggregation(star, columns);
			this.aggregations.add(aggregation);
		}
		constraintses = aggregation.optimizeConstraints(constraintses);
		aggregation.load(measures, constraintses, pinnedSegments);
	}

	/**
	 * Looks for an existing aggregation over a given set of columns, or
	 * returns <code>null</code> if there is none.
	 *
	 * <p>Must be called from synchronized context.
	 **/
	private Aggregation lookupAggregation(
			RolapStar star, RolapStar.Column[] columns)
	{
		for (int i = 0, count = aggregations.size(); i < count; i++) {
			Aggregation aggregation = (Aggregation) aggregations.get(i);
			if (aggregation.star == star &&
					equals(aggregation.columns, columns)) {
				return aggregation;
			}
		}
		return null;
	}

	/** Return whether two arrays of columns are identical. **/
	private static boolean equals(
			RolapStar.Column[] columns1, RolapStar.Column[] columns2) {
		int count = columns1.length;
		if (count != columns2.length) {
			return false;
		}
		for (int j = 0; j < count; j++) {
			if (columns1[j] != columns2[j]) {
				return false;
			}
		}
		return true;
	}

	public Object getCellFromCache(CellRequest request) {
		RolapStar.Measure measure = request.getMeasure();
		Aggregation aggregation = lookupAggregation(
				measure.table.star, request.getColumns());
		if (aggregation == null) {
			return null; // cell is not in any aggregation
		}
		Object o = aggregation.get(
				measure, request.getSingleValues(), null);
		if (o != null) {
			return o;
		}
		throw Util.getRes().newInternal("not found");
	}

	public Object getCellFromCache(CellRequest request, Set pinSet) {
		Util.assertPrecondition(pinSet != null);
		RolapStar.Measure measure = request.getMeasure();
		Aggregation aggregation = lookupAggregation(
				measure.table.star, request.getColumns());
		if (aggregation == null) {
			return null; // cell is not in any aggregation
		}
		return aggregation.get(
				measure, request.getSingleValues(), pinSet);
	}

	public String getDrillThroughSQL(final CellRequest request) {
		return generateSQL(new QuerySpec() {
			public int getMeasureCount() {
				return 1;
			}

			public RolapStar.Measure getMeasure(int i) {
				Util.assertTrue(i == 0);
				return request.getMeasure();
			}

			public RolapStar getStar() {
				return request.getMeasure().table.star;
			}

			public RolapStar.Column[] getColumns() {
				return request.getColumns();
			}

			public Object[] getConstraints(int i) {
				final Object value = request.getValueList().get(i);
				if (value == null) {
					return new Object[0];
				} else {
					return new Object[] {value};
				}
			}
		});
	}

	/**
	 * Generates the query to retrieve the cells for a list of segments.
	 */
	static String generateSQL(Segment[] segments) {
		return generateSQL(new SegmentArrayQuerySpec(segments));
	}

	private static String generateSQL(QuerySpec spec) {
		RolapStar star = spec.getStar();
		SqlQuery sqlQuery;
		try {
			sqlQuery = new SqlQuery(
				star.getJdbcConnection().getMetaData());
		} catch (SQLException e) {
			throw Util.getRes().newInternal("while loading segment", e);
		}
		// add constraining dimensions
		RolapStar.Column[] columns = spec.getColumns();
		int arity = columns.length;
		for (int i = 0; i < arity; i++) {
			RolapStar.Column column = columns[i];
			RolapStar.Table table = column.table;
			if (table.isFunky()) {
				// this is a funky dimension -- ignore for now
				continue;
			}
			table.addToFrom(sqlQuery, false, true);
			String expr = column.getExpression(sqlQuery);
			Object[] constraints = spec.getConstraints(i);
			if (constraints != null) {
				sqlQuery.addWhere(
					expr + " in " + column.quoteValues(constraints));
			}
			sqlQuery.addSelect(expr);
			sqlQuery.addGroupBy(expr);
		}
		// add measures
		for (int i = 0, measureCount = spec.getMeasureCount(); i < measureCount; i++) {
			RolapStar.Measure measure = spec.getMeasure(i);
			Util.assertTrue(measure.table == star.factTable);
			star.factTable.addToFrom(sqlQuery, false, true);
			sqlQuery.addSelect(
				measure.aggregator + "(" + measure.getExpression(sqlQuery) + ")");
		}
		String sql = sqlQuery.toString();
		return sql;
	}

	/**
	 * Contains the information necessary to generate a SQL statement to
	 * retrieve a set of cells.
	 */
	private interface QuerySpec {
		int getMeasureCount();

		RolapStar.Measure getMeasure(int i);

		RolapStar getStar();

		RolapStar.Column[] getColumns();

		Object[] getConstraints(int i);
	}

	/**
	 * Provides the information necessary to generate a SQL statement to
	 * retrieve a list of segments.
	 */
	private static class SegmentArrayQuerySpec implements QuerySpec {
		private final Segment[] segments;

		SegmentArrayQuerySpec(Segment[] segments) {
			this.segments = segments;
			Util.assertPrecondition(segments.length > 0, "segments.length > 0");
			for (int i = 0; i < segments.length; i++) {
				Segment segment = segments[i];
				Util.assertPrecondition(segment.aggregation == segments[0].aggregation);
				int n = segment.axes.length;
				Util.assertTrue(n == segments[0].axes.length);
				for (int j = 0; j < segment.axes.length; j++) {
					// We only require that the two arrays have the same
					// contents, we but happen to know they are the same array,
					// because we constructed them at the same time.
					Util.assertTrue(
							segment.axes[j].constraints ==
							segments[0].axes[j].constraints);
				}
			}
		}

		public int getMeasureCount() {
			return segments.length;
		}

		public RolapStar.Measure getMeasure(int i) {
			return segments[i].measure;
		}

		public RolapStar getStar() {
			return segments[0].aggregation.star;
		}

		public RolapStar.Column[] getColumns() {
			return segments[0].aggregation.columns;
		}

		public Object[] getConstraints(int i) {
			return segments[0].axes[i].constraints;
		}
	}
}

// End RolapAggregationManager.java
