/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 28 August, 2001
*/

package mondrian.rolap.agg;
import mondrian.olap.*;
import mondrian.rolap.*;

import java.util.*;
import java.io.StringWriter;
import java.io.PrintWriter;

/**
 * A <code>Aggregation</code> is a pre-computed aggregation over a set of
 * columns.
 *
 * Rollup operations:<ul>
 * <li>drop an unrestricted column (e.g. state=*)</li>
 * <li>tighten any restriction (e.g. year={1997,1998} becomes
 *     year={1997})</li>
 * <li>restrict an unrestricted column (e.g. year=* becomes
 *     year={1997})</li>
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
 * @version $Id$
 **/
public class Aggregation
{
	RolapStar star;
	RolapStar.Measure measure;
	RolapStar.Column[] columns;
	ArrayList segments;

	Aggregation(
		RolapStar star, RolapStar.Measure measure, RolapStar.Column[] columns)
	{
		this.star = star;
		this.measure = measure;
		this.columns = columns;
		this.segments = new ArrayList();
	}

	/**
	 * Loads a new aggregation.
	 */
	void load(Object[][] constraintses, Collection pinnedSegments)
	{
		Segment segment = new Segment(this, constraintses);
		segments.add(segment);
		int pinCount = 1;
		CachePool.instance().register(segment, pinCount, pinnedSegments);
	}

	/**
	 * Drops constraints, where the list of values is close to the values which
	 * would be returned anyway.
	 **/
	Object[][] optimizeConstraints(Object[][] constraintses)
	{
		Util.assertTrue(constraintses.length == columns.length);
		Object[][] newConstraintses = (Object[][]) constraintses.clone();
		// build a list of constraints sorted by 'bloat factor'
		ConstraintComparator comparator = new ConstraintComparator(
			constraintses);
		Integer[] indexes = new Integer[columns.length];
		double cellCount = 1.0;
		for (int i = 0; i < columns.length; i++) {
			indexes[i] = new Integer(i);
			cellCount *= comparator.getCardinality(i);
		}
		Arrays.sort(indexes, comparator);
		// eliminate constraints one by one, until the estimated cell count
		// doubles and is greater than 100
		double originalCellCount = cellCount,
			maxCellCount = originalCellCount * 2 + 10;
		for (int i = 0; i < indexes.length; i++) {
			int j = ((Integer) indexes[i]).intValue();
			double bloat = comparator.getBloat(j);
			cellCount *= bloat;
			if (cellCount < maxCellCount) {
				// eliminate this constraint
				newConstraintses[j] = null;
			} else {
				break;
			}
		}
		return newConstraintses;
	}

	private class ConstraintComparator implements Comparator {
		Object[][] constraintses;
		ConstraintComparator(Object[][] constraintses)
		{
			this.constraintses = constraintses;
		}
		// implement Comparator
		public int compare(Object o0, Object o1)
		{
			double bloat0 = getBloat(o0),
				bloat1 = getBloat(o1);
			if (bloat0 == bloat1) {
				return 0;
			} else if (bloat0 < bloat1) {
				return -1;
			} else {
				return 1;
			}
		}
		double getBloat(Object o)
		{
			return getBloat(((Integer) o).intValue());
		}
		double getBloat(int i)
		{
			Object[] constraints = constraintses[i];
			if (constraints == null) {
				return 1.0;
			}
			RolapStar.Column column = columns[i];
			int cardinality = column.getCardinality();
			return ((double) cardinality) / ((double) constraints.length);
		}
		/** Returns the cardinality of this column, assuming that the
		 * constraint is not removed. **/
		double getCardinality(int i)
		{
			Object[] constraints = constraintses[i];
			if (constraints == null) {
				RolapStar.Column column = columns[i];
				return column.getCardinality();
			} else {
				return constraints.length;
			}
		}
	};

	Object[][] obsolete_optimizeConstraints(Object[][] constraintses)
	{
		// We currently drop constraints which would retrieve more than about
		// half of the column values (actually (cardinality - 5) / 2). But if
		// each of 10 dimensions caused a 2x bloat, that would bloat the
		// aggregation by 1000x, and that would be a problem. So we ought to
		// limit the total bloat to say 10x, and drop the constraints which
		// would cause the least bloat.
		Util.assertTrue(constraintses.length == columns.length);
		Object[][] newConstraintses = (Object[][]) constraintses.clone();
		for (int i = 0; i < columns.length; i++) {
			Object[] constraints = constraintses[i];
			RolapStar.Column column = columns[i];
			if (constraints == null) {
				continue; // this column is already unconstrained
			}
			int cardinality = column.getCardinality();
			if (cardinality * 2 + 5 > constraints.length) {
				newConstraintses[i] = null;
			}
		}
		return newConstraintses;
	}

	// implement CellReader
//	public Object get(Evaluator evaluator)
//	{
//		RolapEvaluator rolapEvaluator = (RolapEvaluator) evaluator;
//		CellRequest request =
//			RolapAggregationManager.instance().makeRequest(
//				rolapEvaluator.currentMembers);
//		Object[] keys = request.getSingleValues();
//		return get(keys);
//	}

	Object get(Object[] keys)
	{
		for (int i = 0, count = segments.size(); i < count; i++) {
			Segment segment = (Segment) segments.get(i);
			Object o = segment.get(keys);
			if (o != null) {
				// 'Util.nullValue' means right segment, but no fact table rows
				// exist at that coordinate, hence the total is null; 'null'
				// means the value wouldn't be in the segment
				return o;
			}
		}
		throw Util.getRes().newInternal("not found");
	}

	/**
	 * Retrieves the value identified by <code>keys</code>, and pins the
	 * segment which holds it. <code>pinSet</code> ensures that a segment is
	 * only pinned once. Returns <code>null</code> if no segment contains the
	 * cell.
	 **/
	Object getAndPin(Object[] keys, Collection pinSet)
	{
		for (int i = 0, count = segments.size(); i < count; i++) {
			Segment segment = (Segment) segments.get(i);
			Object o = segment.get(keys);
			if (o != null) {
				if (!pinSet.contains(segment)) {
					CachePool.instance().pin(segment, pinSet);
				}
				return o;
			}
		}
		return null;
	}

	// -- classes -------------------------------------------------------------

	public static class Axis
	{
		RolapStar.Column column;
		Object[] constraints; // null if no constraint
		Object[] keys; // actual keys retrieved
		Hashtable mapKeyToOffset; // inversion of keys

		boolean contains(Object key)
		{
			if (constraints == null) {
				return true;
			}
			for (int i = 0; i < constraints.length; i++) {
				if (constraints[i].equals(key)) {
					return true;
				}
			}
			return false;
		}
		double getBytes()
		{
			return 16 + 8 * keys.length;
		}
	}
}

// End Aggregation.java
