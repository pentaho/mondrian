/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2001-2003 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 28 August, 2001
*/

package mondrian.rolap.agg;
import mondrian.olap.Util;
import mondrian.rolap.RolapStar;
import mondrian.rolap.cache.CachePool;
import mondrian.rolap.cache.SoftCacheableReference;
import mondrian.rolap.sql.SqlQuery;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.*;

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
 * <b>Note to developers</b>: {@link Segment} implements
 * {@link CachePool.Cacheable}, and must adhere to the contract that imposes.
 * For this class, that means that references to segments must be made using
 * soft references (see {@link CachePool.SoftCacheableReference}) so that they
 * can be garbage-collected.
 *
 * @author jhyde
 * @since 28 August, 2001
 * @version $Id$
 **/
public class Aggregation
{
	RolapStar star;
	RolapStar.Column[] columns;
	/** List of soft references to segments. **/
	List segmentRefs;
	boolean oracle = false;

	Aggregation(RolapStar star, RolapStar.Column[] columns) {
		this.star = star;
		this.columns = columns;
		this.segmentRefs = Collections.synchronizedList(new ArrayList());

		// find out if this is an oracle DB
		Connection con = null;
		try {
			con = star.getJdbcConnection();
			DatabaseMetaData md = con.getMetaData();
			SqlQuery sqlQuery = new SqlQuery(md);
			this.oracle = sqlQuery.isOracle();
		} catch (SQLException e) {
			throw Util.newInternal(e, "could not query Metadata");
		} finally {
			if (con != null) {
				try {
					con.close();
				} catch (SQLException e) {
					// ignore
				}
			}
		}
	}

	/**
	 * Loads a set of segments into this aggregation, one per measure,
	 * each constrained by the same set of column values, and each pinned
	 * once.
	 *
	 * For example,
	 *   measures = {unit_sales, store_sales},
	 *   state = {CA, OR},
	 *   gender = unconstrained
	 */
	public synchronized void load(
			RolapStar.Measure[] measures, Object[][] constraintses,
			Collection pinnedSegments) {
		Segment[] segments = new Segment[measures.length];
		final CachePool cachePool = CachePool.instance();
		for (int i = 0; i < measures.length; i++) {
			RolapStar.Measure measure = measures[i];
			Segment segment = new Segment(this, measure, constraintses);
			segments[i] = segment;
			SoftCacheableReference ref =
					new SoftCacheableReference(segment);
			this.segmentRefs.add(ref);
			final int pinCount = 1;
			cachePool.register(segment, pinCount, pinnedSegments);
		}
		Segment.load(segments, pinnedSegments);
	}

	/**
	 * we do NOT optimize, we fetch exactly what we need - which performs better
	 * than counting the number of rows and fetching more cells than requested.
	 */
	public synchronized Object[][] optimizeConstraints(Object[][] constraintses)
	{
		Util.assertTrue(constraintses.length == columns.length);
		Object[][] newConstraintses = (Object[][]) constraintses.clone();
		// Oracle can only handle up to 1000 elements inside an IN(..) clause
		if (oracle) {
			final int MAXLEN = 1000;
			for (int i = 0; i < newConstraintses.length; i++) {
				Object[] arr = newConstraintses[i];
				if (arr != null && arr.length > MAXLEN) {
					// FIXME should fetch 1000 and make sure that the rest is fetched later
					newConstraintses[i] = null;
				}
			}
		}
		
		return newConstraintses;
	}
	
	/**
	 * Drops constraints, where the list of values is close to the values which
	 * would be returned anyway.
	 **/
	/*
	public synchronized Object[][] optimizeConstraints(Object[][] constraintses)
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

		// Oracle can only handle up to 1000 elements inside an IN(..) clause
		if (oracle) {
			final int MAXLEN = 1000;
			for (int i = 0; i < newConstraintses.length; i++) {
				Object[] arr = newConstraintses[i];
				if (arr != null && arr.length > MAXLEN) {
					double bloat = comparator.getBloat(i);
					cellCount *= bloat;
					newConstraintses[i] = null;
				}
			}
		}
		
		for (int i = 0; i < indexes.length; i++) {
			int j = indexes[i].intValue();
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
		// Returns the cardinality of this column, assuming that the
		// constraint is not removed.
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
	}
    */

	/**
	 * Retrieves the value identified by <code>keys</code>.
	 *
	 * <p>If <code>pinSet</code> is not null, pins the
	 * segment which holds it. <code>pinSet</code> ensures that a segment is
	 * only pinned once.
	 *
	 * Returns <code>null</code> if no segment contains the cell.
	 **/
	public synchronized Object get(
			RolapStar.Measure measure, Object[] keys, Collection pinSet) {
		
		// if we dont synchronize here, the CachePool.flushIfNecessary may
		// remove elements from segmentRefs while we are iterating. This
		// would cause a ConcurrentModificationException.

		synchronized (CachePool.instance()) {
			for (Iterator it = segmentRefs.iterator(); it.hasNext();) {
				SoftCacheableReference ref = (SoftCacheableReference)it.next();
				Segment segment = (Segment) ref.getCacheable();
				if (segment == null) {
					it.remove();
					continue; // it's being garbage-collected
				}
				if (segment.measure != measure) {
					continue;
				}
				if (segment.isReady()) {
					Object o = segment.get(keys);
					if (o != null) {
						if (pinSet != null) {
							CachePool.instance().pin(segment, pinSet);
						}
						return o;
					}
				} else {
					if (segment.wouldContain(keys)) {
						if (pinSet != null) {
							CachePool.instance().pin(segment, pinSet);
						}
						return null;
					}
				}
			}
		}
		return null;
	}


	/** must <em>not</em> be synchronized */
    void removeSegment(Segment segment) {
        segmentRefs.remove(new SoftCacheableReference(segment));
    }

    // -- classes -------------------------------------------------------------

	public static class Axis
	{
		RolapStar.Column column;
		Object[] constraints; // null if no constraint
		Object[] keys; // actual keys retrieved
		HashMap mapKeyToOffset; // inversion of keys

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
			if (keys == null) {
				return 0;
			}
			return 16 + 8 * keys.length;
		}
	}
}

// End Aggregation.java
