/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2001-2003 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 30 August, 2001
*/

package mondrian.rolap;

import mondrian.olap.Util;
import mondrian.olap.Evaluator;
import mondrian.rolap.agg.CellRequest;
import mondrian.rolap.agg.Segment;

import java.util.*;

/**
 * <code>RolapAggregationManager</code> manages all {@link
 * mondrian.rolap.agg.Aggregation}s in the system. It is a singleton class.
 *
 * <p> The bits of the implementation which depend upon dimensional concepts
 * <code>RolapMember</code>, etc.) live in this class, and the other bits live
 * in the derived class, {@link mondrian.rolap.agg.AggregationManager}.
 *
 * @author jhyde
 * @since 30 August, 2001
 * @version $Id$
 **/
public abstract class RolapAggregationManager implements CellReader {

	/**
	 * Looks through a set of requests and loads aggregations
	 * accordingly.
	 *
	 * @param keySet A set whose keys are {@link ArrayList}s
	 *     which contain {@link mondrian.olap.Member}s
	 * @param pinnedSegments Writes each loaded aggregation into here. The client must
	 *     call {@link CachePool#unpin} on this list.
	 **/
	public void loadAggregations(Set keySet, Collection pinnedSegments)
	{
		RolapMember[] members = RolapUtil.emptyMemberArray;
		Hashtable mapColumnSetToBatch = new Hashtable();
		ArrayList batches = new ArrayList();
		for (Iterator keys = keySet.iterator(); keys.hasNext(); ) {
			ArrayList key = (ArrayList) keys.next();
			members = (RolapMember[]) key.toArray(members);
			CellRequest request = makeRequest(members);
			if (request == null) {
				continue; // invalid location -- ignore it
			}
			ArrayList columnList = request.getBatchKey();
			Batch batch = (Batch) mapColumnSetToBatch.get(columnList);
			if (batch == null) {
				batch = new Batch();
				mapColumnSetToBatch.put(columnList, batch);
				batches.add(batch);
			}
			batch.requests.add(request);
		}
		loadAggregations(batches, pinnedSegments);
		for (Iterator segmentIter = pinnedSegments.iterator(); segmentIter.hasNext();) {
			Segment segment = (Segment) segmentIter.next();
			segment.waitUntilLoaded();
		}
	}

	public abstract void loadAggregations(ArrayList batches, Collection pinnedSegments);

	/**
	 * Creates a request to evaluate the cell identified by
	 * <code>members</code>. If any of the members is the null member, returns
	 * null, since there is no cell. If the measure is calculated, returns
	 * null. 
	 **/
	CellRequest makeRequest(RolapMember[] members) {
		if (!(members[0] instanceof RolapStoredMeasure)) {
			return null;
		}
		RolapStoredMeasure measure = (RolapStoredMeasure) members[0];
		final RolapStar.Measure starMeasure = (RolapStar.Measure)
				measure.starMeasure;
		Util.assertTrue(starMeasure != null);
		RolapStar star = starMeasure.table.star;
		CellRequest request = new CellRequest(starMeasure);
		HashMap mapLevelToColumn = (HashMap) star.mapCubeToMapLevelToColumn.get(measure.cube);
		for (int i = 1; i < members.length; i++) {
			RolapMember member = members[i];
			RolapLevel previousLevel = null;
			for (RolapMember m = member; m != null; m = (RolapMember)
					 m.getParentMember()) {
				if (m.key == null) {
					if (m == m.getHierarchy().getNullMember()) {
						// cannot form a request if one of the members is null
						return null;
					} else if (m.isAll()) {
						continue;
					} else {
						throw Util.getRes().newInternal("why is key null?");
					}
				}
				RolapLevel level = (RolapLevel) m.getLevel();
				if (level == previousLevel) {
					// We are looking at a parent in a parent-child hierarchy,
					// for example, we have moved from Fred to Fred's boss,
					// Wilma. We don't want to include Wilma's key in the
					// request.
					continue;
				}
				previousLevel = level;
				RolapStar.Column column = (RolapStar.Column) mapLevelToColumn.get(level);
				if (column == null) {
					// This hierarchy is not one which qualifies the starMeasure (this happens in
					// virtual cubes). The starMeasure only has a value for the 'all' member of
					// the hierarchy (which this is not).
					return null;
				} else {
					request.addConstrainedColumn(column, m.key);
				}
			}
		}
		return request;
	}
	
	/**
	 * Creates a request to evaluate the cell identified by
	 * <code>members</code>.
	 * If any of the members is an "All" member, then it exapnds that member
	 * adding all of the levels of that member to the request.
	 * If any of the members is the null member, returns
	 * null, since there is no cell. If the measure is calculated, returns null. 
	 * @param members Array of RolapMembers that identify this cell.
	 * @return a CellRequest object for the cell
	 */
	CellRequest makeDrillThroughRequest(
							RolapMember[] members) {
		if (!(members[0] instanceof RolapStoredMeasure)) {
			return null;
		}
		RolapStoredMeasure measure = (RolapStoredMeasure) members[0];
		final RolapStar.Measure starMeasure = (RolapStar.Measure)
				measure.starMeasure;
		Util.assertTrue(starMeasure != null);
		RolapStar star = starMeasure.table.star;
		CellRequest request = new CellRequest(starMeasure);
		HashMap mapLevelToColumn = (HashMap) star.mapCubeToMapLevelToColumn.get(measure.cube);
		for (int i = 1; i < members.length; i++) {
			
			// If this is an All member, then expand children, and add all columns as non-constraining
			if ( members[i].isAll() ) {
				RolapLevel rl = (RolapLevel) members[i].getLevel();
				while ( (rl = (RolapLevel) rl.getChildLevel()) != null ) {
					RolapStar.Column column = (RolapStar.Column) mapLevelToColumn.get(rl);
					if (column == null) {
						// This hierarchy is not one which qualifies the starMeasure (this happens in
						// virtual cubes). The starMeasure only has a value for the 'all' member of
						// the hierarchy (which this is not).
						return null;
					}
					else {
						// add the column as a non-constraining column, by using a null value
						request.addConstrainedColumn(column, null);
					}
				}
			}
			// else its not an All member, so add this member plus parent member columns as constraining
			else {
				RolapLevel previousLevel = null;
				for (RolapMember m = members[i];
					m != null;
					m = (RolapMember) m.getParentMember()) {
					if (m.key == null) {
						if (m == m.getHierarchy().getNullMember()) {
							// cannot form a request if one of the members is null
							return null;
						} else if (m.isAll()) {
							continue;
						} else {
							throw Util.getRes().newInternal("why is key null?");
						}
					}
					RolapLevel level = (RolapLevel) m.getLevel();
					if (level == previousLevel) {
						// We are looking at a parent in a parent-child hierarchy,
						// for example, we have moved from Fred to Fred's boss,
						// Wilma. We don't want to include Wilma's key in the
						// request.
						continue;
					}
					previousLevel = level;
					RolapStar.Column column =
						(RolapStar.Column) mapLevelToColumn.get(level);
					if (column == null) {
						// This hierarchy is not one which qualifies the starMeasure (this happens in
						// virtual cubes). The starMeasure only has a value for the 'all' member of
						// the hierarchy (which this is not).
						return null;
					} else {
						request.addConstrainedColumn(column, m.key);
					}
				}
			}
		}
		return request;
	}

	/**
	 * Returns the value of a cell from an existing aggregation.
	 **/
	public Object getCellFromCache(RolapMember[] members)
	{
		CellRequest request = makeRequest(members);
		if (request == null) {
			return Util.nullValue; // request out of bounds
		}
		return getCellFromCache(request);
	}

	public abstract Object getCellFromCache(CellRequest request);

	/**
	 * If an existing segment contains a cell value, it pins that segment, and
	 * returns the value. Otherwise it returns null.
	 **/
	public Object getCellFromCache(RolapMember[] members, Set pinSet)
	{
		CellRequest request = makeRequest(members);
		if (request == null) {
			return Util.nullValue; // request out of bounds
		}
		return getCellFromCache(request, pinSet);
	}

	public abstract Object getCellFromCache(CellRequest request, Set pinSet);

	public Object getCell(RolapMember[] members)
	{
		CellRequest request = makeRequest(members);
		RolapMeasure measure = (RolapMeasure) members[0];
		final RolapStar.Measure starMeasure = (RolapStar.Measure)
				measure.starMeasure;
		Util.assertTrue(starMeasure != null);
		RolapStar star = starMeasure.table.star;
		return star.getCell(request);
	}

	// implement CellReader
	public Object get(Evaluator evaluator) {
		return getCell(((RolapEvaluator) evaluator).currentMembers);
	}

	public abstract String getDrillThroughSQL(CellRequest request);

	public static class Batch
	{
		public ArrayList requests = new ArrayList();
	}
}

// End RolapAggregationManager.java
