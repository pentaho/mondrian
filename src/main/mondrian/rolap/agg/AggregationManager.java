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

import java.util.*;

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

	void loadAggregation(
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
		return aggregation.get(measure, request.getSingleValues());
	}

	public Object getCellFromCache(CellRequest request, Set pinSet) {
		RolapStar.Measure measure = request.getMeasure();
		Aggregation aggregation = lookupAggregation(
				measure.table.star, request.getColumns());
		if (aggregation == null) {
			return null; // cell is not in any aggregation
		}
		return aggregation.getAndPin(measure, request.getSingleValues(), pinSet);
	}

}

// End RolapAggregationManager.java
