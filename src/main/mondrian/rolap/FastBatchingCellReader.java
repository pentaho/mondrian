package mondrian.rolap;

import mondrian.olap.Evaluator;
import mondrian.olap.Util;
import mondrian.rolap.agg.AggregationManager;
import mondrian.rolap.agg.CellRequest;

import java.util.*;

/**
 * A <code>FastBatchingCellReader</code> doesn't really read cells: when asked
 * to look up the values of stored measures, it lies, and records the fact
 * that the value was asked for.  Later, we can look over the values which
 * are required, fetch them in an efficient way, and re-run the evaluation
 * with a real evaluator.
 *
 * <p>NOTE: When it doesn't know the answer, it lies by returning an error
 * object.  The calling code must be able to deal with that.</p>
 * 
 * <p>This class tries to minimize the amount of storage needed to record the
 * fact that a cell was requested</p>
 */
public class FastBatchingCellReader implements CellReader {

	RolapCube cube;
	Set pinnedSegments;
	Map batches = new HashMap();
	RolapAggregationManager aggMgr = AggregationManager.instance();

	public FastBatchingCellReader(RolapCube cube, Set pinnedSegments) {
		this.cube = cube;
		this.pinnedSegments = pinnedSegments;
	}

	public Object get(Evaluator evaluator) {
		RolapMember[] currentMembers = ((RolapEvaluator) evaluator).currentMembers;
		CellRequest request = RolapAggregationManager.makeRequest(currentMembers, false);
		if (request == null)
			return Util.nullValue; // request out of bounds
		// try to retrieve a cell and simultaneously pin the segment which
		// contains it
		Object o = aggMgr.getCellFromCache(request, pinnedSegments);

		if (o == Boolean.TRUE) {
			// Aggregation is being loaded. (todo: Use better value, or
			// throw special exception)
			return RolapUtil.valueNotReadyException;
		}
		if (o != null) {
			return o;
		}
		// if there is no such cell, record that we need to fetch it, and
		// return 'error'
		recordCellRequest(request);
		return RolapUtil.valueNotReadyException;
	}

	int requestCount = 0;
	void recordCellRequest(CellRequest request) {
		++requestCount;
		Object key = request.getBatchKey();
		Batch batch = (Batch) batches.get(key);
		if (batch == null) {
			batch = new Batch(request);
			batches.put(key, batch);
		}
		batch.add(request);
	}

	boolean loadAggregations(Evaluator evaluator) {
		//System.out.println("requestCount = " + requestCount);
		long t1 = System.currentTimeMillis();
		requestCount = 0;
		if (batches.isEmpty())
			return false;
		for (Iterator it = batches.values().iterator(); it.hasNext();)
			 ((Batch) it.next()).loadAggregation(evaluator);
		batches.clear();
		long t2 = System.currentTimeMillis();
		//System.out.println("loadAggregation " + (t2 - t1));
		return true;
	}

	class Batch {

		RolapStar.Column[] columns;
		ArrayList measuresList = new ArrayList();
		Set[] valueSets;

		public Batch(CellRequest request) {
			columns = request.getColumns();
			valueSets = new HashSet[columns.length];
			for (int i = 0; i < valueSets.length; i++) {
				valueSets[i] = new HashSet();
			}
		}

		public void add(CellRequest request) {
			ArrayList values = request.getValueList();
			for (int j = 0; j < columns.length; j++) {
				valueSets[j].add(values.get(j));
			}
			RolapStar.Measure measure = request.getMeasure();
			if (!measuresList.contains(measure)) {
				if (measuresList.size() > 0) {
					Util.assertTrue(
						measure.table.star == ((RolapStar.Measure) measuresList.get(0)).table.star);
				}
				measuresList.add(measure);
			}
		}

		void loadAggregation(Evaluator evaluator) {
			long t1 = System.currentTimeMillis();
			AggregationManager aggmgr = (AggregationManager) AggregationManager.instance();
			Object[][] constraintses = new Object[columns.length][];
			for (int j = 0; j < columns.length; j++) {
				Object[] constraints;
				Set valueSet = valueSets[j];
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

			// If the database cannot execute "count(distinct ...)", split the
			// distinct aggregations out.
			while (true) {
				// Scan for a measure based upon a distinct aggregation.
				RolapStar.Measure distinctMeasure =	getFirstDistinctMeasure(measuresList);
				if (distinctMeasure == null) {
					break;
				}
				final String expr = distinctMeasure.expression.getGenericExpression();
				final ArrayList distinctMeasuresList = new ArrayList();
				for (int i = 0; i < measuresList.size();) {
					RolapStar.Measure measure = (RolapStar.Measure) measuresList.get(i);
					if (measure.aggregator.distinct
						&& measure.expression.getGenericExpression().equals(expr)) {
						measuresList.remove(i);
						distinctMeasuresList.add(distinctMeasure);
					} else {
						i++;
					}
				}
				RolapStar.Measure[] measures =
					(RolapStar.Measure[]) distinctMeasuresList.toArray(new RolapStar.Measure[0]);
				aggmgr.loadAggregation(measures, columns, constraintses, pinnedSegments, evaluator);
			}
			final int measureCount = measuresList.size();
			if (measureCount > 0) {
				RolapStar.Measure[] measures =
					(RolapStar.Measure[]) measuresList.toArray(new RolapStar.Measure[measureCount]);
				aggmgr.loadAggregation(measures, columns, constraintses, pinnedSegments, evaluator);
			}
			long t2 = System.currentTimeMillis();
			//System.out.println("Batch.loadAggregation " + (t2 - t1));
		}
		
	    /**
	     * Returns the first measure based upon a distinct aggregation, or null if
	     * there is none.
	     * @param measuresList
	     * @return
	     */
	    RolapStar.Measure getFirstDistinctMeasure(ArrayList measuresList) {
	        for (int i = 0; i < measuresList.size(); i++) {
	            RolapStar.Measure measure = (RolapStar.Measure) measuresList.get(i);
	            if (measure.aggregator.distinct) {
	                return measure;
	            }
	        }
	        return null;
	    }
		
	}

}
