/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2004-2005 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import mondrian.olap.Evaluator;
import mondrian.olap.Member;
import mondrian.olap.Util;
import mondrian.rolap.agg.AggregationManager;
import mondrian.rolap.agg.CellRequest;
import mondrian.rolap.agg.ColumnConstraint;

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
 * fact that a cell was requested.</p>
 */
public class FastBatchingCellReader implements CellReader {

    private final RolapCube cube;
    private final Set pinnedSegments;
    private final Map batches = new HashMap();
    RolapAggregationManager aggMgr = AggregationManager.instance();

    public FastBatchingCellReader(RolapCube cube) {
        this.cube = cube;
        this.pinnedSegments = new HashSet();
    }

    public Object get(Evaluator evaluator) {
        final RolapEvaluator rolapEvaluator = (RolapEvaluator) evaluator;
        Member[] currentMembers = rolapEvaluator.getCurrentMembers();
        CellRequest request =
                RolapAggregationManager.makeRequest(currentMembers, false);
        if (request == null) {
            return Util.nullValue; // request out of bounds
        }
        // Try to retrieve a cell and simultaneously pin the segment which
        // contains it.
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

    private int requestCount = 0;
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

    boolean loadAggregations() {
        //System.out.println("requestCount = " + requestCount);
        long t1 = System.currentTimeMillis();
        requestCount = 0;
        if (batches.isEmpty()) {
            return false;
        }
        Iterator it = batches.values().iterator();
        while (it.hasNext()) {
             ((Batch) it.next()).loadAggregation();
        }
        batches.clear();
        long t2 = System.currentTimeMillis();
        if (false) {
            System.out.println("loadAggregation " + (t2 - t1));
        }
        return true;
    }

    class Batch {

        final RolapStar.Column[] columns;
        final List measuresList = new ArrayList();
        final Set[] valueSets;

        public Batch(CellRequest request) {
            columns = request.getColumns();
            valueSets = new HashSet[columns.length];
            for (int i = 0; i < valueSets.length; i++) {
                valueSets[i] = new HashSet();
            }
        }

        public void add(CellRequest request) {
            List values = request.getValueList();
            for (int j = 0; j < columns.length; j++) {
                valueSets[j].add(values.get(j));
            }
            RolapStar.Measure measure = request.getMeasure();
            if (!measuresList.contains(measure)) {
                assert measuresList.size() == 0 ||
                        measure.table.star ==
                        ((RolapStar.Measure) measuresList.get(0)).table.star :
                        "Measure must belong to same star as other measures";
                measuresList.add(measure);
            }
        }

        void loadAggregation() {
            long t1 = System.currentTimeMillis();
            AggregationManager aggmgr = AggregationManager.instance();
            ColumnConstraint[][] constraintses =
                    new ColumnConstraint[columns.length][];
            for (int j = 0; j < columns.length; j++) {
                ColumnConstraint[] constraints;
                Set valueSet = valueSets[j];
                if (valueSet == null) {
                    constraints = null;
                } else {
                    constraints = (ColumnConstraint[]) valueSet.
                            toArray(new ColumnConstraint[valueSet.size()]);
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
                RolapStar.Measure distinctMeasure =
                        getFirstDistinctMeasure(measuresList);
                if (distinctMeasure == null) {
                    break;
                }
                final String expr = distinctMeasure.expression.getGenericExpression();
                final List distinctMeasuresList = new ArrayList();
                for (int i = 0; i < measuresList.size();) {
                    RolapStar.Measure measure = (RolapStar.Measure) measuresList.get(i);
                    if (measure.aggregator.distinct &&
                            measure.expression.getGenericExpression().equals(expr)) {
                        measuresList.remove(i);
                        distinctMeasuresList.add(distinctMeasure);
                    } else {
                        i++;
                    }
                }
                RolapStar.Measure[] measures = (RolapStar.Measure[])
                        distinctMeasuresList.toArray(
                                new RolapStar.Measure[distinctMeasuresList.size()]);
                aggmgr.loadAggregation(measures, columns, constraintses, pinnedSegments);
            }
            final int measureCount = measuresList.size();
            if (measureCount > 0) {
                RolapStar.Measure[] measures = (RolapStar.Measure[])
                        measuresList.toArray(new RolapStar.Measure[measureCount]);
                aggmgr.loadAggregation(measures, columns, constraintses, pinnedSegments);
            }
            long t2 = System.currentTimeMillis();
            if (false) {
                System.out.println("Batch.loadAggregation " + (t2 - t1));
            }
        }

        /**
         * Returns the first measure based upon a distinct aggregation, or null if
         * there is none.
         * @param measuresList
         * @return
         */
        RolapStar.Measure getFirstDistinctMeasure(List measuresList) {
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

// End FastBatchingCellReader.java
