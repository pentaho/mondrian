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

import mondrian.olap.*;
import mondrian.rolap.agg.AggregationManager;
import mondrian.rolap.agg.CellRequest;
import mondrian.rolap.agg.ColumnConstraint;
import mondrian.rolap.aggmatcher.AggGen;

import org.apache.log4j.Logger;
import org.eigenbase.util.property.*;
import org.eigenbase.util.property.Property;

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

    private static final Logger LOGGER =
        Logger.getLogger(FastBatchingCellReader.class);

    /**
     * This static variable controls the generation of aggregate table sql.
     */
    private static boolean generateAggregateSql =
             MondrianProperties.instance().GenerateAggregateSql.get();

    static {
        // Trigger is used to lookup and change the value of the
        // variable that controls generating aggregate table sql.
        // Using a trigger means we don't have to look up the property eveytime.
        MondrianProperties.instance().GenerateAggregateSql.addTrigger(
                new TriggerBase(true) {
                    public void execute(Property property, String value) {
                        generateAggregateSql = property.booleanValue();
                    }
                });
    }

    private final RolapCube cube;
    private final Set pinnedSegments;
    private final Map batches;
    private int requestCount;

    RolapAggregationManager aggMgr = AggregationManager.instance();
    /**
     * Indicates that the reader given incorrect results.
     */
    private boolean dirty;

    public FastBatchingCellReader(RolapCube cube) {
        this.cube = cube;
        this.pinnedSegments = new HashSet();
        this.batches = new HashMap();
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

    public int getMissCount() {
        return requestCount;
    }

    void recordCellRequest(CellRequest request) {
        ++requestCount;
        Object key = request.getBatchKey();
        Batch batch = (Batch) batches.get(key);
        if (batch == null) {
            batch = new Batch(request);

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("FastBatchingCellReader: bitkey=" +
                        request.getBatchKey());
                RolapStar star = cube.getStar();
                if (star != null) {
                    RolapStar.Column[] columns =
                            star.lookupColumns((BitKey) request.getBatchKey());
                    for (int i = 0; i < columns.length; i++) {
                        LOGGER.debug("  " +columns[i]);
                    }
                }
            }
            batches.put(key, batch);
        }
        batch.add(request);
    }

    /**
     * Returns whether this reader has told a lie. This is the case if there
     * are pending batches to load or if {@link #setDirty(boolean)} has been
     * called.
     */
    boolean isDirty() {
        return dirty || !batches.isEmpty();
    }

    /**
     * Loads pending aggregations, if any.
     *
     * @return Whether any aggregations were loaded.
     */
    boolean loadAggregations() {
        long t1 = System.currentTimeMillis();

        requestCount = 0;
        if (batches.isEmpty() && !dirty) {
            return false;
        }
        Iterator it = batches.values().iterator();
        while (it.hasNext()) {
             ((Batch) it.next()).loadAggregation();
        }
        batches.clear();

        if (LOGGER.isDebugEnabled()) {
            long t2 = System.currentTimeMillis();
            LOGGER.debug("loadAggregation (millis): " + (t2 - t1));
        }

        dirty = false;
        return true;
    }

    /**
     * Sets the flag indicating that the reader has told a lie.
     */
    void setDirty(boolean dirty) {
        this.dirty = dirty;
    }

    private static final Logger BATCH_LOGGER = Logger.getLogger(Batch.class);
    class Batch {
        final RolapStar.Column[] columns;
        final BitKey bitKey;
        final List measuresList = new ArrayList();
        final Set[] valueSets;

        public Batch(CellRequest request) {
            columns = request.getColumns();
            bitKey = request.getBatchKey();
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
                assert (measuresList.size() == 0) ||
                        (measure.getStar() ==
                        ((RolapStar.Measure) measuresList.get(0)).getStar()):
                        "Measure must belong to same star as other measures";
                measuresList.add(measure);
            }
        }

        void loadAggregation() {
            if (generateAggregateSql) {
                if (FastBatchingCellReader.this.cube.isVirtual()) {
                    StringBuffer buf = new StringBuffer(64);
                    buf.append("AggGen: Sorry, can not create SQL for virtual Cube \"");
                    buf.append(FastBatchingCellReader.this.cube.getName());
                    buf.append("\", operation not currently supported");
                    BATCH_LOGGER.error(buf.toString());

                } else {
                    AggGen aggGen = new AggGen(
                            FastBatchingCellReader.this.cube.getStar(), columns);
                    if (aggGen.isReady()) {
                        // PRINT TO STDOUT - DO NOT USE BATCH_LOGGER
                        System.out.println("createLost:" +
                            Util.nl + aggGen.createLost());
                        System.out.println("insertIntoLost:" +
                            Util.nl + aggGen.insertIntoLost());
                        System.out.println("createCollapsed:" +
                            Util.nl + aggGen.createCollapsed());
                        System.out.println("insertIntoCollapsed:" +
                            Util.nl + aggGen.insertIntoCollapsed());
                    } else {
                        BATCH_LOGGER.error("AggGen failed");
                    }
                }
            }

            long t1 = System.currentTimeMillis();

            AggregationManager aggmgr = AggregationManager.instance();
            ColumnConstraint[][] constraintses =
                    new ColumnConstraint[columns.length][];
            for (int j = 0; j < columns.length; j++) {
                Set valueSet = valueSets[j];

                ColumnConstraint[] constraints = (valueSet == null)
                    ? null
                    : (ColumnConstraint[]) valueSet.
                            toArray(new ColumnConstraint[valueSet.size()]);

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
                final String expr =
                    distinctMeasure.getExpression().getGenericExpression();
                final List distinctMeasuresList = new ArrayList();
                for (int i = 0; i < measuresList.size();) {
                    RolapStar.Measure measure = (RolapStar.Measure) measuresList.get(i);
                    if (measure.getAggregator().isDistinct() &&
                            measure.getExpression().getGenericExpression().equals(expr)) {
                        measuresList.remove(i);
                        distinctMeasuresList.add(distinctMeasure);
                    } else {
                        i++;
                    }
                }
                RolapStar.Measure[] measures = (RolapStar.Measure[])
                        distinctMeasuresList.toArray(
                            new RolapStar.Measure[distinctMeasuresList.size()]);
                aggmgr.loadAggregation(measures, columns, bitKey,
                            constraintses, pinnedSegments);
            }
            final int measureCount = measuresList.size();
            if (measureCount > 0) {
                RolapStar.Measure[] measures = (RolapStar.Measure[])
                        measuresList.toArray(new RolapStar.Measure[measureCount]);
                aggmgr.loadAggregation(measures, columns, bitKey,
                    constraintses, pinnedSegments);
            }
            if (BATCH_LOGGER.isDebugEnabled()) {
                long t2 = System.currentTimeMillis();
                BATCH_LOGGER.debug("Batch.loadAggregation (millis) " + (t2 - t1));
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
                if (measure.getAggregator().isDistinct()) {
                    return measure;
                }
            }
            return null;
        }

    }

}

// End FastBatchingCellReader.java
