/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2004-2006 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import mondrian.olap.*;
import mondrian.rolap.agg.*;
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
    private final Map<BatchKey, Batch> batches;
    private int requestCount;

    RolapAggregationManager aggMgr = AggregationManager.instance();
    /**
     * Indicates that the reader given incorrect results.
     */
    private boolean dirty;

    public FastBatchingCellReader(RolapCube cube) {
        this.cube = cube;
        this.pinnedSegments = new HashSet();
        this.batches = new HashMap<BatchKey, Batch>();
    }

    public Object get(Evaluator evaluator) {
        final RolapEvaluator rolapEvaluator = (RolapEvaluator) evaluator;
        Member[] currentMembers = rolapEvaluator.getCurrentMembers();
        CellRequest request =
                RolapAggregationManager.makeRequest(currentMembers, false, false);
        if (request == null || request.isUnsatisfiable()) {
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
        if (request.isUnsatisfiable()) {
            return;
        }
        ++requestCount;

        BitKey bitkey = request.getConstrainedColumnsBitKey();
        BatchKey key = new BatchKey(bitkey, request.getMeasure().getStar());
        Batch batch = batches.get(key);
        if (batch == null) {
            batch = new Batch(request);
            batches.put(key, batch);

            if (LOGGER.isDebugEnabled()) {
                StringBuilder buf = new StringBuilder(100);
                buf.append("FastBatchingCellReader: bitkey=");
                buf.append(request.getConstrainedColumnsBitKey());
                buf.append(Util.nl);

                RolapStar.Column[] columns = request.getConstrainedColumns();
                for (RolapStar.Column column : columns) {
                    buf.append("  ");
                    buf.append(column);
                    buf.append(Util.nl);
                }
                LOGGER.debug(buf.toString());
            }
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

    boolean loadAggregations()
    {
        return loadAggregations(null);
    }
    
    /**
     * Loads pending aggregations, if any.
     * 
     * @param query the parent query object that initiated this
     * call
     *
     * @return Whether any aggregations were loaded.
     */
    boolean loadAggregations(Query query) {
        long t1 = System.currentTimeMillis();

        requestCount = 0;
        if (batches.isEmpty() && !dirty) {
            return false;
        }

        // Sort the batches into deterministic order.
        List<Batch> batchList = new ArrayList<Batch>(batches.values());
        Collections.sort(batchList, BatchComparator.instance);

        // Load batches in turn.
        for (Batch batch : batchList) {
            if (query != null) {
                query.checkCancelOrTimeout();
            }
            (batch).loadAggregation();
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
        // these are the CellRequest's constrained columns
        final RolapStar.Column[] columns;
        // this is the CellRequest's constrained column BitKey
        final BitKey constrainedColumnsBitKey;
        final List<RolapStar.Measure> measuresList = new ArrayList<RolapStar.Measure>();
        final Set<ColumnConstraint>[] valueSets;

        public Batch(CellRequest request) {
            columns = request.getConstrainedColumns();
            constrainedColumnsBitKey = request.getConstrainedColumnsBitKey();
            valueSets = new HashSet[columns.length];
            for (int i = 0; i < valueSets.length; i++) {
                valueSets[i] = new HashSet<ColumnConstraint>();
            }
        }

        public void add(CellRequest request) {
            List<ColumnConstraint> values = request.getValueList();
            for (int j = 0; j < columns.length; j++) {
                valueSets[j].add(values.get(j));
            }
            RolapStar.Measure measure = request.getMeasure();
            if (!measuresList.contains(measure)) {
                assert (measuresList.size() == 0) ||
                        (measure.getStar() ==
                        (measuresList.get(0)).getStar()):
                        "Measure must belong to same star as other measures";
                measuresList.add(measure);
            }
        }
        
        /** 
         * This can only be called after the add method has been called. 
         * 
         * @return the RolapStar associated with the Batch's first Measure.
         */
        private RolapStar getStar() {
            RolapStar.Measure measure = measuresList.get(0);
            return measure.getStar();
        }

        void loadAggregation() {
            if (generateAggregateSql) {
                RolapCube cube = FastBatchingCellReader.this.cube;
                if (cube == null || cube.isVirtual()) {
                    StringBuilder buf = new StringBuilder(64);
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

                ColumnConstraint[] constraints;
                if ((valueSet == null)) {
                    constraints = null;
                } else {
                    constraints = new ColumnConstraint[valueSet.size()];
                    valueSet.toArray(constraints);
                    // Sort array to achieve determinism in generated SQL.
                    Arrays.sort(constraints);
                }

                constraintses[j] = constraints;
            }
            // TODO: optimize key sets; drop a constraint if more than x% of
            // the members are requested; whether we should get just the cells
            // requested or expand to a n-cube

            // If the database cannot execute "count(distinct ...)", split the
            // distinct aggregations out.
            if (!getStar().getSqlQueryDialect().allowsCountDistinct()) {
                while (true) {
                    // Scan for a measure based upon a distinct aggregation.
                    RolapStar.Measure distinctMeasure =
                        getFirstDistinctMeasure(measuresList);
                    if (distinctMeasure == null) {
                        break;
                    }
                    final String expr = distinctMeasure.getExpression().
                            getGenericExpression();
                    final List<RolapStar.Measure> distinctMeasuresList = new ArrayList<RolapStar.Measure>();
                    for (int i = 0; i < measuresList.size();) {
                        RolapStar.Measure measure =
                            measuresList.get(i);
                        if (measure.getAggregator().isDistinct() &&
                            measure.getExpression().getGenericExpression().
                                equals(expr))
                        {
                            measuresList.remove(i);
                            distinctMeasuresList.add(distinctMeasure);
                        } else {
                            i++;
                        }
                    }
                    RolapStar.Measure[] measures =
                        distinctMeasuresList.toArray(
                            new RolapStar.Measure[distinctMeasuresList.size()]);
                    aggmgr.loadAggregation(measures, columns,
                            constrainedColumnsBitKey,
                            constraintses, pinnedSegments);
                }
            }

            final int measureCount = measuresList.size();
            if (measureCount > 0) {
                RolapStar.Measure[] measures =
                    measuresList.toArray(new RolapStar.Measure[measureCount]);
                aggmgr.loadAggregation(
                    measures, columns,
                    constrainedColumnsBitKey,
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
         */
        RolapStar.Measure getFirstDistinctMeasure(List<RolapStar.Measure> measuresList) {
            for (int i = 0; i < measuresList.size(); i++) {
                RolapStar.Measure measure = measuresList.get(i);
                if (measure.getAggregator().isDistinct()) {
                    return measure;
                }
            }
            return null;
        }
    }

    /**
     * This is needed because for a Virtual Cube: two CellRequests
     * could have the same BitKey but have different underlying
     * base cubes. Without this, one get the result in the
     * SegmentArrayQuerySpec addMeasure Util.assertTrue being
     * triggered (which is what happened).
     */
    class BatchKey {
        BitKey key;
        RolapStar star;
        BatchKey(BitKey key, RolapStar star) {
            this.key = key;
            this.star = star;
        }
        public int hashCode() {
            return key.hashCode() ^ star.hashCode();
        }
        public boolean equals(Object other) {
            if (other instanceof BatchKey) {
                BatchKey bkey = (BatchKey) other;
                return key.equals(bkey.key) && star.equals(bkey.star);
            } else {
                return false;
            }
        }
        public String toString() {
            return star.getFactTable().getTableName() + " " + key.toString();
        }
    }

    private static class BatchComparator implements Comparator<Batch> {
        static final BatchComparator instance = new BatchComparator();

        private BatchComparator() {}

        public int compare(
            Batch o1, Batch o2) {
            if (o1.columns.length != o2.columns.length) {
                return o1.columns.length - o2.columns.length;
            }
            for (int i = 0; i < o1.columns.length; i++) {
                int c = o1.columns[i].getName().compareTo(
                    o2.columns[i].getName());
                if (c != 0) {
                    return c;
                }
            }
            for (int i = 0; i < o1.columns.length; i++) {
                int c = compare(o1.valueSets[i], o2.valueSets[i]);
                if (c != 0) {
                    return c;
                }
            }
            return 0;
        }

        <T> int compare(Set<T> set1, Set<T> set2) {
            if (set1.size() != set2.size()) {
                return set1.size() - set2.size();
            }
            Iterator<T> iter1 = set1.iterator();
            Iterator<T> iter2 = set2.iterator();
            while (iter1.hasNext()) {
                T v1 = iter1.next();
                T v2 = iter2.next();
                int c = Util.compareKey(v1, v2);
                if (c != 0) {
                    return c;
                }
            }
            return 0;
        }
    }
}

// End FastBatchingCellReader.java
