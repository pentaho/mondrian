/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2004-2007 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import mondrian.olap.*;
import mondrian.rolap.agg.*;
import mondrian.rolap.aggmatcher.AggGen;
import mondrian.rolap.aggmatcher.AggStar;
import mondrian.rolap.sql.SqlQuery;

import org.apache.log4j.Logger;
import org.eigenbase.util.property.*;
import org.eigenbase.util.property.Property;

import java.util.*;

/**
 * A <code>FastBatchingCellReader</code> doesn't really Read cells: when asked
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
    private final Map<AggregationKey, Batch> batches;

    /**
     * Records the number of requests. The field is used for correctness: if
     * the request count stays the same during an operation, you know that the
     * FastBatchingCellReader has not told any lies during that operation, and
     * therefore the result is true. The field is also useful for debugging.
     */
    private int requestCount;

    final AggregationManager aggMgr = AggregationManager.instance();

    private final RolapAggregationManager.PinSet pinnedSegments =
        aggMgr.createPinSet();

    /**
     * Indicates that the reader has given incorrect results.
     */
    private boolean dirty;

    public FastBatchingCellReader(RolapCube cube) {
        assert cube != null;
        this.cube = cube;
        this.batches = new HashMap<AggregationKey, Batch>();
    }

    public Object get(RolapEvaluator evaluator) {
        final CellRequest request =
            RolapAggregationManager.makeRequest(evaluator);
        
        if (request == null || request.isUnsatisfiable()) {
            return Util.nullValue; // request not satisfiable.
        }
        
        // Try to retrieve a cell and simultaneously pin the segment which
        // contains it.
        final Object o = aggMgr.getCellFromCache(request, pinnedSegments);

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

    public final void recordCellRequest(CellRequest request) {
        if (request.isUnsatisfiable()) {
            return;
        }
        ++requestCount;

        final AggregationKey key = new AggregationKey(request);
        Batch batch = batches.get(key);
        if (batch == null) {
            batch = new Batch(request);
            batches.put(key, batch);

            if (LOGGER.isDebugEnabled()) {
                StringBuilder buf = new StringBuilder(100);
                buf.append("FastBatchingCellReader: bitkey=");
                buf.append(request.getConstrainedColumnsBitKey());
                buf.append(Util.nl);

                final RolapStar.Column[] columns =
                    request.getConstrainedColumns();
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

    boolean loadAggregations() {
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
        final long t1 = System.currentTimeMillis();
        if (!isDirty()) {
            return false;
        }

        // Sort the batches into deterministic order.
        List<Batch> batchList = new ArrayList<Batch>(batches.values());
        Collections.sort(batchList, BatchComparator.instance);
        if (shouldUseGroupingFunction()) {
            LOGGER.debug("Using grouping sets");
            List<CompositeBatch> groupedBatches = groupBatches(batchList);
            for (CompositeBatch batch : groupedBatches) {
                loadAggregation(query, batch);
            }
        } else {
            // Load batches in turn.
            for (Batch batch : batchList) {
                loadAggregation(query, batch);
            }
        }

        batches.clear();
        dirty = false;

        if (LOGGER.isDebugEnabled()) {
            final long t2 = System.currentTimeMillis();
            LOGGER.debug("loadAggregation (millis): " + (t2 - t1));
        }

        return true;
    }

    private void loadAggregation(Query query, Loadable batch) {
        if (query != null) {
            query.checkCancelOrTimeout();
        }
        batch.loadAggregation();
    }

    List<CompositeBatch> groupBatches(List<Batch> batchList) {
        Map<AggregationKey, CompositeBatch> batchGroups =
            new HashMap<AggregationKey, CompositeBatch>();
        for (int i = 0; i < batchList.size(); i++) {
            for (int j = i + 1; j < batchList.size() && i < batchList.size();) {
                FastBatchingCellReader.Batch iBatch = batchList.get(i);
                FastBatchingCellReader.Batch jBatch = batchList.get(j);
                if (iBatch.canBatch(jBatch)) {
                    batchList.remove(j);
                    addToCompositeBatch(batchGroups, iBatch, jBatch);
                } else if (jBatch.canBatch(iBatch)) {
                    batchList.set(i, jBatch);
                    batchList.remove(j);
                    addToCompositeBatch(batchGroups, jBatch, iBatch);
                    j = i + 1;
                } else {
                    j++;
                }
            }
        }

        wrapNonBatchedBatchesWithCompositeBatches(batchList, batchGroups);
        ArrayList<CompositeBatch> list =
            new ArrayList<CompositeBatch>(batchGroups.values());
        Collections.sort(list, CompositeBatchComparator.instance);
        return list;
    }

    private void wrapNonBatchedBatchesWithCompositeBatches(
        List<Batch> batchList,
        Map<AggregationKey, CompositeBatch> batchGroups)
    {
        for (Batch batch : batchList) {
            if (batchGroups.get(batch.batchKey) == null) {
                batchGroups.put(batch.batchKey, new CompositeBatch(batch));
            }
        }
    }


    void addToCompositeBatch(
        Map<AggregationKey, CompositeBatch> batchGroups,
        Batch detailedBatch,
        Batch summaryBatch)
    {
        CompositeBatch compositeBatch = batchGroups.get(detailedBatch.batchKey);

        if (compositeBatch == null) {
            compositeBatch = new CompositeBatch(detailedBatch);
            batchGroups.put(detailedBatch.batchKey, compositeBatch);
        }

        FastBatchingCellReader.CompositeBatch compositeBatchOfSummaryBatch =
            batchGroups.remove(summaryBatch.batchKey);

        if (compositeBatchOfSummaryBatch != null) {
            compositeBatch.merge(compositeBatchOfSummaryBatch);
        } else {
            compositeBatch.add(summaryBatch);
        }

    }

    boolean shouldUseGroupingFunction() {
        return MondrianProperties.instance().EnableGroupingSets.get() &&
            doesDBSupportGroupingSets();
    }

    /**
     * Uses Dialect to identify if grouping sets is supported by the
     * database.
     */
    boolean doesDBSupportGroupingSets() {
        return getDialect().supportsGroupingSets();
    }

    /**
     * Returns the SQL dialect. Overridden in some unit tests.
     *
     * @return Dialect
     */
    SqlQuery.Dialect getDialect() {
        final RolapStar star = cube.getStar();
        if (star != null) {
            return star.getSqlQueryDialect();
        } else {
            return cube.getSchema().getDialect();
        }
    }

    /**
     * Sets the flag indicating that the reader has told a lie.
     */
    void setDirty(boolean dirty) {
        this.dirty = dirty;
    }

    /**
     * Set of Batches which can grouped together.
     */
    class CompositeBatch implements Loadable {
        /** Batch with most number of constraint columns */
        final Batch detailedBatch;

        /** Batches whose data can be fetched using rollup on detailed batch */
        final List<Batch> summaryBatches = new ArrayList<Batch>();

        CompositeBatch(Batch detailedBatch) {
            this.detailedBatch = detailedBatch;
        }

        void add(Batch summaryBatch) {
            summaryBatches.add(summaryBatch);
        }

        void merge(CompositeBatch summaryBatch) {
            summaryBatches.add(summaryBatch.detailedBatch);
            summaryBatches.addAll(summaryBatch.summaryBatches);
        }

        public void loadAggregation() {
            GroupingSetsCollector batchCollector =
                new GroupingSetsCollector(true);
            this.detailedBatch.loadAggregation(batchCollector);

            for (Batch batch : summaryBatches) {
                batch.loadAggregation(batchCollector);
            }

            getSegmentLoader().load(
                batchCollector.getGroupingSets(),
                pinnedSegments, 
                detailedBatch.batchKey.getCompoundPredicateList());
        }

        SegmentLoader getSegmentLoader() {
            return new SegmentLoader();
        }
    }

    private static final Logger BATCH_LOGGER = Logger.getLogger(Batch.class);

    /**
     * Encapsulates a common property of {@link Batch} and
     * {@link CompositeBatch}, namely, that they can be asked to load their
     * aggregations into the cache.
     */
    interface Loadable {
        void loadAggregation();
    }

    class Batch implements Loadable {
        // the CellRequest's constrained columns
        final RolapStar.Column[] columns;
        final List<RolapStar.Measure> measuresList =
            new ArrayList<RolapStar.Measure>();
        final Set<StarColumnPredicate>[] valueSets;
        final AggregationKey batchKey;

        public Batch(CellRequest request) {
            columns = request.getConstrainedColumns();
            valueSets = new HashSet[columns.length];
            for (int i = 0; i < valueSets.length; i++) {
                valueSets[i] = new HashSet<StarColumnPredicate>();
            }
            batchKey = new AggregationKey(request);
        }

        public final void add(CellRequest request) {
            final List<StarColumnPredicate> values = request.getValueList();
            for (int j = 0; j < columns.length; j++) {
                valueSets[j].add(values.get(j));
            }
            final RolapStar.Measure measure = request.getMeasure();
            if (!measuresList.contains(measure)) {
                assert (measuresList.size() == 0) ||
                        (measure.getStar() ==
                        (measuresList.get(0)).getStar()):
                        "Measure must belong to same star as other measures";
                measuresList.add(measure);
            }
        }

        /**
         * Returns the RolapStar associated with the Batch's first Measure.
         *
         * <p>This method can only be called after the {@link #add} method has
         * been called.
         *
         * @return the RolapStar associated with the Batch's first Measure
         */
        private RolapStar getStar() {
            RolapStar.Measure measure = measuresList.get(0);
            return measure.getStar();
        }

        public BitKey getConstrainedColumnsBitKey() {
            return batchKey.getConstrainedColumnsBitKey();
        }
        
        public final void loadAggregation() {
            GroupingSetsCollector collectorWithGroupingSetsTurnedOff =
                new GroupingSetsCollector(false);
            loadAggregation(collectorWithGroupingSetsTurnedOff);
        }

        final void loadAggregation(
            GroupingSetsCollector groupingSetsCollector)
        {
            if (generateAggregateSql) {
                generateAggregateSql();
            }
            final StarColumnPredicate[] predicates = initPredicates();
            final long t1 = System.currentTimeMillis();

            final AggregationManager aggmgr = AggregationManager.instance();

            // TODO: optimize key sets; drop a constraint if more than x% of
            // the members are requested; whether we should get just the cells
            // requested or expand to a n-cube

            // If the database cannot execute "count(distinct ...)", split the
            // distinct aggregations out.
            final SqlQuery.Dialect dialect = getDialect();

            int distinctMeasureCount = getDistinctMeasureCount(measuresList);
            boolean tooManyDistinctMeasures =
                distinctMeasureCount > 0 &&
                !dialect.allowsCountDistinct() ||
                distinctMeasureCount > 1 &&
                !dialect.allowsMultipleCountDistinct();

            if (tooManyDistinctMeasures) {
                doSpecialHandlingOfDistinctCountMeasures(
                    aggmgr, predicates, groupingSetsCollector);
            }

            // Load agg(distinct <SQL expression>) measures individually
            // for DBs that does allow multiple distinct SQL measures.
            if (!dialect.allowsMultipleDistinctSqlMeasures()) {

                // Note that the intention was orignially to capture the
                // subquery SQL measures and separate them out; However,
                // without parsing the SQL string, Mondrian cannot distinguish
                // between "col1" + "col2" and subquery. Here the measure list
                // contains both types.

                // See the test case testLoadDistinctSqlMeasure() in
                //  mondrian.rolap.FastBatchingCellReaderTest

                List<RolapStar.Measure> distinctSqlMeasureList =
                    getDistinctSqlMeasures(measuresList);
                for (RolapStar.Measure measure : distinctSqlMeasureList) {
                    RolapStar.Measure[] measures = {measure};
                    aggmgr.loadAggregation(
                        measures, columns,
                        batchKey,
                        predicates,
                        pinnedSegments, groupingSetsCollector);
                    measuresList.remove(measure);
                }
            }

            final int measureCount = measuresList.size();
            if (measureCount > 0) {
                final RolapStar.Measure[] measures =
                    measuresList.toArray(new RolapStar.Measure[measureCount]);
                    aggmgr.loadAggregation(
                        measures, columns,
                        batchKey,
                        predicates,
                        pinnedSegments, groupingSetsCollector);    
            }

            if (BATCH_LOGGER.isDebugEnabled()) {
                final long t2 = System.currentTimeMillis();
                BATCH_LOGGER.debug(
                    "Batch.loadAggregation (millis) " + (t2 - t1));
            }
        }

        private void doSpecialHandlingOfDistinctCountMeasures(
            AggregationManager aggmgr,
            StarColumnPredicate[] predicates,
            GroupingSetsCollector groupingSetsCollector)
        {
            while (true) {
                // Scan for a measure based upon a distinct aggregation.
                final RolapStar.Measure distinctMeasure =
                    getFirstDistinctMeasure(measuresList);
                if (distinctMeasure == null) {
                    break;
                }
                final String expr =
                    distinctMeasure.getExpression().getGenericExpression();
                final List<RolapStar.Measure> distinctMeasuresList =
                    new ArrayList<RolapStar.Measure>();
                for (int i = 0; i < measuresList.size();) {
                    final RolapStar.Measure measure = measuresList.get(i);
                    if (measure.getAggregator().isDistinct() &&
                        measure.getExpression().getGenericExpression().
                            equals(expr)) {
                        measuresList.remove(i);
                        distinctMeasuresList.add(distinctMeasure);
                    } else {
                        i++;
                    }
                }

                // Load all the distinct measures based on the same expression
                // together
                final RolapStar.Measure[] measures =
                    distinctMeasuresList.toArray(
                        new RolapStar.Measure[distinctMeasuresList.size()]);
                aggmgr.loadAggregation(
                    measures, columns,
                    batchKey,
                    predicates,
                    pinnedSegments, groupingSetsCollector);
            }
        }

        private StarColumnPredicate[] initPredicates() {
            StarColumnPredicate[] predicates =
                    new StarColumnPredicate[columns.length];
            for (int j = 0; j < columns.length; j++) {
                Set<StarColumnPredicate> valueSet = valueSets[j];

                StarColumnPredicate predicate;
                if (valueSet == null) {
                    predicate = LiteralStarPredicate.FALSE;
                } else {
                    ValueColumnPredicate[] values =
                        valueSet.toArray(
                            new ValueColumnPredicate[valueSet.size()]);
                    // Sort array to achieve determinism in generated SQL.
                    Arrays.sort(
                        values,
                        ValueColumnConstraintComparator.instance);

                    predicate =
                        new ListColumnPredicate(
                            columns[j],
                            Arrays.asList((StarColumnPredicate[]) values));
                }

                predicates[j] = predicate;
            }
            return predicates;
        }

        private void generateAggregateSql() {
            final RolapCube cube = FastBatchingCellReader.this.cube;
            if (cube == null || cube.isVirtual()) {
                final StringBuilder buf = new StringBuilder(64);
                buf.append("AggGen: Sorry, can not create SQL for virtual Cube \"");
                buf.append(cube.getName());
                buf.append("\", operation not currently supported");
                BATCH_LOGGER.error(buf.toString());

            } else {
                final AggGen aggGen =
                    new AggGen(cube.getName(), cube.getStar(), columns);
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

        /**
         * Returns the first measure based upon a distinct aggregation, or null
         * if there is none.
         */
        final RolapStar.Measure getFirstDistinctMeasure(
            List<RolapStar.Measure> measuresList)
        {
            for (RolapStar.Measure measure : measuresList) {
                if (measure.getAggregator().isDistinct()) {
                    return measure;
                }
            }
            return null;
        }

        /**
         * Returns the number of the measures based upon a distinct
         * aggregation.
         */
        private int getDistinctMeasureCount(
            List<RolapStar.Measure> measuresList)
        {
            int count = 0;
            for (RolapStar.Measure measure : measuresList) {
                if (measure.getAggregator().isDistinct()) {
                    ++count;
                }
            }
            return count;
        }

        /**
         * Returns the list of measures based upon a distinct aggregation
         * containing SQL measure expressions(as opposed to column expressions).
         *
         * This method was initially intended for only those measures that are
         * defined using subqueries(for DBs that support them). However, since
         * Mondrian does not parse the SQL string, the method will count both
         * queries as well as some non query SQL expressions.
         */
        private List<RolapStar.Measure> getDistinctSqlMeasures(
            List<RolapStar.Measure> measuresList)
        {
            List<RolapStar.Measure> distinctSqlMeasureList =
                new ArrayList<RolapStar.Measure>();
            for (RolapStar.Measure measure : measuresList) {
                if (measure.getAggregator().isDistinct() &&
                    measure.getExpression() instanceof
                        MondrianDef.MeasureExpression) {
                    MondrianDef.MeasureExpression measureExpr =
                        (MondrianDef.MeasureExpression) measure.getExpression();
                    MondrianDef.SQL measureSql = measureExpr.expressions[0];
                    // Checks if the SQL contains "SELECT" to detect the case a
                    // subquery is used to define the measure. This is not a
                    // perfect check, because a SQL expression on column names
                    // containing "SELECT" will also be detected. e,g,
                    // count("select beef" + "regular beef").
                    if (measureSql.cdata.toUpperCase().contains("SELECT")) {
                        distinctSqlMeasureList.add(measure);
                    }
                }
            }
            return distinctSqlMeasureList;
        }

        /**
         * Returns whether another Batch can be batched to this Batch.
         *
         * <p>This is possible if:
         * <li>columns list is super set of other batch's constraint columns;
         *     and
         * <li>both have same Fact Table; and
         * <li>matching columns of this and other batch has the same value; and
         * <li>non matching columns of this batch have ALL VALUES
         * </ul>
         */
        boolean canBatch(Batch other) {
            return hasOverlappingBitKeys(other)
                && constraintsMatch(other)
                && hasSameMeasureList(other)
                && haveSameStarAndAggregation(other);
        }

        private boolean constraintsMatch(Batch other) {
            if(areBothDistinctCountBatches(other)){
                if(getConstrainedColumnsBitKey().equals(
                    other.getConstrainedColumnsBitKey())) {
                        return hasSameCompoundPredicate(other)
                            && haveSameValuesForOverlappingColumnsOrHasAllChildrenForOthers(other);
                }
                return hasSameCompoundPredicate(other)
                    || haveSameValuesForOverlappingColumnsOrHasAllChildrenForOthers(other);
            }
            return haveSameValuesForOverlappingColumnsOrHasAllChildrenForOthers(other);
        }

        private boolean areBothDistinctCountBatches(Batch other) {
            return this.hasDistinctCountMeasure()
                && !this.hasNormalMeasures()
                && other.hasDistinctCountMeasure()
                && !other.hasNormalMeasures();
        }

        private boolean hasNormalMeasures() {
            return getDistinctMeasureCount(measuresList) !=  measuresList.size();
        }        

        private boolean hasSameMeasureList(Batch other) {
            return (this.measuresList.size() == other.measuresList.size() &&
                this.measuresList.containsAll(other.measuresList));
        }

        boolean hasOverlappingBitKeys(Batch other) {
            return getConstrainedColumnsBitKey()
                .isSuperSetOf(other.getConstrainedColumnsBitKey());
        }

        boolean hasDistinctCountMeasure() {
            return getDistinctMeasureCount(measuresList) > 0;
        }

        boolean hasSameCompoundPredicate(Batch other) {
            final StarPredicate starPredicate = compoundPredicate();
            final StarPredicate otherStarPredicate = other.compoundPredicate();
            if (starPredicate == null && otherStarPredicate == null) {
                return true;
            } else if (starPredicate != null && otherStarPredicate != null) {
                return starPredicate.equalConstraint(otherStarPredicate);
            }
            return false;
        }

        private StarPredicate compoundPredicate() {
            StarPredicate predicate = null;
            for (Set<StarColumnPredicate> valueSet : valueSets) {
                StarPredicate orPredicate = null;
                for (StarColumnPredicate starColumnPredicate : valueSet) {
                    if (orPredicate == null) {
                        orPredicate = starColumnPredicate;
                    } else {
                        orPredicate = orPredicate.or(starColumnPredicate);
                    }
                }
                if (predicate == null) {
                    predicate = orPredicate;
                } else {
                    predicate = predicate.and(orPredicate);
                }
            }
            for (StarPredicate starPredicate : this.batchKey.getCompoundPredicateList()) {
                if (predicate == null) {
                    predicate = starPredicate;
                } else {
                    predicate = predicate.and(starPredicate);
                }
            }
            return predicate;
        }

        boolean haveSameStarAndAggregation(Batch other) {
            boolean rollup[] = {false};
            boolean otherRollup[] = {false};
            
            boolean hasSameAggregation = getAgg(rollup) == other.getAgg(otherRollup);
            boolean hasSameRollupOption = rollup[0] == otherRollup[0];

            boolean hasSameStar = getStar().equals(other.getStar());
            return hasSameStar && hasSameAggregation && hasSameRollupOption;
        }

        /**
         * @param rollup Out parameter
         * @return AggStar
         */
        private AggStar getAgg(boolean[] rollup) {

            AggregationManager aggregationManager =
                AggregationManager.instance();
            AggStar star =
                aggregationManager.findAgg(getStar(),
                    getConstrainedColumnsBitKey(), makeMeasureBitKey(),
                    rollup);
            return star;
        }

        private BitKey makeMeasureBitKey() {
            BitKey bitKey = getConstrainedColumnsBitKey().emptyCopy();
            for (RolapStar.Measure measure : measuresList) {
                bitKey.set(measure.getBitPosition());
            }
            return bitKey;
        }

        boolean haveSameValuesForOverlappingColumnsOrHasAllChildrenForOthers(
            Batch other)
        {
            for (int j = 0; j < columns.length; j++) {
                boolean isCommonColumn = false;
                for (int i = 0; i < other.columns.length; i++) {
                    if (areSameColumns(other.columns[i], columns[j])) {
                        if (hasSameValues(other.valueSets[i], valueSets[j])) {
                            isCommonColumn = true;
                            break;
                        } else
                            return false;
                    }
                }
                if (!isCommonColumn &&
                    !hasAllValues(columns[j], valueSets[j])) {
                    return false;
                }
            }
            return true;
        }

        private boolean hasAllValues(RolapStar.Column column,
                                     Set<StarColumnPredicate> valueSet)
        {
            return column.getCardinality() == valueSet.size();
        }

        private boolean areSameColumns(RolapStar.Column otherColumn,
                                       RolapStar.Column thisColumn)
        {
            return otherColumn.equals(thisColumn);
        }

        private boolean hasSameValues(Set<StarColumnPredicate> otherValueSet,
                                      Set<StarColumnPredicate> thisValueSet)
        {
            return otherValueSet.equals(thisValueSet);
        }
    }

    private static class CompositeBatchComparator implements Comparator<CompositeBatch> {
        static final CompositeBatchComparator instance = new CompositeBatchComparator();

        public int compare(CompositeBatch o1, CompositeBatch o2) {
            return BatchComparator.instance.compare(o1.detailedBatch, o2.detailedBatch);
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

    private static class ValueColumnConstraintComparator
        implements Comparator<ValueColumnPredicate>
    {
        static final ValueColumnConstraintComparator instance =
            new ValueColumnConstraintComparator();

        private ValueColumnConstraintComparator() {}

        public int compare(
            ValueColumnPredicate o1,
            ValueColumnPredicate o2)
        {
            Object v1 = o1.getValue();
            Object v2 = o2.getValue();
            if (v1.getClass() == v2.getClass() &&
                v1 instanceof Comparable) {
                return ((Comparable) v1).compareTo(v2);
            } else {
                return v1.toString().compareTo(v2.toString());
            }
        }
    }
}

// End FastBatchingCellReader.java
