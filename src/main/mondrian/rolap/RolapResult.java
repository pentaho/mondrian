/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2001-2005 Julian Hyde
// Copyright (C) 2005-2015 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.calc.*;
import mondrian.calc.impl.*;
import mondrian.mdx.*;
import mondrian.olap.*;
import mondrian.olap.fun.*;
import mondrian.olap.fun.VisualTotalsFunDef.VisualTotalMember;
import mondrian.olap.type.ScalarType;
import mondrian.olap.type.SetType;
import mondrian.resource.MondrianResource;
import mondrian.rolap.agg.AggregationManager;
import mondrian.rolap.agg.CellRequestQuantumExceededException;
import mondrian.server.Execution;
import mondrian.server.Locus;
import mondrian.spi.CellFormatter;
import mondrian.util.*;

import org.apache.log4j.Logger;

import java.util.*;


/**
 * A <code>RolapResult</code> is the result of running a query.
 *
 * @author jhyde
 * @since 10 August, 2001
 */
public class RolapResult extends ResultBase {

    static final Logger LOGGER = Logger.getLogger(ResultBase.class);

    private RolapEvaluator evaluator;
    RolapEvaluator slicerEvaluator;
    private final CellKey point;

    private CellInfoContainer cellInfos;
    private FastBatchingCellReader batchingReader;
    private final CellReader aggregatingReader;
    private Modulos modulos = null;
    private final int maxEvalDepth =
            MondrianProperties.instance().MaxEvalDepth.get();

    private final Map<Integer, Boolean> positionsHighCardinality =
        new HashMap<Integer, Boolean>();
    private final Map<Integer, TupleCursor> positionsIterators =
        new HashMap<Integer, TupleCursor>();
    private final Map<Integer, Integer> positionsIndexes =
        new HashMap<Integer, Integer>();
    private final Map<Integer, List<List<Member>>> positionsCurrent =
        new HashMap<Integer, List<List<Member>>>();

    /**
     * Creates a RolapResult.
     *
     * @param execution Execution of a statement
     * @param execute Whether to execute the query
     */
    RolapResult(
        final Execution execution,
        boolean execute)
    {
        super(execution, null);

        this.point = CellKey.Generator.newCellKey(axes.length);
        final AggregationManager aggMgr =
            execution.getMondrianStatement()
                .getMondrianConnection()
                .getServer().getAggregationManager();
        this.aggregatingReader = aggMgr.getCacheCellReader();
        final int expDeps =
            MondrianProperties.instance().TestExpDependencies.get();
        if (expDeps > 0) {
            this.evaluator = new RolapDependencyTestingEvaluator(this, expDeps);
        } else {
            final RolapEvaluatorRoot root =
                new RolapResultEvaluatorRoot(this);
            if (statement.getProfileHandler() != null) {
                this.evaluator = new RolapProfilingEvaluator(root);
            } else {
                this.evaluator = new RolapEvaluator(root);
            }
        }
        RolapCube cube = (RolapCube) query.getCube();
        this.batchingReader =
            new FastBatchingCellReader(execution, cube, aggMgr);

        this.cellInfos =
            (query.axes.length > 4)
                ? new CellInfoMap(point)
                : new CellInfoPool(query.axes.length);

        if (!execute) {
            return;
        }

        boolean normalExecution = true;
        try {
            // This call to clear the cube's cache only has an
            // effect if caching has been disabled, otherwise
            // nothing happens.
            // Clear the local cache before a query has run
            cube.clearCachedAggregations();

            /////////////////////////////////////////////////////////////////
            //
            // Evaluation Algorithm
            //
            // There are three basic steps to the evaluation algorithm:
            // 1) Determine all Members for each axis but do not save
            // information (do not build the RolapAxis),
            // 2) Save all Members for each axis (build RolapAxis).
            // 3) Evaluate and store each Cell determined by the Members
            // of the axes.
            // Step 1 converges on the stable set of Members pre axis.
            // Steps 1 and 2 make sure that the data has been loaded.
            //
            // More detail follows.
            //
            // Explicit and Implicit Members:
            // A Member is said to be 'explicit' if it appears on one of
            // the Axes (one of the RolapAxis Position List of Members).
            // A Member is 'implicit' if it is in the query but does not
            // end up on any Axes (its usage, for example, is in a function).
            // When for a Dimension none of its Members are explicit in the
            // query, then the default Member is used which is like putting
            // the Member in the Slicer.
            //
            // Special Dimensions:
            // There are 2 special dimensions.
            // The first is the Time dimension. If in a schema there is
            // no ALL Member, then Whatever happens to be the default
            // Member is used if Time Members are not explicitly set
            // in the query.
            // The second is the Measures dimension. This dimension
            // NEVER has an ALL Member. A cube's default Measure is set
            // by convention - its simply the first Measure defined in the
            // cube.
            //
            // First a RolapEvaluator is created. During its creation,
            // it gets a Member from each Hierarchy. Each Member is the
            // default Member of the Hierarchy. For most Hierarchies this
            // Member is the ALL Member, but there are cases where 1)
            // a Hierarchy does not have an ALL Member or 2) the Hierarchy
            // has an ALL Member but that Member is not the default Member.
            // In these cases, the default Member is still used, but its
            // use can cause evaluation issues (seemingly strange evaluation
            // results).
            //
            // Next, load all root Members for Hierarchies that have no ALL
            // Member and load ALL Members that are not the default Member.
            //
            // Determine the Members of the Slicer axis (Step 1 above).  Any
            // Members found are added to the AxisMember object. If one of these
            // Members happens to be a Measure, then the Slicer is explicitly
            // specifying the query's Measure and this should be put into the
            // evaluator's context (replacing the default Measure which just
            // happens to be the first Measure defined in the cube).  Other
            // Members found in the AxisMember object are also placed into the
            // evaluator's context since these also are explicitly specified.
            // Also, any other Members in the AxisMember object which have the
            // same Hierarchy as Members in the list of root Members for
            // Hierarchies that have no ALL Member, replace those Members - they
            // Slicer has explicitly determined which ones to use. The
            // AxisMember object is now cleared.
            // The Slicer does not depend upon the other Axes, but the other
            // Axes depend upon both the Slicer and each other.
            //
            // The AxisMember object also checks if the number of Members
            // exceeds the ResultLimit property throwing a
            // TotalMembersLimitExceeded Exception if it does.
            //
            // For all non-Slicer axes, the Members are determined (Step 1
            // above). If a Measure is found in the AxisMember, then an
            // Axis is explicitly specifying a Measure.
            // If any Members in the AxisMember object have the same Hierarchy
            // as a Member in the set of root Members for Hierarchies that have
            // no ALL Member, then replace those root Members with the Member
            // from the AxisMember object. In this case, again, a Member
            // was explicitly specified in an Axis. If this replacement
            // occurs, then one must redo this step with the new Members.
            //
            // Now Step 3 above is done. First to the Slicer Axis and then
            // to the other Axes. Here the Axes are actually generated.
            // If a Member of an Axis is an Calculated Member (and the
            // Calculated Member is not a Member of the Measure Hierarchy),
            // then find the Dimension associated with the Calculated
            // Member and remove Members with the same Dimension in the set of
            // root Members for Hierarchies that have no ALL Member.
            // This is done because via the Calculated Member the Member
            // was implicitly specified in the query. If this removal occurs,
            // then the Axes must be re-evaluated repeating Step 3.
            //
            /////////////////////////////////////////////////////////////////


            // The AxisMember object is used to hold Members that are found
            // during Step 1 when the Axes are determined.
            final AxisMemberList axisMembers = new AxisMemberList();


            // list of ALL Members that are not default Members
            final List<Member> nonDefaultAllMembers = new ArrayList<Member>();

            // List of Members of Hierarchies that do not have an ALL Member
            List<List<Member>> nonAllMembers = new ArrayList<List<Member>>();

            // List of Measures
            final List<Member> measureMembers = new ArrayList<Member>();

            // load all root Members for Hierarchies that have no ALL
            // Member and load ALL Members that are not the default Member.
            // Also, all Measures are are gathered.
            loadSpecialMembers(
                nonDefaultAllMembers, nonAllMembers, measureMembers);

            // clear evaluation cache
            query.clearEvalCache();

            // Save, may be needed by some Expression Calc's
            query.putEvalCache("ALL_MEMBER_LIST", nonDefaultAllMembers);


            final List<List<Member>> emptyNonAllMembers =
                Collections.emptyList();

            // Initial evaluator, to execute slicer.
            // Used by named sets in slicer
            slicerEvaluator = evaluator.push();

            /////////////////////////////////////////////////////////////////
            // Determine Slicer
            //
            axisMembers.setSlicer(true);
            loadMembers(
                emptyNonAllMembers,
                evaluator,
                query.getSlicerAxis(),
                query.slicerCalc,
                axisMembers);
            axisMembers.setSlicer(false);

            // Save unadulterated context for the next time we need to evaluate
            // the slicer.
            final RolapEvaluator savedEvaluator = evaluator.push();

            if (!axisMembers.isEmpty()) {
                for (Member m : axisMembers) {
                    if (m == null) {
                        break;
                    }
                    evaluator.setSlicerContext(m);
                    if (m.isMeasure()) {
                        // A Measure was explicitly declared in the
                        // Slicer, don't need to worry about Measures
                        // for this query.
                        measureMembers.clear();
                    }
                }
                replaceNonAllMembers(nonAllMembers, axisMembers);
                axisMembers.clearMembers();
            }

            // Save evaluator that has slicer as its context.
            slicerEvaluator = evaluator.push();

            /////////////////////////////////////////////////////////////////
            // Execute Slicer
            //
            Axis savedSlicerAxis;
            RolapEvaluator internalSlicerEvaluator;
            do {
                TupleIterable tupleIterable =
                    evalExecute(
                        nonAllMembers,
                        nonAllMembers.size() - 1,
                        savedEvaluator,
                        query.getSlicerAxis(),
                        query.slicerCalc);
                // Materialize the iterable as a list. Although it may take
                // memory, we need the first member below, and besides, slicer
                // axes are generally small.
                TupleList tupleList =
                    TupleCollections.materialize(tupleIterable, true);

                this.slicerAxis = new RolapAxis(tupleList);
                // the slicerAxis may be overwritten during slicer execution
                // if there is a compound slicer.  Save it so that it can be
                // reverted before completing result construction.
                savedSlicerAxis = this.slicerAxis;

                // Use the context created by the slicer for the other
                // axes.  For example, "select filter([Customers], [Store
                // Sales] > 100) on columns from Sales where
                // ([Time].[1998])" should show customers whose 1998 (not
                // total) purchases exceeded 100.
                internalSlicerEvaluator = this.evaluator;
                if (tupleList.size() > 1) {
                    tupleList =
                        removeUnaryMembersFromTupleList(
                            tupleList, evaluator);
                    tupleList =
                        AggregateFunDef.AggregateCalc.optimizeTupleList(
                            evaluator,
                            tupleList,
                            false);
                    evaluator.setSlicerTuples(tupleList);

                    final Calc valueCalc =
                        new ValueCalc(
                            new DummyExp(new ScalarType()));

                    final List<Member> prevSlicerMembers =
                        new ArrayList<Member>();

                    final Calc calcCached =
                        new GenericCalc(
                            new DummyExp(query.slicerCalc.getType()))
                        {
                            public Object evaluate(Evaluator evaluator) {
                                TupleList list = AbstractAggregateFunDef
                                    .processUnrelatedDimensions(
                                        ((RolapEvaluator) evaluator)
                                            .getOptimizedSlicerTuples(null),
                                        evaluator);
                                for (Member member : prevSlicerMembers) {
                                    if (evaluator.getContext(
                                            member.getHierarchy())
                                        instanceof CompoundSlicerRolapMember)
                                    {
                                        evaluator.setContext(member);
                                    }
                                }
                                return AggregateFunDef.AggregateCalc.aggregate(
                                    valueCalc, evaluator, list);
                            }
                            // depend on the full evaluation context
                            public boolean dependsOn(Hierarchy hierarchy) {
                                return true;
                            }
                        };

                    final ExpCacheDescriptor cacheDescriptor =
                        new ExpCacheDescriptor(
                            query.getSlicerAxis().getSet(),
                            calcCached,
                            evaluator);
                    // generate a cached calculation for slicer aggregation
                    final Calc calc = new CacheCalc(
                        query.getSlicerAxis().getSet(),
                        cacheDescriptor);

                    // replace the slicer set with a placeholder to avoid
                    // interaction between the aggregate calc we just created
                    // and any calculated members that might be present in
                    // the slicer.
                    // Arbitrarily picks the first dim of the first tuple
                    // to use as placeholder.
                    if (tupleList.get(0).size() > 1) {
                        for (int i = 1; i < tupleList.get(0).size(); i++) {
                            Member placeholder = setPlaceholderSlicerAxis(
                                (RolapMember)tupleList.get(0).get(i),
                                calc,
                                false);
                            prevSlicerMembers.add(
                                evaluator.setContext(placeholder));
                        }
                    }

                    Member placeholder = setPlaceholderSlicerAxis(
                        (RolapMember)tupleList.get(0).get(0), calc, true);
                    evaluator.setContext(placeholder);
                }
            } while (phase());

            // final slicerEvaluator
            slicerEvaluator = evaluator.push();

            /////////////////////////////////////////////////////////////////
            // Determine Axes
            //
            boolean changed = false;

            // reset to total member count
            axisMembers.clearTotalCellCount();

            for (int i = 0; i < axes.length; i++) {
                final QueryAxis axis = query.axes[i];
                final Calc calc = query.axisCalcs[i];
                loadMembers(
                    emptyNonAllMembers, evaluator, axis, calc, axisMembers);
            }

            if (!axisMembers.isEmpty()) {
            for (Member m : axisMembers) {
                if (m.isMeasure()) {
                    // A Measure was explicitly declared on an
                    // axis, don't need to worry about Measures
                    // for this query.
                    measureMembers.clear();
                    }
                }
                changed = replaceNonAllMembers(nonAllMembers, axisMembers);
                axisMembers.clearMembers();
            }

            if (changed) {
                // only count number of members, do not collect any
                axisMembers.countOnly(true);
                // reset to total member count
                axisMembers.clearTotalCellCount();

                final int savepoint = evaluator.savepoint();
                try {
                    for (int i = 0; i < axes.length; i++) {
                        final QueryAxis axis = query.axes[i];
                        final Calc calc = query.axisCalcs[i];
                        loadMembers(
                            nonAllMembers,
                            evaluator,
                            axis, calc, axisMembers);
                        evaluator.restore(savepoint);
                    }
                } finally {
                    evaluator.restore(savepoint);
                }
            }

            // throws exception if number of members exceeds limit
            axisMembers.checkLimit();

            /////////////////////////////////////////////////////////////////
            // Execute Axes
            //
            final int savepoint = evaluator.savepoint();
            do {
                try {
                    boolean redo;
                    do {
                        evaluator.restore(savepoint);
                        redo = false;
                        for (int i = 0; i < axes.length; i++) {
                            QueryAxis axis = query.axes[i];
                            final Calc calc = query.axisCalcs[i];
                            TupleIterable tupleIterable =
                                evalExecute(
                                    nonAllMembers,
                                    nonAllMembers.size() - 1,
                                    evaluator,
                                    axis,
                                    calc);

                            if (!nonAllMembers.isEmpty()) {
                                final TupleIterator tupleIterator =
                                    tupleIterable.tupleIterator();
                                if (tupleIterator.hasNext()) {
                                    List<Member> tuple0 = tupleIterator.next();
                                    // Only need to process the first tuple on
                                    // the axis.
                                    for (Member m : tuple0) {
                                        if (m.isCalculated()) {
                                            CalculatedMeasureVisitor visitor =
                                                new CalculatedMeasureVisitor();
                                            m.getExpression().accept(visitor);
                                            Dimension dimension =
                                                visitor.dimension;
                                            if (removeDimension(
                                                    dimension, nonAllMembers))
                                            {
                                                redo = true;
                                            }
                                        }
                                    }
                                }
                            }
                            this.axes[i] =
                                new RolapAxis(
                                    TupleCollections.materialize(
                                        tupleIterable, false));
                        }
                    } while (redo);
                } catch (CellRequestQuantumExceededException e) {
                    // Safe to ignore. Need to call 'phase' and loop again.
                }
            } while (phase());

            evaluator.restore(savepoint);

            // Get value for each Cell
            final Locus locus = new Locus(execution, null, "Loading cells");
            Locus.push(locus);
            try {
                executeBody(
                    internalSlicerEvaluator, query, new int[axes.length]);
            } finally {
                Locus.pop(locus);
            }

            // If you are very close to running out of memory due to
            // the number of CellInfo's in cellInfos, then calling this
            // may cause the out of memory one is trying to aviod.
            // On the other hand, calling this can reduce the size of
            // the ObjectPool's internal storage by half (but, of course,
            // it will not reduce the size of the stored objects themselves).
            // Only call this if there are lots of CellInfo.
            if (this.cellInfos.size() > 10000) {
                this.cellInfos.trimToSize();
            }
            // revert the slicer axis so that the original slicer
            // can be included in the result.
            this.slicerAxis  = savedSlicerAxis;
        } catch (ResultLimitExceededException ex) {
            // If one gets a ResultLimitExceededException, then
            // don't count on anything being worth caching.
            normalExecution = false;

            // De-reference data structures that might be holding
            // partial results but surely are taking up memory.
            evaluator = null;
            slicerEvaluator = null;
            cellInfos = null;
            batchingReader = null;
            for (int i = 0; i < axes.length; i++) {
                axes[i] = null;
            }
            slicerAxis = null;

            query.clearEvalCache();

            throw ex;
        } finally {
            if (normalExecution) {
                // Expression cache duration is for each query. It is time to
                // clear out the whole expression cache at the end of a query.
                evaluator.clearExpResultCache(true);
            }
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("RolapResult<init>: " + Util.printMemory());
            }
        }
    }

    /**
     * Sets slicerAxis to a dummy placeholder RolapAxis containing
     * a single item TupleList with the null member of hierarchy.
     * This is used with compound slicer evaluation to avoid the slicer
     * tuple list from interacting with the aggregate calc which rolls up
     * the set.  This member will contain the AggregateCalc which rolls
     * up the set on the slicer.
     */
    private Member setPlaceholderSlicerAxis(
        final RolapMember member, final Calc calc, boolean setAxis)
    {
        ValueFormatter formatter;
        if (member.getDimension().isMeasures()) {
            formatter = ((RolapMeasure)member).getFormatter();
        } else {
            formatter = null;
        }

        CompoundSlicerRolapMember placeholderMember =
            new CompoundSlicerRolapMember(
                (RolapMember)member.getHierarchy().getNullMember(),
                calc, formatter);


        placeholderMember.setProperty(
            Property.FORMAT_STRING.getName(),
            member.getPropertyValue(Property.FORMAT_STRING.getName()));
        placeholderMember.setProperty(
            Property.FORMAT_EXP_PARSED.getName(),
            member.getPropertyValue(Property.FORMAT_EXP_PARSED.getName()));

        if (setAxis) {
            TupleList dummyList = TupleCollections.createList(1);
            dummyList.addTuple(placeholderMember);
            this.slicerAxis = new RolapAxis(dummyList);
        }
        return placeholderMember;
    }

    private boolean phase() {
        if (batchingReader.isDirty()) {
            execution.tracePhase(
                batchingReader.getHitCount(),
                batchingReader.getMissCount(),
                batchingReader.getPendingCount());
                // flush the expression cache during each
                // phase of loading aggregations
                evaluator.clearExpResultCache(false);

            return batchingReader.loadAggregations();
        } else {
            return false;
        }
    }

    /**
     * This function removes single instance members from the compound slicer,
     * enabling more regular slicer behavior for those members. For instance,
     * calculated members can override the context of these members correctly.
     *
     * @param tupleList The list to shrink.
     * @param evaluator The slicer evaluator.
     * @return a new list of tuples reduced in size.
     */
    private TupleList removeUnaryMembersFromTupleList(
        TupleList tupleList, RolapEvaluator evaluator)
    {
        // we can remove any unary coordinates from the compound slicer, and
        // account for them in the slicer evaluator.

        // First, determine if there are any unary members within the tuples.
        List<Member> first = null;
        boolean unary[] = null;
        for (List<Member> tuple : tupleList) {
            if (first == null) {
                first = tuple;
                unary = new boolean[tuple.size()];
                for (int i = 0 ; i < unary.length; i++) {
                    unary[i] = true;
                }
            } else {
                for (int i = 0; i < tuple.size(); i++) {
                    if (unary[i] && !tuple.get(i).equals(first.get(i))) {
                        unary[i] = false;
                    }
                }
            }
        }
        int toRemove = 0;
        for (int i = 0; i < unary.length; i++) {
            if (unary[i]) {
                evaluator.setContext(first.get(i));
                toRemove++;
            }
        }

        // remove the unnecessary members from the compound slicer
        if (toRemove > 0) {
          TupleList newList =
              new ListTupleList(
                  tupleList.getArity() - toRemove,
                  new ArrayList<Member>());
          for (List<Member> tuple : tupleList) {
              List<Member> ntuple = new ArrayList<Member>();
              for (int i = 0; i < tuple.size(); i++) {
                  if (!unary[i]) {
                      ntuple.add(tuple.get(i));
                  }
              }
              newList.add(ntuple);
          }
          tupleList = newList;
        }
        return tupleList;
    }

    @Override
    public void close() {
        super.close();
    }

    protected boolean removeDimension(
        Dimension dimension,
        List<List<Member>> memberLists)
    {
        for (int i = 0; i < memberLists.size(); i++) {
            List<Member> memberList = memberLists.get(i);
            if (memberList.get(0).getDimension().equals(dimension)) {
                memberLists.remove(i);
                return true;
            }
        }
        return false;
    }

    public final Execution getExecution() {
        return execution;
    }

    private static class CalculatedMeasureVisitor
        extends MdxVisitorImpl
    {
        Dimension dimension;

        CalculatedMeasureVisitor() {
        }

        public Object visit(DimensionExpr dimensionExpr) {
            dimension = dimensionExpr.getDimension();
            return null;
        }

        public Object visit(HierarchyExpr hierarchyExpr) {
            Hierarchy hierarchy = hierarchyExpr.getHierarchy();
            dimension = hierarchy.getDimension();
            return null;
        }

        public Object visit(MemberExpr memberExpr)  {
            Member member = memberExpr.getMember();
            dimension = member.getHierarchy().getDimension();
            return null;
        }
    }

    protected boolean replaceNonAllMembers(
        List<List<Member>> nonAllMembers,
        AxisMemberList axisMembers)
    {
        boolean changed = false;
        List<Member> mList = new ArrayList<Member>();
        for (ListIterator<List<Member>> it = nonAllMembers.listIterator();
                it.hasNext();)
        {
            List<Member> ms = it.next();
            Hierarchy h = ms.get(0).getHierarchy();
            mList.clear();
            for (Member m : axisMembers) {
                if (m.getHierarchy().equals(h)) {
                    mList.add(m);
                }
            }
            if (! mList.isEmpty()) {
                changed = true;
                it.set(new ArrayList<Member>(mList));
            }
        }
        return changed;
    }

    protected void loadMembers(
        List<List<Member>> nonAllMembers,
        RolapEvaluator evaluator,
        QueryAxis axis,
        Calc calc,
        AxisMemberList axisMembers)
    {
        int attempt = 0;
        evaluator.setCellReader(batchingReader);
        while (true) {
            axisMembers.clearAxisCount();
            final int savepoint = evaluator.savepoint();
            try {
                evalLoad(
                    nonAllMembers,
                    nonAllMembers.size() - 1,
                    evaluator,
                    axis,
                    calc,
                    axisMembers);
            } catch (CellRequestQuantumExceededException e) {
                // Safe to ignore. Need to call 'phase' and loop again.
                // Decrement count because it wasn't a recursive formula that
                // caused the iteration.
                --attempt;
            } finally {
                evaluator.restore(savepoint);
            }

            if (!phase()) {
                break;
            } else {
                // Clear invalid expression result so that the next evaluation
                // will pick up the newly loaded aggregates.
                evaluator.clearExpResultCache(false);
            }

            if (attempt++ > maxEvalDepth) {
                throw Util.newInternal(
                    "Failed to load all aggregations after "
                    + maxEvalDepth
                    + " passes; there's probably a cycle");
            }
        }
    }

    void evalLoad(
        List<List<Member>> nonAllMembers,
        int cnt,
        Evaluator evaluator,
        QueryAxis axis,
        Calc calc,
        AxisMemberList axisMembers)
    {
        final int savepoint = evaluator.savepoint();
        try {
            if (cnt < 0) {
                executeAxis(evaluator, axis, calc, false, axisMembers);
            } else {
                for (Member m : nonAllMembers.get(cnt)) {
                    evaluator.setContext(m);
                    evalLoad(
                        nonAllMembers, cnt - 1, evaluator,
                        axis, calc, axisMembers);
                }
            }
        } finally {
            evaluator.restore(savepoint);
        }
    }

    TupleIterable evalExecute(
        List<List<Member>> nonAllMembers,
        int cnt,
        RolapEvaluator evaluator,
        QueryAxis queryAxis,
        Calc calc)
    {
        final int savepoint = evaluator.savepoint();
        final int arity = calc == null ? 0 : calc.getType().getArity();
        if (cnt < 0) {
            try {
                final TupleIterable axis =
                    executeAxis(evaluator, queryAxis, calc, true, null);
                return axis;
            } finally {
                evaluator.restore(savepoint);
            }
            // No need to clear expression cache here as no new aggregates are
            // loaded(aggregatingReader reads from cache).
        } else {
            try {
                TupleList axisResult = TupleCollections.emptyList(arity);
                for (Member m : nonAllMembers.get(cnt)) {
                    evaluator.setContext(m);
                    TupleIterable axis =
                        evalExecute(
                            nonAllMembers, cnt - 1,
                            evaluator, queryAxis, calc);
                    boolean ordered = false;
                    if (queryAxis != null) {
                        ordered = queryAxis.isOrdered();
                    }
                    axisResult = mergeAxes(axisResult, axis, ordered);
                }
                return axisResult;
            } finally {
                evaluator.restore(savepoint);
            }
        }
    }

    /**
     * Finds all root Members 1) whose Hierarchy does not have an ALL
     * Member, 2) whose default Member is not the ALL Member and 3)
     * all Measures.
     *
     * @param nonDefaultAllMembers  List of all root Members for Hierarchies
     * whose default Member is not the ALL Member.
     * @param nonAllMembers List of root Members for Hierarchies that have no
     * ALL Member.
     * @param measureMembers  List all Measures
     */
    protected void loadSpecialMembers(
        List<Member> nonDefaultAllMembers,
        List<List<Member>> nonAllMembers,
        List<Member> measureMembers)
    {
        SchemaReader schemaReader = evaluator.getSchemaReader();
        Member[] evalMembers = evaluator.getMembers();
        for (Member em : evalMembers) {
            if (em.isCalculated()) {
                continue;
            }
            Hierarchy h = em.getHierarchy();
            Dimension d = h.getDimension();
            if (d.getDimensionType() == DimensionType.TimeDimension) {
                continue;
            }
            if (!em.isAll()) {
                List<Member> rootMembers =
                    schemaReader.getHierarchyRootMembers(h);
                if (em.isMeasure()) {
                    for (Member mm : rootMembers) {
                        measureMembers.add(mm);
                    }
                } else {
                    if (h.hasAll()) {
                        for (Member m : rootMembers) {
                            if (m.isAll()) {
                                nonDefaultAllMembers.add(m);
                                break;
                            }
                        }
                    } else {
                        nonAllMembers.add(rootMembers);
                    }
                }
            }
        }
    }

    protected Logger getLogger() {
        return LOGGER;
    }

    public final RolapCube getCube() {
        return evaluator.getCube();
    }

    // implement Result
    public Axis[] getAxes() {
        return axes;
    }

    /**
     * Get the Cell for the given Cell position.
     *
     * @param pos Cell position.
     * @return the Cell associated with the Cell position.
     */
    public Cell getCell(int[] pos) {
        if (pos.length != point.size()) {
            throw Util.newError(
                "coordinates should have dimension " + point.size());
        }

        for (int i = 0; i < pos.length; i++) {
            if (positionsHighCardinality.get(i)) {
                final Locus locus = new Locus(execution, null, "Loading cells");
                Locus.push(locus);
                try {
                    executeBody(evaluator, statement.getQuery(), pos);
                } finally {
                    Locus.pop(locus);
                }
                break;
            }
        }

        CellInfo ci = cellInfos.lookup(pos);
        if (ci.value == null) {
            for (int i = 0; i < pos.length; i++) {
                int po = pos[i];
                if (po < 0 || po >= axes[i].getPositions().size()) {
                    throw Util.newError("coordinates out of range");
                }
            }
            ci.value = Util.nullValue;
        }

        return new RolapCell(this, pos.clone(), ci);
    }

    private TupleIterable executeAxis(
        Evaluator evaluator,
        QueryAxis queryAxis,
        Calc axisCalc,
        boolean construct,
        AxisMemberList axisMembers)
    {
        if (queryAxis == null) {
            // Create an axis containing one position with no members (not
            // the same as an empty axis).
            return new DelegatingTupleList(
                0,
                Collections.singletonList(Collections.<Member>emptyList()));
        }
        final int savepoint = evaluator.savepoint();
        try {
            evaluator.setNonEmpty(queryAxis.isNonEmpty());
            evaluator.setEvalAxes(true);
            final TupleIterable iterable =
                ((IterCalc) axisCalc).evaluateIterable(evaluator);
            if (axisCalc.getClass().getName().indexOf("OrderFunDef") != -1) {
                queryAxis.setOrdered(true);
            }
            if (iterable instanceof TupleList) {
                TupleList list = (TupleList) iterable;
                if (construct) {
                } else if (axisMembers != null) {
                    axisMembers.mergeTupleList(list);
                }
            } else {
                // Iterable
                TupleCursor cursor = iterable.tupleCursor();
                if (construct) {
                } else if (axisMembers != null) {
                    axisMembers.mergeTupleIter(cursor);
                }
            }
            return iterable;
        } finally {
            evaluator.restore(savepoint);
        }
    }

    private void executeBody(
        RolapEvaluator evaluator,
        Query query,
        final int[] pos)
    {
        // Compute the cells several times. The first time, use a dummy
        // evaluator which collects requests.
        int count = 0;
        final int savepoint = evaluator.savepoint();
        while (true) {
            evaluator.setCellReader(batchingReader);
            try {
                executeStripe(query.axes.length - 1, evaluator, pos);
            } catch (CellRequestQuantumExceededException e) {
                // Safe to ignore. Need to call 'phase' and loop again.
                // Decrement count because it wasn't a recursive formula that
                // caused the iteration.
                --count;
            }
            evaluator.restore(savepoint);

            // Retrieve the aggregations collected.
            //
            if (!phase()) {
                // We got all of the cells we needed, so the result must be
                // correct.
                return;
            } else {
                // Clear invalid expression result so that the next evaluation
                // will pick up the newly loaded aggregates.
                evaluator.clearExpResultCache(false);
            }

            if (count++ > maxEvalDepth) {
                if (evaluator instanceof RolapDependencyTestingEvaluator) {
                    // The dependency testing evaluator can trigger new
                    // requests every cycle. So let is run as normal for
                    // the first N times, then run it disabled.
                    ((RolapDependencyTestingEvaluator.DteRoot)
                        evaluator.root).disabled = true;
                    if (count > maxEvalDepth * 2) {
                        throw Util.newInternal(
                            "Query required more than " + count
                            + " iterations");
                    }
                } else {
                    throw Util.newInternal(
                        "Query required more than " + count + " iterations");
                }
            }

            cellInfos.clear();
        }
    }

    boolean isDirty() {
        return batchingReader.isDirty();
    }

    /**
     * Evaluates an expression. Intended for evaluating named sets.
     *
     * <p>Does not modify the contents of the evaluator.
     *
     * @param calc Compiled expression
     * @param slicerEvaluator Evaluation context for slicers
     * @param contextEvaluator Evaluation context (optional)
     * @return Result
     */
    Object evaluateExp(
        Calc calc,
        RolapEvaluator slicerEvaluator,
        Evaluator contextEvaluator)
    {
        int attempt = 0;

        RolapEvaluator evaluator = slicerEvaluator.push();
        if (contextEvaluator != null && contextEvaluator.isEvalAxes()) {
            evaluator.setEvalAxes(true);
        }

        final int savepoint = evaluator.savepoint();
        boolean dirty = batchingReader.isDirty();
        try {
            while (true) {
                evaluator.restore(savepoint);

                evaluator.setCellReader(batchingReader);
                Object preliminaryValue = calc.evaluate(evaluator);

                if (preliminaryValue instanceof TupleIterable) {
                    // During the preliminary phase, we have to materialize the
                    // tuple lists or the evaluation lower down won't take into
                    // account all the tuples.
                    TupleIterable iterable = (TupleIterable) preliminaryValue;
                    final TupleCursor cursor = iterable.tupleCursor();
                    while (cursor.forward()) {
                        // ignore
                    }
                }

                if (!phase()) {
                    break;
                } else {
                    // Clear invalid expression result so that the next
                    // evaluation will pick up the newly loaded aggregates.
                    evaluator.clearExpResultCache(false);
                }

                if (attempt++ > maxEvalDepth) {
                    throw Util.newInternal(
                        "Failed to load all aggregations after "
                        + maxEvalDepth + "passes; there's probably a cycle");
                }
            }

            // If there were pending reads when we entered, some of the other
            // expressions may have been evaluated incorrectly. Set the
            // reader's 'dirty' flag so that the caller knows that it must
            // re-evaluate them.
            if (dirty) {
                batchingReader.setDirty(true);
            }

            evaluator.restore(savepoint);
            evaluator.setCellReader(aggregatingReader);
            final Object o = calc.evaluate(evaluator);
            return o;
        } finally {
            evaluator.restore(savepoint);
        }
    }

    private void executeStripe(
        int axisOrdinal,
        RolapEvaluator revaluator,
        final int[] pos)
    {
        if (axisOrdinal < 0) {
            RolapAxis axis = (RolapAxis) slicerAxis;
            TupleList tupleList = axis.getTupleList();
            final Iterator<List<Member>> tupleIterator = tupleList.iterator();
            if (tupleIterator.hasNext()) {
                final List<Member> members = tupleIterator.next();
                execution.checkCancelOrTimeout();
                final int savepoint = revaluator.savepoint();
                revaluator.setContext(members);
                Object o;
                try {
                    o = revaluator.evaluateCurrent();
                } catch (MondrianEvaluationException e) {
                    LOGGER.warn("Mondrian: exception in executeStripe.", e);
                    o = e;
                } finally {
                    revaluator.restore(savepoint);
                }

                CellInfo ci = null;

                // Get the Cell's format string and value formatting
                // Object.
                try {
                    // This code is a combination of the code found in
                    // the old RolapResult
                    // <code>getCellNoDefaultFormatString</code> method and
                    // the old RolapCell <code>getFormattedValue</code> method.

                    // Create a CellInfo object for the given position
                    // integer array.
                    ci = cellInfos.create(point.getOrdinals());

                    String cachedFormatString = null;

                    // Determine if there is a CellFormatter registered for
                    // the current Cube's Measure's Dimension. If so,
                    // then find or create a CellFormatterValueFormatter
                    // for it. If not, then find or create a Locale based
                    // FormatValueFormatter.
                    final RolapCube cube = getCube();
                    Hierarchy measuresHierarchy =
                        cube.getMeasuresHierarchy();
                    RolapMeasure m =
                        (RolapMeasure) revaluator.getContext(measuresHierarchy);
                    ValueFormatter valueFormatter = m.getFormatter();
                    if (valueFormatter == null) {
                        cachedFormatString = revaluator.getFormatString();
                        Locale locale =
                            statement.getMondrianConnection().getLocale();
                        valueFormatter = formatValueFormatters.get(locale);
                        if (valueFormatter == null) {
                            valueFormatter = new FormatValueFormatter(locale);
                            formatValueFormatters.put(locale, valueFormatter);
                        }
                    }

                    ci.formatString = cachedFormatString;
                    ci.valueFormatter = valueFormatter;
                } catch (ResultLimitExceededException e) {
                    // Do NOT ignore a ResultLimitExceededException!!!
                    throw e;
                } catch (CellRequestQuantumExceededException e) {
                    // We need to throw this so another phase happens.
                    throw e;
                } catch (MondrianEvaluationException e) {
                    // ignore but warn
                    LOGGER.warn("Mondrian: exception in executeStripe.", e);
                } catch (Error e) {
                    // Errors indicate fatal JVM problems; do not discard
                    throw e;
                } catch (Throwable e) {
                    LOGGER.warn("Mondrian: exception in executeStripe.", e);
                    Util.discard(e);
                }

                if (o != RolapUtil.valueNotReadyException) {
                    ci.value = o;
                }
            }
        } else {
            RolapAxis axis = (RolapAxis) axes[axisOrdinal];
            TupleList tupleList = axis.getTupleList();
            Util.discard(tupleList.size()); // force materialize
            if (isAxisHighCardinality(axisOrdinal, tupleList)) {
                final int limit =
                    MondrianProperties.instance().HighCardChunkSize.get();
                if (positionsIterators.get(axisOrdinal) == null) {
                    final TupleCursor tupleCursor = tupleList.tupleCursor();
                    positionsIterators.put(axisOrdinal, tupleCursor);
                    positionsIndexes.put(axisOrdinal, 0);
                    final List<List<Member>> subPositions =
                        new ArrayList<List<Member>>();
                    for (int i = 0; i < limit && tupleCursor.forward(); i++) {
                        subPositions.add(tupleCursor.current());
                    }
                    positionsCurrent.put(axisOrdinal, subPositions);
                }
                final TupleCursor tupleCursor =
                    positionsIterators.get(axisOrdinal);
                final int positionIndex = positionsIndexes.get(axisOrdinal);
                List<List<Member>> subTuples =
                    positionsCurrent.get(axisOrdinal);

                if (subTuples == null) {
                    return;
                }

                int pi;
                if (pos[axisOrdinal] > positionIndex + subTuples.size() - 1
                        && subTuples.size() == limit)
                {
                    pi = positionIndex + subTuples.size();
                    positionsIndexes.put(
                        axisOrdinal, positionIndex + subTuples.size());
                    subTuples.subList(0, subTuples.size()).clear();
                    for (int i = 0; i < limit && tupleCursor.forward(); i++) {
                        subTuples.add(tupleCursor.current());
                    }
                    positionsCurrent.put(axisOrdinal, subTuples);
                } else {
                    pi = positionIndex;
                }
                for (final List<Member> tuple : subTuples) {
                    point.setAxis(axisOrdinal, pi);
                    final int savepoint = revaluator.savepoint();
                    try {
                        revaluator.setContext(tuple);
                        execution.checkCancelOrTimeout();
                        executeStripe(axisOrdinal - 1, revaluator, pos);
                    } finally {
                        revaluator.restore(savepoint);
                    }
                    pi++;
                }
            } else {
                for (List<Member> tuple : tupleList) {
                    List<Member> measures =
                        new ArrayList<Member>(
                            statement.getQuery().getMeasuresMembers());
                    for (Member measure : measures) {
                        if (measure instanceof RolapBaseCubeMeasure) {
                            RolapBaseCubeMeasure baseCubeMeasure =
                                (RolapBaseCubeMeasure) measure;
                            if (baseCubeMeasure.getAggregator()
                                == RolapAggregator.DistinctCount)
                            {
                                processDistinctMeasureExpr(
                                    tuple, baseCubeMeasure);
                            }
                        }
                    }
                }

                int tupleIndex = 0;
                for (final List<Member> tuple : tupleList) {
                    point.setAxis(axisOrdinal, tupleIndex);
                    final int savepoint = revaluator.savepoint();
                    try {
                        revaluator.setContext(tuple);
                        execution.checkCancelOrTimeout();
                        executeStripe(axisOrdinal - 1, revaluator, pos);
                    } finally {
                        revaluator.restore(savepoint);
                    }
                    tupleIndex++;
                }
            }
        }
    }

    private boolean isAxisHighCardinality(
        int axisOrdinal,
        TupleList tupleList)
    {
        Boolean highCardinality =
            positionsHighCardinality.get(axisOrdinal);
        if (highCardinality != null) {
            return highCardinality;
        }
        highCardinality = false;
        //noinspection LoopStatementThatDoesntLoop
        List<Member> tuple = !tupleList.isEmpty()
            ? tupleList.get(0)
            : null;
        if (tuple != null && !tuple.isEmpty()) {
            Dimension dimension = tuple.get(0).getDimension();
            highCardinality = dimension.isHighCardinality();
            if (highCardinality) {
                LOGGER.warn(
                    MondrianResource.instance()
                        .HighCardinalityInDimension.str(
                            dimension.getUniqueName()));
            }
        }
        positionsHighCardinality.put(axisOrdinal, highCardinality);
        return highCardinality;
    }

    /**
     * Distinct counts are aggregated separately from other measures.
     * We need to apply filters to each level in the query.
     *
     * <p>Replace VisualTotalMember expressions with new expressions
     * where all leaf level members are included.</p>
     *
     * <p>Example.
     * For MDX query:
     *
     * <blockquote><pre>
     * WITH SET [XL_Row_Dim_0] AS
     *         VisualTotals(
     *           Distinct(
     *             Hierarchize(
     *               {Ascendants([Store].[All Stores].[USA].[CA]),
     *                Descendants([Store].[All Stores].[USA].[CA])})))
     *        select NON EMPTY
     *          Hierarchize(
     *            Intersect(
     *              {DrilldownLevel({[Store].[All Stores]})},
     *              [XL_Row_Dim_0])) ON COLUMNS
     *        from [HR]
     *        where [Measures].[Number of Employees]</pre></blockquote>
     *
     * <p>For member [Store].[All Stores],
     * we replace aggregate expression
     *
     * <blockquote><pre>
     * Aggregate({[Store].[All Stores].[USA]})
     * </pre></blockquote>
     *
     * with
     *
     * <blockquote><pre>
     * Aggregate({[Store].[All Stores].[USA].[CA].[Alameda].[HQ],
     *               [Store].[All Stores].[USA].[CA].[Beverly Hills].[Store 6],
     *               [Store].[All Stores].[USA].[CA].[Los Angeles].[Store 7],
     *               [Store].[All Stores].[USA].[CA].[San Diego].[Store 24],
     *               [Store].[All Stores].[USA].[CA].[San Francisco].[Store 14]
     *              })
     * </pre></blockquote>
     *
     * <p>TODO:
     * Can be optimized. For that particular query
     * we don't need to go to the lowest level.
     * We can simply replace it with:
     * <pre>Aggregate({[Store].[All Stores].[USA].[CA]})</pre>
     * Because all children of [Store].[All Stores].[USA].[CA] are included.</p>
     */
    private List<Member> processDistinctMeasureExpr(
        List<Member> tuple,
        RolapBaseCubeMeasure measure)
    {
        for (Member member : tuple) {
            if (!(member instanceof VisualTotalMember)) {
                continue;
            }
            evaluator.setContext(measure);
            List<Member> exprMembers = new ArrayList<Member>();
            processMemberExpr(member, exprMembers);
            ((VisualTotalMember) member).setExpression(evaluator, exprMembers);
        }
        return tuple;
    }

    private static void processMemberExpr(Object o, List<Member> exprMembers) {
        if (o instanceof Member && o instanceof RolapCubeMember) {
            exprMembers.add((Member) o);
        } else if (o instanceof VisualTotalMember) {
            VisualTotalMember member = (VisualTotalMember) o;
            Exp exp = member.getExpression();
            processMemberExpr(exp, exprMembers);
        } else if (o instanceof Exp && !(o instanceof MemberExpr)) {
            Exp exp = (Exp)o;
            ResolvedFunCall funCall = (ResolvedFunCall)exp;
            Exp[] exps = funCall.getArgs();
            processMemberExpr(exps, exprMembers);
        } else if (o instanceof Exp[]) {
            Exp[] exps = (Exp[]) o;
            for (Exp exp : exps) {
                processMemberExpr(exp, exprMembers);
            }
        } else if (o instanceof MemberExpr) {
            MemberExpr memberExp = (MemberExpr) o;
            Member member = memberExp.getMember();
            processMemberExpr(member, exprMembers);
        }
    }

    /**
     * Converts a set of cell coordinates to a cell ordinal.
     *
     * <p>This method can be expensive, because the ordinal is computed from the
     * length of the axes, and therefore the axes need to be instantiated.
     */
    int getCellOrdinal(int[] pos) {
        if (modulos == null) {
            makeModulos();
        }
        return modulos.getCellOrdinal(pos);
    }

    /**
     * Instantiates the calculator to convert cell coordinates to a cell ordinal
     * and vice versa.
     *
     * <p>To create the calculator, any axis that is based upon an Iterable is
     * converted into a List - thus increasing memory usage.
     */
    protected void makeModulos() {
        modulos = Modulos.Generator.create(axes);
    }

    /**
     * Called only by RolapCell. Use this when creating an Evaluator
     * is not required.
     *
     * @param pos Coordinates of cell
     * @return Members which form the context of the given cell
     */
    RolapMember[] getCellMembers(int[] pos) {
        RolapMember[] members = (RolapMember[]) evaluator.getMembers().clone();
        for (int i = 0; i < pos.length; i++) {
            Position position = axes[i].getPositions().get(pos[i]);
            for (Member member : position) {
                RolapMember m = (RolapMember) member;
                int ordinal = m.getHierarchy().getOrdinalInCube();
                members[ordinal] = m;
            }
        }
        return members;
    }

    Evaluator getRootEvaluator() {
        return evaluator;
    }

    Evaluator getEvaluator(int[] pos) {
        // Set up evaluator's context, so that context-dependent format
        // strings work properly.
        Evaluator cellEvaluator = evaluator.push();
        populateEvaluator(cellEvaluator, pos);
        return cellEvaluator;
    }

    void populateEvaluator(Evaluator evaluator, int[] pos) {
        for (int i = -1; i < axes.length; i++) {
            Axis axis;
            int index;
            if (i < 0) {
                axis = slicerAxis;
                if (axis.getPositions().isEmpty()) {
                    continue;
                }
                index = 0;
            } else {
                axis = axes[i];
                index = pos[i];
            }
            Position position = axis.getPositions().get(index);
            evaluator.setContext(position);
        }
    }

    /**
     * Collection of members found on an axis.
     *
     * <p>The behavior depends on the mode (i.e. the kind of axis).
     * If it collects, it generally eliminates duplicates. It also has a mode
     * where it only counts members, does not collect them.</p>
     *
     * <p>This class does two things. First it collects all Members
     * found during the Member-Determination phase.
     * Second, it counts how many Members are on each axis and
     * forms the product, the totalCellCount which is checked against
     * the ResultLimit property value.</p>
     */
    private static class AxisMemberList implements Iterable<Member> {
        private final List<Member> members;
        private final int limit;
        private boolean isSlicer;
        private int totalCellCount;
        private int axisCount;
        private boolean countOnly;

        AxisMemberList() {
            this.countOnly = false;
            this.members = new ConcatenableList<Member>();
            this.totalCellCount = 1;
            this.axisCount = 0;
            // Now that the axes are evaluated, make sure that the number of
            // cells does not exceed the result limit.
            this.limit = MondrianProperties.instance().ResultLimit.get();
        }

        public Iterator<Member> iterator() {
            return members.iterator();
        }

        void setSlicer(final boolean isSlicer) {
            this.isSlicer = isSlicer;
        }

        boolean isEmpty() {
            return this.members.isEmpty();
        }

        void countOnly(boolean countOnly) {
            this.countOnly = countOnly;
        }

        void checkLimit() {
            if (this.limit > 0) {
                this.totalCellCount *= this.axisCount;
                if (this.totalCellCount > this.limit) {
                    throw MondrianResource.instance().TotalMembersLimitExceeded
                        .ex(
                            this.totalCellCount,
                            this.limit);
                }
                this.axisCount = 0;
            }
        }

        void clearAxisCount() {
            this.axisCount = 0;
        }

        void clearTotalCellCount() {
            this.totalCellCount = 1;
        }

        void clearMembers() {
            this.members.clear();
            this.axisCount = 0;
            this.totalCellCount = 1;
        }

        void mergeTupleList(TupleList list) {
            mergeTupleIter(list.tupleCursor());
        }

        private void mergeTupleIter(TupleCursor cursor) {
            while (cursor.forward()) {
                mergeTuple(cursor);
            }
        }

        private Member getTopParent(Member m) {
            while (true) {
                Member parent = m.getParentMember();
                if (parent == null) {
                    return m;
                }
                m = parent;
            }
        }

        private void mergeTuple(final TupleCursor cursor) {
            final int arity = cursor.getArity();
            for (int i = 0; i < arity; i++) {
                mergeMember(cursor.member(i));
            }
        }

        private void mergeMember(final Member member) {
            this.axisCount++;
            if (! countOnly) {
                if (isSlicer) {
                    if (! members.contains(member)) {
                        members.add(member);
                    }
                } else {
                    if (member.isNull()) {
                        return;
                    } else if (member.isMeasure()) {
                        return;
                    } else if (member.isCalculated()) {
                        return;
                    } else if (member.isAll()) {
                        return;
                    }
                    Member topParent = getTopParent(member);
                    if (! this.members.contains(topParent)) {
                        this.members.add(topParent);
                    }
                }
            }
        }
    }

    /**
     * Extension to {@link RolapEvaluatorRoot} which is capable
     * of evaluating sets and named sets.<p/>
     *
     * A given set is only evaluated once each time a query is executed; the
     * result is added to the {@link #namedSetEvaluators} cache on first execution
     * and re-used.<p/>
     *
     * <p>Named sets are always evaluated in the context of the slicer.<p/>
     */
    protected static class RolapResultEvaluatorRoot
        extends RolapEvaluatorRoot
    {
        /**
         * Maps the names of sets to their values. Populated on demand.
         */
        private final Map<String, RolapSetEvaluator> setEvaluators =
            new HashMap<String, RolapSetEvaluator>();
        private final Map<String, RolapNamedSetEvaluator> namedSetEvaluators =
            new HashMap<String, RolapNamedSetEvaluator>();

        final RolapResult result;
        private static final Object CycleSentinel = new Object();
        private static final Object NullSentinel = new Object();

        public RolapResultEvaluatorRoot(RolapResult result) {
            super(result.execution);
            this.result = result;
        }

        protected Evaluator.NamedSetEvaluator evaluateNamedSet(
            final NamedSet namedSet,
            boolean create)
        {
            final String name = namedSet.getNameUniqueWithinQuery();
            RolapNamedSetEvaluator value;
            if (namedSet.isDynamic() && !create) {
                value = null;
            } else {
                value = namedSetEvaluators.get(name);
            }
            if (value == null) {
                value = new RolapNamedSetEvaluator(this, namedSet);
                namedSetEvaluators.put(name, value);
            }
            return value;
        }

        protected Evaluator.SetEvaluator evaluateSet(
            final Exp exp,
            boolean create)
        {
            // Sanity check: This expression HAS to return a set.
            if (! (exp.getType() instanceof SetType)) {
                throw Util.newInternal(
                    "Trying to evaluate set but expression does not return a set");
            }


            // Should be acceptable to use the string representation of the
            // expression as the name
            final String name = exp.toString();
            RolapSetEvaluator value;

            // pedro, 20120914 - I don't quite understand the !create, I was
            // kind'a expecting the opposite here. But I'll maintain the same
            // logic
            if (!create) {
                value = null;
            } else {
                value = setEvaluators.get(name);
            }
            if (value == null) {
                value = new RolapSetEvaluator(this, exp);
                setEvaluators.put(name, value);
            }
            return value;
        }

        public Object getParameterValue(ParameterSlot slot) {
            if (slot.isParameterSet()) {
                return slot.getParameterValue();
            }

            // Look in other places for the value. Which places we look depends
            // on the scope of the parameter.
            Parameter.Scope scope = slot.getParameter().getScope();
            switch (scope) {
            case System:
                // TODO: implement system params

                // fall through
            case Schema:
                // TODO: implement schema params

                // fall through
            case Connection:
                // if it's set in the session, return that value

                // fall through
            case Statement:
                break;

            default:
                throw Util.badValue(scope);
            }

            // Not set in any accessible scope. Evaluate the default value,
            // then cache it.
            Object liftedValue = slot.getCachedDefaultValue();
            Object value;
            if (liftedValue != null) {
                if (liftedValue == CycleSentinel) {
                    throw MondrianResource.instance()
                        .CycleDuringParameterEvaluation.ex(
                            slot.getParameter().getName());
                }
                if (liftedValue == NullSentinel) {
                    value = null;
                } else {
                    value = liftedValue;
                }
                return value;
            }
            // Set value to a sentinel, so we can detect cyclic evaluation.
            slot.setCachedDefaultValue(CycleSentinel);
            value =
                result.evaluateExp(
                    slot.getDefaultValueCalc(), result.slicerEvaluator, null);
            if (value == null) {
                liftedValue = NullSentinel;
            } else {
                liftedValue = value;
            }
            slot.setCachedDefaultValue(liftedValue);
            return value;
        }
    }

    /**
     * Formatter to convert values into formatted strings.
     *
     * <p>Every Cell has a value, a format string (or CellFormatter) and a
     * formatted value string.
     * There are a wide range of possible values (pick a Double, any
     * Double - its a value). Because there are lots of possible values,
     * there are also lots of possible formatted value strings. On the
     * other hand, there are only a very small number of format strings
     * and CellFormatter's. These formatters are to be cached
     * in a synchronized HashMaps in order to limit how many copies
     * need to be kept around.
     *
     * <p>
     * There are two implementations of the ValueFormatter interface:<ul>
     * <li>{@link CellFormatterValueFormatter}, which formats using a
     * user-registered {@link CellFormatter}; and
     * <li> {@link FormatValueFormatter}, which takes the {@link Locale} object.
     * </ul>
     */
    interface ValueFormatter {
        /**
         * Formats a value according to a format string.
         *
         * @param value Value
         * @param formatString Format string
         * @return Formatted value
         */
        String format(Object value, String formatString);

        /**
         * Formatter that always returns the empty string.
         */
        public static final ValueFormatter EMPTY = new ValueFormatter() {
            public String format(Object value, String formatString) {
                return "";
            }
        };
    }

    /**
     * A CellFormatterValueFormatter uses a user-defined {@link CellFormatter}
     * to format values.
     */
    static class CellFormatterValueFormatter implements ValueFormatter {
        final CellFormatter cf;

        /**
         * Creates a CellFormatterValueFormatter
         *
         * @param cf Cell formatter
         */
        CellFormatterValueFormatter(CellFormatter cf) {
            this.cf = cf;
        }
        public String format(Object value, String formatString) {
            return cf.formatCell(value);
        }
    }

    /**
     * A FormatValueFormatter takes a {@link Locale}
     * as a parameter and uses it to get the {@link mondrian.util.Format}
     * to be used in formatting an Object value with a
     * given format string.
     */
    static class FormatValueFormatter implements ValueFormatter {
        final Locale locale;

        /**
         * Creates a FormatValueFormatter.
         *
         * @param locale Locale
         */
        FormatValueFormatter(Locale locale) {
            this.locale = locale;
        }

        public String format(Object value, String formatString) {
            if (value == Util.nullValue) {
                value = null;
            }
            if (value instanceof Throwable) {
                return "#ERR: " + value.toString();
            }
            Format format = getFormat(formatString);
            return format.format(value);
        }

        private Format getFormat(String formatString) {
            return Format.get(formatString, locale);
        }
    }

    /**
     * Synchronized Map from Locale to ValueFormatter. It is expected that
     * there will be only a small number of Locale's.
     * Should these be a WeakHashMap?
     */
    protected static final Map<Locale, ValueFormatter>
        formatValueFormatters =
            Collections.synchronizedMap(new HashMap<Locale, ValueFormatter>());

    /**
     * A CellInfo contains all of the information that a Cell requires.
     * It is placed in the cellInfos map during evaluation and
     * serves as a constructor parameter for {@link RolapCell}.
     *
     * <p>During the evaluation stage they are mutable but after evaluation has
     * finished they are not changed.
     */
    static class CellInfo {
        Object value;
        String formatString;
        ValueFormatter valueFormatter;
        long key;

        /**
         * Creates a CellInfo representing the position of a cell.
         *
         * @param key Ordinal representing the position of a cell
         */
        CellInfo(long key) {
            this(key, null, null, ValueFormatter.EMPTY);
        }

        /**
         * Creates a CellInfo with position, value, format string and formatter
         * of a cell.
         *
         * @param key Ordinal representing the position of a cell
         * @param value Value of cell, or null if not yet known
         * @param formatString Format string of cell, or null
         * @param valueFormatter Formatter for cell, or null
         */
        CellInfo(
            long key,
            Object value,
            String formatString,
            ValueFormatter valueFormatter)
        {
            this.key = key;
            this.value = value;
            this.formatString = formatString;
            this.valueFormatter = valueFormatter;
        }

        public int hashCode() {
            // Combine the upper 32 bits of the key with the lower 32 bits.
            // We used to use 'key ^ (key >>> 32)' but that was bad, because
            // CellKey.Two encodes (i, j) as
            // (i * Integer.MAX_VALUE + j), which is practically the same as
            // (i << 32, j). If i and j were
            // both k bits long, all of the hashcodes were k bits long too!
            return (int) (key ^ (key >>> 11) ^ (key >>> 24));
        }

        public boolean equals(Object o) {
            if (o instanceof CellInfo) {
                CellInfo that = (CellInfo) o;
                return that.key == this.key;
            } else {
                return false;
            }
        }

        /**
         * Returns the formatted value of the Cell
         * @return formatted value of the Cell
         */
        String getFormatValue() {
            return valueFormatter.format(value, formatString);
        }
    }

    /**
     * API for the creation and
     * lookup of {@link CellInfo} objects. There are two implementations,
     * one that uses a Map for storage and the other uses an ObjectPool.
     */
    interface CellInfoContainer {
        /**
         * Returns the number of CellInfo objects in this container.
         * @return  the number of CellInfo objects.
         */
        int size();
        /**
         * Reduces the size of the internal data structures needed to
         * support the current entries. This should be called after
         * all CellInfo objects have been added to container.
         */
        void trimToSize();
        /**
         * Removes all CellInfo objects from container. Does not
         * change the size of the internal data structures.
         */
        void clear();
        /**
         * Creates a new CellInfo object, adds it to the container
         * a location <code>pos</code> and returns it.
         *
         * @param pos where to store CellInfo object.
         * @return the newly create CellInfo object.
         */
        CellInfo create(int[] pos);
        /**
         * Gets the CellInfo object at the location <code>pos</code>.
         *
         * @param pos where to find the CellInfo object.
         * @return the CellInfo found or null.
         */
        CellInfo lookup(int[] pos);
    }

    /**
     * Implementation of {@link CellInfoContainer} which uses a {@link Map} to
     * store CellInfo Objects.
     *
     * <p>Note that the CellKey point instance variable is the same
     * Object (NOT a copy) that is used and modified during
     * the recursive calls to executeStripe - the
     * <code>create</code> method relies on this fact.
     */
    static class CellInfoMap implements CellInfoContainer {
        private final Map<CellKey, CellInfo> cellInfoMap;
        private final CellKey point;

        /**
         * Creates a CellInfoMap
         *
         * @param point Cell position
         */
        CellInfoMap(CellKey point) {
            this.point = point;
            this.cellInfoMap = new HashMap<CellKey, CellInfo>();
        }
        public int size() {
            return this.cellInfoMap.size();
        }
        public void trimToSize() {
            // empty
        }
        public void clear() {
            this.cellInfoMap.clear();
        }
        public CellInfo create(int[] pos) {
            CellKey key = this.point.copy();
            CellInfo ci = this.cellInfoMap.get(key);
            if (ci == null) {
                ci = new CellInfo(0);
                this.cellInfoMap.put(key, ci);
            }
            return ci;
        }
        public CellInfo lookup(int[] pos) {
            CellKey key = CellKey.Generator.newCellKey(pos);
            return this.cellInfoMap.get(key);
        }
    }

    /**
     * Implementation of {@link CellInfoContainer} which uses an
     * {@link ObjectPool} to store {@link CellInfo} Objects.
     *
     * <p>There is an inner interface (<code>CellKeyMaker</code>) and
     * implementations for 0 through 4 axes that convert the Cell
     * position integer array into a long.
     *
     * <p>
     * It should be noted that there is an alternate approach.
     * As the <code>executeStripe</code>
     * method is recursively called, at each call it is known which
     * axis is being iterated across and it is known whether or
     * not the Position object for that axis is a List or just
     * an Iterable. It it is a List, then one knows the real
     * size of the axis. If it is an Iterable, then one has to
     * use one of the MAX_AXIS_SIZE values. Given that this information
     * is available when one recursives down to the next
     * <code>executeStripe</code> call, the Cell ordinal, the position
     * integer array could converted to an <code>long</code>, could
     * be generated on the call stack!! Just a thought for the future.
     */
    static class CellInfoPool implements CellInfoContainer {
        /**
         * The maximum number of Members, 2,147,483,647, that can be any given
         * Axis when the number of Axes is 2.
         */
        protected static final long MAX_AXIS_SIZE_2 = 2147483647;
        /**
         * The maximum number of Members, 2,000,000, that can be any given
         * Axis when the number of Axes is 3.
         */
        protected static final long MAX_AXIS_SIZE_3 = 2000000;
        /**
         * The maximum number of Members, 50,000, that can be any given
         * Axis when the number of Axes is 4.
         */
        protected static final long MAX_AXIS_SIZE_4 = 50000;

        /**
         * Implementations of CellKeyMaker convert the Cell
         * position integer array to a <code>long</code>.
         *
         * <p>Generates a long ordinal based upon the values of the integers
         * stored in the cell position array. With this mechanism, the
         * Cell information can be stored using a long key (rather than
         * the array integer of positions) thus saving memory. The trick
         * is to use a 'large number' per axis in order to convert from
         * position array to long key where the 'large number' is greater
         * than the number of members in the axis.
         * The largest 'long' is java.lang.Long.MAX_VALUE which is
         * 9,223,372,036,854,776,000. The product of the maximum number
         * of members per axis must be less than this maximum 'long'
         * value (otherwise one gets hashing collisions).</p>
         *
         * <p>For a single axis, the maximum number of members is equal to
         * the max 'long' number, 9,223,372,036,854,776,000.
         *
         * <p>For two axes, the maximum number of members is the square root
         * of the max 'long' number, 9,223,372,036,854,776,000, which is
         * slightly bigger than 2,147,483,647 (which is the maximum integer).
         *
         * <p>For three axes, the maximum number of members per axis is the
         * cube root of the max 'long' which is about 2,000,000.
         *
         * <p>For four axes the forth root is about 50,000.
         *
         * <p>For five or more axes, the maximum number of members per axis
         * based upon the root of the maximum 'long' number,
         * start getting too small to guarantee that it will be
         * smaller than the number of members on a given axis and so
         * we must resort to the Map-base Cell container.
         */
        interface CellKeyMaker {
            long generate(int[] pos);
        }

        /**
         * For axis of size 0.
         */
        static class Zero implements CellKeyMaker {
            public long generate(int[] pos) {
                return 0;
            }
        }

        /**
         * For axis of size 1.
         */
        static class One implements CellKeyMaker {
            public long generate(int[] pos) {
                return pos[0];
            }
        }

        /**
         * For axis of size 2.
         */
        static class Two implements CellKeyMaker {
            public long generate(int[] pos) {
                long l = pos[0];
                l += (MAX_AXIS_SIZE_2 * (long) pos[1]);
                return l;
            }
        }

        /**
         * For axis of size 3.
         */
        static class Three implements CellKeyMaker {
            public long generate(int[] pos) {
                long l = pos[0];
                l += (MAX_AXIS_SIZE_3 * (long) pos[1]);
                l += (MAX_AXIS_SIZE_3 * MAX_AXIS_SIZE_3 * (long) pos[2]);
                return l;
            }
        }

        /**
         * For axis of size 4.
         */
        static class Four implements CellKeyMaker {
            public long generate(int[] pos) {
                long l = pos[0];
                l += (MAX_AXIS_SIZE_4 * (long) pos[1]);
                l += (MAX_AXIS_SIZE_4 * MAX_AXIS_SIZE_4 * (long) pos[2]);
                l += (MAX_AXIS_SIZE_4 * MAX_AXIS_SIZE_4 * MAX_AXIS_SIZE_4
                      * (long) pos[3]);
                return l;
            }
        }

        private final ObjectPool<CellInfo> cellInfoPool;
        private final CellKeyMaker cellKeyMaker;

        CellInfoPool(int axisLength) {
            this.cellInfoPool = new ObjectPool<CellInfo>();
            this.cellKeyMaker = createCellKeyMaker(axisLength);
        }

        CellInfoPool(int axisLength, int initialSize) {
            this.cellInfoPool = new ObjectPool<CellInfo>(initialSize);
            this.cellKeyMaker = createCellKeyMaker(axisLength);
        }

        private static CellKeyMaker createCellKeyMaker(int axisLength) {
            switch (axisLength) {
            case 0:
                return new Zero();
            case 1:
                return new One();
            case 2:
                return new Two();
            case 3:
                return new Three();
            case 4:
                return new Four();
            default:
                throw new RuntimeException(
                    "Creating CellInfoPool with axisLength=" + axisLength);
            }
        }

        public int size() {
            return this.cellInfoPool.size();
        }
        public void trimToSize() {
            this.cellInfoPool.trimToSize();
        }
        public void clear() {
            this.cellInfoPool.clear();
        }
        public CellInfo create(int[] pos) {
            long key = this.cellKeyMaker.generate(pos);
            return this.cellInfoPool.add(new CellInfo(key));
        }
        public CellInfo lookup(int[] pos) {
            long key = this.cellKeyMaker.generate(pos);
            return this.cellInfoPool.add(new CellInfo(key));
        }
    }

    static TupleList mergeAxes(
        TupleList axis1,
        TupleIterable axis2,
        boolean ordered)
    {
        if (axis1.isEmpty() && axis2 instanceof TupleList) {
            return (TupleList) axis2;
        }
        Set<List<Member>> set = new HashSet<List<Member>>();
        TupleList list = TupleCollections.createList(axis2.getArity());
        for (List<Member> tuple : axis1) {
            if (set.add(tuple)) {
                list.add(tuple);
            }
        }
        int halfWay = list.size();
        for (List<Member> tuple : axis2) {
            if (set.add(tuple)) {
                list.add(tuple);
            }
        }

        // if there are unique members on both axes and no order function,
        // sort the list to ensure default order
        if (halfWay > 0 && halfWay < list.size() && !ordered) {
            list = FunUtil.hierarchizeTupleList(list, false);
        }

        return list;
    }

    /**
     * Member which holds the AggregateCalc used when evaluating
     * a compound slicer.  This is used to better handle some cases
     * where calculated members elsewhere in the query can override
     * the context of the slicer members.
     * See MONDRIAN-1226.
     */
    public class CompoundSlicerRolapMember extends DelegatingRolapMember
    implements RolapMeasure
    {
        private final Calc calc;
        private final ValueFormatter valueFormatter;

        public CompoundSlicerRolapMember(
            RolapMember placeholderMember, Calc calc, ValueFormatter formatter)
        {
            super(placeholderMember);
            this.calc = calc;
            valueFormatter = formatter;
        }

        @Override
        public boolean isEvaluated() {
            return true;
        }

        @Override
        public Exp getExpression() {
            return new DummyExp(calc.getType());
        }

        @Override
        public Calc getCompiledExpression(RolapEvaluatorRoot root) {
            return calc;
        }

        @Override
        public int getSolveOrder() {
            return 0;
        }

        public ValueFormatter getFormatter() {
            return valueFormatter;
        }
    }
}

// End RolapResult.java
