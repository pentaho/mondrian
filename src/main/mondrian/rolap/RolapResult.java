/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2001-2002 Kana Software, Inc.
// Copyright (C) 2001-2006 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 10 August, 2001
*/

package mondrian.rolap;
import mondrian.calc.Calc;
import mondrian.calc.ParameterSlot;
import mondrian.olap.*;
import mondrian.olap.fun.MondrianEvaluationException;
import mondrian.resource.MondrianResource;
import mondrian.rolap.agg.AggregationManager;
import mondrian.util.Format;
import mondrian.util.Bug;
import mondrian.util.ObjectPool;

import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;
import java.util.HashMap;
import java.util.Map;
import java.util.Locale;

/**
 * A <code>RolapResult</code> is the result of running a query.
 *
 * @author jhyde
 * @since 10 August, 2001
 * @version $Id$
 */
class RolapResult extends ResultBase {

    private static final Logger LOGGER = Logger.getLogger(ResultBase.class);

    private RolapEvaluator evaluator;
    private final CellKey point;

    private CellInfoContainer cellInfos;
    private FastBatchingCellReader batchingReader;
    AggregatingCellReader aggregatingReader = new AggregatingCellReader();
    private Modulos modulos = null;
    private final int maxEvalDepth =
            MondrianProperties.instance().MaxEvalDepth.get();

    RolapResult(Query query, boolean execute) {
        super(query, new Axis[query.axes.length]);

        this.point = CellKey.Generator.create(query.axes.length);
        final int expDeps = MondrianProperties.instance().TestExpDependencies.get();
        if (expDeps > 0) {
            this.evaluator = new RolapDependencyTestingEvaluator(this, expDeps);
        } else {
            final RolapEvaluator.RolapEvaluatorRoot root =
                    new RolapResultEvaluatorRoot(this);
            this.evaluator = new RolapEvaluator(root);
        }
        RolapCube rcube = (RolapCube) query.getCube();
        this.batchingReader = new FastBatchingCellReader(rcube);

        this.cellInfos = (query.axes.length > 4)
            ?  new CellInfoMap(point) : new CellInfoPool(query.axes.length);


        if (!execute) {
            return;
        }

        // for use in debugging Checkin_7634
        Util.discard(Bug.Checkin7634DoOld);
        boolean normalExecution = true;
        try {
            // This call to clear the cube's cache only has an
            // effect if caching has been disabled, otherwise
            // nothing happens.
            // Clear the local cache before a query has run
            rcube.clearCachedAggregations();
            // Check if there are modifications to the global aggregate cache
            rcube.checkAggregateModifications();

            // An array of lists which will hold each axis' implicit members
            // (does not include slicer axis).
            // One might imagine that one could have an axisMembers list per
            // non-slicer axis and then keep track on a per-axis basis and
            // finally only re-evaluate those axes for which the other axes have
            // implicit members, but some junit test fail when this approach is
            // taken.
            List<Member> axisMembers = new ArrayList<Member>();

            // This holds all members explicitly in the slicer.
            // Slicer members can not be over-ridden by implict members found
            // during execution of the other axes.
            List<Member> slicerMembers = new ArrayList<Member>();

            for (int i = -1; i < axes.length; i++) {
                QueryAxis axis;
                final Calc calc;
                if (i == -1) {
                    axis = query.slicerAxis;
                    calc = query.slicerCalc;
                } else {
                    axis = query.axes[i];
                    calc = query.axisCalcs[i];
                }

                int attempt = 0;
                while (true) {
                    evaluator.setCellReader(batchingReader);
                    Axis axisResult =
                        executeAxis(evaluator.push(), axis, calc, false, axisMembers);
                    Util.discard(axisResult);
                    evaluator.clearExpResultCache();
                    if (!batchingReader.loadAggregations(query)) {
                        break;
                    }
                    if (attempt++ > maxEvalDepth) {
                        throw Util.newInternal("Failed to load all aggregations after " +
                                maxEvalDepth +
                                "passes; there's probably a cycle");
                    }
                }

                evaluator.setCellReader(aggregatingReader);
                Axis axisResult =
                    executeAxis(evaluator.push(), axis, calc, true, null);
                evaluator.clearExpResultCache();

                if (i == -1) {
                    this.slicerAxis = axisResult;
                    // Use the context created by the slicer for the other
                    // axes.  For example, "select filter([Customers], [Store
                    // Sales] > 100) on columns from Sales where
                    // ([Time].[1998])" should show customers whose 1998 (not
                    // total) purchases exceeded 100.

                    // Getting the Position list's size and the Position
                    // at index == 0 will, in fact, cause an Iterable-base
                    // Axis Position List to become a List-base Axis
                    // Position List (and increae memory usage), but for
                    // the slicer axis, the number of Positions is very
                    // small, so who cares.
                    switch (this.slicerAxis.getPositions().size()) {
                    case 0:
                        throw MondrianResource.instance().EmptySlicer.ex();
                    case 1:
                        break;
                    default:
                        throw MondrianResource.instance().CompoundSlicer.ex();
                    }
                    Position position = this.slicerAxis.getPositions().get(0);
                    evaluator.setContext(position);
                    for (Member member: position) {
                        slicerMembers.add(member);
                    }
                } else {
                    this.axes[i] = axisResult;
                }
            }

            if (Bug.Checkin7641UseOptimizer) {
                purge(axisMembers, slicerMembers);

                boolean didEvaluatorReplacementMember = false;
                RolapEvaluator rolapEval = evaluator;
                for (Member m : axisMembers) {
                    if (rolapEval.setContextConditional(m) != null) {
                        // Do not break out of loops but set change flag.
                        // There may be more than one Member that has to be
                        // replaced.
                        didEvaluatorReplacementMember = true;
                    }
                }

                if (didEvaluatorReplacementMember) {
                    // Must re-evaluate axes because one of the evaluator's
                    // members has changed. Do not have to re-evaluate the
                    // slicer axis or any axis whose members used during evaluation
                    // were not over-ridden by members from the evaluation of
                    // different axes (if you just have rows and columns, then
                    // if rows contributed a member to axisMembers, then columns must
                    // be re-evaluated and if columns contributed, then rows must
                    // be re-evaluated).
                    for (int i = 0; i < axes.length; i++) {
                        QueryAxis axis = query.axes[i];
                        final Calc calc = query.axisCalcs[i];
                        evaluator.setCellReader(aggregatingReader);
                        Axis axisResult =
                            executeAxis(evaluator.push(), axis, calc, true, null);
                        evaluator.clearExpResultCache();
                        this.axes[i] = axisResult;
                    }
                }
            }


            // Now that the axes are evaluated, make sure that the number of
            // cells does not exceed the result limit.
            // If we set the ResultLimit, then we force a conversion of
            // Iterable-base Axis Position Lists into List-based version -
            // thats just the way it is.
            int limit = MondrianProperties.instance().ResultLimit.get();
            if (limit > 0) {
                // result limit exceeded, throw an exception
                long n = 1;
                for (Axis axis : axes) {
                    n = n * axis.getPositions().size();
                }
                if (n > limit) {
                    throw MondrianResource.instance().
                        LimitExceededDuringCrossjoin.ex(n, limit);
                }
            }

            executeBody(query);

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

        } catch (ResultLimitExceededException ex) {
            // If one gets a ResultLimitExceededException, then
            // don't count on anything being worth caching.
            normalExecution = false;

            // De-reference data structures that might be holding
            // partial results but surely are taking up memory.
            evaluator = null;
            cellInfos = null;
            batchingReader = null;
            for (int i = 0; i < axes.length; i++) {
                axes[i] = null;
            }
            slicerAxis = null;

            throw ex;

        } finally {
            if (normalExecution) {
                // Push all modifications to the aggregate cache to the global
                // cache so each thread can start using it
                rcube.pushAggregateModificationsToGlobalCache();

                evaluator.clearExpResultCache();
            }
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("RolapResult<init>: " + Util.printMemory());
            }
        }
    }

    protected Logger getLogger() {
        return LOGGER;
    }
    public Cube getCube() {
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

        CellInfo ci = cellInfos.lookup(pos);
        if (ci.value == null) {
            ci.value = Util.nullValue;
        }

        return new RolapCell(this, pos.clone(), ci);
    }

    private Axis executeAxis(
        Evaluator evaluator,
        QueryAxis axis,
        Calc axisCalc,
        boolean construct,
        List<Member> axisMembers)
    {
        Axis axisResult = null;
        if (axis == null) {
            // Create an axis containing one position with no members (not
            // the same as an empty axis).
            if (construct) {
                axisResult = new RolapAxis.SingleEmptyPosition();
            }

        } else {
            evaluator.setNonEmpty(axis.isNonEmpty());
            evaluator.setEvalAxes(true);
            Object value = axisCalc.evaluate(evaluator);
            evaluator.setNonEmpty(false);
            if (value != null) {
                // List or Iterable of Member or Member[]
                if (value instanceof List) {
                    List<Object> list = (List) value;
                    if (construct) {
                        if (list.size() == 0) {
                            // should be???
                            axisResult = new RolapAxis.NoPosition();
                        } else if (list.get(0) instanceof Member[]) {
                            axisResult =
                                new RolapAxis.MemberArrayList((List<Member[]>)value);
                        } else {
                            axisResult =
                                new RolapAxis.MemberList((List<Member>)value);
                        }
                    } else {
                        if (list.size() != 0) {
                            if (list.get(0) instanceof Member[]) {
                                for (Member[] o : (List<Member[]>) value) {
                                    merge(axisMembers, o);
                                }
                            } else {
                                for (Member o : (List<Member>) value) {
                                    merge(axisMembers, o);
                                }
                            }
                        }
                    }
                } else {
                    // Iterable
                    Iterable<Object> iter = (Iterable) value;
                    Iterator it = iter.iterator();
                    if (construct) {
                        if (! it.hasNext()) {
                            axisResult = new RolapAxis.NoPosition();
                        } else if (it.next() instanceof Member[]) {
                            axisResult = new RolapAxis.MemberArrayIterable(
                                            (Iterable<Member[]>)value);
                        } else {
                            axisResult = new RolapAxis.MemberIterable(
                                            (Iterable<Member>)value);
                        }
                    } else {
                        if (it.hasNext()) {
                            Object o = it.next();
                            if (o instanceof Member[]) {
                                merge(axisMembers, (Member[]) o);
                                while (it.hasNext()) {
                                    o = it.next();
                                    merge(axisMembers, (Member[]) o);
                                }
                            } else {
                                merge(axisMembers, (Member) o);
                                while (it.hasNext()) {
                                    o = it.next();
                                    merge(axisMembers, (Member) o);
                                }
                            }
                        }
                    }
                }
            }
            evaluator.setEvalAxes(false);
        }
        return axisResult;
    }

    private void executeBody(Query query) {
        try {
            // Compute the cells several times. The first time, use a dummy
            // evaluator which collects requests.
            int count = 0;
            while (true) {

                evaluator.setCellReader(this.batchingReader);
                executeStripe(query.axes.length - 1,
                                (RolapEvaluator) evaluator.push());
                evaluator.clearExpResultCache();

                // Retrieve the aggregations collected.
                //
                if (!batchingReader.loadAggregations(query)) {
                    // We got all of the cells we needed, so the result must be
                    // correct.
                    return;
                }
                if (count++ > maxEvalDepth) {
                    if (evaluator instanceof RolapDependencyTestingEvaluator) {
                        // The dependency testing evaluator can trigger new
                        // requests every cycle. So let is run as normal for
                        // the first N times, then run it disabled.
                        ((RolapDependencyTestingEvaluator.DteRoot) evaluator.root).disabled = true;
                        if (count > maxEvalDepth * 2) {
                            throw Util.newInternal("Query required more than "
                                + count + " iterations");
                        }
                    } else {
                        throw Util.newInternal("Query required more than "
                            + count + " iterations");
                    }
                }

                cellInfos.clear();
            }
        } finally {

        }
    }

    boolean isDirty() {
        return batchingReader.isDirty();
    }

    private Object evaluateExp(Calc calc, Evaluator evaluator) {
        int attempt = 0;
        boolean dirty = batchingReader.isDirty();
        while (true) {
            RolapEvaluator ev = (RolapEvaluator) evaluator.push();

            ev.setCellReader(batchingReader);
            Object preliminaryValue = calc.evaluate(ev);
            Util.discard(preliminaryValue);
            if (!batchingReader.loadAggregations(evaluator.getQuery())) {
                break;
            }
            if (attempt++ > maxEvalDepth) {
                throw Util.newInternal(
                        "Failed to load all aggregations after " +
                        maxEvalDepth + "passes; there's probably a cycle");
            }
        }

        // If there were pending reads when we entered, some of the other
        // expressions may have been evaluated incorrectly. Set the reaader's
        // 'dirty' flag so that the caller knows that it must re-evaluate them.
        if (dirty) {
            batchingReader.setDirty(true);
        }
        RolapEvaluator ev = (RolapEvaluator) evaluator.push();
        ev.setCellReader(aggregatingReader);
        Object value = calc.evaluate(ev);
        return value;
    }

    /**
     * An <code>AggregatingCellReader</code> reads cell values from the
     * {@link RolapAggregationManager}.
     */
    private static class AggregatingCellReader implements CellReader {
        private final RolapAggregationManager aggMan =
            AggregationManager.instance();

        // implement CellReader
        public Object get(Evaluator evaluator) {
            final RolapEvaluator rolapEvaluator = (RolapEvaluator) evaluator;
            return aggMan.getCellFromCache(rolapEvaluator.getCurrentMembers());
        }

        public int getMissCount() {
            return aggMan.getMissCount();
        }
    }

    private void executeStripe(int axisOrdinal, RolapEvaluator revaluator) {
        if (axisOrdinal < 0) {
            Axis axis = slicerAxis;
            List<Position> positions = axis.getPositions();
            for (Position position: positions) {
                getQuery().checkCancelOrTimeout();
                revaluator.setContext(position);
                Object o;
                try {
                    o = revaluator.evaluateCurrent();
                } catch (MondrianEvaluationException e) {
                    o = e;
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
                    ValueFormatter valueFormatter;

                    // Determine if there is a CellFormatter registered for
                    // the current Cube's Measure's Dimension. If so,
                    // then find or create a CellFormatterValueFormatter
                    // for it. If not, then find or create a Locale based
                    // FormatValueFormatter.
                    RolapCube cube = (RolapCube) getCube();
                    Dimension measuresDim =
                            cube.getMeasuresHierarchy().getDimension();
                    RolapMeasure m =
                            (RolapMeasure) revaluator.getContext(measuresDim);
                    CellFormatter cf = m.getFormatter();
                    if (cf != null) {
                        valueFormatter = cellFormatters.get(cf);
                        if (valueFormatter == null) {
                            valueFormatter = new CellFormatterValueFormatter(cf);
                            cellFormatters.put(cf, valueFormatter);
                        }
                    } else {
                        cachedFormatString = revaluator.getFormatString();
                        Locale locale = query.getConnection().getLocale();
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

                } catch (MondrianEvaluationException e) {
                    // ignore

                } catch (Throwable e) {
                    Util.discard(e);
                }

                if (o == RolapUtil.valueNotReadyException) {
                    continue;
                }

                ci.value = o;
            }
        } else {
            Axis axis = axes[axisOrdinal];
            List<Position> positions = axis.getPositions();
            int i = 0;
            for (Position position: positions) {
                point.setAxis(axisOrdinal, i);
                revaluator.setContext(position);
                getQuery().checkCancelOrTimeout();
                executeStripe(axisOrdinal - 1, revaluator);
                i++;
            }
        }
    }

    /**
     * Converts a cell ordinal to a set of cell coordinates. Converse of
     * {@link #getCellOrdinal}. For example, if this result is 10 x 10 x 10,
     * then cell ordinal 537 has coordinates (5, 3, 7).
     * <p
     * This method is no longer used.
     */
    int[] getCellPos(int cellOrdinal) {
        if (modulos == null) {
            makeModulos();
        }
        return modulos.getCellPos(cellOrdinal);
    }

    /**
     * Converts a set of cell coordinates to a cell ordinal. Converse of
     * {@link #getCellPos}.
     * <p
     * This method is no longer used.
     */
    int getCellOrdinal(int[] pos) {
        if (modulos == null) {
            makeModulos();
        }
        return modulos.getCellOrdinal(pos);
    }

    /*
     * This method is no longer used.
     */
    protected void makeModulos() {
        // Any axis that's based upon an Iterable is converted into
        // a List - thus increasing memory usage.
        modulos = Modulos.Generator.create(axes);
    }

    /**
     * Called only by RolapCell.
     *
     * @param pos
     * @return
     */
    RolapEvaluator getCellEvaluator(int[] pos) {
        final RolapEvaluator cellEvaluator = (RolapEvaluator) evaluator.push();
        for (int i = 0; i < pos.length; i++) {
            Position position = axes[i].getPositions().get(pos[i]);
            cellEvaluator.setContext(position);
        }
        return cellEvaluator;
    }

    /**
     * Called only by RolapCell. Use this when creating an Evaluator
     * (using method getCellEvaluator) is not required.
     *
     * @param pos
     * @return
     */
    Member[] getCellMembers(int[] pos) {
        Member[] members = evaluator.getMembers().clone();
        final Cube cube = getCube();
        for (int i = 0; i < pos.length; i++) {
            Position position = axes[i].getPositions().get(pos[i]);
            for (Member member: position) {
                RolapMember m = (RolapMember) member;
                int ordinal = m.getDimension().getOrdinal(cube);
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
        for (int i = -1; i < axes.length; i++) {
            Axis axis;
            int index;
            if (i < 0) {
                axis = slicerAxis;
                index = 0;
            } else {
                axis = axes[i];
                index = pos[i];
            }
            Position position = axis.getPositions().get(index);
            cellEvaluator.setContext(position);
        }
        return cellEvaluator;
    }

    private Member getTopParent(Member m) {
        Member parent = m.getParentMember();
        return (parent == null) ? m : getTopParent(parent);
    }

    /**
     * Add each top-level member of the axis' members to the membersList if
     * the top-level member is not the 'all' member (or null or a measure).
     *
     * @param axisMembers
     * @param axis
     */
    private void merge(List<Member> axisMembers, Axis axis) {
        for (Position position : axis.getPositions()) {
            merge(axisMembers, position);
        }
    }

    private void merge(List<Member> axisMembers, List<Member> members) {
        for (Member member : members) {
            merge(axisMembers, member);
        }
    }
    private void merge(List<Member> axisMembers, Member[] members) {
        for (Member member : members) {
            merge(axisMembers, member);
        }
    }

    private void merge(List<Member> axisMembers, Member member) {
        Member topParent = getTopParent(member);
        if (topParent.isNull()) {
            return;
        } else if (topParent.isMeasure()) {
            return;
        } else if (topParent.isCalculated()) {
            return;
        } else if (topParent.isAll()) {
            return;
        }
        axisMembers.add(topParent);
    }

    /**
     * Remove each member from the axisMembers list when the member's
     * hierarchy is the same a one of the slicerMembers' hierarchy.
     * (If it is in the slicer, then remove it from the axisMembers list).
     *
     * @param axisMembers
     * @param slicerMembers
     */
    private void purge(List<Member> axisMembers, List<Member> slicerMembers) {
        // if a member is in slicerMembers, then remove the "corresponding"
        // member for the axisMembers list
        for (Member slicerMember : slicerMembers) {
            purge(axisMembers, slicerMember);
        }
    }
    private void purge(List<Member> axisMembers, Member slicerMember) {
        Hierarchy hier = slicerMember.getHierarchy();
        Iterator<Member> it = axisMembers.iterator();
        while (it.hasNext()) {
            Member member = it.next();
            if (member.getHierarchy().equals(hier)) {
                it.remove();
                break;
            }
        }
    }

    /**
     * Extension to {@link RolapEvaluator.RolapEvaluatorRoot} which is capable
     * of evaluating named sets.<p/>
     *
     * A given set is only evaluated once each time a query is executed; the
     * result is added to the {@link #namedSetValues} cache on first execution
     * and re-used.<p/>
     *
     * Named sets are always evaluated in the context of the slicer.<p/>
     */
    protected static class RolapResultEvaluatorRoot
            extends RolapEvaluator.RolapEvaluatorRoot {
        /**
         * Maps the names of sets to their values. Populated on demand.
         */
        private final Map<String, Object> namedSetValues = new HashMap<String, Object>();

        /**
         * Evaluator containing context resulting from evaluating the slicer.
         */
        private RolapEvaluator slicerEvaluator;
        private final RolapResult result;
        private static final Object Sentinel = new Object();

        public RolapResultEvaluatorRoot(RolapResult result) {
            super(result.query);
            this.result = result;
        }

        protected void init(Evaluator evaluator) {
            slicerEvaluator = (RolapEvaluator) evaluator;
        }

        protected Object evaluateNamedSet(String name, Exp exp) {
            Object value = namedSetValues.get(name);
            if (value == null) {
                final RolapEvaluator.RolapEvaluatorRoot root =
                    slicerEvaluator.root;
                final Calc calc = root.getCompiled(exp, false);
                Object o = result.evaluateExp(calc, slicerEvaluator.push());
                List list;
                if (o instanceof List) {
                    list = (List) o;
                } else {
                    // Iterable

                    // TODO:
                    // Here, we have to convert the Iterable into a List,
                    // materialize it, because in the class
                    // mondrian.mdx.NamedSetExpr the Calc returned by the
                    // 'accept' method is an AbstractListCalc (hence we must
                    // provide a list here). It would be better if the
                    // NamedSetExpr class knew if it needed a ListCalc or
                    // an IterCalc.
                    Iterable iter = (Iterable) o;
                    list = new ArrayList();
                    for (Object e: iter) {
                        list.add(e);
                    }
                }
                // Make immutable, just in case expressions are modifying the
                // results we give them.
                value = Collections.unmodifiableList(list);
                namedSetValues.put(name, value);
            }
            return value;
        }

        protected void clearNamedSets() {
            namedSetValues.clear();
        }

        public Object getParameterValue(ParameterSlot slot) {
            Object value = slot.getParameterValue();
            if (value != null) {
                return value;
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
            value = slot.getCachedDefaultValue();
            if (value != null) {
                if (value == Sentinel) {
                    throw MondrianResource.instance().
                        CycleDuringParameterEvaluation.ex(
                        slot.getParameter().getName());
                }
                return value;
            }
            // Set value to a sentinel, so we can detect cyclic evaluation.
            slot.setCachedDefaultValue(Sentinel);
            value = result.evaluateExp(
                slot.getDefaultValueCalc(), slicerEvaluator.push());
            slot.setCachedDefaultValue(value);
            return value;
        }
    }

    /**
     * Every Cell has a value, a format string (or CellFormatter) and a
     * formatted value string.
     * There are a wide range of possible values (pick a Double, any
     * Double - its a value). Because there are lots of possible values,
     * there are also lots of possible formatted value strings. On the
     * other hand, there are only a very small number of format strings
     * and CellFormatter's. These formatters are to be cached
     * in a synchronized HashMaps in order to limit how many copies
     * need to be kept around.
     * <p>
     * There are two implementations of the ValueFormatter interface:
     * the CellFormatterValueFormatter which formats using a
     * user registered CellFormatter and the FormatValueFormatter
     * which takes the Locale object.
     */
    interface ValueFormatter {
        String format(Object value, String formatString);
    }

    /**
     * A CellFormatterValueFormatter takes a user-defined CellFormatter
     * as a parameter and uses the CellFormatter to format Object values.
     */
    class CellFormatterValueFormatter implements ValueFormatter{
        final CellFormatter cf;
        CellFormatterValueFormatter(CellFormatter cf) {
            this.cf = cf;
        }
        public String format(Object value, String formatString) {
            return cf.formatCell(value);
        }
    }
    /**
     * A FormatValueFormatter takes a Locale
     * as a parameter and use it to get the mondrian.util.Format
     * Object to be used in formatting an Object value with a
     * given format string.
     */
    class FormatValueFormatter implements ValueFormatter{
        final Locale locale;
        FormatValueFormatter(Locale locale) {
            this.locale = locale;
        }
        public String format(Object value, String formatString) {
            if (value == Util.nullValue) {
                Format format = getFormat(formatString);
                return format.format(null);
            } else if (value instanceof Throwable) {
                return "#ERR: " + value.toString();
            } else if (value instanceof String) {
                return (String) value;
            } else {
                Format format = getFormat(formatString);
                return format.format(value);
            }
        }
        private Format getFormat(String formatString) {
            return Format.get(formatString, locale);
        }
    }

    /*
     * Generate a long ordinal based upon the values of the integers
     * stored in the cell position array. With this mechanism, the
     * Cell information can be stored using a long key (rather than
     * the array integer of positions) thus saving memory. The trick
     * is to use a 'large number' per axis in order to convert from
     * position array to long key where the 'large number' is greater
     * than the number of members in the axis.
     * The largest 'long' is java.lang.Long.MAX_VALUE which is
     * 9,223,372,036,854,776,000. The product of the maximum number
     * of members per axis must be less than this maximum 'long'
     * value (otherwise one gets hashing collisions).
     * <p>
     * For a single axis, the maximum number of members is equal to
     * the max 'long' number, 9,223,372,036,854,776,000.
     * <p>
     * For two axes, the maximum number of members is the square root
     * of the max 'long' number, 9,223,372,036,854,776,000, which is
     * slightly bigger than 2,147,483,647 (which is the maximum integer).
     * <p>
     * For three axes, the maximum number of members per axis is the
     * cube root of the max 'long' which is about 2,000,000
     * <p>
     * For four axes the forth root is about 50,000.
     * <p>
     * For five or more axes, the maximum number of members per axis
     * based upon the root of the maximum 'long' number,
     * start getting too small to guarantee that it will be
     * smaller than the number of members on a given axis and so
     * we must resort to the Map-base Cell container.
     */



    /**
     * Synchronized Map from Locale to ValueFormatter. It is expected that
     * there will be only a small number of Locale's.
     * Should these be a WeakHashMap?
     */
    protected static final Map<Locale, ValueFormatter>
            formatValueFormatters =
            Collections.synchronizedMap(new HashMap<Locale, ValueFormatter>());

    /**
     * Synchronized Map from CellFormatter to ValueFormatter.
     * CellFormatter's are defined in schema files. It is expected
     * the there will only be a small number of CellFormatter's.
     * Should these be a WeakHashMap?
     */
    protected static final Map<CellFormatter, ValueFormatter>
            cellFormatters =
            Collections.synchronizedMap(new HashMap<CellFormatter, ValueFormatter>());

    /**
     * CellInfo's contain all of the information that a Cell requires.
     * They are placed in the cellInfos Map during evaluation and
     * serve as a constructor parameter for RolapCell. During the
     * evaluation stage they are mutable but after evaluation has
     * finished they are not changed.
     */
    static class CellInfo {
        Object value;
        String formatString;
        ValueFormatter valueFormatter;
        long key;

        CellInfo(long key) {
            this(key, null, null, null);
        }
        CellInfo(long key, Object value,
                 String formatString,
                 ValueFormatter valueFormatter) {
            this.key = key;
            this.value = value;
            this.formatString = formatString;
            this.valueFormatter = valueFormatter;
        }
        public int hashCode() {
            return (int)(key ^ (key >>> 32));
        }
        public boolean equals(Object o) {
            if (o instanceof CellInfo) {
                CellInfo that = (CellInfo) o;
                return that.key == this.key;
            } else {
                return false;
            }
        }
        String getFormatValue() {
            return valueFormatter.format(value, formatString);
        }
    }

    /**
     * The CellInfoContainer defines the API for the creation and
     * lookup of CellInfo objects. There are two implementations,
     * one that uses a Map for storage and the other uses an ObjectPool.
     */
    interface CellInfoContainer {
        /**
         * Return the number of CellInfo objects in this container.
         * @return  the number of CellInfo objects.
         */
        int size();
        /**
         * Reduce the size of the internal data structures need to
         * support the current entries. This should be called after
         * all CellInfo objects have been added to container.
         */
        void trimToSize();
        /**
         * Remove all CellInfo objects from container. Does not
         * change the size of the internal data structures.
         */
        void clear();
        /**
         * Create a new CellInfo object, add it to the container
         * a location <code>pos</code> and return it.
         *
         * @param pos where to store CellInfo object.
         * @return the newly create CellInfo object.
         */
        CellInfo create(int[] pos);
        /**
         * Get the CellInfo object at the location <code>pos</code>.
         *
         * @param pos where to find the CellInfo object.
         * @return the CellInfo found or null.
         */
        CellInfo lookup(int[] pos);
    }

    /**
     * The CellInfoMap uses a Map to store CellInfo Objects.
     * Note that the CellKey point instance variable is the same
     * Object (NOT a copy) that is used and modified during
     * the recursive calls to executeStripe - the
     * <code>create</code> method relies on this fact.
     */
    static class CellInfoMap implements CellInfoContainer {
        private final Map<CellKey, CellInfo> cellInfoMap;
        private final CellKey point;
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
            CellKey key = this.point.make(pos);
            return this.cellInfoMap.get(key);
        }
    }

    /**
     * The CellInfoPool uses an ObjectPool to store CellInfo Objects.
     * There is an inner interface (<code>CellKeyMaker</code>) and
     * implementations for 0 through 4 axes that convert the Cell
     * position integer array into a long.
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
                l += (MAX_AXIS_SIZE_4 * MAX_AXIS_SIZE_4 * MAX_AXIS_SIZE_4 * (long) pos[3]);
                return l;
            }
        }

        private final ObjectPool<CellInfo> cellInfoPool;
        private final CellKeyMaker cellKeyMaker;

        CellInfoPool(int axisLength) {
            this.cellInfoPool = new ObjectPool<CellInfo>();
            switch (axisLength) {
            case 0:
                this.cellKeyMaker = new Zero();
                break;
            case 1:
                this.cellKeyMaker = new One();
                break;
            case 2:
                this.cellKeyMaker = new Two();
                break;
            case 3:
                this.cellKeyMaker = new Three();
                break;
            case 4:
                this.cellKeyMaker = new Four();
                break;
            default:
                throw new RuntimeException(
                    "Creating CellInfoPool with axisLength=" +axisLength);
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
}

// End RolapResult.java
