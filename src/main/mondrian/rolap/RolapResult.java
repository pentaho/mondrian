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
import mondrian.util.Bug;

import org.apache.log4j.Logger;

import java.util.*;

/**
 * A <code>RolapResult</code> is the result of running a query.
 *
 * @author jhyde
 * @since 10 August, 2001
 * @version $Id$
 */
class RolapResult extends ResultBase {

    private static final Logger LOGGER = Logger.getLogger(ResultBase.class);

    private final RolapEvaluator evaluator;
    private final CellKey point;
    private final Map<CellKey, Object> cellValues;
    private final Map<CellKey, String> formatStrings;
    private final FastBatchingCellReader batchingReader;
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
        this.cellValues = new HashMap<CellKey, Object>();
        this.formatStrings = new HashMap<CellKey, String>();
        if (!execute) {
            return;
        }

        // for use in debugging Checkin_7634
        Util.discard(Bug.Checkin7634DoOld);
        try {
            // An array of lists which will hold each axis' implicit members (does
            // not include slicer axis).
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
        } finally {
            evaluator.clearExpResultCache();
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
    public Cell getCell(int[] pos) {
        if (pos.length != point.size()) {
            throw Util.newError(
                    "coordinates should have dimension " + point.size());
        }
        CellKey key = point.make(pos);
        Object value = cellValues.get(key);
        if (value == null) {
            value = Util.nullValue;
        }
        String formatString = formatStrings.get(key);
        if (formatString == null) {
            Cell cell = getCellNoDefaultFormatString(key);
            Util.discard(cell.getFormattedValue());   
            formatString = cell.getCachedFormatString(); 
            if (formatString == null) {
                formatString = "Standard";
            }
        }        
        return new RolapCell(this, (int[]) pos.clone(), value, formatString);
    }
        
    private Cell getCellNoDefaultFormatString(CellKey key) {
        Object value = cellValues.get(key);
        if (value == null) {
            value = Util.nullValue;
        }
        String formatString = formatStrings.get(key);
        return new RolapCell(this, key.getOrdinals(), value, formatString);
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
                if ((value instanceof List)) {
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
                cellValues.clear();
                formatStrings.clear();

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
            }
        } finally {
            // This call to clear the cube's cache only has an
            // effect if caching has been disabled, otherwise
            // nothing happens.
            RolapCube cube = (RolapCube) query.getCube();
            cube.clearCachedAggregations();
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

    private void executeStripe(int axisOrdinal, RolapEvaluator evaluator) {
        if (axisOrdinal < 0) {
            Axis axis = slicerAxis;
            List<Position> positions = axis.getPositions();
            for (Position position: positions) {
                evaluator.getQuery().checkCancelOrTimeout();
                evaluator.setContext(position);
                Object o;
                try {
                    o = evaluator.evaluateCurrent();
                } catch (MondrianEvaluationException e) {
                    o = e;
                }
                if (o == null) {
                    continue;
                }                
                
                CellKey key = point.copy();
                // Compute the formatted value, to ensure that any needed values
                // are in the cache.  Also compute this when value is null
                // (formatting of null values)
                try {
                    // Get the cell without default format string, if it returns
                    // null getFormattedValue() will try to evaluate a new
                    // format string

                    // The previous implementation created a RolapCell
                    // with its int array of position indexes and then
                    // used that array to set the context of the 
                    // Evaluator (and as a result, forcing Iterable-base
                    // RolapAxis to become List-base). Thats not necessary 
                    // since at this point the current evaluator already has
                    // the correct context, so we just use it here.
                    //
                    // This code is a combination of the code found in
                    // the getCellNoDefaultFormatString method and 
                    // the RolapCell getFormattedValue method.
                    Object value = cellValues.get(key);
                    if (value == null) {
                        value = Util.nullValue;
                    }
                    String cachedFormatString = formatStrings.get(key);

                    RolapCube cube = (RolapCube) getCube();
                    Dimension measuresDim = 
                            cube.getMeasuresHierarchy().getDimension();
                    RolapMeasure m = 
                            (RolapMeasure) evaluator.getContext(measuresDim);
                    CellFormatter cf = m.getFormatter();
                    if (cf != null) {
                        cf.formatCell(value);
                    } else {                                
                        if (cachedFormatString == null) {
                            cachedFormatString = evaluator.getFormatString();
                        }
                        evaluator.format(value, cachedFormatString);    
                    } 

                    formatStrings.put(key, cachedFormatString);
                    
                } catch (MondrianEvaluationException e) {
                    // ignore
                } catch (Throwable e) {
                    Util.discard(e);
                }
                if (o == RolapUtil.valueNotReadyException) {
                    continue;
                }                
                cellValues.put(key, o);                
            }
        } else {
            Axis axis = axes[axisOrdinal];
            List<Position> positions = axis.getPositions();
            int i = 0;
            for (Position position: positions) {
                point.setAxis(axisOrdinal, i);
                evaluator.setContext(position);
                evaluator.getQuery().checkCancelOrTimeout();
                executeStripe(axisOrdinal - 1, evaluator);
                i++;
            }
        }
    }

    /**
     * Converts a cell ordinal to a set of cell coordinates. Converse of
     * {@link #getCellOrdinal}. For example, if this result is 10 x 10 x 10,
     * then cell ordinal 537 has coordinates (5, 3, 7).
     */
    public int[] getCellPos(int cellOrdinal) {
        if (modulos == null) {
            makeModulos();
        }
        return modulos.getCellPos(cellOrdinal);
    }

    /**
     * Converts a set of cell coordinates to a cell ordinal. Converse of
     * {@link #getCellPos}.
     */
    int getCellOrdinal(int[] pos) {
        if (modulos == null) {
            makeModulos();
        }
        return modulos.getCellOrdinal(pos);
    }

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
                List list = null;
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
}

// End RolapResult.java
