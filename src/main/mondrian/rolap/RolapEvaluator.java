/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2001-2002 Kana Software, Inc.
// Copyright (C) 2001-2010 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 10 August, 2001
*/

package mondrian.rolap;
import mondrian.calc.*;
import mondrian.olap.*;
import mondrian.olap.fun.FunUtil;
import mondrian.util.Format;
import mondrian.spi.Dialect;

import org.apache.log4j.Logger;

import java.util.*;

/**
 * <code>RolapEvaluator</code> evaluates expressions in a dimensional
 * environment.
 *
 * <p>The context contains a member (which may be the default member)
 * for every dimension in the current cube. Certain operations, such as
 * evaluating a calculated member or a tuple, change the current context. The
 * evaluator's {@link #push} method creates a clone of the current evaluator
 * so that you can revert to the original context once the operation has
 * completed.
 *
 * <h3>Developers note</h3>
 *
 * <p>Many of the methods in this class are performance-critical. Where
 * possible they are declared 'final' so that the JVM can optimize calls to
 * these methods. If future functionality requires it, the 'final' modifier
 * can be removed and these methods can be overridden.
 *
 * @author jhyde
 * @since 10 August, 2001
 * @version $Id$
 */
public class RolapEvaluator implements Evaluator {
    private static final Logger LOGGER = Logger.getLogger(RolapEvaluator.class);

    /**
     * Dummy value to represent null results in the expression cache.
     */
    private static final Object nullResult = new Object();

    private final RolapMember[] currentMembers;
    private final RolapEvaluator parent;
    protected CellReader cellReader;
    private final int depth;

    private Member expandingMember;
    private boolean firstExpanding;
    private boolean nonEmpty;
    protected final RolapEvaluatorRoot root;
    private int iterationLength;
    private boolean evalAxes;

    private final RolapCalculation[] calcMembers;
    private int calcMemberCount;

    /**
     * List of lists of tuples or members, rarely used, but overrides the
     * ordinary dimensional context if set when a cell value comes to be
     * evaluated.
     */
    protected List<List<Member[]>> aggregationLists;

    private final List<Member> slicerMembers;
    private Boolean nativeEnabled;
    private Member[] nonAllMembers;

    /**
     * States of the finite state machine for determining the max solve order
     * for the "scoped" behavior.
     */
    private enum ScopedMaxSolveOrderFinderState {
        START,
        AGG_SCOPE,
        CUBE_SCOPE,
        QUERY_SCOPE
    }

    /**
     * Creates a non-root evaluator.
     *
     * @param root Root context for stack of evaluators (contains information
     *   which does not change during the evaluation)
     * @param parent Parent evaluator, not null
     */
    protected RolapEvaluator(
        RolapEvaluatorRoot root,
        RolapEvaluator parent)
    {
        this.iterationLength = 1;
        this.root = root;
        assert parent != null;
        this.parent = parent;

        depth = parent.depth + 1;
        nonEmpty = parent.nonEmpty;
        nativeEnabled = parent.nativeEnabled;
        evalAxes = parent.evalAxes;
        cellReader = parent.cellReader;
        currentMembers = parent.currentMembers.clone();
        calcMembers = parent.calcMembers.clone();
        calcMemberCount = parent.calcMemberCount;
        slicerMembers = new ArrayList<Member>(parent.slicerMembers);
        if (parent.aggregationLists != null) {
            aggregationLists =
                new ArrayList<List<Member[]>>(parent.aggregationLists);
        } else {
            aggregationLists = null;
        }
        expandingMember = parent.expandingMember;
    }

    /**
     * Creates a root evaluator.
     *
     * @param root Shared context between this evaluator and its children
     */
    public RolapEvaluator(RolapEvaluatorRoot root) {
        this.iterationLength = 1;
        this.root = root;
        this.parent = null;
        depth = 0;
        nonEmpty = false;
        nativeEnabled =
            MondrianProperties.instance().EnableNativeNonEmpty.get();
        evalAxes = false;
        cellReader = null;
        currentMembers = root.defaultMembers.clone();
        calcMembers = new RolapCalculation[currentMembers.length];
        calcMemberCount = 0;
        slicerMembers = new ArrayList<Member>();
        aggregationLists = null;
        for (RolapMember member : currentMembers) {
            if (member.isEvaluated()) {
                addCalcMember(new RolapMemberCalculation(member));
            }
        }

        // we expect client to set CellReader

        root.init(this);
    }

    /**
     * Creates an evaluator.
     */
    public static Evaluator create(Query query) {
        final RolapEvaluatorRoot root = new RolapEvaluatorRoot(query);
        return new RolapEvaluator(root);
    }

    /**
     * Returns the base (non-virtual) cube that the current measure in the
     * context belongs to.
     * @return Cube
     */
    public RolapCube getMeasureCube() {
        RolapCube measureCube = null;
        if (currentMembers[0] instanceof RolapStoredMeasure) {
            measureCube = ((RolapStoredMeasure) currentMembers[0]).getCube();
        }
        return measureCube;
    }

    /**
     * If IgnoreMeasureForNonJoiningDimension is set to true and one or more
     * members are on unrelated dimension for the measure in current context
     * then returns true.
     *
     * @param members
     * dimensions for the members need to be checked whether
     * related or unrelated
     * @return boolean
     */
    public boolean needToReturnNullForUnrelatedDimension(Member[] members) {
        RolapCube virtualCube = getCube();
        RolapCube baseCube = getMeasureCube();
        if (virtualCube.isVirtual() && baseCube != null) {
            if (virtualCube.shouldIgnoreUnrelatedDimensions(baseCube.getName()))
            {
                return false;
            } else if (MondrianProperties.instance()
                .IgnoreMeasureForNonJoiningDimension.get())
            {
                Set<Dimension> nonJoiningDimensions =
                    baseCube.nonJoiningDimensions(members);
                if (!nonJoiningDimensions.isEmpty()) {
                    return true;
                }
            }
        }

        return false;
    }

    public boolean nativeEnabled() {
        return nativeEnabled;
    }

    public boolean currentIsEmpty() {
        // If a cell evaluates to null, it is always deemed empty.
        Object o = evaluateCurrent();
        if (o == Util.nullValue || o == null) {
            return true;
        }
        // For other cell values (e.g. zero), the cell is deemed empty if the
        // number of fact table rows is zero.
        final RolapEvaluator eval2 = push(getCube().getFactCountMeasure());
        o = eval2.evaluateCurrent();
        return o == null
           || (o instanceof Number && ((Number) o).intValue() == 0);
    }

    public void setNativeEnabled(final Boolean nativeEnabled) {
        this.nativeEnabled = nativeEnabled;
    }

    protected final Logger getLogger() {
        return LOGGER;
    }

    public final Member[] getMembers() {
        return currentMembers;
    }

    public final Member[] getNonAllMembers() {
        if (nonAllMembers == null) {
            final List<RolapMember> members = new ArrayList<RolapMember>();
            for (RolapMember rolapMember : currentMembers) {
                if (!rolapMember.isAll()) {
                    members.add(rolapMember);
                }
            }
            nonAllMembers = members.toArray(new Member[members.size()]);
        }
        return nonAllMembers;
    }

    public final List<List<Member[]>> getAggregationLists() {
        return aggregationLists;
    }

    final void setCellReader(CellReader cellReader) {
        this.cellReader = cellReader;
    }

    public final RolapCube getCube() {
        return root.cube;
    }

    public final Query getQuery() {
        return root.query;
    }

    public final int getDepth() {
        return depth;
    }

    public final RolapEvaluator getParent() {
        return parent;
    }

    public final SchemaReader getSchemaReader() {
        return root.schemaReader;
    }

    public Date getQueryStartTime() {
        return root.getQueryStartTime();
    }

    public Dialect getDialect() {
        return root.currentDialect;
    }

    public final RolapEvaluator push(Member[] members) {
        final RolapEvaluator evaluator = _push();
        evaluator.setContext(members);
        return evaluator;
    }

    public final RolapEvaluator push(Member member) {
        final RolapEvaluator evaluator = _push();
        evaluator.setContext(member);
        return evaluator;
    }

    public Evaluator push(boolean nonEmpty) {
        final RolapEvaluator evaluator = _push();
        evaluator.setNonEmpty(nonEmpty);
        return evaluator;
    }

    public Evaluator push(boolean nonEmpty, boolean nativeEnabled) {
        final RolapEvaluator evaluator = _push();
        evaluator.setNonEmpty(nonEmpty);
        evaluator.setNativeEnabled(nativeEnabled);
        return evaluator;
    }

    public final RolapEvaluator push() {
        return _push();
    }

    public RolapEvaluator push(RolapCalculation calc) {
        RolapEvaluator evaluator = push();
        evaluator.addCalcMember(calc);
        return evaluator;
    }

    /**
     * Creates a clone of the current validator.
     */
    protected RolapEvaluator _push() {
        getQuery().checkCancelOrTimeout();
        return new RolapEvaluator(root, this);
    }

    public final RolapEvaluator pop() {
        return parent;
    }

    public final Evaluator pushAggregation(List<Member[]> list) {
        RolapEvaluator newEvaluator = _push();
        newEvaluator.addToAggregationList(list);
        clearHierarchyFromRegularContext(list, newEvaluator);
        return newEvaluator;
    }

    private void addToAggregationList(List<Member[]> list) {
        if (aggregationLists == null) {
            aggregationLists = new ArrayList<List<Member[]>>();
        }
        aggregationLists.add(list);
    }

    private void clearHierarchyFromRegularContext(
        List<Member[]> list,
        RolapEvaluator newEvaluator)
    {
        Member[] tuple = list.get(0);
        for (Member member : tuple) {
            newEvaluator.setContext(member.getHierarchy().getAllMember());
        }
    }

    /**
     * Returns true if the other object is a {@link RolapEvaluator} with
     * identical context.
     */
    public final boolean equals(Object obj) {
        if (!(obj instanceof RolapEvaluator)) {
            return false;
        }
        RolapEvaluator that = (RolapEvaluator) obj;
        return Arrays.equals(this.currentMembers, that.currentMembers);
    }

    public final int hashCode() {
        return Util.hashArray(0, this.currentMembers);
    }

    /**
     * Adds a slicer member to the evaluator context, and remember it as part
     * of the slicer. The slicer members are passed onto derived evaluators
     * so that functions using those evaluators can choose to ignore the
     * slicer members. One such function is CrossJoin emptiness check.
     *
     * @param member a member in the slicer
     */
    public final void setSlicerContext(Member member) {
        setContext(member);
        slicerMembers.add(member);
    }

    /**
     * Return the list of slicer members in the current evaluator context.
     * @return slicerMembers
     */
    public final List<Member> getSlicerMembers() {
        return slicerMembers;
    }

    public final Member setContext(Member member) {
        final RolapMember m = (RolapMember) member;
        final int ordinal = m.getHierarchy().getOrdinalInCube();
        final RolapMember previous = currentMembers[ordinal];

        // If the context is unchanged, save ourselves some effort. It would be
        // a mistake to use equals here; we might treat the visual total member
        // 'Gender.All' the same as the true 'Gender.All' because they have the
        // same unique name, and that would be wrong.
        if (m == previous) {
            return m;
        }
        if (previous.isEvaluated()) {
            removeCalcMember(new RolapMemberCalculation(previous));
        }
        currentMembers[ordinal] = m;
        if (m.isEvaluated()) {
            addCalcMember(new RolapMemberCalculation(m));
        }
        nonAllMembers = null;
        return previous;
    }

    public final void setContext(List<Member> memberList) {
        int i = 0;
        for (Member member : memberList) {
            // more than one usage
            if (member == null) {
                if (getLogger().isDebugEnabled()) {
                    getLogger().debug(
                        "RolapEvaluator.setContext: member == null "
                        + " , count=" + i);
                }
                assert false;
                continue;
            }
            setContext(member);
        }
    }

    public final void setContext(Member[] members) {
        for (final Member member : members) {
        // more than one usage
            if (member == null) {
                if (getLogger().isDebugEnabled()) {
                    getLogger().debug(
                        "RolapEvaluator.setContext: "
                        + "member == null, memberList: "
                        + Arrays.asList(members));
                }
                assert false;
                continue;
            }

            setContext(member);
        }
    }

    public final RolapMember getContext(Hierarchy hierarchy) {
        return currentMembers[((RolapHierarchy) hierarchy).getOrdinalInCube()];
    }

    /**
     * More specific version of {@link #getContext(mondrian.olap.Hierarchy)},
     * for internal code.
     *
     * @param hierarchy Hierarchy
     * @return current member
     */
    public final RolapMember getContext(RolapHierarchy hierarchy) {
        return currentMembers[hierarchy.getOrdinalInCube()];
    }

    public final Object evaluateCurrent() {
        // Get the member in the current context which is (a) calculated, and
        // (b) has the highest solve order. If there are no calculated members,
        // go ahead and compute the cell.
        RolapCalculation maxSolveMember;
        switch (calcMemberCount) {
        case 0:
            final Object o = cellReader.get(this);
            if (o == Util.nullValue) {
                return null;
            }
            return o;

        case 1:
            maxSolveMember = calcMembers[0];
            break;

        default:
            switch (root.solveOrderMode) {
            case ABSOLUTE:
                maxSolveMember = getAbsoluteMaxSolveOrder();
                break;
            case SCOPED:
                maxSolveMember = getScopedMaxSolveOrder();
                break;
            default:
                throw Util.unexpected(root.solveOrderMode);
            }
        }
        final RolapEvaluator evaluator = maxSolveMember.pushSelf(this);
        final Calc calc = maxSolveMember.getCompiledExpression(root);
        final Object o = calc.evaluate(evaluator);
        if (o == Util.nullValue) {
            return null;
        }
        return o;
    }

    void setExpanding(Member member) {
        assert member != null;
        expandingMember = member;
        firstExpanding = true;
        final int memberCount = currentMembers.length;
        if (depth > memberCount) {
            if (depth % memberCount == 0) {
                checkRecursion(parent);
            }
        }
    }

    /**
     * Returns the calculated member being currently expanded.
     *
     * <p>This can be useful if many calculated members are generated with
     * essentially the same expression. The compiled expression can call this
     * method to find which instance of the member is current, and therefore the
     * calculated members can share the same {@link Calc} object.
     *
     * @return Calculated member currently being expanded
     */
    Member getExpanding() {
        return expandingMember;
    }

    /**
     * Makes sure that there is no evaluator with identical context on the
     * stack.
     *
     * @param eval Evaluator
     * @throws mondrian.olap.fun.MondrianEvaluationException if there is a loop
     */
    private static void checkRecursion(RolapEvaluator eval) {
        // Find the nearest ancestor which is expanding a calculated member.
        // (The starting evaluator has just been pushed, so may not have the
        // state it will have when recursion happens.)
        while (true) {
            if (eval == null) {
                return;
            }
            if (eval.firstExpanding) {
                break;
            }
            eval = eval.parent;
        }

        // Find an ancestor evaluator that has identical context to this one:
        // same member context, and expanding the same calculation.
        outer:
        for (RolapEvaluator eval2 = eval.parent;
             eval2 != null;
            eval2 = eval2.parent)
        {
            // Ignore ancestors which are not the first level expanding a
            // member. (They are dummy evaluators created to avoid stomping on
            // context while iterating over a set, say.)
            if (!eval2.firstExpanding
                || eval2.expandingMember != eval.expandingMember)
            {
                continue;
            }
            for (int i = 0; i < eval.currentMembers.length; i++) {
                final Member member = eval2.currentMembers[i];

                // more than one usage
                if (member == null) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(
                            "RolapEvaluator.checkRecursion: member == null "
                            + " , count=" + i);
                    }
                    continue;
                }

                final RolapMember parentMember =
                    eval.getContext(member.getHierarchy());
                if (member != parentMember) {
                    continue outer;
                }
            }
            throw FunUtil.newEvalException(
                null,
                "Infinite loop while evaluating calculated member '"
                + eval.expandingMember + "'; context stack is "
                + eval.getContextString());
        }
    }

    private String getContextString() {
        final boolean skipDefaultMembers = true;
        final StringBuilder buf = new StringBuilder("{");
        int frameCount = 0;
        for (RolapEvaluator eval = this; eval != null; eval = eval.parent) {
            if (eval.expandingMember == null) {
                continue;
            }
            if (frameCount++ > 0) {
                buf.append(", ");
            }
            buf.append("(");
            int memberCount = 0;
            for (Member m : eval.currentMembers) {
                if (skipDefaultMembers
                    && m == m.getHierarchy().getDefaultMember())
                {
                    continue;
                }
                if (memberCount++ > 0) {
                    buf.append(", ");
                }
                buf.append(m.getUniqueName());
            }
            buf.append(")");
        }
        buf.append("}");
        return buf.toString();
    }

    public final Object getProperty(String name, Object defaultValue) {
        Object o = defaultValue;
        int maxSolve = Integer.MIN_VALUE;
        int i = -1;
        for (Member member : getMembers()) {
            i++;
            // more than one usage
            if (member == null) {
                if (getLogger().isDebugEnabled()) {
                    getLogger().debug(
                        "RolapEvaluator.getProperty: member == null "
                        + " , count=" + i);
                }
                continue;
            }

            // Don't call member.getPropertyValue unless this member's
            // solve order is greater than one we've already seen.
            // The getSolveOrder call is cheap call compared to the
            // getPropertyValue call, and when we're evaluating millions
            // of members, this has proven to make a significant performance
            // difference.
            final int solve = member.getSolveOrder();
            if (solve > maxSolve) {
                final Object p = member.getPropertyValue(name);
                if (p != null) {
                    o = p;
                    maxSolve = solve;
                }
            }
        }
        return o;
    }

    /**
     * Returns the format string for this cell. This is computed by evaluating
     * the format expression in the current context, and therefore different
     * cells may have different format strings.
     *
     * @post return != null
     */
    public final String getFormatString() {
        final Exp formatExp =
            (Exp) getProperty(Property.FORMAT_EXP.name, null);
        if (formatExp == null) {
            return "Standard";
        }
        final Calc formatCalc = root.getCompiled(formatExp, true, null);
        final Object o = formatCalc.evaluate(this);
        if (o == null) {
            return "Standard";
        }
        return o.toString();
    }

    private Format getFormat() {
        final String formatString = getFormatString();
        return getFormat(formatString);
    }

    private Format getFormat(String formatString) {
        return Format.get(formatString, root.connection.getLocale());
    }

    public final Locale getConnectionLocale() {
        return root.connection.getLocale();
    }

    public final String format(Object o) {
        if (o == Util.nullValue) {
            Format format = getFormat();
            return format.format(null);
        } else if (o instanceof Throwable) {
            return "#ERR: " + o.toString();
        } else if (o instanceof String) {
            return (String) o;
        } else {
            Format format = getFormat();
            return format.format(o);
        }
    }

    public final String format(Object o, String formatString) {
        if (o == Util.nullValue) {
            Format format = getFormat(formatString);
            return format.format(null);
        } else if (o instanceof Throwable) {
            return "#ERR: " + o.toString();
        } else if (o instanceof String) {
            return (String) o;
        } else {
            Format format = getFormat(formatString);
            return format.format(o);
        }
    }

    /**
     * Creates a key which uniquely identifes an expression and its
     * context. The context includes members of dimensions which the
     * expression is dependent upon.
     */
    private Object getExpResultCacheKey(ExpCacheDescriptor descriptor) {
        final List<Object> key = new ArrayList<Object>();
        key.add(descriptor.getExp());

        // in NON EMPTY mode the result depends on everything, e.g.
        // "NON EMPTY [Customer].[Name].members" may return different results
        // for 1997-01 and 1997-02
        if (nonEmpty) {
            key.addAll(Arrays.asList(currentMembers));
            return key;
        }

        final int[] hierarchyOrdinals =
            descriptor.getDependentHierarchyOrdinals();
        for (int i = 0; i < hierarchyOrdinals.length; i++) {
            final int hierarchyOrdinal = hierarchyOrdinals[i];
            final Member member = currentMembers[hierarchyOrdinal];

            // more than one usage
            if (member == null) {
                getLogger().debug(
                    "RolapEvaluator.getExpResultCacheKey: "
                    + "member == null; hierarchyOrdinal=" + i);
                continue;
            }

            key.add(member);
        }
        return key;
    }

    public final Object getCachedResult(ExpCacheDescriptor cacheDescriptor) {
        // Look up a cached result, and if not present, compute one and add to
        // cache. Use a dummy value to represent nulls.
        final Object key = getExpResultCacheKey(cacheDescriptor);
        Object result = root.getCacheResult(key);
        if (result == null) {
            boolean aggCacheDirty = cellReader.isDirty();
            int aggregateCacheMissCountBefore = cellReader.getMissCount();
            result = cacheDescriptor.evaluate(this);
            int aggregateCacheMissCountAfter = cellReader.getMissCount();

            boolean isValidResult;

            if (!aggCacheDirty
                && (aggregateCacheMissCountBefore
                    == aggregateCacheMissCountAfter))
            {
                // Cache the evaluation result as valid result if the
                // evaluation did not use any missing aggregates. Missing
                // aggregates could be used when aggregate cache is not fully
                // loaded, or if new missing aggregates are seen.
                isValidResult = true;
            } else {
                // Cache the evaluation result as invalid result if the
                // evaluation uses missing aggregates.
                isValidResult = false;
            }
            root.putCacheResult(
                key,
                result == null ? nullResult : result,
                isValidResult);
        } else if (result == nullResult) {
            result = null;
        }

        return result;
    }

    public final void clearExpResultCache(boolean clearValidResult) {
        root.clearResultCache(clearValidResult);
    }

    public final boolean isNonEmpty() {
        return nonEmpty;
    }

    public final void setNonEmpty(boolean nonEmpty) {
        this.nonEmpty = nonEmpty;
    }

    public final RuntimeException newEvalException(Object context, String s) {
        return FunUtil.newEvalException((FunDef) context, s);
    }

    public final NamedSetEvaluator getNamedSetEvaluator(
        NamedSet namedSet,
        boolean create)
    {
        return root.evaluateNamedSet(namedSet, create);
    }

    public final int getMissCount() {
        return cellReader.getMissCount();
    }

    public final Object getParameterValue(ParameterSlot slot) {
        return root.getParameterValue(slot);
    }

    final void addCalcMember(RolapCalculation member) {
        assert member != null;
        calcMembers[calcMemberCount++] = member;
    }

    /**
     * Returns the member with the highest solve order according to AS2000
     * rules. This was the behavior prior to solve order mode being
     * configurable.
     *
     * <p>The SOLVE_ORDER value is absolute regardless of where it is defined;
     * e.g. a query defined calculated member with a SOLVE_ORDER of 1 always
     * takes precedence over a cube defined value of 2.
     *
     * <p>No special consideration is given to the aggregate function.
     */
    private RolapCalculation getAbsoluteMaxSolveOrder() {
        // Find member with the highest solve order.
        RolapCalculation maxSolveMember = calcMembers[0];
        for (int i = 1; i < calcMemberCount; i++) {
            RolapCalculation member = calcMembers[i];
            if (expandsBefore(member, maxSolveMember)) {
                maxSolveMember = member;
            }
        }
        return maxSolveMember;
    }

    /**
     * Returns the member with the highest solve order according to AS2005
     * scoping rules.
     *
     * <p>By default, cube calculated members are resolved before any session
     * scope calculated members, and session scope members are resolved before
     * any query defined calculation.  The SOLVE_ORDER value only applies within
     * the scope in which it was defined.
     *
     * <p>The aggregate function is always applied to base members; i.e. as if
     * SOLVE_ORDER was defined to be the lowest value in a given evaluation in a
     * SSAS2000 sense.
     */
    private RolapCalculation getScopedMaxSolveOrder() {
        // Finite state machine that determines the member with the highest
        // solve order.
        RolapCalculation maxSolveMember = null;
        ScopedMaxSolveOrderFinderState state =
            ScopedMaxSolveOrderFinderState.START;
        for (int i = 0; i < calcMemberCount; i++) {
            RolapCalculation member = calcMembers[i];
            switch (state) {
            case START:
                maxSolveMember = member;
                if (maxSolveMember.containsAggregateFunction()) {
                    state = ScopedMaxSolveOrderFinderState.AGG_SCOPE;
                } else if (maxSolveMember.isCalculatedInQuery()) {
                    state = ScopedMaxSolveOrderFinderState.QUERY_SCOPE;
                } else {
                    state = ScopedMaxSolveOrderFinderState.CUBE_SCOPE;
                }
                break;

            case AGG_SCOPE:
                if (member.containsAggregateFunction()) {
                    if (expandsBefore(member, maxSolveMember)) {
                        maxSolveMember = member;
                    }
                } else if (member.isCalculatedInQuery()) {
                    maxSolveMember = member;
                    state = ScopedMaxSolveOrderFinderState.QUERY_SCOPE;
                } else {
                    maxSolveMember = member;
                    state = ScopedMaxSolveOrderFinderState.CUBE_SCOPE;
                }
                break;

            case CUBE_SCOPE:
                if (member.containsAggregateFunction()) {
                    continue;
                }

                if (member.isCalculatedInQuery()) {
                    maxSolveMember = member;
                    state = ScopedMaxSolveOrderFinderState.QUERY_SCOPE;
                } else if (expandsBefore(member, maxSolveMember)) {
                    maxSolveMember = member;
                }
                break;

            case QUERY_SCOPE:
                if (member.containsAggregateFunction()) {
                    continue;
                }

                if (member.isCalculatedInQuery()) {
                    if (expandsBefore(member, maxSolveMember)) {
                        maxSolveMember = member;
                    }
                }
                break;
            }
        }

        return maxSolveMember;
    }

    /**
     * Returns whether a given calculation expands before another.
     * A calculation expands before another if its solve order is higher,
     * or if its solve order is the same and its dimension ordinal is lower.
     *
     * @param calc1 First calculated member or tuple
     * @param calc2 Second calculated member or tuple
     * @return Whether calc1 expands before calc2
     */
    private boolean expandsBefore(
        RolapCalculation calc1,
        RolapCalculation calc2)
    {
        final int solveOrder1 = calc1.getSolveOrder();
        final int solveOrder2 = calc2.getSolveOrder();
        if (solveOrder1 > solveOrder2) {
            return true;
        } else {
            return solveOrder1 == solveOrder2
                && calc1.getHierarchyOrdinal()
                    < calc2.getHierarchyOrdinal();
        }
    }

    void removeCalcMember(RolapCalculation previous) {
        for (int i = 0; i < calcMemberCount; i++) {
            final RolapCalculation calcMember = calcMembers[i];
            if (calcMember.equals(previous)) {
                // overwrite this member with the end member
                --calcMemberCount;
                calcMembers[i] = calcMembers[calcMemberCount];
                calcMembers[calcMemberCount] = null; // to allow gc
            }
        }
    }

    public final int getIterationLength() {
        return iterationLength;
    }

    public final void setIterationLength(int length) {
        iterationLength = length;
    }

    public final boolean isEvalAxes() {
        return evalAxes;
    }

    public final void setEvalAxes(boolean evalAxes) {
        this.evalAxes = evalAxes;
    }

    /**
     * Checks if unrelated dimensions to the measure in the current context
     * should be ignored.
     * @return boolean
     */
    public boolean shouldIgnoreUnrelatedDimensions() {
        return getCube().shouldIgnoreUnrelatedDimensions(
            getMeasureCube().getName());
    }
}

// End RolapEvaluator.java
