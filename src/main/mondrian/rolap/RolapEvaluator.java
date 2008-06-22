/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2001-2002 Kana Software, Inc.
// Copyright (C) 2001-2007 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 10 August, 2001
*/

package mondrian.rolap;
import mondrian.calc.*;
import mondrian.olap.*;
import mondrian.olap.fun.FunUtil;
import mondrian.rolap.sql.SqlQuery;
import mondrian.resource.MondrianResource;
import mondrian.util.Format;

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
    private final Evaluator parent;
    protected CellReader cellReader;
    private final int depth;

    private Member expandingMember;
    private boolean nonEmpty;
    protected final RolapEvaluatorRoot root;
    private int iterationLength;
    private boolean evalAxes;

    private final Member[] calcMembers;
    private int calcMemberCount;

    /**
     * List of lists of tuples or members, rarely used, but overrides the
     * ordinary dimensional context if set when a cell value comes to be
     * evaluated.
     */
    protected List<List<RolapMember>> aggregationLists;

    private final List<Member> slicerMembers;

    /**
     * Creates an evaluator.
     *
     * @param root Root context for stack of evaluators (contains information
     *   which does not change during the evaluation)
     * @param parent Parent evaluator, or null if this is the root
     */
    protected RolapEvaluator(
            RolapEvaluatorRoot root,
            RolapEvaluator parent) {
        this.iterationLength = 1;
        this.root = root;
        this.parent = parent;

        if (parent == null) {
            depth = 0;
            nonEmpty = false;
            evalAxes = false;
            cellReader = null;
            currentMembers = new RolapMember[root.cube.getDimensions().length];
            calcMembers = new Member[this.currentMembers.length];
            calcMemberCount = 0;
            slicerMembers = new ArrayList<Member>();
            aggregationLists = null;
        } else {
            depth = parent.depth + 1;
            nonEmpty = parent.nonEmpty;
            evalAxes = parent.evalAxes;
            cellReader = parent.cellReader;
            currentMembers = parent.currentMembers.clone();
            calcMembers = parent.calcMembers.clone();
            calcMemberCount = parent.calcMemberCount;
            slicerMembers = new ArrayList<Member> (parent.slicerMembers);
            if (parent.aggregationLists != null) {
                aggregationLists =
                        new ArrayList<List<RolapMember>>(parent.aggregationLists);
            } else {
                aggregationLists = null;
            }
        }
    }

    /**
     * Creates an evaluator with no parent.
     *
     * @param root Shared context between this evaluator and its children
     */
    public RolapEvaluator(RolapEvaluatorRoot root) {
        this(root, null);

        // we expect client to set CellReader

        final SchemaReader scr = this.root.schemaReader;
        final Dimension[] dimensions = this.root.cube.getDimensions();
        for (final Dimension dimension : dimensions) {
            final int ordinal = dimension.getOrdinal(this.root.cube);
            final Hierarchy hier = dimension.getHierarchy();

            final RolapMember member =
                (RolapMember) scr.getHierarchyDefaultMember(hier);

            // If there is no member, we cannot continue.
            if (member == null) {
                throw MondrianResource.instance().InvalidHierarchyCondition
                    .ex(hier.getUniqueName());
            }

            // This fragment is a concurrency bottleneck, so use a cache of
            // hierarchy usages.
            final HierarchyUsage hierarchyUsage =
                this.root.cube.getFirstUsage(hier);
            if (hierarchyUsage != null) {
                member.makeUniqueName(hierarchyUsage);
            }

            currentMembers[ordinal] = member;
            if (member.isCalculated()) {
                addCalcMember(member);
            }
        }

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
     * @param members
     * dimensions for the members need to be checked whether
     * related or unrelated
     * @return boolean
     */
    public boolean needToReturnNullForUnrelatedDimension(Member[] members) {
        RolapCube virtualCube = getCube();
        RolapCube baseCube = getMeasureCube();
        if (virtualCube.isVirtual() && baseCube != null) {
            if (virtualCube.shouldIgnoreUnrelatedDimensions(baseCube.getName())) {
                return false;
            } else if (MondrianProperties.instance()
                .IgnoreMeasureForNonJoiningDimension.get()) {
                Set<Dimension> nonJoiningDimensions =
                    baseCube.nonJoiningDimensions(members);
                if (!nonJoiningDimensions.isEmpty()) {
                    return true;
                }
            }
        }

        return false;
    }

    protected static class RolapEvaluatorRoot {
        final Map<Object, Object> expResultCache =
            new HashMap<Object, Object>();
        final Map<Object, Object> tmpExpResultCache =
            new HashMap<Object, Object>();
        final RolapCube cube;
        final RolapConnection connection;
        final SchemaReader schemaReader;
        final Map<List<Object>, Calc> compiledExps =
            new HashMap<List<Object>, Calc>();
        private final Query query;
        private final Date queryStartTime;
        final SqlQuery.Dialect currentDialect;
        
        /**
         * Default members of each hierarchy, from the schema reader's
         * perspective. Finding the default member is moderately expensive, but
         * happens very often.
         */
        private final RolapMember[] defaultMembers;

        public RolapEvaluatorRoot(Query query) {
            this.query = query;
            this.cube = (RolapCube) query.getCube();
            this.connection = (RolapConnection) query.getConnection();
            this.schemaReader = query.getSchemaReader(true);
            this.queryStartTime = new Date();
            List<RolapMember> list = new ArrayList<RolapMember>();
            for (Dimension dimension : cube.getDimensions()) {
                list.add(
                    (RolapMember) schemaReader.getHierarchyDefaultMember(
                        dimension.getHierarchy()));
            }
            this.defaultMembers = list.toArray(new RolapMember[list.size()]);
            this.currentDialect = SqlQuery.Dialect.create(schemaReader.getDataSource());
        }

        /**
         * Implements a cheap-and-cheerful mapping from expressions to compiled
         * expressions.
         *
         * <p>TODO: Save compiled expressions somewhere better.
         *
         * @param exp Expression
         * @param scalar Whether expression is scalar
         * @param resultStyle Preferred result style; if null, use query's default
         *     result style; ignored if expression is scalar
         * @return compiled expression
         */
        final Calc getCompiled(
            Exp exp,
            boolean scalar,
            ResultStyle resultStyle)
        {
            List<Object> key = Arrays.asList(exp, scalar, resultStyle);
            Calc calc = compiledExps.get(key);
            if (calc == null) {
                calc = query.compileExpression(exp, scalar, resultStyle);
                compiledExps.put(key, calc);
            }
            return calc;
        }

        /**
         * Evaluates a named set.
         *
         * <p>The default implementation throws
         * {@link UnsupportedOperationException}.
         */
        protected Object evaluateNamedSet(String name, Exp exp) {
            throw new UnsupportedOperationException();
        }

        /**
         * First evaluator calls this method on construction.
         */
        protected void init(Evaluator evaluator) {
        }

        /**
         * Returns the value of a parameter, evaluating its default expression
         * if necessary.
         *
         * <p>The default implementation throws
         * {@link UnsupportedOperationException}.
         */
        public Object getParameterValue(ParameterSlot slot) {
            throw new UnsupportedOperationException();
        }

        /**
         * Puts result in cache.
         *
         * @param key key
         * @param result value to be cached
         * @param isValidResult indicate if this result is valid
         */
        public final void putCacheResult(
            Object key,
            Object result,
            boolean isValidResult)
        {
            if (isValidResult) {
                expResultCache.put(key, result);
            } else {
                tmpExpResultCache.put(key, result);
            }
        }

        /**
         * Gets result from cache.
         *
         * @param key cache key
         * @return cached expression
         */
        public final Object getCacheResult(Object key) {
            Object result = expResultCache.get(key);
            if (result == null) {
                result = tmpExpResultCache.get(key);
            }
            return result;
        }

        /**
         * Clears the expression result cache.
         *
         * @param clearValidResult whether to clear valid expression results
         */
        public final void clearResultCache(boolean clearValidResult) {
            if (clearValidResult) {
                expResultCache.clear();
            }
            tmpExpResultCache.clear();
        }
        
        /**
         * Get query start time.
         * 
         * @return the query start time
         */ 
        public Date getQueryStartTime() {
            return queryStartTime;
        }
    }

    protected final Logger getLogger() {
        return LOGGER;
    }

    public final Member[] getMembers() {
        return currentMembers;
    }

    public final List<List<RolapMember>> getAggregationLists() {
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

    public final Evaluator getParent() {
        return parent;
    }

    public final SchemaReader getSchemaReader() {
        return root.schemaReader;
    }

    public Date getQueryStartTime() {
        return root.getQueryStartTime();
    }
    
    public SqlQuery.Dialect getDialect() {
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

    public final RolapEvaluator push() {
        return _push();
    }

    /**
     * Creates a clone of the current validator.
     */
    protected RolapEvaluator _push() {
        getQuery().checkCancelOrTimeout();
        return new RolapEvaluator(root, this);
    }

    public final Evaluator pop() {
        return parent;
    }

    public final Evaluator pushAggregation(List list) {
        RolapEvaluator newEvaluator = _push();
        newEvaluator.addToAggregationList(list);
        clearHierarchyFromRegularContext(list, newEvaluator);
        return newEvaluator;
    }
        
    private void addToAggregationList(List list){
        if (aggregationLists == null) {
            aggregationLists = new ArrayList<List<RolapMember>>();
        }
        aggregationLists.add(list);
    }
        
    private void clearHierarchyFromRegularContext(
        List list,
        RolapEvaluator newEvaluator)
    {
        if (containsTuple(list)) {
            Member[] tuple = (Member[]) list.get(0);
            for (Member member : tuple) {
                newEvaluator.setContext(member.getHierarchy().getAllMember());
            }
        } else {
            newEvaluator.setContext(((RolapMember) list.get(0)).getHierarchy()
                .getAllMember());
        }
    }

    private boolean containsTuple(List rolapList) {
        return rolapList.get(0) instanceof Member[];
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
        final int ordinal = m.getDimension().getOrdinal(root.cube);
        final Member previous = currentMembers[ordinal];
        if (m.equals(previous)) {
            return m;
        }
        if (previous.isCalculated()) {
            removeCalcMember(previous);
        }
        currentMembers[ordinal] = m;
        if (m.isCalculated()) {
            addCalcMember(m);
        }
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
            } else {
                setContext(member);
            }
            i++;
        }
    }

    public final void setContext(Member[] members) {
        for (int i = 0; i < members.length; i++) {
            final Member member = members[i];

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

    public final RolapMember getContext(Dimension dimension) {
        return currentMembers[dimension.getOrdinal(root.cube)];
    }

    public final Object evaluateCurrent() {
        // Get the member in the current context which is (a) calculated, and
        // (b) has the highest solve order; returns null if there are no
        // calculated members.
        final Member maxSolveMember = peekCalcMember();
        if (maxSolveMember == null) {
            final Object o = cellReader.get(this);
            if (o == Util.nullValue) {
                return null;
            }
            return o;
        }
        // REVIEW this operation is executed frequently, and computing the
        // default member of a hierarchy for a given role is not cheap
        final RolapMember defaultMember =
            root.defaultMembers[
                maxSolveMember.getDimension().getOrdinal(root.cube)];

        final RolapEvaluator evaluator = push(defaultMember);
        evaluator.setExpanding(maxSolveMember);
        final Exp exp = maxSolveMember.getExpression();
        final Calc calc = root.getCompiled(exp, true, null);
        final Object o = calc.evaluate(evaluator);
        if (o == Util.nullValue) {
            return null;
        }
        return o;
    }

    private void setExpanding(Member member) {
        expandingMember = member;
        final int memberCount = currentMembers.length;
        if (depth > memberCount) {
            if (depth % memberCount == 0) {
                checkRecursion((RolapEvaluator) parent);
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
            if (eval.expandingMember != null) {
                break;
            }
            eval = (RolapEvaluator) eval.getParent();
        }

        outer:
        for (RolapEvaluator eval2 = (RolapEvaluator) eval.getParent();
                 eval2 != null;
                 eval2 = (RolapEvaluator) eval2.getParent()) {
            if (eval2.expandingMember != eval.expandingMember) {
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
                    eval.getContext(member.getDimension());
                if (member != parentMember) {
                    continue outer;
                }
            }
            throw FunUtil.newEvalException(
                null,
                "Infinite loop while evaluating calculated member '" +
                eval.expandingMember + "'; context stack is " +
                eval.getContextString());
        }
    }

    private String getContextString() {
        final boolean skipDefaultMembers = true;
        final StringBuilder buf = new StringBuilder("{");
        int frameCount = 0;
        for (RolapEvaluator eval = this; eval != null;
                 eval = (RolapEvaluator) eval.getParent()) {
            if (eval.expandingMember == null) {
                continue;
            }
            if (frameCount++ > 0) {
                buf.append(", ");
            }
            buf.append("(");
            int memberCount = 0;
            for (Member m : eval.currentMembers) {
                if (skipDefaultMembers &&
                    m == m.getHierarchy().getDefaultMember()) {
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
        for (int i = 0; i < currentMembers.length; i++) {
            final Member member = currentMembers[i];

            // more than one usage
            if (member == null) {
                if (getLogger().isDebugEnabled()) {
                    getLogger().debug(
                        "RolapEvaluator.getProperty: member == null "
                         + " , count=" + i);
                }
                continue;
            }

            final Object p = member.getPropertyValue(name);
            if (p != null) {
                final int solve = member.getSolveOrder();
                if (solve > maxSolve) {
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
        final int[] dimensionOrdinals =
            descriptor.getDependentDimensionOrdinals();
        for (int i = 0; i < dimensionOrdinals.length; i++) {
            final int dimensionOrdinal = dimensionOrdinals[i];
            final Member member = currentMembers[dimensionOrdinal];

            // more than one usage
            if (member == null) {
                getLogger().debug(
                        "RolapEvaluator.getExpResultCacheKey: " +
                        "member == null; dimensionOrdinal=" + i);
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

            if (!aggCacheDirty &&
                (aggregateCacheMissCountBefore == aggregateCacheMissCountAfter)) {
                // Cache the evaluation result as valid result if the
                // evaluation did not use any missing aggregates. Missing aggregates
                // could be used when aggregate cache is not fully loaded, or if 
                // new missing aggregates are seen.
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

    public final Object evaluateNamedSet(String name, Exp exp) {
        return root.evaluateNamedSet(name, exp);
    }

    public final int getMissCount() {
        return cellReader.getMissCount();
    }

    public final Object getParameterValue(ParameterSlot slot) {
        return root.getParameterValue(slot);
    }

    final void addCalcMember(Member member) {
        assert member != null;
        assert member.isCalculated();
        calcMembers[calcMemberCount++] = member;
    }

    private Member peekCalcMember() {
        switch (calcMemberCount) {
        case 0:
            return null;

        case 1:
            return calcMembers[0];

        default:
            // Find member with the highest solve order.
            Member maxSolveMember = calcMembers[0];
            int maxSolve = maxSolveMember.getSolveOrder();
            for (int i = 1; i < calcMemberCount; i++) {
                Member member = calcMembers[i];
                int solve = member.getSolveOrder();
                if (solve >= maxSolve) {
                    // If solve orders tie, the dimension with the lower
                    // ordinal wins.
                    if (solve > maxSolve
                        || member.getDimension().getOrdinal(root.cube)
                        < maxSolveMember.getDimension().getOrdinal(root.cube))
                    {
                        maxSolve = solve;
                        maxSolveMember = member;
                    }
                }
            }
            return maxSolveMember;
        }
    }

    private void removeCalcMember(Member previous) {
        for (int i = 0; i < calcMemberCount; i++) {
            final Member calcMember = calcMembers[i];
            if (calcMember == previous) {
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
        return getCube().
            shouldIgnoreUnrelatedDimensions(getMeasureCube().getName());
    }
}

// End RolapEvaluator.java
