/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2001-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 10 August, 2001
*/

package mondrian.rolap;
import mondrian.calc.Calc;
import mondrian.olap.*;
import mondrian.olap.fun.FunUtil;
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

    private final Member[] currentMembers;
    private final Evaluator parent;
    protected CellReader cellReader;
    private final int depth;

    private Member expandingMember;
    private boolean nonEmpty;
    protected final RolapEvaluatorRoot root;

    /**
     * Creates an evaluator.
     */
    protected RolapEvaluator(
            RolapEvaluatorRoot root,
            RolapEvaluator parent,
            CellReader cellReader,
            Member[] currentMembers) {
        this.root = root;
        this.parent = parent;
        if (parent == null) {
            this.depth = 0;
            this.nonEmpty = false;
        } else {
          this.depth = parent.depth + 1;
          this.nonEmpty = parent.nonEmpty;
        }

        this.cellReader = cellReader;
        if (currentMembers == null) {
            this.currentMembers = new Member[root.cube.getDimensions().length];
        } else {
            this.currentMembers = currentMembers;
        }
        calcMembers = new Member[this.currentMembers.length];
        calcMemberCount = 0;
        for (int i = 0; i < this.currentMembers.length; i++) {
            Member member = this.currentMembers[i];
            if (member != null && member.isCalculated()) {
                addCalcMember(member);
            }
        }
    }

    /**
     * Creates an evaluator with no parent.
     *
     * @param root Shared context between this evaluator and its children
     */
    public RolapEvaluator(RolapEvaluatorRoot root) {
        this(root, null, null, null);

        // we expect client to set CellReader

        SchemaReader scr = this.root.connection.getSchemaReader();
        Dimension[] dimensions = this.root.cube.getDimensions();
        for (int i = 0; i < dimensions.length; i++) {
            final Dimension dimension = dimensions[i];
            final int ordinal = dimension.getOrdinal(this.root.cube);
            final Hierarchy hier = dimension.getHierarchy();

            Member member = scr.getHierarchyDefaultMember(hier);

            // If there is no member, we cannot continue.
            if (member == null) {
                throw MondrianResource.instance().InvalidHierarchyCondition.ex(hier.getUniqueName());
            }

            HierarchyUsage[] hierarchyUsages = this.root.cube.getUsages(hier);
            if (hierarchyUsages.length != 0) {
                ((RolapMember) member).makeUniqueName(hierarchyUsages[0]);
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

    protected static class RolapEvaluatorRoot {
        final Map expResultCache = new HashMap();
        final RolapCube cube;
        final RolapConnection connection;
        final SchemaReader schemaReader;
        final Map compiledExps = new HashMap();
        final private Query query;


        public RolapEvaluatorRoot(Query query) {
            this.query = query;
            this.cube = (RolapCube) query.getCube();
            this.connection = (RolapConnection) query.getConnection();
            this.schemaReader = query.getSchemaReader(true);
        }

        /**
         * Implements a cheap-and-cheerful mapping from expressions to compiled
         * expressions.
         *
         * <p>TODO: Save compiled expressions somewhere better.
         */
        Calc getCompiled(Exp exp, boolean scalar) {
            Calc calc = (Calc) compiledExps.get(exp);
            if (calc == null) {
                calc = query.compileExpression(exp, scalar);
                compiledExps.put(exp, calc);
            }
            return calc;
        }

        /**
         * Evaluates a named set.<p/>
         *
         * The default implementation throws
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
    }

    protected Logger getLogger() {
        return LOGGER;
    }

    Member[] getCurrentMembers() {
        return this.currentMembers;
    }

    void setCellReader(CellReader cellReader) {
        this.cellReader = cellReader;
    }

    public Cube getCube() {
        return root.cube;
    }

    public Query getQuery() {
        return root.query;
    }

    public int getDepth() {
        return depth;
    }

    public Evaluator getParent() {
        return parent;
    }

    public SchemaReader getSchemaReader() {
        return root.schemaReader;
    }

    public Evaluator push(Member[] members) {
        final RolapEvaluator evaluator = _push();
        evaluator.setContext(members);
        return evaluator;
    }

    public Evaluator push(Member member) {
        final RolapEvaluator evaluator = _push();
        evaluator.setContext(member);
        return evaluator;
    }

    public Evaluator push() {
        return _push();
    }

    /**
     * Creates a clone of the current validator.
     */
    protected RolapEvaluator _push() {
        Member[] cloneCurrentMembers = (Member[]) this.currentMembers.clone();
        return new RolapEvaluator(
                root,
                this,
                cellReader,
                cloneCurrentMembers);
    }

    public Evaluator pop() {
        return parent;
    }

    /**
     * Returns true if the other object is a {@link RolapEvaluator} with
     * identical context.
     */
    public boolean equals(Object obj) {
        if (!(obj instanceof RolapEvaluator)) {
            return false;
        }
        RolapEvaluator that = (RolapEvaluator) obj;
        return Arrays.equals(this.currentMembers, that.currentMembers);
    }

    public Member setContext(Member member) {
        RolapMember m = (RolapMember) member;
        int ordinal = m.getDimension().getOrdinal(root.cube);
        Member previous = currentMembers[ordinal];
        if (previous.isCalculated()) {
            removeCalcMember(previous);
        }
        currentMembers[ordinal] = m;
        if (m.isCalculated()) {
            addCalcMember(m);
        }
        return previous;
    }

    public void setContext(Member[] members) {
        for (int i = 0; i < members.length; i++) {
            Member member = members[i];

            // more than one usage
            assert member != null;
            if (member == null) {
                if (getLogger().isDebugEnabled()) {
                    getLogger().debug(
                        "RolapEvaluator.setContext: member == null "
                         + " , count=" + i);
                }
                continue;
            }

            setContext(member);
        }
    }

    public Member getContext(Dimension dimension) {
        return currentMembers[dimension.getOrdinal(root.cube)];
    }

    public Object evaluateCurrent() {
        // Get the member in the current context which is (a) calculated, and
        // (b) has the highest solve order; returns null if there are no
        // calculated members.
        Member maxSolveMember = peekCalcMember();
        if (maxSolveMember == null) {
            Object o = cellReader.get(this);
            if (o == Util.nullValue) {
                o = null;
            }
            return o;
        }
        RolapMember defaultMember = (RolapMember)
                maxSolveMember.getHierarchy().getDefaultMember();
        Util.assertTrue(
                defaultMember != maxSolveMember,
                "default member must not be calculated");
        RolapEvaluator evaluator = (RolapEvaluator) push(defaultMember);
        evaluator.setExpanding(maxSolveMember);
        final Exp exp = maxSolveMember.getExpression();
        Calc calc = root.getCompiled(exp, true);
        return calc.evaluate(evaluator);
    }

    private void setExpanding(Member member) {
        expandingMember = member;
        int memberCount = currentMembers.length;
        if (depth > memberCount) {
            if (depth % memberCount == 0) {
                checkRecursion((RolapEvaluator) parent);
            }
        }
    }

    /**
     * Makes sure that there is no evaluator with identical context on the
     * stack.
     *
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
                Member member = eval2.currentMembers[i];

                // more than one usage
                if (member == null) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(
                            "RolapEvaluator.checkRecursion: member == null "
                             + " , count=" + i);
                    }
                    continue;
                }

                Member parentMember = eval.getContext(member.getDimension());
                if (member != parentMember) {
                    continue outer;
                }
            }
            throw FunUtil.newEvalException(null,
                "Infinite loop while evaluating calculated member '" +
                eval.expandingMember + "'; context stack is " +
                eval.getContextString());
        }
    }

    private String getContextString() {
        boolean skipDefaultMembers = true;
        StringBuffer sb = new StringBuffer("{");
        int frameCount = 0;
        for (RolapEvaluator eval = this; eval != null;
                 eval = (RolapEvaluator) eval.getParent()) {
            if (eval.expandingMember == null) {
                continue;
            }
            if (frameCount++ > 0) {
                sb.append(", ");
            }
            sb.append("(");
            int memberCount = 0;
            for (int j = 0; j < eval.currentMembers.length; j++) {
                Member m = eval.currentMembers[j];
                if (skipDefaultMembers &&
                        m == m.getHierarchy().getDefaultMember()) {
                    continue;
                }
                if (memberCount++ > 0) {
                    sb.append(", ");
                }
                sb.append(m.getUniqueName());
            }
            sb.append(")");
        }
        sb.append("}");
        return sb.toString();
    }

    public Object getProperty(String name, Object defaultValue) {
        Object o = defaultValue;
        int maxSolve = Integer.MIN_VALUE;
        for (int i = 0; i < currentMembers.length; i++) {
            Member member = currentMembers[i];

            // more than one usage
            if (member == null) {
                if (getLogger().isDebugEnabled()) {
                    getLogger().debug(
                        "RolapEvaluator.getProperty: member == null "
                         + " , count=" + i);
                }
                continue;
            }

            Object p = member.getPropertyValue(name);
            if (p != null) {
                int solve = member.getSolveOrder();
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
    String getFormatString() {
        Exp formatExp = (Exp) getProperty(Property.FORMAT_EXP.name, null);
        if (formatExp == null) {
            return "Standard";
        }
        Calc formatCalc = root.getCompiled(formatExp, true);
        Object o = formatCalc.evaluate(this);
        return o.toString();
    }

    private Format getFormat() {
        String formatString = getFormatString();
        return Format.get(formatString, root.connection.getLocale());
    }

    /**
     * Converts a value of this member into a string according to this member's
     * format specification.
     **/
    String format(Evaluator evaluator, Object o) {
        return getFormat().format(o);
    }

    public String format(Object o) {
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

    /**
     * Creates a key which uniquely identifes an expression and its
     * context. The context includes members of dimensions which the
     * expression is dependent upon.
     */
    private Object getExpResultCacheKey(ExpCacheDescriptor descriptor) {
        List key = new ArrayList();
        key.add(descriptor.getExp());
        int[] dimensionOrdinals = descriptor.getDependentDimensionOrdinals();
        for (int i = 0; i < dimensionOrdinals.length; i++) {
            int dimensionOrdinal = dimensionOrdinals[i];
            Member member = currentMembers[dimensionOrdinal];

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

    public Object getCachedResult(ExpCacheDescriptor cacheDescriptor) {
        // Look up a cached result, and if not present, compute one and add to
        // cache. Use a dummy value to represent nulls.
        Object key = getExpResultCacheKey(cacheDescriptor);
        Object result = root.expResultCache.get(key);
        if (result == null) {
            result = cacheDescriptor.evaluate(this);
            root.expResultCache.put(key, result == null ? nullResult : result);
        } else if (result == nullResult) {
            result = null;
        }
        return result;
    }

    public void clearExpResultCache() {
        root.expResultCache.clear();
    }

    public boolean isNonEmpty() {
        return nonEmpty;
    }

    public void setNonEmpty(boolean nonEmpty) {
        this.nonEmpty = nonEmpty;
    }

    public RuntimeException newEvalException(Object context, String s) {
        return FunUtil.newEvalException((FunDef) context, s);
    }

    public Object evaluateNamedSet(String name, Exp exp) {
        return root.evaluateNamedSet(name, exp);
    }

    public Member[] getMembers() {
        return currentMembers;
    }

    public int getMissCount() {
        return cellReader.getMissCount();
    }

    private final Member[] calcMembers;
    private int calcMemberCount;

    void addCalcMember(Member member) {
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
                    if (solve > maxSolve ||
                            member.getDimension().getOrdinal(root.cube) <
                            maxSolveMember.getDimension().getOrdinal(
                                    root.cube)) {
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
            Member calcMember = calcMembers[i];
            if (calcMember == previous) {
                // overwrite this member with the end member
                --calcMemberCount;
                calcMembers[i] = calcMembers[calcMemberCount];
                calcMembers[calcMemberCount] = null; // to allow gc
            }
        }
    }
}

// End RolapEvaluator.java
