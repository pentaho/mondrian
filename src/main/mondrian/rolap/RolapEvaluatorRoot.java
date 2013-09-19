/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.rolap;

import mondrian.calc.*;
import mondrian.olap.*;
import mondrian.server.Execution;
import mondrian.server.Statement;
import mondrian.spi.Dialect;
import mondrian.spi.DialectManager;

import java.util.*;

/**
 * Context at the root of a tree of evaluators.
 *
 * <p>Contains the context that does not change as evaluation context is
 * pushed/popped.
 *
 * @author jhyde
 * @since Nov 11, 2008
 */
class RolapEvaluatorRoot {
    final Map<Object, Object> expResultCache = new HashMap<Object, Object>();
    final Map<Object, Object> tmpExpResultCache =
        new HashMap<Object, Object>();
    final RolapCube cube;
    final RolapConnection connection;
    final SchemaReader schemaReader;
    final Map<CompiledExpKey, Calc> compiledExps =
        new HashMap<CompiledExpKey, Calc>();
    final Statement statement;
    final Query query;
    private final Date queryStartTime;
    final Dialect currentDialect;

    /**
     * Default members of each hierarchy, from the schema reader's
     * perspective. Finding the default member is moderately expensive, but
     * happens very often.
     */
    final RolapMember[] defaultMembers;
    final int[] nonAllPositions;
    int nonAllPositionCount;

    final SolveOrderMode solveOrderMode =
        Util.lookup(
            SolveOrderMode.class,
            MondrianProperties.instance().SolveOrderMode.get().toUpperCase(),
            SolveOrderMode.ABSOLUTE);

    final Set<Exp> activeNativeExpansions = new HashSet<Exp>();

    /**
     * The size of the command stack at which we will next check for recursion.
     */
    int recursionCheckCommandCount;
    public final Execution execution;

    /**
     * Creates a RolapEvaluatorRoot.
     *
     * @param statement statement
     * @deprecated
     */
    public RolapEvaluatorRoot(Statement statement) {
        this(statement, null);
    }

    public RolapEvaluatorRoot(Execution execution) {
        this(execution.getMondrianStatement(), execution);
    }

    private RolapEvaluatorRoot(Statement statement, Execution execution) {
        this.execution = execution;
        this.statement = statement;
        this.query = statement.getQuery();
        this.cube = (RolapCube) query.getCube();
        this.connection = statement.getMondrianConnection();
        this.schemaReader = query.getSchemaReader(true);
        this.queryStartTime = new Date();
        List<RolapMember> list = new ArrayList<RolapMember>();
        nonAllPositions = new int[cube.getHierarchies().size()];
        nonAllPositionCount = 0;
        for (RolapHierarchy hierarchy : cube.getHierarchies()) {
            RolapMember defaultMember =
                (RolapMember) schemaReader.getHierarchyDefaultMember(hierarchy);
            assert defaultMember != null;

            if (ScenarioImpl.isScenario(hierarchy)
                && connection.getScenario() != null)
            {
                defaultMember =
                    ((ScenarioImpl) connection.getScenario()).getMember();
            }

            // This fragment is a concurrency bottleneck, so use a cache of
            // hierarchy usages.
            final HierarchyUsage hierarchyUsage = cube.getFirstUsage(hierarchy);
            if (hierarchyUsage != null) {
                if (defaultMember instanceof RolapMemberBase) {
                ((RolapMemberBase) defaultMember).makeUniqueName(
                    hierarchyUsage);
                }
            }

            list.add(defaultMember);
            if (!defaultMember.isAll()) {
                nonAllPositions[nonAllPositionCount] =
                    hierarchy.getOrdinalInCube();
                nonAllPositionCount++;
            }
        }
        this.defaultMembers = list.toArray(new RolapMember[list.size()]);
        this.currentDialect =
            DialectManager.createDialect(schemaReader.getDataSource(), null);

        this.recursionCheckCommandCount = (defaultMembers.length << 4);
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
        CompiledExpKey key = new CompiledExpKey(exp, scalar, resultStyle);
        Calc calc = compiledExps.get(key);
        if (calc == null) {
            calc =
                statement.getQuery().compileExpression(
                    exp, scalar, resultStyle);
            compiledExps.put(key, calc);
        }
        return calc;
    }

    /**
     * Just a simple key of Exp/scalar/resultStyle, used for keeping
     * compiled expressions.  Previous to the introduction of this
     * class, the key was a list constructed as Arrays.asList(exp, scalar,
     * resultStyle) and having poorer performance on equals, hashCode,
     * and construction.
     */
    private static class CompiledExpKey {
        private final Exp exp;
        private final boolean scalar;
        private final ResultStyle resultStyle;
        private int hashCode = Integer.MIN_VALUE;

        private CompiledExpKey(
            Exp exp,
            boolean scalar,
            ResultStyle resultStyle)
        {
            this.exp = exp;
            this.scalar = scalar;
            this.resultStyle = resultStyle;
        }

        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof CompiledExpKey)) {
                return false;
            }
            CompiledExpKey otherKey = (CompiledExpKey)other;
            return this.scalar == otherKey.scalar
                   && this.resultStyle == otherKey.resultStyle
                   && this.exp.equals(otherKey.exp);
        }

        public int hashCode() {
            if (hashCode != Integer.MIN_VALUE) {
                return hashCode;
            } else {
                int hash = 0;
                hash = Util.hash(hash, scalar);
                hash = Util.hash(hash, resultStyle);
                this.hashCode = Util.hash(hash, exp);
            }
            return this.hashCode;
        }
    }

    /**
     * Evaluates a named set.
     *
     * <p>The default implementation throws
     * {@link UnsupportedOperationException}.
     *
     * @param namedSet Named set
     * @param create Whether to create named set evaluator if not found
     */
    protected Evaluator.NamedSetEvaluator evaluateNamedSet(
        NamedSet namedSet,
        boolean create)
    {
        throw new UnsupportedOperationException();
    }


    /**
     * Evaluates a named set represented by an expression.
     *
     * <p>The default implementation throws
     * {@link UnsupportedOperationException}.
     *
     * @param exp Expression
     * @param create Whether to create named set evaluator if not found
     */
    protected Evaluator.SetEvaluator evaluateSet(
        Exp exp,
        boolean create)
    {
        throw new UnsupportedOperationException();
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

// End RolapEvaluatorRoot.java
