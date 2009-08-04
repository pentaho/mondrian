/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2008-2009 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import mondrian.olap.*;
import mondrian.calc.*;
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
 * @version $Id$
 */
class RolapEvaluatorRoot {
    final Map<Object, Object> expResultCache = new HashMap<Object, Object>();
    final Map<Object, Object> tmpExpResultCache = new HashMap<Object, Object>();
    final RolapCube cube;
    final RolapConnection connection;
    final SchemaReader schemaReader;
    final Map<List<Object>, Calc> compiledExps =
        new HashMap<List<Object>, Calc>();
    final Query query;
    private final Date queryStartTime;
    final Dialect currentDialect;

    /**
     * Default members of each hierarchy, from the schema reader's
     * perspective. Finding the default member is moderately expensive, but
     * happens very often.
     */
    final RolapMember[] defaultMembers;

    final MondrianProperties.SolveOrderModeEnum solveOrderMode =
        Util.lookup(
            MondrianProperties.SolveOrderModeEnum.class,
            MondrianProperties.instance().SolveOrderMode.get().toUpperCase(),
            MondrianProperties.SolveOrderModeEnum.ABSOLUTE);

    /**
     * Creates a RolapEvaluatorRoot.
     *
     * @param query Query
     */
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
        this.currentDialect =
            DialectManager.createDialect(schemaReader.getDataSource(), null);
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
    protected Evaluator.NamedSetEvaluator evaluateNamedSet(
        String name,
        Exp exp)
    {
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

// End RolapEvaluatorRoot.java
