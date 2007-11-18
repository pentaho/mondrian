/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2005-2007 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.fun;

import mondrian.calc.*;
import mondrian.olap.*;
import mondrian.resource.MondrianResource;
import mondrian.mdx.UnresolvedFunCall;

import java.util.*;

/**
 * Abstract base class for all aggregate functions (<code>Aggregate</code>,
 * <code>Sum</code>, <code>Avg</code>, et cetera).
 *
 * @author jhyde
 * @since 2005/8/14
 * @version $Id$
 */
public class AbstractAggregateFunDef extends FunDefBase {
    public AbstractAggregateFunDef(FunDef dummyFunDef) {
        super(dummyFunDef);
    }

    protected Exp validateArg(
            Validator validator, Exp[] args, int i, int category) {
        // If expression cache is enabled, wrap first expression (the set)
        // in a function which will use the expression cache.
        if (i == 0) {
            if (MondrianProperties.instance().EnableExpCache.get()) {
                Exp arg = args[0];
                final Exp cacheCall = new UnresolvedFunCall(
                        CacheFunDef.NAME,
                        Syntax.Function,
                        new Exp[] {arg});
                return validator.validate(cacheCall, false);
            }
        }
        return super.validateArg(validator, args, i, category);
    }

    /**
     * Evaluates the list of members used in computing the aggregate.
     * Keeps track of the number of iterations that will be required to
     * iterate over the members needed to compute the aggregate within the
     * current context.  In doing so, also determines if the cross product
     * of all iterations across all parent evaluation contexts will exceed the
     * limit set in the properties file.
     *
     * @param listCalc calculator used to evaluate the member list
     * @param evaluator current evalutor
     *
     * @return list of evaluated members
     */
    protected static List evaluateCurrentList(
        ListCalc listCalc,
        Evaluator evaluator)
    {
        List memberList = listCalc.evaluateList(evaluator);

        int currLen = memberList.size();
        crossProd(evaluator, currLen);

        return memberList;
    }

    protected Iterable evaluateCurrentIterable(
        IterCalc iterCalc,
        Evaluator evaluator)
    {
        Iterable iter = iterCalc.evaluateIterable(evaluator);

        int currLen = 0;
        crossProd(evaluator, currLen);

        return iter;
    }

    private static void crossProd(Evaluator evaluator, int currLen) {
        long iterationLimit =
            MondrianProperties.instance().IterationLimit.get();
        if (iterationLimit > 0) {
            int productLen = currLen;
            Evaluator parent = evaluator.getParent();
            while (parent != null) {
                productLen *= parent.getIterationLength();
                parent = parent.getParent();
            }
            if (productLen > iterationLimit) {
                throw MondrianResource.instance().
                    IterationLimitExceeded.ex(iterationLimit);
            }
        }
        evaluator.setIterationLength(currLen);
    }
}

// End AbstractAggregateFunDef.java
