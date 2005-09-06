/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2005-2005 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.fun;

import mondrian.olap.*;

import java.util.List;

/**
 * Definition of the <code>AGGREGATE</code> MDX function.
 *
 * @author jhyde
 * @since 2005/8/14
 * @version $Id$
 */
class AggregateFunDef extends AbstractAggregateFunDef {
    public AggregateFunDef(FunDef dummyFunDef) {
        super(dummyFunDef);
    }

    public Object evaluate(Evaluator evaluator, Exp[] args) {
        List members = (List) getArg(evaluator, args, 0);

        ExpBase exp = (ExpBase) getArgNoEval(args, 1, valueFunCall);
        Aggregator aggregator =
                (Aggregator) evaluator.getProperty(
                        Property.AGGREGATION_TYPE.name, null);
        if (aggregator == null) {
            throw newEvalException(null, "Could not find an aggregator in the current evaluation context");
        }
        Aggregator rollup = aggregator.getRollup();
        if (rollup == null) {
            throw newEvalException(null, "Don't know how to rollup aggregator '" + aggregator + "'");
        }
        return rollup.aggregate(evaluator.push(), members, exp);
    }

    MultiResolver newResolver() {
        return new Resolver();
    }

    public static class Resolver extends MultiResolver {
        public Resolver() {
            super("Aggregate", "Aggregate(<Set>[, <Numeric Expression>])",
                    "Returns a calculated value using the appropriate aggregate function, based on the context of the query.",
                    new String[]{"fnx", "fnxn"});
        }

        protected FunDef createFunDef(final Exp[] args, FunDef dummyFunDef) {
            return new AggregateFunDef(dummyFunDef);
        }
    }
}

// End AggregateFunDef.java
