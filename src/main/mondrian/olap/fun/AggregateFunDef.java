/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2005-2005 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.fun;

import mondrian.olap.*;
import mondrian.calc.*;
import mondrian.calc.impl.AbstractCalc;
import mondrian.calc.impl.ValueCalc;
import mondrian.mdx.ResolvedFunCall;

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

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final ListCalc listCalc = compiler.compileList(call.getArg(0));
        final Calc calc = call.getArgCount() > 1 ?
                compiler.compileScalar(call.getArg(1), true) :
                new ValueCalc(call);
        return new AbstractCalc(call) {
            public Object evaluate(Evaluator evaluator) {
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
                final List list = listCalc.evaluateList(evaluator);
                return rollup.aggregate(evaluator.push(), list, calc);
            }

            public Calc[] getCalcs() {
                return new Calc[] {listCalc, calc};
            }

            public boolean dependsOn(Dimension dimension) {
                return anyDependsButFirst(getCalcs(), dimension);
            }
        };
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
