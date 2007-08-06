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

import mondrian.olap.*;
import mondrian.calc.*;
import mondrian.calc.impl.GenericCalc;
import mondrian.calc.impl.ValueCalc;
import mondrian.calc.impl.AbstractDoubleCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.rolap.RolapAggregator;

import java.util.List;

/**
 * Definition of the <code>AGGREGATE</code> MDX function.
 *
 * @author jhyde
 * @since 2005/8/14
 * @version $Id$
 */
class AggregateFunDef extends AbstractAggregateFunDef {
    static final ReflectiveMultiResolver resolver =
            new ReflectiveMultiResolver(
                    "Aggregate", "Aggregate(<Set>[, <Numeric Expression>])",
                    "Returns a calculated value using the appropriate aggregate function, based on the context of the query.",
                    new String[]{"fnx", "fnxn"},
                    AggregateFunDef.class);

    public AggregateFunDef(FunDef dummyFunDef) {
        super(dummyFunDef);
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final ListCalc listCalc = compiler.compileList(call.getArg(0));
        final Calc calc = call.getArgCount() > 1 ?
                compiler.compileScalar(call.getArg(1), true) :
                new ValueCalc(call);
        return new AbstractDoubleCalc(call, new Calc[]{listCalc,calc}) {
            public double evaluateDouble(Evaluator evaluator) {
                Aggregator aggregator =
                    (Aggregator) evaluator.getProperty(
                        Property.AGGREGATION_TYPE.name, null);
                if (aggregator == null) {
                    throw newEvalException(
                        null,
                        "Could not find an aggregator in the current evaluation context");
                }
                Aggregator rollup = aggregator.getRollup();
                if (rollup == null) {
                    throw newEvalException(
                        null,
                        "Don't know how to rollup aggregator '" + aggregator + "'");
                }
                final List list = evaluateCurrentList(listCalc, evaluator);
                if (aggregator == RolapAggregator.DistinctCount) {
                    // Can't aggregate distinct-count values. To evaluate a
                    // distinct-count across multiple members, we need to gather
                    // the members together, then evaluate the collection of
                    // members all at once. To do this, we postpone evaluation,
                    // and create a lambda function containing the members.
                    for (Object o : list) {
                        // Currently assume the list consists of members.
                        // We could in principle handle lists of tuples.
                        if (!(o instanceof Member)) {
                            throw new UnsupportedOperationException(
                                "aggregating distinct-count over lists of " +
                                    "tuples is not supported");
                        }
                    }
                    Evaluator evaluator2 =
                        evaluator.pushAggregation((List<Member>) list);
                    final Object o = evaluator2.evaluateCurrent();
                    final Number number = (Number) o;
                    return GenericCalc.numberToDouble(number);
                }
                return (Double) rollup.aggregate(evaluator.push(), list, calc);
            }

            public Calc[] getCalcs() {
                return new Calc[] {listCalc, calc};
            }

            public boolean dependsOn(Dimension dimension) {
                if (dimension.isMeasures()) {
                    return true;
                }
                return anyDependsButFirst(getCalcs(), dimension);
            }
        };
    }
}

// End AggregateFunDef.java
