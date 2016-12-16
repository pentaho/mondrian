/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.olap.fun;

import mondrian.calc.*;
import mondrian.calc.impl.AbstractDoubleCalc;
import mondrian.calc.impl.ValueCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;

/**
 * Definition of the <code>Sum</code> MDX function.
 *
 * @author jhyde
 * @since Mar 23, 2006
 */
class SumFunDef extends AbstractAggregateFunDef {
    static final ReflectiveMultiResolver Resolver =
        new ReflectiveMultiResolver(
            "Sum",
            "Sum(<Set>[, <Numeric Expression>])",
            "Returns the sum of a numeric expression evaluated over a set.",
            new String[]{"fnx", "fnxn"},
            SumFunDef.class);

    public SumFunDef(FunDef dummyFunDef) {
        super(dummyFunDef);
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        // What is the desired type to use to get the underlying values
        for (ResultStyle r : compiler.getAcceptableResultStyles()) {
            Calc calc;
            switch (r) {
            case ITERABLE:
            case ANY:
                // Consumer wants ITERABLE or ANY to be used
                //return compileCallIterable(call, compiler);
                calc = compileCall(call, compiler, ResultStyle.ITERABLE);
                if (calc != null) {
                    return calc;
                }
                break;
            case MUTABLE_LIST:
                // Consumer wants MUTABLE_LIST
                calc = compileCall(call, compiler, ResultStyle.MUTABLE_LIST);
                if (calc != null) {
                    return calc;
                }
                break;
            case LIST:
                // Consumer wants LIST to be used
                //return compileCallList(call, compiler);
                calc = compileCall(call, compiler, ResultStyle.LIST);
                if (calc != null) {
                    return calc;
                }
                break;
            }
        }
        throw ResultStyleException.generate(
            ResultStyle.ITERABLE_LIST_MUTABLELIST_ANY,
            compiler.getAcceptableResultStyles());
    }

    protected Calc compileCall(
        final ResolvedFunCall call,
        ExpCompiler compiler,
        ResultStyle resultStyle)
    {
        final Calc ncalc = compiler.compileIter(call.getArg(0));
        if (ncalc == null) {
            return null;
        }
        final Calc calc = call.getArgCount() > 1
            ? compiler.compileScalar(call.getArg(1), true)
            : new ValueCalc(call);
        // we may have asked for one sort of Calc, but here's what we got.
        if (ncalc instanceof ListCalc) {
            return genListCalc(call, (ListCalc) ncalc, calc);
        } else {
            return genIterCalc(call, (IterCalc) ncalc, calc);
        }
    }

    protected Calc genIterCalc(
        final ResolvedFunCall call,
        final IterCalc iterCalc,
        final Calc calc)
    {
        return new AbstractDoubleCalc(call, new Calc[] {iterCalc, calc}) {
            public double evaluateDouble(Evaluator evaluator) {
                TupleIterable iterable =
                    evaluateCurrentIterable(iterCalc, evaluator);
                final int savepoint = evaluator.savepoint();
                try {
                    return sumDouble(evaluator, iterable, calc);
                } finally {
                    evaluator.restore(savepoint);
                }
            }

            public boolean dependsOn(Hierarchy hierarchy) {
                return anyDependsButFirst(getCalcs(), hierarchy);
            }
        };
    }

    protected Calc genListCalc(
        final ResolvedFunCall call,
        final ListCalc listCalc,
        final Calc calc)
    {
        return new AbstractDoubleCalc(call, new Calc[] {listCalc, calc}) {
            public double evaluateDouble(Evaluator evaluator) {
                TupleList memberList = evaluateCurrentList(listCalc, evaluator);
                final int savepoint = evaluator.savepoint();
                try {
                    evaluator.setNonEmpty(false);
                    return
                        sumDouble(
                            evaluator, memberList, calc);
                } finally {
                    evaluator.restore(savepoint);
                }
            }

            public boolean dependsOn(Hierarchy hierarchy) {
                return anyDependsButFirst(getCalcs(), hierarchy);
            }
        };
    }
}

// End SumFunDef.java
