/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2006-2009 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.fun;

import mondrian.olap.*;
import mondrian.calc.*;
import mondrian.calc.impl.ValueCalc;
import mondrian.calc.impl.AbstractDoubleCalc;
import mondrian.mdx.ResolvedFunCall;

import java.util.List;

/**
 * Definition of the <code>Sum</code> MDX function.
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 23, 2006
 */
class SumFunDef extends AbstractAggregateFunDef {
    static final ReflectiveMultiResolver Resolver = new ReflectiveMultiResolver(
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
        final Calc calc = call.getArgCount() > 1 ?
            compiler.compileScalar(call.getArg(1), true) :
            new ValueCalc(call);
        // we may have asked for one sort of Calc, but here's what we got.
        if (ncalc instanceof ListCalc) {
            return genListCalc(call, ncalc, calc);
        } else {
            return genIterCalc(call, ncalc, calc);
        }
    }

    protected Calc genIterCalc(
        final ResolvedFunCall call, final Calc ncalc, final Calc calc)
    {
        return new AbstractDoubleCalc(call, new Calc[] {ncalc, calc}) {
            public double evaluateDouble(Evaluator evaluator) {
                IterCalc iterCalc = (IterCalc) ncalc;
                Iterable iterable =
                    evaluateCurrentIterable(iterCalc, evaluator);
                return sumDouble(evaluator.push(), iterable, calc);
            }

            public boolean dependsOn(Dimension dimension) {
                return anyDependsButFirst(getCalcs(), dimension);
            }
        };
    }

    protected Calc genListCalc(
        final ResolvedFunCall call, final Calc ncalc, final Calc calc)
    {
        return new AbstractDoubleCalc(call, new Calc[] {ncalc, calc}) {
            public double evaluateDouble(Evaluator evaluator) {
                ListCalc listCalc = (ListCalc) ncalc;
                List memberList = evaluateCurrentList(listCalc, evaluator);
                return sumDouble(evaluator.push(false), memberList, calc);
            }

            public boolean dependsOn(Dimension dimension) {
                return anyDependsButFirst(getCalcs(), dimension);
            }
        };
    }
}

// End SumFunDef.java
