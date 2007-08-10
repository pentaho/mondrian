/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2007 Julian Hyde
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
import java.util.Collections;

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
            switch (r) {
            case ITERABLE:
            case ANY:
                // Consumer wants ITERABLE or ANY to be used
                //return compileCallIterable(call, compiler);
                return compileCall(call, compiler, ResultStyle.ITERABLE);
            case MUTABLE_LIST:
                // Consumer wants MUTABLE_LIST
                return compileCall(call, compiler, ResultStyle.MUTABLE_LIST);
            case LIST:
                // Consumer wants LIST to be used
                //return compileCallList(call, compiler);
                return compileCall(call, compiler, ResultStyle.LIST);
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
        final Calc ncalc = compiler.compileAs(
            call.getArg(0),
            null,
            Collections.singletonList(resultStyle));
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
    protected Calc genIterCalc(final ResolvedFunCall call,
            final Calc ncalc, final Calc calc) {
        return new AbstractDoubleCalc(call, new Calc[] {ncalc, calc}) {
            public double evaluateDouble(Evaluator evaluator) {
                IterCalc iterCalc = (IterCalc) ncalc;
                Iterable iterable =
                    evaluateCurrentIterable(iterCalc, evaluator);
                return sumDouble(evaluator.push(), iterable, calc);
            }

            public Calc[] getCalcs() {
                return new Calc[] {ncalc, calc};
            }

            public boolean dependsOn(Dimension dimension) {
                return anyDependsButFirst(getCalcs(), dimension);
            }
        };
    }

    protected Calc genListCalc(final ResolvedFunCall call,
            final Calc ncalc, final Calc calc) {
        return new AbstractDoubleCalc(call, new Calc[] {ncalc, calc}) {
            public double evaluateDouble(Evaluator evaluator) {
                ListCalc listCalc = (ListCalc) ncalc;
                List memberList = evaluateCurrentList(listCalc, evaluator);
                return sumDouble(evaluator.push(), memberList, calc);
            }

            public Calc[] getCalcs() {
                return new Calc[] {ncalc, calc};
            }

            public boolean dependsOn(Dimension dimension) {
                return anyDependsButFirst(getCalcs(), dimension);
            }
        };
    }
}

// End SumFunDef.java
