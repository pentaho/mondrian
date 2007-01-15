/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2006 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.fun;

import mondrian.olap.*;
import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;
import mondrian.calc.ExpCompiler.ResultStyle;
import mondrian.calc.ListCalc;
import mondrian.calc.IterCalc;
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
        ResultStyle[] rs = compiler.getAcceptableResultStyles();
/*
// RME
for (int i = 0; i < rs.length; i++) {
System.out.println("SumFunDef.compileCall: "+rs[i]);
}
*/
        // What is the desired type to use to get the underlying values
        for (int i = 0; i < rs.length; i++) {
            switch (rs[i]) {
            case ITERABLE :
            case ANY :
                // Consumer wants ITERABLE or ANY to be used
                return compileCallIterable(call, compiler);
            case MUTABLE_LIST:
            case LIST :
                // Consumer wants MUTABLE_LIST or LIST to be used
                return compileCallList(call, compiler);
            }
        }
        throw ResultStyleException.generate(
            new ResultStyle[] {
                ResultStyle.ITERABLE,
                ResultStyle.LIST,
                ResultStyle.MUTABLE_LIST,
                ResultStyle.ANY
            },
            rs
        );
    }

    protected Calc compileCallIterable(final ResolvedFunCall call, 
            ExpCompiler compiler) {
        final Calc ncalc = compiler.compile(call.getArg(0));
        final Calc calc = call.getArgCount() > 1 ?
                compiler.compileScalar(call.getArg(1), true) :
                new ValueCalc(call);
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
    protected Calc compileCallList(final ResolvedFunCall call, 
            ExpCompiler compiler) {
        final Calc ncalc = compiler.compile(call.getArg(0));
        final Calc calc = call.getArgCount() > 1 ?
                compiler.compileScalar(call.getArg(1), true) :
                new ValueCalc(call);
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
