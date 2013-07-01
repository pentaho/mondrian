/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2006-2012 Pentaho
// All Rights Reserved.
*/
package mondrian.olap.fun;

import mondrian.calc.*;
import mondrian.calc.impl.AbstractBooleanCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;

/**
 * Definition of the <code>IS</code> MDX function.
 *
 * @see IsNullFunDef
 * @author jhyde
 * @since Mar 23, 2006
 */
class IsFunDef extends FunDefBase {
    private static final String[] SIGNATURES = {
        "ibmm", "ibll", "ibhh", "ibdd", "ibtt"
    };

    public static void define(FunTable.Builder builder) {
        for (String signature : SIGNATURES) {
            builder.define(
                new ReflectiveMultiResolver(
                    "IS",
                    "<Expression> IS <Expression>",
                    "Returns whether two objects are the same",
                    new String[]{signature},
                    IsFunDef.class));
        }
    }

    public IsFunDef(FunDef dummyFunDef) {
        super(dummyFunDef);
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final int category = parameterCategories[0];
        switch (category) {
        case Category.Tuple:
            final TupleCalc tupleCalc0 = compiler.compileTuple(call.getArg(0));
            final TupleCalc tupleCalc1 = compiler.compileTuple(call.getArg(1));
            return new AbstractBooleanCalc(
                call, new Calc[] {tupleCalc0, tupleCalc1})
            {
                public boolean evaluateBoolean(Evaluator evaluator) {
                    Member[] o0 = tupleCalc0.evaluateTuple(evaluator);
                    Member[] o1 = tupleCalc1.evaluateTuple(evaluator);
                    return equalTuple(o0, o1);
                }
            };
        default:
            final Calc calc0 =
                compileCategory(compiler, category, call.getArg(0));
            final Calc calc1 =
                compileCategory(compiler, category, call.getArg(1));
            return new AbstractBooleanCalc(call, new Calc[] {calc0, calc1}) {
                public boolean evaluateBoolean(Evaluator evaluator) {
                    Object o0 = calc0.evaluate(evaluator);
                    Object o1 = calc1.evaluate(evaluator);
                    return o0.equals(o1);
                }
            };
        }
    }

    private static Calc compileCategory(
        ExpCompiler compiler, int category, Exp exp)
    {
        switch (category) {
        case Category.Member:
            return compiler.compileMember(exp);
        case Category.Level:
            return compiler.compileLevel(exp);
        case Category.Hierarchy:
            return compiler.compileHierarchy(exp);
        case Category.Dimension:
            return compiler.compileDimension(exp);
        default:
            throw new AssertionError(category);
        }
    }
}

// End IsFunDef.java
