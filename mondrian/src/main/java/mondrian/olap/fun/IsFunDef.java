/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/


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
    static final ReflectiveMultiResolver Resolver =
        new ReflectiveMultiResolver(
            "IS",
            "<Expression> IS <Expression>",
            "Returns whether two objects are the same",
            new String[] {"ibmm", "ibll", "ibhh", "ibdd", "ibtt"},
            IsFunDef.class);

    public IsFunDef(FunDef dummyFunDef) {
        super(dummyFunDef);
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final int category = call.getArg(0).getCategory();
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
            assert category == call.getArg(1).getCategory();
            final Calc calc0 = compiler.compile(call.getArg(0));
            final Calc calc1 = compiler.compile(call.getArg(1));
            return new AbstractBooleanCalc(call, new Calc[] {calc0, calc1}) {
                public boolean evaluateBoolean(Evaluator evaluator) {
                    Object o0 = calc0.evaluate(evaluator);
                    Object o1 = calc1.evaluate(evaluator);
                    return o0.equals(o1);
                }
            };
        }
    }
}

// End IsFunDef.java
