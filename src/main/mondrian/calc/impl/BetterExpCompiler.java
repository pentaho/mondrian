/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2007 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.calc.impl;

import mondrian.olap.*;
import mondrian.olap.type.*;
import mondrian.calc.*;

import java.util.List;
import java.util.ArrayList;

/**
 * Enhanced expression compiler. It can generate code to convert between
 * scalar types.
 *
 * @author jhyde
 * @version $Id$
 * @since Sep 29, 2005
 */
public class BetterExpCompiler extends AbstractExpCompiler {
    public BetterExpCompiler(Evaluator evaluator, Validator validator) {
        super(evaluator, validator);
    }

    public BetterExpCompiler(
        Evaluator evaluator,
        Validator validator,
        List<ResultStyle> resultStyles)
    {
        super(evaluator, validator, resultStyles);
    }

    public DoubleCalc compileDouble(Exp exp) {
        final Calc calc = compileScalar(exp, false);
        if (calc instanceof DoubleCalc) {
            return (DoubleCalc) calc;
        } else if (calc instanceof IntegerCalc) {
            final IntegerCalc integerCalc = (IntegerCalc) calc;
            return new AbstractDoubleCalc(exp, new Calc[] {integerCalc}) {
                public double evaluateDouble(Evaluator evaluator) {
                    final int result = integerCalc.evaluateInteger(evaluator);
                    return (double) result;
                }
            };
        } else {
            throw Util.newInternal("cannot cast " + exp);
        }
    }

    public TupleCalc compileTuple(Exp exp) {
        final Calc calc = compile(exp);
        final Type type = exp.getType();
        if (type instanceof TupleType) {
            assert calc instanceof TupleCalc;
            return (TupleCalc) calc;
        } else if (type instanceof MemberType) {
            assert calc instanceof MemberCalc;
            final MemberCalc memberCalc = (MemberCalc) calc;
            return new AbstractTupleCalc(exp, new Calc[] {memberCalc}) {
                public Member[] evaluateTuple(Evaluator evaluator) {
                    return new Member[] {memberCalc.evaluateMember(evaluator)};
                }
            };
        } else {
            throw Util.newInternal("cannot cast " + exp);
        }
    }

    public ListCalc compileList(Exp exp, boolean mutable) {
        final ListCalc listCalc = super.compileList(exp, mutable);
        if (mutable && listCalc.getResultStyle() == ResultStyle.LIST) {
            // Wrap the expression in an expression which creates a mutable
            // copy.
            return new CopyListCalc(listCalc);
        }
        return listCalc;
    }

    private static class CopyListCalc extends AbstractListCalc {
        private final ListCalc listCalc;

        public CopyListCalc(ListCalc listCalc) {
            super(new DummyExp(listCalc.getType()), new Calc[]{listCalc});
            this.listCalc = listCalc;
        }

        public List evaluateList(Evaluator evaluator) {
            List list = listCalc.evaluateList(evaluator);
            return new ArrayList(list);
        }
    }
}

// End BetterExpCompiler.java
