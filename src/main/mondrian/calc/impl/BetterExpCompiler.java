/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2006 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.calc.impl;

import mondrian.olap.*;
import mondrian.calc.*;

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
        }
        throw Util.newInternal("cannot cast " + exp);
    }
}

// End BetterExpCompiler.java
