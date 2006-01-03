/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2006-2006 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.calc.impl;

import mondrian.olap.*;
import mondrian.olap.fun.FunUtil;
import mondrian.olap.type.NumericType;
import mondrian.calc.impl.AbstractCalc;
import mondrian.calc.DoubleCalc;
import mondrian.calc.Calc;

/**
 * Abstract implementation of the {@link mondrian.calc.DoubleCalc} interface.
 *
 * <p>The derived class must
 * implement the {@link #evaluateDouble(mondrian.olap.Evaluator)} method,
 * and the {@link #evaluate(mondrian.olap.Evaluator)} method will call it.
 *
 * @author jhyde
 * @version $Id$
 * @since Sep 27, 2005
 */
public abstract class AbstractDoubleCalc
        extends AbstractCalc
        implements DoubleCalc {
    private final Calc[] calcs;

    protected AbstractDoubleCalc(Exp exp, Calc[] calcs) {
        super(exp);
        this.calcs = calcs;
        assert getType() instanceof NumericType;
    }

    public Object evaluate(Evaluator evaluator) {
        final double d = evaluateDouble(evaluator);
        if (d == FunUtil.DoubleNull) {
            return null;
        }
        return new Double(d);
    }

    public Calc[] getCalcs() {
        return calcs;
    }
}

// End AbstractDoubleCalc.java
