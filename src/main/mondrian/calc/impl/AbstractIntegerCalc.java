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

import mondrian.olap.Evaluator;
import mondrian.olap.Exp;
import mondrian.olap.type.NumericType;
import mondrian.calc.impl.AbstractCalc;
import mondrian.calc.IntegerCalc;
import mondrian.calc.Calc;

/**
 * Abstract implementation of the {@link mondrian.calc.IntegerCalc} interface.
 *
 * <p>The derived class must
 * implement the {@link #evaluateInteger(mondrian.olap.Evaluator)} method,
 * and the {@link #evaluate(mondrian.olap.Evaluator)} method will call it.
 *
 * @author jhyde
 * @version $Id$
 * @since Sep 26, 2005
 */
public abstract class AbstractIntegerCalc
        extends AbstractCalc
        implements IntegerCalc {
    private final Calc[] calcs;

    protected AbstractIntegerCalc(Exp exp, Calc[] calcs) {
        super(exp);
        this.calcs = calcs;
        assert getType() instanceof NumericType;
    }

    public Object evaluate(Evaluator evaluator) {
        return new Integer(evaluateInteger(evaluator));
    }

    public Calc[] getCalcs() {
        return calcs;
    }
}

// End AbstractIntegerCalc.java
