/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2006-2009 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.calc.impl;

import mondrian.olap.*;
import mondrian.olap.fun.FunUtil;
import mondrian.olap.type.NumericType;
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
    implements DoubleCalc
{
    /**
     * Creates an AbstractDoubleCalc.
     *
     * @param exp Source expression
     * @param calcs Child compiled expressions
     */
    protected AbstractDoubleCalc(Exp exp, Calc[] calcs) {
        super(exp, calcs);
        assert getType() instanceof NumericType;
    }

    public Object evaluate(Evaluator evaluator) {
        final double d = evaluateDouble(evaluator);
        if (d == FunUtil.DoubleNull) {
            return null;
        }
        return new Double(d);
    }
}

// End AbstractDoubleCalc.java
