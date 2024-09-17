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

package mondrian.calc.impl;

import mondrian.calc.Calc;
import mondrian.calc.IntegerCalc;
import mondrian.olap.Evaluator;
import mondrian.olap.Exp;
import mondrian.olap.fun.FunUtil;
import mondrian.olap.type.NumericType;

/**
 * Abstract implementation of the {@link mondrian.calc.IntegerCalc} interface.
 *
 * <p>The derived class must
 * implement the {@link #evaluateInteger(mondrian.olap.Evaluator)} method,
 * and the {@link #evaluate(mondrian.olap.Evaluator)} method will call it.
 *
 * @author jhyde
 * @since Sep 26, 2005
 */
public abstract class AbstractIntegerCalc
    extends AbstractCalc
    implements IntegerCalc
{
    /**
     * Creates an AbstractIntegerCalc.
     *
     * @param exp Source expression
     * @param calcs Child compiled expressions
     */
    protected AbstractIntegerCalc(Exp exp, Calc[] calcs) {
        super(exp, calcs);
        assert getType() instanceof NumericType;
    }

    public Object evaluate(Evaluator evaluator) {
        int i = evaluateInteger(evaluator);
        if (i == FunUtil.IntegerNull) {
            return null;
        } else {
            return i;
        }
    }
}

// End AbstractIntegerCalc.java
