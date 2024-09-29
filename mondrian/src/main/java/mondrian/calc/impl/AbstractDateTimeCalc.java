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
import mondrian.calc.DateTimeCalc;
import mondrian.olap.Evaluator;
import mondrian.olap.Exp;

/**
 * Abstract implementation of the {@link mondrian.calc.DateTimeCalc} interface.
 *
 * <p>The derived class must
 * implement the {@link #evaluateDateTime(mondrian.olap.Evaluator)} method,
 * and the {@link #evaluate(mondrian.olap.Evaluator)} method will call it.
 *
 * @author jhyde
 * @since Sep 26, 2005
 */
public abstract class AbstractDateTimeCalc
    extends AbstractCalc
    implements DateTimeCalc
{
    /**
     * Creates an AbstractDateTimeCalc.
     *
     * @param exp Source expression
     * @param calcs Child compiled expressions
     */
    protected AbstractDateTimeCalc(Exp exp, Calc[] calcs) {
        super(exp, calcs);
    }

    public Object evaluate(Evaluator evaluator) {
        return evaluateDateTime(evaluator);
    }
}

// End AbstractDateTimeCalc.java
