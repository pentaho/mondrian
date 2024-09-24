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

import mondrian.calc.BooleanCalc;
import mondrian.calc.Calc;
import mondrian.olap.Evaluator;
import mondrian.olap.Exp;

/**
 * Abstract implementation of the {@link mondrian.calc.BooleanCalc} interface.
 *
 * <p>The derived class must
 * implement the {@link #evaluateBoolean(mondrian.olap.Evaluator)} method,
 * and the {@link #evaluate(mondrian.olap.Evaluator)} method will call it.
 *
 * @author jhyde
 * @since Sep 26, 2005
 */
public abstract class AbstractBooleanCalc
    extends AbstractCalc
    implements BooleanCalc
{
    /**
     * Creates an AbstractBooleanCalc.
     *
     * @param exp Source expression
     * @param calcs Child compiled expressions
     */
    public AbstractBooleanCalc(Exp exp, Calc[] calcs) {
        super(exp, calcs);
        // now supports int and double conversion (see
        // AbstractExpCompiler.compileBoolean():
        // assert getType() instanceof BooleanType;
    }

    public Object evaluate(Evaluator evaluator) {
        return Boolean.valueOf(evaluateBoolean(evaluator));
    }
}

// End AbstractBooleanCalc.java
