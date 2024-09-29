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
import mondrian.calc.DimensionCalc;
import mondrian.olap.Evaluator;
import mondrian.olap.Exp;
import mondrian.olap.type.DimensionType;

/**
 * Abstract implementation of the {@link mondrian.calc.DimensionCalc} interface.
 *
 * <p>The derived class must
 * implement the {@link #evaluateDimension(mondrian.olap.Evaluator)} method,
 * and the {@link #evaluate(mondrian.olap.Evaluator)} method will call it.
 *
 * @author jhyde
 * @since Sep 26, 2005
 */
public abstract class AbstractDimensionCalc
    extends AbstractCalc
    implements DimensionCalc
{
    /**
     * Creates an AbstractDimensionCalc.
     *
     * @param exp Source expression
     * @param calcs Child compiled expressions
     */
    protected AbstractDimensionCalc(Exp exp, Calc[] calcs) {
        super(exp, calcs);
        assert getType() instanceof DimensionType;
    }

    public Object evaluate(Evaluator evaluator) {
        return evaluateDimension(evaluator);
    }
}

// End AbstractDimensionCalc.java
