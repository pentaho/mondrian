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

import mondrian.olap.Evaluator;
import mondrian.olap.Exp;
import mondrian.olap.type.DimensionType;
import mondrian.calc.impl.AbstractCalc;
import mondrian.calc.DimensionCalc;
import mondrian.calc.Calc;

/**
 * Abstract implementation of the {@link mondrian.calc.DimensionCalc} interface.
 *
 * <p>The derived class must
 * implement the {@link #evaluateDimension(mondrian.olap.Evaluator)} method,
 * and the {@link #evaluate(mondrian.olap.Evaluator)} method will call it.
 *
 * @author jhyde
 * @version $Id$
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
