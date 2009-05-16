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
import mondrian.calc.TupleCalc;
import mondrian.calc.Calc;

/**
 * Abstract implementation of the {@link mondrian.calc.TupleCalc} interface.
 *
 * <p>The derived class must
 * implement the {@link #evaluateTuple(mondrian.olap.Evaluator)} method,
 * and the {@link #evaluate(mondrian.olap.Evaluator)} method will call it.
 *
 * @author jhyde
 * @version $Id$
 * @since Sep 27, 2005
 */
public abstract class AbstractTupleCalc
    extends AbstractCalc
    implements TupleCalc
{
    /**
     * Creates an AbstractTupleCalc.
     *
     * @param exp Source expression
     * @param calcs Child compiled expressions
     */
    protected AbstractTupleCalc(Exp exp, Calc[] calcs) {
        super(exp, calcs);
    }

    public Object evaluate(Evaluator evaluator) {
        return evaluateTuple(evaluator);
    }
}

// End AbstractTupleCalc.java
