/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2008-2008 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.calc.impl;

import mondrian.olap.Evaluator;
import mondrian.olap.Exp;
import mondrian.calc.*;

/**
 * Abstract implementation of the {@link mondrian.calc.DateTimeCalc} interface.
 *
 * <p>The derived class must
 * implement the {@link #evaluateDateTime(mondrian.olap.Evaluator)} method,
 * and the {@link #evaluate(mondrian.olap.Evaluator)} method will call it.
 *
 * @author jhyde
 * @version $Id$
 * @since Sep 26, 2005
 */
public abstract class AbstractDateTimeCalc
    extends AbstractCalc
    implements DateTimeCalc
{
    private final Calc[] calcs;

    protected AbstractDateTimeCalc(Exp exp, Calc[] calcs) {
        super(exp);
        this.calcs = calcs;
    }

    public Object evaluate(Evaluator evaluator) {
        return evaluateDateTime(evaluator);
    }

    public Calc[] getCalcs() {
        return calcs;
    }
}

// End AbstractDateTimeCalc.java
