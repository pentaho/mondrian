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
import mondrian.calc.*;

/**
 * Abstract implementation of the {@link mondrian.calc.VoidCalc} interface.
 *
 * <p>The derived class must
 * implement the {@link #evaluateVoid(mondrian.olap.Evaluator)} method,
 * and the {@link #evaluate(mondrian.olap.Evaluator)} method will call it
 * and return <code>null</code>.
 *
 * @author jhyde
 * @version $Id$
 * @since Sep 29, 2005
 */
public class AbstractVoidCalc extends GenericCalc implements VoidCalc {
    private final Calc[] calcs;

    protected AbstractVoidCalc(Exp exp, Calc[] calcs) {
        super(exp);
        this.calcs = calcs;
    }

    public Object evaluate(Evaluator evaluator) {
        evaluateVoid(evaluator);
        return null;
    }

    public Calc[] getCalcs() {
        return calcs;
    }
}

// End AbstractVoidCalc.java
