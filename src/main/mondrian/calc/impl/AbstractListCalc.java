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
import mondrian.olap.type.SetType;
import mondrian.calc.impl.AbstractCalc;
import mondrian.calc.ListCalc;
import mondrian.calc.Calc;

import java.util.List;
import java.util.Collections;

/**
 * Abstract implementation of the {@link mondrian.calc.ListCalc} interface.
 *
 * <p>The derived class must
 * implement the {@link #evaluateList(mondrian.olap.Evaluator)} method,
 * and the {@link #evaluate(mondrian.olap.Evaluator)} method will call it.
 *
 * @author jhyde
 * @version $Id$
 * @since Sep 27, 2005
 */
public abstract class AbstractListCalc
        extends AbstractCalc
        implements ListCalc {
    private final Calc[] calcs;

    protected AbstractListCalc(Exp exp, Calc[] calcs) {
        super(exp);
        this.calcs = calcs;
        assert getType() instanceof SetType : "expecting a set: " + getType();
    }

    public Object evaluate(Evaluator evaluator) {
        final List list = evaluateList(evaluator);
        assert list != null : "null as empty list is deprecated";
        return list;
    }

    public Calc[] getCalcs() {
        return calcs;
    }
}

// End AbstractListCalc.java
