/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2007 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.calc.impl;

import mondrian.olap.Evaluator;
import mondrian.olap.Exp;
import mondrian.olap.type.SetType;
import mondrian.calc.IterCalc;
import mondrian.calc.Calc;
import mondrian.calc.ResultStyle;

/**
 * Abstract implementation of the {@link mondrian.calc.IterCalc} interface.
 *
 * <p>The derived class must
 * implement the {@link #evaluateIterable(mondrian.olap.Evaluator)} method,
 * and the {@link #evaluate(mondrian.olap.Evaluator)} method will call it.
 *
 * @author <a>Richard M. Emberson</a>
 * @version $Id$
 * @since Jan 14, 2007
 */

public abstract class AbstractIterCalc
        extends AbstractCalc
        implements IterCalc {
    private final Calc[] calcs;

    /**
     * Creates an abstract implementation of a compiled expression which returns
     * an Iterable.
     *
     * @param exp Expression which was compiled
     * @param calcs List of child compiled expressions (for dependency
     *   analysis)
     */
    protected AbstractIterCalc(Exp exp, Calc[] calcs) {
        super(exp);
        this.calcs = calcs;
        assert getType() instanceof SetType : "expecting a set: " + getType();
    }

    public Object evaluate(Evaluator evaluator) {
        final Iterable iter = evaluateIterable(evaluator);
        return iter;
    }

    public Calc[] getCalcs() {
        return calcs;
    }

    public ResultStyle getResultStyle() {
        return ResultStyle.ITERABLE;
    }
}

// End AbstractIterCalc.java
