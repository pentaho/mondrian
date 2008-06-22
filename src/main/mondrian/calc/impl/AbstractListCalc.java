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
import mondrian.calc.ListCalc;
import mondrian.calc.Calc;
import mondrian.calc.ResultStyle;

import java.util.List;

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
    private final boolean mutable;

    /**
     * Creates an abstract implementation of a compiled expression which returns
     * a mutable list.
     *
     * @param exp Expression which was compiled
     * @param calcs List of child compiled expressions (for dependency
     *   analysis)
     */
    protected AbstractListCalc(Exp exp, Calc[] calcs) {
        this(exp, calcs, true);
    }

    /**
     * Creates an abstract implementation of a compiled expression which returns
     * a list.
     *
     * @param exp Expression which was compiled
     * @param calcs List of child compiled expressions (for dependency
     *   analysis)
     * @param mutable Whether the list is mutable
     */
    protected AbstractListCalc(Exp exp, Calc[] calcs, boolean mutable) {
        super(exp);
        this.calcs = calcs;
        this.mutable = mutable;
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

    public ResultStyle getResultStyle() {
        return mutable ?
            ResultStyle.MUTABLE_LIST :
            ResultStyle.LIST;
    }
}

// End AbstractListCalc.java
