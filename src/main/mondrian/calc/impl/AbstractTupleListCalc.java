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

import mondrian.olap.*;
import mondrian.olap.type.SetType;
import mondrian.calc.*;

import java.util.List;

/**
 * Abstract implementation of the {@link mondrian.calc.ListCalc} interface
 * for expressions that return a list of tuples but never a list of members.
 *
 * <p>The derived class must
 * implement the {@link #evaluateTupleList(mondrian.olap.Evaluator)} method,
 * and the {@link #evaluate(mondrian.olap.Evaluator)} method will call it.
 *
 * @see AbstractListCalc
 *
 * @author jhyde
 * @version $Id$
 * @since Oct 24, 2008
 */
public abstract class AbstractTupleListCalc
    extends AbstractCalc
    implements TupleListCalc
{
    private final boolean mutable;

    /**
     * Creates an abstract implementation of a compiled expression which
     * returns a mutable list of tuples.
     *
     * @param exp Expression which was compiled
     * @param calcs List of child compiled expressions (for dependency
     *   analysis)
     */
    protected AbstractTupleListCalc(Exp exp, Calc[] calcs) {
        this(exp, calcs, true);
    }

    /**
     * Creates an abstract implementation of a compiled expression which
     * returns a list.
     *
     * @param exp Expression which was compiled
     * @param calcs List of child compiled expressions (for dependency
     *   analysis)
     * @param mutable Whether the list is mutable
     */
    protected AbstractTupleListCalc(Exp exp, Calc[] calcs, boolean mutable) {
        super(exp, calcs);
        this.mutable = mutable;
        assert type instanceof SetType : "expecting a set: " + getType();
        assert getType().getArity() > 1;
    }

    public SetType getType() {
        return (SetType) super.getType();
    }

    public final Object evaluate(Evaluator evaluator) {
        final List<Member[]> tupleList = evaluateTupleList(evaluator);
        assert tupleList != null : "null as empty tuple list is deprecated";
        return tupleList;
    }

    public ResultStyle getResultStyle() {
        return mutable ?
            ResultStyle.MUTABLE_LIST :
            ResultStyle.LIST;
    }

    public String toString() {
        return "AbstractTupleListCalc object";
    }

    // override return type
    public final List<Member[]> evaluateList(Evaluator evaluator) {
        return evaluateTupleList(evaluator);
    }
}

// End AbstractTupleListCalc.java
