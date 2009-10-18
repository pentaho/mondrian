/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2008-2009 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.calc.impl;

import mondrian.olap.*;
import mondrian.calc.DummyExp;
import mondrian.calc.Calc;
import mondrian.calc.TupleIterCalc;

import java.util.List;
import java.util.ArrayList;

/**
 * Adapter that converts a {@link mondrian.calc.IterCalc} to a
 * {@link mondrian.calc.ListCalc}.
 *
 * @author jhyde
 * @version $Id$
 * @since Oct 23, 2008
 */
public class IterableTupleListCalc extends AbstractTupleListCalc {
    private final TupleIterCalc iterCalc;

    /**
     * Creates an IterableListCalc.
     *
     * @param iterCalc Calculation that returns an iterable.
     */
    public IterableTupleListCalc(TupleIterCalc iterCalc) {
        super(new DummyExp(iterCalc.getType()), new Calc[] {iterCalc});
        this.iterCalc = iterCalc;
    }

    public List<Member[]> evaluateTupleList(Evaluator evaluator) {
        // A TupleIterCalc is allowed to return a list. If so, save the copy.
        final Iterable<Member[]> iterable =
            iterCalc.evaluateTupleIterable(evaluator);
        if (iterable instanceof List) {
            return Util.cast((List) iterable);
        }

        final List<Member[]> list = new ArrayList<Member[]>();
        for (Member[] o : iterable) {
            list.add(o);
        }
        return list;
    }
}

// End IterableTupleListCalc.java
