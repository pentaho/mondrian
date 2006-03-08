/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2006 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.calc;

import mondrian.olap.Evaluator;

/**
 * Compiled expression whose result is an <code>int</code>.<p/>
 *
 * When implementing this interface, it is convenient to extend
 * {@link mondrian.calc.impl.AbstractIntegerCalc}, but it is not required.
 *
 * @author jhyde
 * @version $Id$
 * @since Sep 27, 2005
 */
public interface IntegerCalc extends Calc {
    /**
     * Evaluates this expression to yield an <code>int</code> value.
     * If the result is null, returns the special
     * {@link mondrian.olap.fun.FunUtil#IntegerNull} value.
     *
     * @param evaluator Evaluation context
     * @return evaluation result
     */
    int evaluateInteger(Evaluator evaluator);
}

// End IntegerCalc.java
