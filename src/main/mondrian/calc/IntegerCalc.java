/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.calc;

import mondrian.olap.Evaluator;

/**
 * Compiled expression whose result is an <code>int</code>.
 *
 * <p>When implementing this interface, it is convenient to extend
 * {@link mondrian.calc.impl.AbstractIntegerCalc}, but it is not required.
 *
 * @author jhyde
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
