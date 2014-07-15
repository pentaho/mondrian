/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2006-2006 Pentaho
// All Rights Reserved.
*/
package mondrian.calc;

import mondrian.olap.Evaluator;

/**
 * Compiled expression whose result is a <code>double</code>.
 *
 * <p>When implementing this interface, it is convenient to extend
 * {@link mondrian.calc.impl.AbstractDoubleCalc}, but it is not required.
 *
 * @author jhyde
 * @since Sep 27, 2005
 */
public interface DoubleCalc extends Calc {
    /**
     * Evaluates this expression to yield a <code>double</code> value.
     * If the result is null, returns the special
     * {@link mondrian.olap.fun.FunUtil#DoubleNull} value.
     *
     * @param evaluator Evaluation context
     * @return evaluation result
     */
    double evaluateDouble(Evaluator evaluator);
}

// End DoubleCalc.java
