/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2006-2007 Pentaho
// All Rights Reserved.
*/
package mondrian.calc;

import mondrian.olap.Evaluator;

/**
 * Compiled expression whose result is a {@link String}.
 *
 * <p>When implementing this interface, it is convenient to extend
 * {@link mondrian.calc.impl.AbstractStringCalc}, but it is not required.
 *
 * @author jhyde
 * @since Sep 26, 2005
 */
public interface StringCalc extends Calc {
    /**
     * Evaluates this expression to yield a {@link String} value.
     *
     * @param evaluator Evaluation context
     * @return evaluation result
     */
    String evaluateString(Evaluator evaluator);
}

// End StringCalc.java
