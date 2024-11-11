/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


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
