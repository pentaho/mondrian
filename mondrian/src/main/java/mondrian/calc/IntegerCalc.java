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
