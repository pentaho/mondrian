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
