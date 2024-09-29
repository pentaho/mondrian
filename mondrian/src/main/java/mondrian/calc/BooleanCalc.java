/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/


package mondrian.calc;

import mondrian.olap.Evaluator;

/**
 * Compiled expression whose result is a <code>boolean</code>.
 *
 * <p>When implementing this interface, it is convenient to extend
 * {@link mondrian.calc.impl.AbstractBooleanCalc}, but it is not required.
 *
 * @author jhyde
 * @since Sep 27, 2005
 */
public interface BooleanCalc extends Calc {
    /**
     * Evaluates this expression to yield a <code>boolean</code> value.
     * If the result is null, returns the special
     * {@link mondrian.olap.fun.FunUtil#BooleanNull} value.
     *
     * @param evaluator Evaluation context
     * @return evaluation result
     */
    boolean evaluateBoolean(Evaluator evaluator);
}

// End BooleanCalc.java
