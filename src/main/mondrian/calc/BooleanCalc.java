/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2006-2006 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.calc;

import mondrian.olap.Evaluator;

/**
 * Compiled expression whose result is a <code>boolean</code>.<p/>
 *
 * When implementing this interface, it is convenient to extend
 * {@link mondrian.calc.impl.AbstractBooleanCalc}, but it is not required.
 *
 * @author jhyde
 * @version $Id$
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
