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
import mondrian.olap.Level;

/**
 * Expression which yields a {@link mondrian.olap.Level}.
 *
 * <p>When implementing this interface, it is convenient to extend
 * {@link mondrian.calc.impl.AbstractLevelCalc}, but it is not required.
 *
 * @author jhyde
 * @since Sep 26, 2005
 */
public interface LevelCalc extends Calc {
    /**
     * Evaluates this expression to yield a level.
     *
     * <p>Never returns null.
     *
     * @param evaluator Evaluation context
     * @return a level
     */
    Level evaluateLevel(Evaluator evaluator);
}

// End LevelCalc.java
