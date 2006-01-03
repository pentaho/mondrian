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

import mondrian.olap.Dimension;
import mondrian.olap.Evaluator;

/**
 * Expression which yields a {@link mondrian.olap.Dimension}.<p/>
 *
 * When implementing this interface, it is convenient to extend
 * {@link mondrian.calc.impl.AbstractDimensionCalc}, but it is not required.
 *
 * @author jhyde
 * @version $Id$
 * @since Sep 26, 2005
 */
public interface DimensionCalc extends Calc {
    /**
     * Evaluates this expression to yield a dimension.<p/>
     *
     * Never returns null.
     *
     * @param evaluator Evaluation context
     * @return a dimension
     */
    Dimension evaluateDimension(Evaluator evaluator);
}

// End DimensionCalc.java
