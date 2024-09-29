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

import mondrian.olap.Dimension;
import mondrian.olap.Evaluator;

/**
 * Expression which yields a {@link mondrian.olap.Dimension}.
 *
 * <p>When implementing this interface, it is convenient to extend
 * {@link mondrian.calc.impl.AbstractDimensionCalc}, but it is not required.
 *
 * @author jhyde
 * @since Sep 26, 2005
 */
public interface DimensionCalc extends Calc {
    /**
     * Evaluates this expression to yield a dimension.
     *
     * <p>Never returns null.
     *
     * @param evaluator Evaluation context
     * @return a dimension
     */
    Dimension evaluateDimension(Evaluator evaluator);
}

// End DimensionCalc.java
