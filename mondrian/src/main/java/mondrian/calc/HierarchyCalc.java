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
import mondrian.olap.Hierarchy;

/**
 * Expression which yields a {@link mondrian.olap.Hierarchy}.
 *
 * <p>When implementing this interface, it is convenient to extend
 * {@link mondrian.calc.impl.AbstractHierarchyCalc}, but it is not required.
 *
 * @author jhyde
 * @since Sep 26, 2005
 */
public interface HierarchyCalc extends Calc {
    /**
     * Evaluates this expression to yield a hierarchy.
     *
     * <p>Never returns null.
     *
     * @param evaluator Evaluation context
     * @return a hierarchy
     */
    Hierarchy evaluateHierarchy(Evaluator evaluator);
}

// End HierarchyCalc.java
