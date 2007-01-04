/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2006 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
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
 * @version $Id$
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
