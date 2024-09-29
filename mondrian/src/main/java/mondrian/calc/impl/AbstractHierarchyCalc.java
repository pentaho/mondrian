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


package mondrian.calc.impl;

import mondrian.calc.Calc;
import mondrian.calc.HierarchyCalc;
import mondrian.olap.Evaluator;
import mondrian.olap.Exp;
import mondrian.olap.type.HierarchyType;

/**
 * Abstract implementation of the {@link mondrian.calc.HierarchyCalc} interface.
 *
 * <p>The derived class must
 * implement the {@link #evaluateHierarchy(mondrian.olap.Evaluator)} method,
 * and the {@link #evaluate(mondrian.olap.Evaluator)} method will call it.
 *
 * @author jhyde
 * @since Sep 26, 2005
 */
public abstract class AbstractHierarchyCalc
    extends AbstractCalc
    implements HierarchyCalc
{
    /**
     * Creates an AbstractHierarchyCalc.
     *
     * @param exp Source expression
     * @param calcs Child compiled expressions
     */
    protected AbstractHierarchyCalc(Exp exp, Calc[] calcs) {
        super(exp, calcs);
        assert getType() instanceof HierarchyType;
    }

    public Object evaluate(Evaluator evaluator) {
        return evaluateHierarchy(evaluator);
    }
}

// End AbstractHierarchyCalc.java
