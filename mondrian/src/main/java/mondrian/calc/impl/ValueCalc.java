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
import mondrian.olap.*;

/**
 * Expression which yields the value of the current member in the current
 * dimensional context.
 *
 * @see mondrian.calc.impl.MemberValueCalc
 *
 * @author jhyde
 * @since Sep 27, 2005
 */
public class ValueCalc extends GenericCalc {
    /**
     * Creates a ValueCalc.
     *
     * @param exp Source expression
     */
    public ValueCalc(Exp exp) {
        super(exp, new Calc[0]);
    }

    public Object evaluate(Evaluator evaluator) {
        return evaluator.evaluateCurrent();
    }

    public boolean dependsOn(Hierarchy hierarchy) {
        return true;
    }
}

// End ValueCalc.java
