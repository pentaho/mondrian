/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2006-2009 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.calc.impl;

import mondrian.olap.*;
import mondrian.calc.Calc;

/**
 * Expression which yields the value of the current member in the current
 * dimensional context.
 *
 * @see mondrian.calc.impl.MemberValueCalc
 *
 * @author jhyde
 * @version $Id$
 * @since Sep 27, 2005
 */
public class ValueCalc extends GenericCalc {
    /**
     * Creates an ValueCalc.
     *
     * @param exp Source expression
     */
    public ValueCalc(Exp exp) {
        super(exp, new Calc[0]);
    }

    public Object evaluate(Evaluator evaluator) {
        return evaluator.evaluateCurrent();
    }

    public boolean dependsOn(Dimension dimension) {
        return true;
    }
}

// End ValueCalc.java
