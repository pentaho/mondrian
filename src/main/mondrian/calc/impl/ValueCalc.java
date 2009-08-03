/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2006 Julian Hyde
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
    public ValueCalc(Exp exp) {
        super(exp);
    }

    public Object evaluate(Evaluator evaluator) {
        return evaluator.evaluateCurrent();
    }

    public Calc[] getCalcs() {
        return new Calc[0];
    }

    public boolean dependsOn(Dimension dimension) {
        return true;
    }
}

// End ValueCalc.java
