/*
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2002-2007 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
*/

package mondrian.calc.impl;

import junit.framework.TestCase;
import mondrian.olap.type.NullType;
import mondrian.olap.fun.FunUtil;

/**
 * Test for <code>ConstantCalc</code>
 * @author Matt
 * @version $Id$
 */
public class ConstantCalcTest extends TestCase {
    public void testNullEvaluatesToConstantDoubleNull() {
        ConstantCalc constantCalc = new ConstantCalc(new NullType(), null);
        assertEquals(FunUtil.DoubleNull,constantCalc.evaluateDouble(null));
    }

    public void testNullEvaluatesToConstantIntegerNull() {
        ConstantCalc constantCalc = new ConstantCalc(new NullType(), null);
        assertEquals(FunUtil.IntegerNull,constantCalc.evaluateInteger(null));
    }
}

// End ConstantCalcTest.java