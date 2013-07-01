/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2002-2005 Julian Hyde
// Copyright (C) 2005-2009 Pentaho and others
// All Rights Reserved.
*/
package mondrian.calc.impl;

import mondrian.olap.fun.FunUtil;
import mondrian.olap.type.NullType;

import junit.framework.TestCase;

/**
 * Test for <code>ConstantCalc</code>
 * @author Matt
 */
public class ConstantCalcTest extends TestCase {
    public void testNullEvaluatesToConstantDoubleNull() {
        ConstantCalc constantCalc = new ConstantCalc(new NullType(), null);
        assertEquals(FunUtil.DoubleNull, constantCalc.evaluateDouble(null));
    }

    public void testNullEvaluatesToConstantIntegerNull() {
        ConstantCalc constantCalc = new ConstantCalc(new NullType(), null);
        assertEquals(FunUtil.IntegerNull, constantCalc.evaluateInteger(null));
    }
}

// End ConstantCalcTest.java
