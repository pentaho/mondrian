/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 - 2026 by Pentaho Canada Inc. : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2030-06-15
 ******************************************************************************/



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
