/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2003-2005 Julian Hyde
// Copyright (C) 2005-2007 Pentaho
// All Rights Reserved.
//
// jhyde, Feb 14, 2003
*/
package mondrian.udf;

import mondrian.test.FoodMartTestCase;

/**
 * <code>NullValueTest</code> is a test case which tests simple queries
 * expressions.
 *
 * @author <a>Richard M. Emberson</a>
 * @since Mar 01 2007
 */
public class NullValueTest extends FoodMartTestCase {

    public NullValueTest() {
        super();
    }
    public NullValueTest(String name) {
        super(name);
    }

    public void testNullValue() {
        String s = executeExpr(" NullValue()/NullValue() ");
        assertEquals("", s);

        s = executeExpr(" NullValue()/NullValue() = NULL ");
        assertEquals("false", s);

        boolean hasException = false;
        try {
            s = executeExpr(" NullValue() IS NULL ");
        } catch (Exception ex) {
            hasException = true;
        }
        assertTrue(hasException);

        // I believe that these IsEmpty results are correct.
        // The NullValue function does not represent a cell.
        s = executeExpr(" IsEmpty(NullValue()) ");
        assertEquals("false", s);

        // NullValue()/NullValue() evaluates to DoubleNull
        // but DoubleNull evaluates to null, so this seems
        // to be broken??
        // s = executeExpr(" IsEmpty(NullValue()/NullValue()) ");
        // assertEquals("false", s);

        s = executeExpr(" 4 + NullValue() ");
        assertEquals("4", s);

        s = executeExpr(" NullValue() - 4 ");
        assertEquals("-4", s);

        s = executeExpr(" 4*NullValue() ");
        assertEquals("", s);

        s = executeExpr(" NullValue()*4 ");
        assertEquals("", s);

        s = executeExpr(" 4/NullValue() ");
        assertEquals("Infinity", s);

        s = executeExpr(" NullValue()/4 ");
        assertEquals("", s);
/*
*/
    }
}

// End NullValueTest.java
