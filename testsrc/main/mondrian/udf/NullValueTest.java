/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2003-2007 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Feb 14, 2003
*/
package mondrian.udf;

import mondrian.test.FoodMartTestCase;
import mondrian.olap.*;
import mondrian.udf.*;

import junit.framework.Assert;

/**
 * <code>NullValueTest</code> is a test case which tests simple queries
 * expressions.
 *
 * @author <a>Richard M. Emberson</a>
 * @since Mar 01 2007
 * @version $Id$
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
