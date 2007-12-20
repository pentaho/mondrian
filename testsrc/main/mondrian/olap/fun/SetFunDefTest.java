/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2001-2007 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// ajogleka, 19 December, 2007
*/
package mondrian.olap.fun;

import mondrian.test.FoodMartTestCase;

/**
 * <code>SetFunDefTest</code> tests the <code>SetFunDef</code>
 *
 * @author ajogleka
 * @version $Id$
 * @since 19 December, 2007
 */
public class SetFunDefTest extends FoodMartTestCase {

    public void testSetWithMembersFromDifferentHierarchies() {
        assertQueryFailsInSetValidation(
            "with member store.x as " +
            "'{[Gender].[M],[Store].[All Stores].[USA].[CA]}' " +
            " SELECT store.x on 0, [measures].[customer count] on 1 from sales");
    }

    public void testSetWith2TuplesWithDifferentHierarchies() {
        assertQueryFailsInSetValidation(
            "with member store.x as '{([Gender].[M],[Store].[All Stores].[USA].[CA])," +
            "([Store].[All Stores].[USA].[OR],[Gender].[F])}'\n" +
            " SELECT store.x on 0, [measures].[customer count] on 1 from sales");
    }

    private void assertQueryFailsInSetValidation(String query) {
        try {
            assertQueryReturns(query, "");
            fail();
        } catch (Throwable e) {
            while (e.getCause() != null) {
                e = e.getCause();
            }
            assertTrue(e.getMessage().
                contains("Mondrian Error:All arguments to function '{}' " +
                "must have same hierarchy"));
        }
    }
}
