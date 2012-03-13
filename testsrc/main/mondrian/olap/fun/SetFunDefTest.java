/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2001-2005 Julian Hyde
// Copyright (C) 2005-2011 Pentaho and others
// All Rights Reserved.
*/
package mondrian.olap.fun;

import mondrian.test.FoodMartTestCase;

/**
 * Unit test for the set constructor function <code>{ ... }</code>,
 * {@link SetFunDef}.
 *
 * @author ajogleka
 * @since 19 December, 2007
 */
public class SetFunDefTest extends FoodMartTestCase {

    public void testSetWithMembersFromDifferentHierarchies() {
        assertQueryFailsInSetValidation(
            "with member store.[Stores].x as "
            + "'{[Gender].[M],[Store].[USA].[CA]}' "
            + " SELECT store.x on 0, [measures].[customer count] on 1 from sales");
    }

    public void testSetWith2TuplesWithDifferentHierarchies() {
        assertQueryFailsInSetValidation(
            "with member store.[Stores].x as '{([Gender].[M],[Store].[All Stores].[USA].[CA]),"
            + "([Store].[USA].[OR],[Gender].[F])}'\n"
            + " SELECT store.x on 0, [measures].[customer count] on 1 from sales");
    }

    private void assertQueryFailsInSetValidation(String query) {
        assertQueryThrows(
            query,
            "Mondrian Error:All arguments to function '{}' "
            + "must have same hierarchy");
    }
}

// End SetFunDefTest.java
