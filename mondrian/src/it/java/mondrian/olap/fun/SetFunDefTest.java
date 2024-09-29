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
            "with member store.x as "
            + "'{[Gender].[M],[Store].[USA].[CA]}' "
            + " SELECT store.x on 0, [measures].[customer count] on 1 from sales");
    }

    public void testSetWith2TuplesWithDifferentHierarchies() {
        assertQueryFailsInSetValidation(
            "with member store.x as '{([Gender].[M],[Store].[All Stores].[USA].[CA]),"
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
