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

package mondrian.rolap;

import mondrian.olap.MondrianProperties;
import mondrian.test.FoodMartTestCase;
import mondrian.util.Bug;

/**
 * Test case for '&amp;[..]' capability in MDX identifiers.
 *
 * <p>This feature used
 * <a href="http://jira.pentaho.com/browse/MONDRIAN-485">bug MONDRIAN-485,
 * "Member key treated as member name in WHERE"</a>
 * as a placeholder.
 *
 * @author pierluiggi@users.sourceforge.net
 */
public class IndexedValuesTest extends FoodMartTestCase {
    public IndexedValuesTest(final String name) {
        super(name);
    }

    public IndexedValuesTest() {
        super();
    }

    public void testQueryWithIndex() {
        final String desiredResult =
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Org Salary]}\n"
            + "{[Measures].[Count]}\n"
            + "Axis #2:\n"
            + "{[Employees].[Sheri Nowmer]}\n"
            + "Row #0: $39,431.67\n"
            + "Row #0: 7,392\n";

        // Query using name
        assertQueryReturns(
            "SELECT {[Measures].[Org Salary], [Measures].[Count]} "
            + "ON COLUMNS, "
            + "{[Employees].[Sheri Nowmer]} "
            + "ON ROWS FROM [HR]",
            desiredResult);

        // Member keys only work with SsasCompatibleNaming=true
        if (!MondrianProperties.instance().SsasCompatibleNaming.get()) {
            return;
        }

        // Query using key; expect same result.
        assertQueryReturns(
            "SELECT {[Measures].[Org Salary], [Measures].[Count]} "
            + "ON COLUMNS, "
            + "{[Employees].&[1]} "
            + "ON ROWS FROM [HR]",
            desiredResult);

        // Cannot find members that are not at root of hierarchy.
        // (We should fix this.)
        assertQueryReturns(
            "SELECT {[Measures].[Org Salary], [Measures].[Count]} "
            + "ON COLUMNS, "
            + "{[Employees].&[4]} "
            + "ON ROWS FROM [HR]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Org Salary]}\n"
            + "{[Measures].[Count]}\n"
            + "Axis #2:\n"
            + "{[Employees].[Sheri Nowmer].[Michael Spence]}\n"
            + "Row #0: \n"
            + "Row #0: \n");

        // "level.&key" syntax
        assertQueryReturns(
            "SELECT [Measures] ON COLUMNS, "
            + "{[Product].[Product Name].&[9]} "
            + "ON ROWS FROM [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[Drink].[Beverages].[Pure Juice Beverages].[Juice].[Washington].[Washington Cranberry Juice]}\n"
            + "Row #0: 130\n");
    }
}

// End IndexedValuesTest.java
