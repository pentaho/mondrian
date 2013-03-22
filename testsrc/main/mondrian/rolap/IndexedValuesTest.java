/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2007-2013 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.olap.MondrianProperties;
import mondrian.test.FoodMartTestCase;

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
            + "{[Employee].[Employees].[Sheri Nowmer]}\n"
            + "Row #0: $39,431.67\n"
            + "Row #0: 7,392\n";

        // Query using name
        assertQueryReturns(
            "SELECT {[Measures].[Org Salary], [Measures].[Count]} "
            + "ON COLUMNS, "
            + "{[Employees].[Sheri Nowmer]} "
            + "ON ROWS FROM [HR]",
            desiredResult);

        // Query using key; expect same result.
        assertQueryReturns(
            "SELECT {[Measures].[Org Salary], [Measures].[Count]} "
            + "ON COLUMNS, "
            + "{[Employee].[Employees].&[1]} "
            + "ON ROWS FROM [HR]",
            desiredResult);

        // Cannot find members that are not at root of hierarchy.
        // (We should fix this.)
        assertQueryReturns(
            "SELECT {[Measures].[Org Salary], [Measures].[Count]} "
            + "ON COLUMNS, "
            + "{[Employee].[Employees].&[4]} "
            + "ON ROWS FROM [HR]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Org Salary]}\n"
            + "{[Measures].[Count]}\n"
            + "Axis #2:\n"
            + "{[Employee].[Employees].[Sheri Nowmer].[Michael Spence]}\n"
            + "Row #0: \n"
            + "Row #0: \n");

        // "level.&key" syntax
        assertQueryReturns(
            "SELECT [Measures] ON COLUMNS, "
            + "{[Product].[Products].[Product Name].&[9]} "
            + "ON ROWS FROM [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[Products].[Drink].[Beverages].[Pure Juice Beverages].[Juice].[Washington].[Washington Cranberry Juice]}\n"
            + "Row #0: 130\n");
    }

    public void testAttemptInjectionWithNonNumericKeyValue() {
        // If SQL injection attempt is not caught, will return internal error
        // "More than one member in level [Product].[Products].[Product Name]
        // with key [1 or true or 2]". Mondrian must see that the value is
        // invalid and generate 'WHERE FALSE'.
        assertQueryThrows(
            "SELECT [Measures]on 0,\n"
            + "{[Product].[Products].&[1 or true or 2]} on 1\n"
            + "FROM [Sales]",
            "MDX object '[Product].[Products].&[1 or true or 2]' not found in "
            + "cube 'Sales'");
    }
}

// End IndexedValuesTest.java
