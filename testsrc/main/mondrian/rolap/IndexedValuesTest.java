/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2007-2010 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import mondrian.test.FoodMartTestCase;

/**
 * Test case for '&amp;[..]' capability in MDX identifiers.
 *
 * @author pierluiggi@users.sourceforge.net
 * @version $Id$
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

        // Query using key; expect same result.
        assertQueryReturns(
            "SELECT {[Measures].[Org Salary], [Measures].[Count]} "
            + "ON COLUMNS, "
            + "{[Employees].&[1]} "
            + "ON ROWS FROM [HR]",
            desiredResult);

        // Cannot find members that are not at root of hierarchy.
        // (We should fix this.)
        assertQueryThrows(
            "SELECT {[Measures].[Org Salary], [Measures].[Count]} "
            + "ON COLUMNS, "
            + "{[Employees].&[4]} "
            + "ON ROWS FROM [HR]",
            "MDX object '[Employees].&[4]' not found in cube 'HR'");

        // "level.&key" syntax not supported
        // (We should fix this.)
        assertQueryThrows(
            "SELECT [Measures] ON COLUMNS, "
            + "{[Product].[Product Name].&[9]} "
            + "ON ROWS FROM [Sales]",
            "MDX object '[Employees].&[4]' not found in cube 'HR'");
    }
}

// End IndexedValuesTest.java
