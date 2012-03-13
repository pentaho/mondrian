/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2007-2010 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.test.FoodMartTestCase;
import mondrian.util.Bug;

/**
 * Test case for '&amp;[..]' capability in MDX identifiers.
 *
 * @see Bug#BugMondrian485Fixed
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

        // Query using key; expect same result.
        assertQueryReturns(
            "SELECT {[Measures].[Org Salary], [Measures].[Count]} "
            + "ON COLUMNS, "
            + "{[Employees].&[1]} "
            + "ON ROWS FROM [HR]",
            desiredResult);

        if (!Bug.BugMondrian485Fixed) {
            return;
        }

        // Cannot find members that are not at root of hierarchy.
        // (We should fix this.)
        assertQueryReturns(
            "SELECT {[Measures].[Org Salary], [Measures].[Count]} "
            + "ON COLUMNS, "
            + "{[Employees].&[4]} "
            + "ON ROWS FROM [HR]",
            "something");

        // "level.&key" syntax
        assertQueryReturns(
            "SELECT [Measures] ON COLUMNS, "
            + "{[Product].[Product Name].&[9]} "
            + "ON ROWS FROM [Sales]",
            "something");
    }
}

// End IndexedValuesTest.java
