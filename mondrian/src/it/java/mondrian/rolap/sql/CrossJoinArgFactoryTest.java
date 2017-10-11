/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (c) 2002-2017 Hitachi Vantara.  All rights reserved.
*/
package mondrian.rolap.sql;

import mondrian.test.FoodMartTestCase;

/**
 * Tests for CrossJoinArgFactory
 *
 * @author Yury Bakhmutski
 */
public class CrossJoinArgFactoryTest  extends FoodMartTestCase {

     /**
     * test for MONDRIAN-2287 issue. Tests if correct result is returned
     * instead of CCE throwing.
     */
    public void testCrossJoinExample() {
        String query =
                "with "
                + " member [Measures].[aa] as '([Measures].[Store Cost],[Gender].[M])'"
                + " member [Measures].[bb] as '([Measures].[Store Cost],[Gender].[M].PrevMember)'"
                + " select"
                + " non empty"
                + " crossjoin("
                + " {[Store].[All Stores].[USA].[CA]},"
                + " {[Measures].[aa], [Measures].[bb]}"
                + " ) on columns,"
                + " non empty"
                + " [Marital Status].[Marital Status].members on rows"
                + " from sales";
        String expected = "Axis #0:\n"
                + "{}\n"
                + "Axis #1:\n"
                + "{[Store].[USA].[CA], [Measures].[aa]}\n"
                + "{[Store].[USA].[CA], [Measures].[bb]}\n"
                + "Axis #2:\n"
                + "{[Marital Status].[M]}\n"
                + "{[Marital Status].[S]}\n"
                + "Row #0: 15,339.94\n"
                + "Row #0: 15,941.98\n"
                + "Row #1: 16,598.87\n"
                + "Row #1: 15,649.64\n";
        assertQueryReturns(query, expected);
    }
}

// End CrossJoinArgFactoryTest.java
