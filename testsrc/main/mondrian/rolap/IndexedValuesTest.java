/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2007-2009 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import mondrian.test.FoodMartTestCase;
import mondrian.olap.*;

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
        final Connection conn = getConnection();

        // Query using name
        final String queryStr1 =
                "SELECT {[Measures].[Org Salary], [Measures].[Count]} "
                    + "ON COLUMNS, "
                    + "{[Employees].[Sheri Nowmer]} "
                    + "ON ROWS FROM [HR]";
        final Query query1 = conn.parseQuery(queryStr1);
        final Result result1 = conn.execute(query1);

        // Query using key
        final String queryStr2 =
                "SELECT {[Measures].[Org Salary], [Measures].[Count]} "
                    + "ON COLUMNS, "
                    + "{[Employees].&[1]} "
                    + "ON ROWS FROM [HR]";
        final Query query2 = conn.parseQuery(queryStr2);
        final Result result2 = conn.execute(query2);

        // Results of two previous queries must be the same
        assertEquals(
            result2.getCell(new int[] {0,0}).getValue(),
            result1.getCell(new int[] {0,0}).getValue());
        assertEquals(
            result2.getCell(new int[] {1,0}).getValue(),
            result1.getCell(new int[] {1,0}).getValue());
    }

}

// End IndexedValuesTest.java
