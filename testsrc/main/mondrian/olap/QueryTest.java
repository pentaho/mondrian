/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 1998-2002 Kana Software, Inc.
// Copyright (C) 2001-2011 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// Shishir, 08 May, 2007
*/
package mondrian.olap;

import mondrian.server.Statement;
import mondrian.test.FoodMartTestCase;
import mondrian.test.TestContext;

/**
 * Query test.
 *
 * @version $Id$
 */
public class QueryTest extends FoodMartTestCase {
    private QueryPart[] cellProps = {
        new CellProperty("[Value]"),
        new CellProperty("[Formatted_Value]"),
        new CellProperty("[Format_String]")
    };
    private QueryAxis[] axes = new QueryAxis[0];
    private Formula[] formulas = new Formula[0];
    private Query queryWithCellProps;
    private Query queryWithoutCellProps;

    protected void setUp() throws Exception {
        super.setUp();

        TestContext testContext = getTestContext();
        ConnectionBase connection =
            (ConnectionBase) testContext.getConnection();
        final Statement statement =
            connection.getInternalStatement();

        try {
            queryWithCellProps =
                new Query(
                    statement, formulas, axes, "Sales",
                    null, cellProps, false);
            queryWithoutCellProps =
                new Query(
                    statement, formulas, axes, "Sales",
                    null, new QueryPart[0], false);
        } finally {
            statement.close();
        }
    }

    public void testHasCellPropertyWhenQueryHasCellProperties() {
        assertTrue(queryWithCellProps.hasCellProperty("Value"));
        assertFalse(queryWithCellProps.hasCellProperty("Language"));
    }

    public void testIsCellPropertyEmpty() {
        assertTrue(queryWithoutCellProps.isCellPropertyEmpty());
    }
}

// End QueryTest.java
