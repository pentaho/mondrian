/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (c) 2002-2017 Hitachi Vantara.  All rights reserved.
*/
package mondrian.udf;

import mondrian.test.FoodMartTestCase;
import mondrian.test.TestContext;

/**
 * Tests the CurrentDateMemberUdf class.
 *
 * @author Luc Boudreau
 */
public class CurrentDateMemberUdfTest extends FoodMartTestCase {
    public CurrentDateMemberUdfTest() {
        super();
    }
    public CurrentDateMemberUdfTest(String name) {
        super(name);
    }

    public void testCurrentDateMemberUdf() {
        TestContext context = TestContext.instance().create(
            null,
            null,
            null,
            null,
            "<UserDefinedFunction name=\"MockCurrentDateMember\" "
            + "className=\"mondrian.udf.MockCurrentDateMember\" /> ",
            null);
        context.assertQueryReturns(
            "SELECT NON EMPTY {[Measures].[Org Salary]} ON COLUMNS, "
            + "NON EMPTY {MockCurrentDateMember([Time].[Time], \"[yyyy]\")} ON ROWS "
            + "FROM [HR] ",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Org Salary]}\n"
            + "Axis #2:\n"
            + "{[Time].[1997]}\n"
            + "Row #0: $39,431.67\n");
    }

    /**
     * test for MONDRIAN-2256 issue. Tests if method returns member with
     * dimension info or not. To get a number as a result you should change
     * current year to 1997. In this case expected should be ended with
     * "266,773\n"
    */
    public void testGetReturnType() {
        String query = "WITH MEMBER [Time].[YTD] AS SUM( YTD(CurrentDateMember"
             + "([Time], '[\"Time\"]\\.[yyyy]\\.[Qq].[m]')), Measures.[Unit Sales]) SELECT Time.YTD on 0 FROM sales";
        String expected = "Axis #0:\n" + "{}\n" + "Axis #1:\n"
             + "{[Time].[YTD]}\n" + "Row #0: \n";
        assertQueryReturns(query, expected);
    }
}

// End CurrentDateMemberUdfTest.java
