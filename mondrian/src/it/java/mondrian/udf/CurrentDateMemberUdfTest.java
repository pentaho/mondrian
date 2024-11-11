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
