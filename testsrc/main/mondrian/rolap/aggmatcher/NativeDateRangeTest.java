/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2014-2014 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap.aggmatcher;

import mondrian.test.TestContext;

/**
 * Testcase for non-collapsed levels in agg tables.
 *
 * @author Luc Boudreau
 */
public class NativeDateRangeTest extends AggTableTestCase {

    private static final String CUBE_1 =
        "<Cube name=\"date_cube\">\n"
        + "    <Table name=\"fact_with_date\">\n"
        + "    </Table>\n"
        + "    <Dimension name=\"FooTime\">\n"
        + "        <Hierarchy name=\"FooTime\" hasAll=\"true\" allMemberName=\"All the time\" type=\"TimeDimension\">\n"
        + "            <Level name=\"DateLevel\" column=\"date_col\" type=\"Date\" levelType=\"TimeDays\" uniqueMembers=\"true\"/>\n"
        + "        </Hierarchy>\n"
        + "    </Dimension>"
        + "    <Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\"/>\n"
        + "</Cube>\n";

    public NativeDateRangeTest() {
        super();
    }

    public NativeDateRangeTest(String name) {
        super(name);
    }

    protected String getFileName() {
        return "native_date_range_test.csv";
    }

    @Override
    protected String getCubeDescription() {
        return CUBE_1;
    }

    public void testSingleJoin() throws Exception {
        if (!isApplicable()) {
            return;
        }

        final String mdx =
            "select {[FooTime].[DateLevel].&[2010-03-21] : [FooTime].[DateLevel].&[2010-03-22]} on 0\n"
            + "from [date_cube]";

        final TestContext context = getTestContext();

        // We expect the correct cell value + 1 if the agg table is used.
        context.assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[FooTime].[2010-03-21]}\n"
            + "{[FooTime].[2010-03-22]}\n"
            + "Row #0: 20\n"
            + "Row #0: 40\n");
    }
}
// End NativeDateRangeTest.java