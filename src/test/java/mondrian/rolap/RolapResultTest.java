/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2005-2011 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.olap.Result;
import mondrian.rolap.aggmatcher.AggTableTestCase;
import mondrian.test.TestContext;

/**
 * Testcase for
 *
 *
 * @author <a>Richard M. Emberson</a>
 * @since Feb 21 2007
 */
public class RolapResultTest extends AggTableTestCase {

    private static final String RolapResultTest = "RolapResultTest.csv";
    private static final String DIRECTORY =
        "target/test-classes/mondrian/rolap";

    private static final String RESULTS_ALL =
        "Axis #0:\n"
        + "{}\n"
        + "Axis #1:\n"
        + "{[D1].[a]}\n"
        + "{[D1].[b]}\n"
        + "{[D1].[c]}\n"
        + "Axis #2:\n"
        + "{[D2].[x]}\n"
        + "{[D2].[y]}\n"
        + "{[D2].[z]}\n"
        + "Row #0: 5\n"
        + "Row #0: \n"
        + "Row #0: \n"
        + "Row #1: \n"
        + "Row #1: 10\n"
        + "Row #1: \n"
        + "Row #2: \n"
        + "Row #2: \n"
        + "Row #2: 15\n";

    private static final String RESULTS =
        "Axis #0:\n"
        + "{}\n"
        + "Axis #1:\n"
        + "{[D1].[a]}\n"
        + "{[D1].[b]}\n"
        + "{[D1].[c]}\n"
        + "Axis #2:\n"
        + "{[D2].[x]}\n"
        + "{[D2].[y]}\n"
        + "{[D2].[z]}\n"
        + "Row #0: 5\n"
        + "Row #0: \n"
        + "Row #0: \n"
        + "Row #1: \n"
        + "Row #1: 10\n"
        + "Row #1: \n"
        + "Row #2: \n"
        + "Row #2: \n"
        + "Row #2: 15\n";

    //boolean useImplicitMembers;
    public RolapResultTest() {
        super();
    }

    public RolapResultTest(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        super.setUp();
    }

    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public void testAll() throws Exception {
        if (!isApplicable()) {
            return;
        }

        String mdx =
            "select "
            + " filter({[D1].[a],[D1].[b],[D1].[c]}, "
            + "    [Measures].[Value] > 0) "
            + " ON COLUMNS, "
            + " {[D2].[x],[D2].[y],[D2].[z]} "
            + " ON ROWS "
            + "from FTAll";

        getCubeTestContext().assertQueryReturns(mdx, RESULTS_ALL);
/*
        Result result = getCubeTestContext().executeQuery(mdx);
        String resultString = TestContext.toString(result);
//System.out.println(resultString);

        assertTrue(resultString.equals(RESULTS_ALL));
*/
    }

    public void testD1() throws Exception {
        if (!isApplicable()) {
            return;
        }
        String mdx =
            "select "
            + " filter({[D1].[a],[D1].[b],[D1].[c]}, "
            + "    [Measures].[Value] > 0) "
            + " ON COLUMNS, "
            + " {[D2].[x],[D2].[y],[D2].[z]} "
            + " ON ROWS "
            + "from FT1";

        //getCubeTestContext().assertQueryReturns(mdx, RESULTS);
        Result result = getCubeTestContext().executeQuery(mdx);
        String resultString = TestContext.toString(result);
//System.out.println(resultString);
/*
 This is what is produced
Axis #0:
{}
Axis #1:
Axis #2:
{[D2].[x]}
{[D2].[y]}
{[D2].[z]}
*/
        assertEquals(resultString, RESULTS);
    }

    public void testD2() throws Exception {
        if (!isApplicable()) {
            return;
        }
        String mdx =
            "select "
            + " NON EMPTY filter({[D1].[a],[D1].[b],[D1].[c]}, "
            + "    [Measures].[Value] > 0) "
            + " ON COLUMNS, "
            + " {[D2].[x],[D2].[y],[D2].[z]} "
            + " ON ROWS "
            + "from FT2";

        getCubeTestContext().assertQueryReturns(mdx, RESULTS);
/*
        Result result = getCubeTestContext().executeQuery(mdx);
        String resultString = TestContext.toString(result);
//System.out.println(resultString);
        assertTrue(resultString.equals(RESULTS));
*/
    }

    /**
     * This ought to give the same result as the above testD2() method.
     * In this case, the FT2Extra cube has a default measure with no
     * data (null) for all members. This default measure is used
     * in the evaluation even though there is an implicit use of the
     * measure [Measures].[Value].
     *
     * @throws Exception
     */
    public void _testNullDefaultMeasure() throws Exception {
        if (!isApplicable()) {
            return;
        }

        String mdx =
            "select "
            + " NON EMPTY filter({[D1].[a],[D1].[b],[D1].[c]}, "
            + "    [Measures].[Value] > 0) "
            + " ON COLUMNS, "
            + " {[D2].[x],[D2].[y],[D2].[z]} "
            + " ON ROWS "
            + "from FT2Extra";

        //getCubeTestContext().assertQueryReturns(mdx, RESULTS);
        Result result = getCubeTestContext().executeQuery(mdx);
        String resultString = TestContext.toString(result);
        assertTrue(resultString.equals(RESULTS));
    }




    protected String getFileName() {
        return RolapResultTest;
    }
    protected String getDirectoryName() {
        return DIRECTORY;
    }

    protected String getCubeDescription() {
        return
            "<Cube name='FTAll'>\n"
            + "<Table name='FT1' />\n"
            + "<Dimension name='D1' foreignKey='d1_id' >\n"
            + " <Hierarchy hasAll='true' primaryKey='d1_id'>\n"
            + " <Table name='D1'/>\n"
            + " <Level name='Name' column='name' type='String' uniqueMembers='true'/>\n"
            + " </Hierarchy>\n"
            + "</Dimension>\n"
            + "<Dimension name='D2' foreignKey='d2_id' >\n"
            + " <Hierarchy hasAll='true' primaryKey='d2_id'>\n"
            + " <Table name='D2'/>\n"
            + " <Level name='Name' column='name' type='String' uniqueMembers='true'/>\n"
            + " </Hierarchy>\n"
            + "</Dimension>\n"

            + "<Measure name='Value' \n"
            + "    column='value' aggregator='sum'\n"
            + "   formatString='#,###'/>\n"
            + "</Cube> \n"

            + "<Cube name='FT1'>\n"
            + "<Table name='FT1' />\n"
            + "<Dimension name='D1' foreignKey='d1_id' >\n"
            + " <Hierarchy hasAll='false' defaultMember='[D1].[d]' primaryKey='d1_id'>\n"
            + " <Table name='D1'/>\n"
            + " <Level name='Name' column='name' type='String' uniqueMembers='true'/>\n"
            + " </Hierarchy>\n"
            + "</Dimension>\n"
            + "<Dimension name='D2' foreignKey='d2_id' >\n"
            + " <Hierarchy hasAll='false' defaultMember='[D2].[w]' primaryKey='d2_id'>\n"
            + " <Table name='D2'/>\n"
            + " <Level name='Name' column='name' type='String' uniqueMembers='true'/>\n"
            + " </Hierarchy>\n"
            + "</Dimension>\n"

            + "<Measure name='Value' \n"
            + "    column='value' aggregator='sum'\n"
            + "   formatString='#,###'/>\n"
            + "</Cube> \n"

            + "<Cube name='FT2'>\n"
            + "<Table name='FT2'/>\n"
            + "<Dimension name='D1' foreignKey='d1_id' >\n"
            + " <Hierarchy hasAll='false' defaultMember='[D1].[d]' primaryKey='d1_id'>\n"
            + " <Table name='D1'/>\n"
            + " <Level name='Name' column='name' type='String' uniqueMembers='true'/>\n"
            + " </Hierarchy>\n"
            + "</Dimension>\n"
            + "<Dimension name='D2' foreignKey='d2_id' >\n"
            + " <Hierarchy hasAll='false' defaultMember='[D2].[w]' primaryKey='d2_id'>\n"
            + " <Table name='D2'/>\n"
            + " <Level name='Name' column='name' type='String' uniqueMembers='true'/>\n"
            + " </Hierarchy>\n"
            + "</Dimension>\n"

            + "<Measure name='Value' \n"
            + "    column='value' aggregator='sum'\n"
            + "   formatString='#,###'/>\n"
            + "</Cube>\n"

            + "<Cube name='FT2Extra'>\n"
            + "<Table name='FT2'/>\n"
            + "<Dimension name='D1' foreignKey='d1_id' >\n"
            + " <Hierarchy hasAll='false' defaultMember='[D1].[d]' primaryKey='d1_id'>\n"
            + " <Table name='D1'/>\n"
            + " <Level name='Name' column='name' type='String' uniqueMembers='true'/>\n"
            + " </Hierarchy>\n"
            + "</Dimension>\n"
            + "<Dimension name='D2' foreignKey='d2_id' >\n"
            + " <Hierarchy hasAll='false' defaultMember='[D2].[w]' primaryKey='d2_id'>\n"
            + " <Table name='D2'/>\n"
            + " <Level name='Name' column='name' type='String' uniqueMembers='true'/>\n"
            + " </Hierarchy>\n"
            + "</Dimension>\n"
            + "<Measure name='VExtra' \n"
            + "    column='vextra' aggregator='sum'\n"
            + "   formatString='#,###'/>\n"
            + "<Measure name='Value' \n"
            + "    column='value' aggregator='sum'\n"
            + "   formatString='#,###'/>\n"
            + "</Cube>";
    }

    public void testNonAllPromotionMembers() {
        TestContext testContext = TestContext.instance().createSubstitutingCube(
            "Sales",
            "<Dimension name=\"Promotions2\" foreignKey=\"promotion_id\">\n"
            + "  <Hierarchy hasAll=\"false\" primaryKey=\"promotion_id\">\n"
            + "    <Table name=\"promotion\"/>\n"
            + "    <Level name=\"Promotion2 Name\" column=\"promotion_name\" uniqueMembers=\"true\"/>\n"
            + "  </Hierarchy>\n"
            + "</Dimension>");

        testContext.assertQueryReturns(
            "select {[Promotion2 Name].[Price Winners], [Promotion2 Name].[Sale Winners]} * {Tail([Time].[Year].Members,3)} ON COLUMNS, "
            + "NON EMPTY Crossjoin({[Store].CurrentMember.Children},  {[Store Type].[All Store Types].Children}) ON ROWS "
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Promotions2].[Price Winners], [Time].[Time].[1997]}\n"
            + "{[Promotions2].[Price Winners], [Time].[Time].[1998]}\n"
            + "{[Promotions2].[Sale Winners], [Time].[Time].[1997]}\n"
            + "{[Promotions2].[Sale Winners], [Time].[Time].[1998]}\n"
            + "Axis #2:\n"
            + "{[Store].[USA], [Store].[Store Type].[Mid-Size Grocery]}\n"
            + "{[Store].[USA], [Store].[Store Type].[Small Grocery]}\n"
            + "{[Store].[USA], [Store].[Store Type].[Supermarket]}\n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: 444\n"
            + "Row #0: \n"
            + "Row #1: 23\n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #2: 1,271\n"
            + "Row #2: \n"
            + "Row #2: \n"
            + "Row #2: \n");
    }
}

// End RolapResultTest.java
