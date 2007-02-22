/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2005-2006 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import mondrian.test.TestContext;
import mondrian.rolap.aggmatcher.*;
import mondrian.olap.MondrianProperties;
import mondrian.olap.Result;

/** 
 * Testcase for
 *
 * 
 * @author <a>Richard M. Emberson</a>
 * @since Feb 21 2007
 * @version $Id$
 */
public class RolapResultTest extends AggTableTestCase {

    private static final String RolapResultTest = "RolapResultTest.csv";
    private static final String DIRECTORY = "testsrc/main/mondrian/rolap";

    private static final String RESULTS_ALL =
                "Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[D1].[All D1s].[a]}\n" +
                "{[D1].[All D1s].[b]}\n" +
                "{[D1].[All D1s].[c]}\n" +
                "Axis #2:\n" +
                "{[D2].[All D2s].[x]}\n" +
                "{[D2].[All D2s].[y]}\n" +
                "{[D2].[All D2s].[z]}\n" +
                "Row #0: 5\n" +
                "Row #0: \n" +
                "Row #0: \n" +
                "Row #1: \n" +
                "Row #1: 10\n" +
                "Row #1: \n" +
                "Row #2: \n" +
                "Row #2: \n" +
                "Row #2: 15\n";

    private static final String RESULTS =
                "Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[D1].[a]}\n" +
                "{[D1].[b]}\n" +
                "{[D1].[c]}\n" +
                "Axis #2:\n" +
                "{[D2].[x]}\n" +
                "{[D2].[y]}\n" +
                "{[D2].[z]}\n" +
                "Row #0: 5\n" +
                "Row #0: \n" +
                "Row #0: \n" +
                "Row #1: \n" +
                "Row #1: 10\n" +
                "Row #1: \n" +
                "Row #2: \n" +
                "Row #2: \n" +
                "Row #2: 15\n";

    boolean useImplicitMembers;
    public RolapResultTest() {
        super();
    }
    public RolapResultTest(String name) {
        super(name);
    }
    protected void setUp() throws Exception {
        super.setUp();

        useImplicitMembers = MondrianProperties.instance().UseImplicitMembers.get();
        MondrianProperties.instance().UseImplicitMembers.set(false);
    }
    protected void tearDown() throws Exception {
        MondrianProperties.instance().UseImplicitMembers.set(useImplicitMembers);

        super.tearDown();
    }
    public void testAll() throws Exception {
        if (!isApplicable()) {
            return;
        }

        String mdx = "select " +
                     " filter({[D1].[a],[D1].[b],[D1].[c]}, " +
                     "    [Measures].[Value] > 0) "+
                     " ON COLUMNS, " +
                     " {[D2].[x],[D2].[y],[D2].[z]} "+
                     " ON ROWS " +
                     "from FTAll";

        Result result = getCubeTestContext().executeQuery(mdx);
        String resultString = TestContext.toString(result);
//System.out.println(resultString);

        assertTrue(resultString.equals(RESULTS_ALL));
    }

    public void _testD1() throws Exception {
        if (!isApplicable()) {
            return;
        }

        String mdx = "select " +
                     " filter({[D1].[a],[D1].[b],[D1].[c]}, " +
                     "    [Measures].[Value] > 0) "+
                     " ON COLUMNS, " +
                     " {[D2].[x],[D2].[y],[D2].[z]} "+
                     " ON ROWS " +
                     "from FT1";

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
        assertTrue(resultString.equals(RESULTS));
    }

    public void testD2() throws Exception {
        if (!isApplicable()) {
            return;
        }

        String mdx = "select " +
                     " filter({[D1].[a],[D1].[b],[D1].[c]}, " +
                     "    [Measures].[Value] > 0) "+
                     " ON COLUMNS, " +
                     " {[D2].[x],[D2].[y],[D2].[z]} "+
                     " ON ROWS " +
                     "from FT2";

        Result result = getCubeTestContext().executeQuery(mdx);
        String resultString = TestContext.toString(result);
//System.out.println(resultString);
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
            "<Cube name='FTAll'>\n" +
            "<Table name='FT1' />\n" +
            "<Dimension name='D1' foreignKey='d1_id' >\n" +
            " <Hierarchy hasAll='true' primaryKey='d1_id'>\n" +
            " <Table name='D1'/>\n" +
            " <Level name='Name' column='name' type='String' uniqueMembers='true'/>\n" +
            " </Hierarchy>\n" +
            "</Dimension>\n" +
            "<Dimension name='D2' foreignKey='d2_id' >\n" +
            " <Hierarchy hasAll='true' primaryKey='d2_id'>\n" +
            " <Table name='D2'/>\n" +
            " <Level name='Name' column='name' type='String' uniqueMembers='true'/>\n" +
            " </Hierarchy>\n" +
            "</Dimension>\n" +

            "<Measure name='Value' \n" +
            "    column='value' aggregator='sum'\n" +
            "   formatString='#,###'/>\n" +
            "</Cube> \n" +


            "<Cube name='FT1'>\n" +
            "<Table name='FT1' />\n" +
            "<Dimension name='D1' foreignKey='d1_id' >\n" +
            " <Hierarchy hasAll='false' defaultMember='[D1].[d]' primaryKey='d1_id'>\n" +
            " <Table name='D1'/>\n" +
            " <Level name='Name' column='name' type='String' uniqueMembers='true'/>\n" +
            " </Hierarchy>\n" +
            "</Dimension>\n" +
            "<Dimension name='D2' foreignKey='d2_id' >\n" +
            " <Hierarchy hasAll='false' defaultMember='[D2].[w]' primaryKey='d2_id'>\n" +
            " <Table name='D2'/>\n" +
            " <Level name='Name' column='name' type='String' uniqueMembers='true'/>\n" +
            " </Hierarchy>\n" +
            "</Dimension>\n" +

            "<Measure name='Value' \n" +
            "    column='value' aggregator='sum'\n" +
            "   formatString='#,###'/>\n" +
            "</Cube> \n" +



            "<Cube name='FT2'>\n" +
            "<Table name='FT2'/>\n" +
            "<Dimension name='D1' foreignKey='d1_id' >\n" +
            " <Hierarchy hasAll='false' defaultMember='[D1].[d]' primaryKey='d1_id'>\n" +
            " <Table name='D1'/>\n" +
            " <Level name='Name' column='name' type='String' uniqueMembers='true'/>\n" +
            " </Hierarchy>\n" +
            "</Dimension>\n" +
            "<Dimension name='D2' foreignKey='d2_id' >\n" +
            " <Hierarchy hasAll='false' defaultMember='[D2].[w]' primaryKey='d2_id'>\n" +
            " <Table name='D2'/>\n" +
            " <Level name='Name' column='name' type='String' uniqueMembers='true'/>\n" +
            " </Hierarchy>\n" +
            "</Dimension>\n" +

            "<Measure name='Value' \n" +
            "    column='value' aggregator='sum'\n" +
            "   formatString='#,###'/>\n" +
            "</Cube>";
    }
}

// End RolapResultTest.java
