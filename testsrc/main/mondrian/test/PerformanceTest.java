/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2009-2009 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.test;

import mondrian.olap.Result;

/**
 * Various unit tests concerned with performance.
 *
 * @author jhyde
 * @since August 7, 2006
 * @version $Id$
 */
public class PerformanceTest extends FoodMartTestCase {

    public PerformanceTest(String name) {
        super(name);
    }

    /**
     * Test case for
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-550">
     * Bug MONDRIAN-550, "Performance bug with NON EMPTY and large axes"</a>.
     */
    public void testBugMondrian550() {
        final TestContext testContext = getBugMondrian550Schema();

        // On my Latitude D630:
        // Takes 137 seconds before bug fixed.
        // Takes 13 seconds after bug fixed.
        final Result result = testContext.executeQuery(
            "select NON EMPTY {[Store Name sans All].Members} ON COLUMNS,\n"
            + "  NON EMPTY Hierarchize(Union({[ACC].[All]}, [ACC].[All].Children)) ON ROWS\n"
            + "from [Sales]\n"
            + "where ([Time].[1997].[Q4], [Measures].[EXP2])");
        assertEquals(13, result.getAxes()[0].getPositions().size());
        assertEquals(3262, result.getAxes()[1].getPositions().size());
    }

    /**
     * As {@link #testBugMondrian550()} but with tuples on the rows axis.
     */
    public void testBugMondrian550Tuple() {
        final TestContext testContext = getBugMondrian550Schema();

        // On my Latitude D630:
        // Takes 252 seconds before bug fixed.
        // Takes 45 seconds after bug fixed.
        final Result result2 = testContext.executeQuery(
            "select NON EMPTY {[Store Name sans All].Members} ON COLUMNS,\n"
            + "  NON EMPTY Hierarchize(Union({[ACC].[All]}, [ACC].[All].Children))\n"
            + "   * [Gender].Children ON ROWS\n"
            + "from [Sales]\n"
            + "where ([Time].[1997].[Q4], [Measures].[EXP2])");
        assertEquals(13, result2.getAxes()[0].getPositions().size());
        assertEquals(3263, result2.getAxes()[1].getPositions().size());
    }

    private TestContext getBugMondrian550Schema() {
        return TestContext.createSubstitutingCube(
            "Sales",
            "      <Dimension name=\"ACC\" caption=\"Account\" type=\"StandardDimension\" foreignKey=\"customer_id\">\n"
            + "         <Hierarchy hasAll=\"true\" allMemberName=\"All\" primaryKey=\"customer_id\">\n"
            + "            <Table name=\"customer\"/>\n"
            + "            <Level name=\"CODE\" caption=\"Account\" uniqueMembers=\"true\" column=\"account_num\" type=\"String\"/>\n"
            + "         </Hierarchy>\n"
            + "      </Dimension>\n"
            + "      <Dimension name=\"Store Name sans All\" type=\"StandardDimension\" foreignKey=\"store_id\">\n"
            + "         <Hierarchy hasAll=\"false\" primaryKey=\"store_id\">\n"
            + "            <Table name=\"store\" />\n"
            + "            <Level name=\"Store Name\" uniqueMembers=\"true\" column=\"store_number\" type=\"Numeric\" ordinalColumn=\"store_name\"/>\n"
            + "         </Hierarchy>\n"
            + "      </Dimension>\n",
            "      <CalculatedMember dimension=\"Measures\" name=\"EXP2_4\" formula=\"IIf([ACC].CurrentMember.Level.Ordinal = [ACC].[All].Ordinal, Sum([ACC].[All].Children, [Measures].[Unit Sales]),     [Measures].[Unit Sales])\"/>\n"
            + "      <CalculatedMember dimension=\"Measures\" name=\"EXP2\" formula=\"IIf(0 &#60; [Measures].[EXP2_4], [Measures].[EXP2_4], NULL)\"/>\n");
    }
}

// End PerformanceTest.java
