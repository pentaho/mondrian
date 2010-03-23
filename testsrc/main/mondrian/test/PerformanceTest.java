/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2009-2010 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.test;

import mondrian.olap.*;
import mondrian.util.Bug;

import org.apache.log4j.Logger;

import java.util.*;

/**
 * Various unit tests concerned with performance.
 *
 * @author jhyde
 * @since August 7, 2006
 * @version $Id$
 */
public class PerformanceTest extends FoodMartTestCase {
    /**
     * Certain tests are enabled only if logging is enabled.
     */
    private static final Logger LOGGER =
        Logger.getLogger(PerformanceTest.class);

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


    /**
     * Test case for
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-641">
     * Bug MONDRIAN-641</a>, "Large NON EMPTY result performs poorly with
     * ResultStyle.ITERABLE".  Runs in ~10 seconds with ResultStyle.LIST,
     * 99+ seconds with ITERABLE (on DELL Latitude D630).
     */
    public void testBug641() {
        if (Bug.BugMondrian641Fixed) {
            long start = System.currentTimeMillis();
            Result result = executeQuery(
                    "select  non empty  {  crossjoin( customers.[city].members, "
                    + "crossjoin( [store type].[store type].members,  "
                    + "product.[product name].members)) }"
                    + " on 0 from sales");
            printDuration("Bug ", start);
            assertEquals(51148, result.getAxes()[0].getPositions().size());
        }
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

    /**
     * Tests performance when an MDX query contains a very large explicit set.
     */
    public void testVeryLargeExplicitSet() {
        // All numbers in the following are on mackerel (a Dell D630 laptop
        // running Vista and JDK 1.6 against Access).
        Result result;
        final TestContext testContext = getTestContext();
        long t0 = System.currentTimeMillis();
        final Axis axis = testContext.executeAxis("Customers.Members");
        printDuration("Execute axis", t0); // 5s on mackerel
        final List<Position> positionList = axis.getPositions();
        assertEquals(10407, positionList.size());

        // Take customers 0-2000 and 5000-7000. Using contiguous bursts,
        // Mondrian has a chance to optimize how it reads cells from the
        // database.
        List<Member> memberList = new ArrayList<Member>();
        for (int i = 0; i < positionList.size(); i++) {
            Position position = positionList.get(i);
            ++i;
            if (i < 2000 || i >= 5000 && i < 7000) {
                memberList.add(position.get(0));
            }
        }

        // Build a query with an explcit member list.
        if (LOGGER.isDebugEnabled()) {
            StringBuilder buf = new StringBuilder();
            for (Member member : memberList) {
                if (buf.length() > 0) {
                    buf.append(", ");
                }
                buf.append(member);
            }
            final String mdx =
                "WITH SET [Selected Customers] AS {" + buf + "}\n"
                + "SELECT {[Measures].[Unit Sales],\n"
                + "        [Measures].[Store Sales]} on 0,\n"
                + "  [Selected Customers] on 1\n"
                + "FROM [Sales]";
            t0 = System.currentTimeMillis();
            result = testContext.executeQuery(mdx);
            printDuration("First execute", t0); // 75s on mackerel
            assertEquals(
                memberList.size(),
                result.getAxes()[1].getPositions().size());

            t0 = System.currentTimeMillis();
            result = testContext.executeQuery(mdx);
            printDuration("Second execute", t0); // 65s on mackerel
            assertEquals(
                memberList.size(),
                result.getAxes()[1].getPositions().size());
        }

        // Much more efficient technique. Use a parameter, and bind to array.
        // Cuts out a lot of parsing, so takes 2.4s as opposed to 65s.
        Query query =
            testContext.getConnection().parseQuery(
                "WITH SET [Selected Customers]\n"
                + "  AS Parameter('Foo', [Customers], {}, 'Description')\n"
                + "SELECT {[Measures].[Unit Sales],\n"
                + "        [Measures].[Store Sales]} on 0,\n"
                + "  [Selected Customers] on 1\n"
                + "FROM [Sales]");
        query.setParameter("Foo", memberList);
        t0 = System.currentTimeMillis();
        result = testContext.getConnection().execute(query);
        printDuration("Param query, 1st execute", t0); // 2424ms on mackerel
        t0 = System.currentTimeMillis();
        result = testContext.getConnection().execute(query);
        printDuration("Param query, 2nd execute", t0); // 51ms on mackerel
        assertEquals(
            memberList.size(),
            result.getAxes()[1].getPositions().size());
    }

    /**
     * Test case for
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-639">
     * Bug MONDRIAN-639, "RolapNamedSetEvaluator anon classes implement
     * Iterable, causing performance regression from 2.4 in
     * FunUtil.count()"</a>.
     */
    public void testBugMondrian639() {
        // On my mac mini:
        // takes 233 seconds before bug fixed
        // takes 4.5 seconds after bug fixed
        long start = System.currentTimeMillis();
        Result result = executeQuery(
            "WITH SET [cjoin] AS "
            + "crossjoin(customers.members, [store type].[store type].members) "
            + "MEMBER [Measures].[total_available_count] "
            + "AS Format(COUNT([cjoin]), \"#####\") "
            + "SELECT"
            + "{[cjoin]} ON COLUMNS, "
            + "{[Measures].[total_available_count]} ON ROWS "
            + "FROM sales");
        printDuration("Bug 639 (count() iterations)", start);
        assertEquals(62442, result.getAxes()[0].getPositions().size());
        assertEquals(1, result.getAxes()[1].getPositions().size());
    }

    /***
     * Tests performance of a larger schema with a large number of result cells.
     * Runs in 12.2 seconds when RolapEvaluator.getProperty uses currentMember.
     * Runs in 7.5 seconds when RolapEvaluator.getProperty uses nonAllMemberMap.
     * The performance boost gets more significant as the schema size grows.
     */
    public void testBigResultsWithBigSchemaPerforms() {
        if (!LOGGER.isDebugEnabled()) {
            return;
        }
        TestContext testContext = TestContext.createSubstitutingCube(
            "Sales",
            extraGenders(1000),
            null);
        String mdx =
            "with "
            + " member [Measures].[one] as '1'"
            + " member [Measures].[two] as '2'"
            + " member [Measures].[three] as '3'"
            + " member [Measures].[four] as '4'"
            + " member [Measures].[five] as '5'"
            + " select "
            + "{[Measures].[one],[Measures].[two],[Measures].[three],[Measures].[four],[Measures].[five]}"
            + " on 0, "
            + "Crossjoin([Customers].[name].members,[Store].[Store Name].members)"
            + " on 1 from sales";
        long start = System.currentTimeMillis();
        testContext.executeQuery(mdx);
        printDuration("getProperty taking a long time", start);
    }

    private String extraGenders(final int numGenders) {
        final StringBuilder builder = new StringBuilder();
        for (int i = 0; i < numGenders;i++) {
            builder.append(
                String.format(
                    "<Dimension name=\"Gender%d \" foreignKey=\"customer_id\">"
                    + "<Hierarchy hasAll=\"true\" allMemberName=\"All Gender\" primaryKey=\"customer_id\">"
                    + "      <Table name=\"customer\"/>"
                    + "      <Level name=\"Gender\" column=\"gender\" uniqueMembers=\"true\"/>"
                    + "    </Hierarchy>"
                    + "</Dimension>", i));
        }
        return builder.toString();
    }

    /**
     * Runs a query that performs a lot of in-memory calculation.
     *
     * <p>Timings (branch / change / host / DBMS / jdk / timings (s) / mean):
     * <ul>
     * <li>mondrian-3.2 13366 marmalade oracle jdk1.6 592 588 581 571 avg 583
     * <li>mondrian-3.2 13367 marmalade oracle jdk1.6 643 620 631 671 avg 641
     * <li>mondrian-3.2 13397 marmalade oracle jdk1.6 604 626
     * <li>mondrian-3.2 13467 marmalade oracle jdk1.6 610 574
     * <li>mondrian-3.2 13489 marmalade oracle jdk1.6 565 561 579 596 avg 575
     * <li>mondrian-3.2 13490 marmalade oracle jdk1.6 607 611 581 605 avg 601
     * </ul>
     */
    public void testInMemoryCalc() {
        if (!LOGGER.isDebugEnabled()) {
            // Test is too expensive to run as part of standard regress.
            // Take 10h on hudson (MySQL)!!!
            return;
        }
        final String result =
            "Axis #0:\n"
            + "{[Time].[1997].[Q3]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Store Sales]}\n"
            + "{[Measures].[Typical Store Sales]}\n"
            + "{[Measures].[Ratio]}\n"
            + "Axis #2:\n"
            + "{[Product].[All Products].[Food].[Baked Goods].[Bread].[Sliced Bread].[Modell].[Modell Rye Bread], [Customers].[All Customers].[USA].[OR].[Salem].[Joan Johnson]}\n"
            + "{[Product].[All Products].[Non-Consumable].[Household].[Plastic Products].[Plastic Utensils].[Denny].[Denny Plastic Knives], [Customers].[All Customers].[USA].[OR].[Lebanon].[Pat Pinkston]}\n"
            + "{[Product].[All Products].[Food].[Starchy Foods].[Starchy Foods].[Rice].[Shady Lake].[Shady Lake Thai Rice], [Customers].[All Customers].[USA].[CA].[Grossmont].[Anne Silva]}\n"
            + "{[Product].[All Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Better].[Better Regular Ramen Soup], [Customers].[All Customers].[USA].[CA].[Coronado].[Robert Brink]}\n"
            + "{[Product].[All Products].[Non-Consumable].[Health and Hygiene].[Bathroom Products].[Mouthwash].[Bird Call].[Bird Call Laundry Detergent], [Customers].[All Customers].[USA].[CA].[Downey].[Eric Renn]}\n"
            + "Row #0: 19.65\n"
            + "Row #0: 3.12\n"
            + "Row #0: 6.30\n"
            + "Row #1: 15.56\n"
            + "Row #1: 2.80\n"
            + "Row #1: 5.56\n"
            + "Row #2: 11.24\n"
            + "Row #2: 2.10\n"
            + "Row #2: 5.35\n"
            + "Row #3: 11.22\n"
            + "Row #3: 2.46\n"
            + "Row #3: 4.56\n"
            + "Row #4: 6.33\n"
            + "Row #4: 1.71\n"
            + "Row #4: 3.70\n";
        final String mdx =
            "with member [Measures].[Typical Store Sales] as\n"
            + "  Max(\n"
            + "    [Customers].Siblings,\n"
            + "    Min(\n"
            + "      [Product].Siblings,\n"
            + "      Avg(\n"
            + "        [Time].Siblings,\n"
            + "        [Measures].[Store Sales])))\n"
            + "member [Measures].[Ratio] as\n"
            + "  [Measures].[Store Sales]\n"
            + "   / [Measures].[Typical Store Sales]\n"
            + "select\n"
            + "  {\n"
            + "    [Measures].[Store Sales],\n"
            + "    [Measures].[Typical Store Sales],\n"
            + "    [Measures].[Ratio]\n"
            + "  } on 0,\n"
            + "  TopCount(\n"
            + "    Filter(\n"
            + "      NonEmptyCrossJoin("
            + "        [Product].[Product Name].Members,\n"
            + "        [Customers].[Name].Members),\n"
            + "      [Measures].[Ratio] > 1.1\n"
            + "      and [Measures].[Store Sales] > 5),\n"
            + "    5,\n"
            + "    [Measures].[Ratio]) on 1\n"
            + "from [Sales]\n"
            + "where [Time].[1997].[Q3]";
        final long start = System.currentTimeMillis();
        assertQueryReturns(mdx, result);
        printDuration("in-memory calc", start);
    }

    private void printDuration(String desc, long t0) {
        final long t1 = System.currentTimeMillis();
        final long duration = t1 - t0;
        LOGGER.debug(desc + " took " + duration + " millis");
    }
}

// End PerformanceTest.java
