/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2017 Hitachi Vantara..  All rights reserved.
*/

package mondrian.rolap;

import mondrian.calc.ResultStyle;
import mondrian.olap.*;
import mondrian.test.FoodMartTestCase;
import mondrian.test.TestContext;
import mondrian.util.Bug;

import junit.framework.Assert;

import java.lang.ref.SoftReference;
import java.util.ArrayList;
import java.util.List;

/**
 * Unit-test for non cacheable elementos of high dimensions.
 *
 * @author jlopez, lcanals
 * @since May, 2008
 */
public class HighDimensionsTest extends FoodMartTestCase {
    public HighDimensionsTest() {
    }

    public HighDimensionsTest(String name) {
        super(name);
    }

    public void testBug1971406() throws Exception {
        if (!MondrianProperties.instance().EnableNativeCrossJoin.get()) {
            return;
        }
        final Connection connection = TestContext.instance()
            .getConnection();
        Query query = connection.parseQuery(
            "with set necj as "
            + "NonEmptyCrossJoin(NonEmptyCrossJoin("
            + "[Customers].[Name].members,[Store].[Store Name].members),"
            + "[Product].[Product Name].members) "
            + "select {[Measures].[Unit Sales]} on columns,"
            + "tail(intersect(necj,necj,ALL),5) on rows from sales");
        final long t0 = System.currentTimeMillis();
        Result result = connection.execute(query);
        for (final Position o : result.getAxes()[0].getPositions()) {
            assertNotNull(o.get(0));
        }
        final long t1 = System.currentTimeMillis();
        final long elapsed = t1 - t0;

        // scale up for slower CPUs
        double scaleFactor = computeCpuScaleFactor();
        final long target = (long) (90000 * scaleFactor);
        assertTrue(
            "Query execution took " + elapsed + " milliseconds, "
            + "which is outside target of  " + target + " milliseconds",
            elapsed <= target);
    }

    /**
     * Computes a scale factor indicating the relative system speed. This is
     * necessary for environments that might run code coverage, etc.
     *
     * <p>The method performs a benchmark computation, and returns is the ratio
     * of the elapsed time for the current system versus the baseline. If the
     * current system takes, say, 2 seconds to perform the benchmark
     * computation, the method returns 2.0, meaning that the system would be
     * expected to take twice as long to do a typical CPU-intensive task.
     *
     * @return Multiplier for how long this system would require to do a typical
     * CPU-intensive task versus the baseline system
     */
    private static double computeCpuScaleFactor() {
        final long nt0 = System.currentTimeMillis();
        int t = 0;
        for (int i = 0; i < Integer.MAX_VALUE; i++) {
            t += i;
        }
        final long nt1 = System.currentTimeMillis();
        long benchmarkTime = 0;

        // this check is here to make sure the compiler doesn't optimize
        // out the above loop
        if (t > 0) {
            benchmarkTime = nt1 - nt0;
        }

        // The benchmark takes takes about 1 second on the baseline system, a
        // Dell Latitude D820 Laptop.
        double scaleFactor = (double) benchmarkTime / 1000d;
        if (false) {
            System.out.println("scale factor = " + scaleFactor);
        }
        return scaleFactor;
    }

    public void testPromotionsTwoDimensions() throws Exception {
        if (!Bug.BugMondrian486Fixed) {
            return;
        }
        execHighCardTest(
            "select {[Measures].[Unit Sales]} on columns,\n"
            + "{[Promotions].[Promotion Name].Members} on rows\n"
            + "from [Sales Ragged]",
            1,
            "Promotions",
            highCardResults, null, true, 51);
    }


    public void testHead() throws Exception {
        if (!Bug.BugMondrian486Fixed) {
            return;
        }
        execHighCardTest(
            "select {[Measures].[Unit Sales]} on columns,\n"
            + "head({[Promotions].[Promotion Name].Members},40) "
            + "on rows from [Sales Ragged]",
            1,
            "Promotions",
            first40HighCardResults, null, true, 40);
    }

    // disabled pending fix of bug MONDRIAN-527
    public void _testTopCount() throws Exception {
        final Connection connection = TestContext.instance()
            .getConnection();
        final StringBuffer buffer = new StringBuffer();
        Query query =
            connection.parseQuery(
                "select {[Measures].[Unit Sales]} on columns,\n"
                + "TopCount({[Promotions].[Promotion Name].Members},41, "
                + "[Measures].[Unit Sales]) "
                + "on rows from [Sales Ragged]");
        Result result = connection.execute(query);
        int i = 0;
        String topcount40HighCardResults = null;
        String topcount41HighCardResults = null;
        for (final Position o : result.getAxes()[1].getPositions()) {
            buffer.append(o.get(0));
            i++;
            if (i == 40) {
                topcount40HighCardResults = buffer.toString();
            }
        }
        topcount41HighCardResults = buffer.toString();

        execHighCardTest(
            "select {[Measures].[Unit Sales]} on columns,\n"
            + "TopCount({[Promotions].[Promotion Name].Members},40, "
            + "[Measures].[Unit Sales]) "
            + "on rows from [Sales Ragged]",
            1,
            "Promotions",
            topcount40HighCardResults, topcount40Cells, false, 40);
        execHighCardTest(
            "select {[Measures].[Unit Sales]} on columns,\n"
            + "TopCount({[Promotions].[Promotion Name].Members},41, "
            + "[Measures].[Unit Sales]) "
            + "on rows from [Sales Ragged]",
            1,
            "Promotions",
            topcount41HighCardResults, topcount41Cells, false, 41);
        execHighCardTest(
            "select {[Measures].[Unit Sales]} on columns,\n"
            + "TopCount({[Promotions].[Promotion Name].Members},40, "
            + "[Measures].[Unit Sales]) "
            + "on rows from [Sales Ragged]",
            1,
            "Promotions",
            topcount40HighCardResults, topcount40Cells, false, 40);
    }


    public void testNonEmpty() throws Exception {
        if (!Bug.BugMondrian486Fixed) {
            return;
        }
        execHighCardTest(
            "select {[Measures].[Unit Sales]} on columns,\n"
            + "non empty {[Promotions].[Promotion Name].Members} "
            + "on rows from [Sales Ragged]",
            1,
            "Promotions",
            nonEmptyHighCardResults, nonEmptyCells, true, 48);
    }

    public void testFilter() throws Exception {
        if (!Bug.BugMondrian486Fixed) {
            return;
        }
        execHighCardTest(
            "select [Measures].[Unit Sales] on columns, "
            + "filter([Promotions].[Promotion Name].Members, "
            + "[Measures].[Unit Sales]>0) "
            + "on rows from [Sales Ragged]",
            1,
            "Promotions",
            nonEmptyHighCardResults, nonEmptyCells, true, 48);

        execHighCardTest(
            "select [Measures].[Unit Sales] on columns, "
            + "filter([Promotions].[Promotion Name].Members, "
            + "[Measures].[Unit Sales]>4000) "
            + "on rows from [Sales Ragged]",
            1,
            "Promotions",
            moreThan4000highCardResults, moreThan4000Cells, true, 3);
    }

    public void testMondrian1488() {
        //  MONDRIAN-1501 / MONDRIAN-1488
        // Both involve an attempt to modify the list backing
        // HighCardSqlTupleReader when handling null values.
        // Requires use of a dim flagged as high card which has null members
        TestContext ctx = TestContext.instance().create(
            null,
            "<Cube name=\"highCard\"> \n"
            + "  <Table name=\"sales_fact_1997\"/> \n"
            + "<Dimension name=\"StoreSize\" foreignKey=\"customer_id\"  highCardinality=\"true\">\n"
            + "    <Hierarchy hasAll=\"true\" primaryKey=\"store_id\">\n"
            + "      <Table name=\"store\"/>\n"
            + "      <Level name=\"Sqft\" column=\"store_sqft\" type=\"Numeric\" uniqueMembers=\"true\"/>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>"
            + "  <Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\"/> \n"
            + "</Cube> \n",
            null,
            null,
            null,
            null);
        // this will throw an exception if .remove is called on the HCSTR list
        ctx.executeQuery(
            "select NON EMPTY filter([StoreSize].[Sqft].members, 1=1) on 0 from highCard");
    }

    //
    // Private Stuff --------------------------------------------
    //

    /**
     * Executes query test trying to [Promotions].[Promotion Name] elements
     * into an axis from the results.
     */
    private void execHighCardTest(
        final String queryString,
        final int axisIndex,
        final String highDimensionName,
        final String results,
        final String results2,
        final boolean shouldForget,
        final int resultLimit)
        throws Exception
    {
        propSaver.set(MondrianProperties.instance().ResultLimit, resultLimit);
        final TestContext testContext =
            TestContext.instance().createSubstitutingCube(
                "Sales Ragged",
                "<Dimension name=\"Promotions\" highCardinality=\"true\" "
                + "foreignKey=\"promotion_id\">"
                + "    <Hierarchy hasAll=\"true\" "
                + "            allMemberName=\"All Promotions\" "
                + "            primaryKey=\"promotion_id\">"
                + "        <Table name=\"promotion\"/>"
                + "        <Level name=\"Promotion Name\" "
                + "                column=\"promotion_name\" "
                + "                uniqueMembers=\"true\"/>"
                + "    </Hierarchy>"
                + "</Dimension>");

        final Connection connection = testContext.getConnection();
        final Query query = connection.parseQuery(queryString);
        query.setResultStyle(ResultStyle.ITERABLE);
        Result result = connection.execute(query);
        StringBuffer buffer = new StringBuffer();
        StringBuffer buffer2 = new StringBuffer();

        final List<SoftReference> softReferences =
                new ArrayList<SoftReference>();
        // Tests results aren't got from database before this point
        int ii = 0;
        for (final Position o
            : result.getAxes()[axisIndex].getPositions())
        {
            assertNotNull(o.get(0));
            buffer2.append(result.getCell(
                new int[]{0, ii}).getValue().toString());
            ii++;
            softReferences.add(new SoftReference(o.get(0)));
            buffer.append(o.get(0).toString());
        }
        assertEquals(buffer.toString().length(), results.length());
        if (results2 != null) {
            assertEquals(buffer2.toString().length(), results2.length());
        }
        buffer2 = null;
        buffer = null;

        if (!shouldForget) {
            return;
        }

        // Tests that really results over ResultLimit are erased from
        // memory
        final List overloader = new ArrayList();
        try {
            for (;;) {
                overloader.add(new long[99999999]);
            }
        } catch (OutOfMemoryError out) {
            // OK, outofmemory
        }
        System.gc();

        for (int i = 4; i < ii - 40; i++) {
            assertNull(softReferences.get(i).get());
        }
        for (int i = 4; i < ii - 40; i++) {
            try {
                result.getAxes()[axisIndex].getPositions().get(i).get(0);
                Assert.fail("Expected exception");
            } catch (RuntimeException nsee) {
                // Everything is ok: RuntimeException of type
                // RuntimeException is expected.
            }
        }
    }


    private static final String first40HighCardResults =
        "[Promotions].[Bag Stuffers]"
        + "[Promotions].[Best Savings]"
        + "[Promotions].[Big Promo]"
        + "[Promotions].[Big Time Discounts]"
        + "[Promotions].[Big Time Savings]"
        + "[Promotions].[Bye Bye Baby]"
        + "[Promotions].[Cash Register Lottery]"
        + "[Promotions].[Coupon Spectacular]"
        + "[Promotions].[Dimes Off]"
        + "[Promotions].[Dollar Cutters]"
        + "[Promotions].[Dollar Days]"
        + "[Promotions].[Double Down Sale]"
        + "[Promotions].[Double Your Savings]"
        + "[Promotions].[Fantastic Discounts]"
        + "[Promotions].[Free For All]"
        + "[Promotions].[Go For It]"
        + "[Promotions].[Green Light Days]"
        + "[Promotions].[Green Light Special]"
        + "[Promotions].[High Roller Savings]"
        + "[Promotions].[I Cant Believe It Sale]"
        + "[Promotions].[Money Grabbers]"
        + "[Promotions].[Money Savers]"
        + "[Promotions].[Mystery Sale]"
        + "[Promotions].[No Promotion]"
        + "[Promotions].[One Day Sale]"
        + "[Promotions].[Pick Your Savings]"
        + "[Promotions].[Price Cutters]"
        + "[Promotions].[Price Destroyers]"
        + "[Promotions].[Price Savers]"
        + "[Promotions].[Price Slashers]"
        + "[Promotions].[Price Smashers]"
        + "[Promotions].[Price Winners]"
        + "[Promotions].[Sale Winners]"
        + "[Promotions].[Sales Days]"
        + "[Promotions].[Sales Galore]"
        + "[Promotions].[Save-It Sale]"
        + "[Promotions].[Saving Days]"
        + "[Promotions].[Savings Galore]"
        + "[Promotions].[Shelf Clearing Days]"
        + "[Promotions].[Shelf Emptiers]";

    private static final String nonEmptyHighCardResults =
        "[Promotions].[Bag Stuffers]"
        + "[Promotions].[Best Savings]"
        + "[Promotions].[Big Promo]"
        + "[Promotions].[Big Time Discounts]"
        + "[Promotions].[Big Time Savings]"
        + "[Promotions].[Bye Bye Baby]"
        + "[Promotions].[Cash Register Lottery]"
        + "[Promotions].[Dimes Off]"
        + "[Promotions].[Dollar Cutters]"
        + "[Promotions].[Dollar Days]"
        + "[Promotions].[Double Down Sale]"
        + "[Promotions].[Double Your Savings]"
        + "[Promotions].[Free For All]"
        + "[Promotions].[Go For It]"
        + "[Promotions].[Green Light Days]"
        + "[Promotions].[Green Light Special]"
        + "[Promotions].[High Roller Savings]"
        + "[Promotions].[I Cant Believe It Sale]"
        + "[Promotions].[Money Savers]"
        + "[Promotions].[Mystery Sale]"
        + "[Promotions].[No Promotion]"
        + "[Promotions].[One Day Sale]"
        + "[Promotions].[Pick Your Savings]"
        + "[Promotions].[Price Cutters]"
        + "[Promotions].[Price Destroyers]"
        + "[Promotions].[Price Savers]"
        + "[Promotions].[Price Slashers]"
        + "[Promotions].[Price Smashers]"
        + "[Promotions].[Price Winners]"
        + "[Promotions].[Sale Winners]"
        + "[Promotions].[Sales Days]"
        + "[Promotions].[Sales Galore]"
        + "[Promotions].[Save-It Sale]"
        + "[Promotions].[Saving Days]"
        + "[Promotions].[Savings Galore]"
        + "[Promotions].[Shelf Clearing Days]"
        + "[Promotions].[Shelf Emptiers]"
        + "[Promotions].[Super Duper Savers]"
        + "[Promotions].[Super Savers]"
        + "[Promotions].[Super Wallet Savers]"
        + "[Promotions].[Three for One]"
        + "[Promotions].[Tip Top Savings]"
        + "[Promotions].[Two Day Sale]"
        + "[Promotions].[Two for One]"
        + "[Promotions].[Unbeatable Price Savers]"
        + "[Promotions].[Wallet Savers]"
        + "[Promotions].[Weekend Markdown]"
        + "[Promotions].[You Save Days]";

    private static final String highCardResults =
        "[Promotions].[Bag Stuffers]"
        + "[Promotions].[Best Savings]"
        + "[Promotions].[Big Promo]"
        + "[Promotions].[Big Time Discounts]"
        + "[Promotions].[Big Time Savings]"
        + "[Promotions].[Bye Bye Baby]"
        + "[Promotions].[Cash Register Lottery]"
        + "[Promotions].[Coupon Spectacular]"
        + "[Promotions].[Dimes Off]"
        + "[Promotions].[Dollar Cutters]"
        + "[Promotions].[Dollar Days]"
        + "[Promotions].[Double Down Sale]"
        + "[Promotions].[Double Your Savings]"
        + "[Promotions].[Fantastic Discounts]"
        + "[Promotions].[Free For All]"
        + "[Promotions].[Go For It]"
        + "[Promotions].[Green Light Days]"
        + "[Promotions].[Green Light Special]"
        + "[Promotions].[High Roller Savings]"
        + "[Promotions].[I Cant Believe It Sale]"
        + "[Promotions].[Money Grabbers]"
        + "[Promotions].[Money Savers]"
        + "[Promotions].[Mystery Sale]"
        + "[Promotions].[No Promotion]"
        + "[Promotions].[One Day Sale]"
        + "[Promotions].[Pick Your Savings]"
        + "[Promotions].[Price Cutters]"
        + "[Promotions].[Price Destroyers]"
        + "[Promotions].[Price Savers]"
        + "[Promotions].[Price Slashers]"
        + "[Promotions].[Price Smashers]"
        + "[Promotions].[Price Winners]"
        + "[Promotions].[Sale Winners]"
        + "[Promotions].[Sales Days]"
        + "[Promotions].[Sales Galore]"
        + "[Promotions].[Save-It Sale]"
        + "[Promotions].[Saving Days]"
        + "[Promotions].[Savings Galore]"
        + "[Promotions].[Shelf Clearing Days]"
        + "[Promotions].[Shelf Emptiers]"
        + "[Promotions].[Super Duper Savers]"
        + "[Promotions].[Super Savers]"
        + "[Promotions].[Super Wallet Savers]"
        + "[Promotions].[Three for One]"
        + "[Promotions].[Tip Top Savings]"
        + "[Promotions].[Two Day Sale]"
        + "[Promotions].[Two for One]"
        + "[Promotions].[Unbeatable Price Savers]"
        + "[Promotions].[Wallet Savers]"
        + "[Promotions].[Weekend Markdown]"
        + "[Promotions].[You Save Days]";

    private static final String moreThan4000highCardResults =
        "[Promotions].[Cash Register Lottery]"
        + "[Promotions].[No Promotion]"
        + "[Promotions].[Price Savers]";

    private static final String moreThan4000Cells =
        "4792.0195448.04094.0";

    private static final String nonEmptyCells =
        "901.02081.01789.0932.0700.0921.04792.01219.0"
        + "781.01652.01959.0843.01638.0689.01607.0436.0"
        + "2654.0253.0899.01021.0195448.01973.0323.01624.0"
        + "2173.04094.01148.0504.01294.0444.02055.02572.0"
        + "2203.01446.01382.0754.02118.02628.02497.01183.0"
        + "1155.0525.02053.0335.02100.0916.0914.03145.0";

    private static final String topcount40Cells =
        "195448.04792.04094.03145.02654.02628.02572.02497.0"
        + "2203.02173.02118.02100.02081.02055.02053.01973.0"
        + "1959.01789.01652.01638.01624.01607.01446.01382.0"
        + "1294.01219.01183.01155.01148.01021.0932.0921.0"
        + "916.0914.0901.0899.0843.0781.0754.0700.0";

    private static final String topcount41Cells =
        "195448.04792.04094.03145.02654.02628.02572.02497.02203.0"
        + "2173.02118.02100.02081.02055.02053.01973.01959.01789.0"
        + "1652.01638.01624.01607.01446.01382.01294.01219.01183.0"
        + "1155.01148.01021.0932.0921.0916.0914.0901.0899.0843.0781"
        + ".0754.0700.0689.0";
}

// End HighDimensionsTest.java

