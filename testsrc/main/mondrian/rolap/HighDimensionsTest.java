/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2007 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import mondrian.calc.ResultStyle;
import mondrian.olap.*;
import mondrian.test.FoodMartTestCase;
import mondrian.test.TestContext;
import mondrian.util.Bug;
import junit.framework.Assert;

import java.util.*;
import java.lang.ref.*;

/**
 * Unit-test for non cacheable elementos of high dimensions.
 *
 * @author jlopez, lcanals
 * @version $Id$
 * @since May, 2008
 */
public class HighDimensionsTest extends FoodMartTestCase {
    public HighDimensionsTest() {
    }

    public HighDimensionsTest(String name) {
        super(name);
    }

    public void testBug1971406() throws Exception {
        final Connection connection = TestContext.instance()
            .getFoodMartConnection();
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
        assertTrue(t1 - t0 < 60000);
    }


    public void testPromotionsTwoDimensions() throws Exception {
        if (!Bug.Bug2446228Fixed) {
            return;
        }
        execHighCardTest("select {[Measures].[Unit Sales]} on columns,\n"
                    + "{[Promotions].[Promotion Name].Members} on rows\n"
                    + "from [Sales Ragged]", 1, "Promotions",
                    highCardResults, null, true);
    }


    public void testHead() throws Exception {
        if (!Bug.Bug2446228Fixed) {
            return;
        }
        execHighCardTest("select {[Measures].[Unit Sales]} on columns,\n"
                    + "head({[Promotions].[Promotion Name].Members},40) "
                    + "on rows from [Sales Ragged]", 1, "Promotions",
                    first40HighCardResults, null, true);
    }

    public void testTopCount() throws Exception {
        final Connection connection = TestContext.instance()
            .getFoodMartConnection();
        final StringBuffer buffer = new StringBuffer();
        Query query = connection.parseQuery(
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

        execHighCardTest("select {[Measures].[Unit Sales]} on columns,\n"
                    + "TopCount({[Promotions].[Promotion Name].Members},40, "
                    + "[Measures].[Unit Sales]) "
                    + "on rows from [Sales Ragged]", 1, "Promotions",
                    topcount40HighCardResults, topcount40Cells, false);
        execHighCardTest("select {[Measures].[Unit Sales]} on columns,\n"
                    + "TopCount({[Promotions].[Promotion Name].Members},41, "
                    + "[Measures].[Unit Sales]) "
                    + "on rows from [Sales Ragged]", 1, "Promotions",
                    topcount41HighCardResults, topcount41Cells, false);
        execHighCardTest("select {[Measures].[Unit Sales]} on columns,\n"
                    + "TopCount({[Promotions].[Promotion Name].Members},40, "
                    + "[Measures].[Unit Sales]) "
                    + "on rows from [Sales Ragged]", 1, "Promotions",
                    topcount40HighCardResults, topcount40Cells, false);
    }


    public void testNonEmpty() throws Exception {
        if (!Bug.Bug2446228Fixed) {
            return;
        }
        execHighCardTest("select {[Measures].[Unit Sales]} on columns,\n"
                    + "non empty {[Promotions].[Promotion Name].Members} "
                    + "on rows from [Sales Ragged]", 1, "Promotions",
                    nonEmptyHighCardResults, nonEmptyCells, true);
    }

    public void testFilter() throws Exception {
         execHighCardTest("select [Measures].[Unit Sales] on columns, "
                    + "filter([Promotions].[Promotion Name].Members, "
                    + "[Measures].[Unit Sales]>0) "
                    + "on rows from [Sales Ragged]", 1, "Promotions",
                    nonEmptyHighCardResults, nonEmptyCells, true);
         execHighCardTest("select [Measures].[Unit Sales] on columns, "
                    + "filter([Promotions].[Promotion Name].Members, "
                    + "[Measures].[Unit Sales]>4000) "
                    + "on rows from [Sales Ragged]", 1, "Promotions",
                    moreThan4000highCardResults, moreThan4000Cells , true);
    }





    //
    // Private Stuff --------------------------------------------
    //

    /**
     * Executes query test trying to [Promotions].[Promotion Name] elements
     * into an axis from the results.
     */
    private void execHighCardTest(final String queryString, final int axisIndex,
            final String highDimensionName, final String results,
            final String results2, final boolean shouldForget)
            throws Exception {
        final int old = MondrianProperties.instance()
                    .ResultLimit.get();
        try {
            MondrianProperties.instance().ResultLimit.set(40);
            final TestContext testContext = TestContext.createSubstitutingCube(
                    "Sales Ragged",
                    "<Dimension name=\"Promotions\" highCardinality=\"true\" "
                    + "foreignKey=\"promotion_id\">"
                        + "<Hierarchy hasAll=\"true\" "
                            + "allMemberName=\"All Promotions\" "
                            + "primaryKey=\"promotion_id\">"
                                + "<Table name=\"promotion\"/>"
                                + "<Level name=\"Promotion Name\" "
                                    + "column=\"promotion_name\" "
                                    + "uniqueMembers=\"true\"/>"
                        + "</Hierarchy>"
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
            for (final Position o :
                result.getAxes()[axisIndex].getPositions()) {
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
                    Assert.fail("Expected runtime exception of type "
                            + "RuntimeException");
                } catch (RuntimeException nsee) {
                    // Everything is ok: RuntimeException of type
                    // RuntimeException is expected.
                }
            }
        } finally {
            MondrianProperties.instance().ResultLimit.set(old);
        }
    }


    private static final String first40HighCardResults =
                 "[Promotions].[All Promotions].[Bag Stuffers]"
                + "[Promotions].[All Promotions].[Best Savings]"
                + "[Promotions].[All Promotions].[Big Promo]"
                + "[Promotions].[All Promotions].[Big Time Discounts]"
                + "[Promotions].[All Promotions].[Big Time Savings]"
                + "[Promotions].[All Promotions].[Bye Bye Baby]"
                + "[Promotions].[All Promotions].[Cash Register Lottery]"
                + "[Promotions].[All Promotions].[Coupon Spectacular]"
                + "[Promotions].[All Promotions].[Dimes Off]"
                + "[Promotions].[All Promotions].[Dollar Cutters]"
                + "[Promotions].[All Promotions].[Dollar Days]"
                + "[Promotions].[All Promotions].[Double Down Sale]"
                + "[Promotions].[All Promotions].[Double Your Savings]"
                + "[Promotions].[All Promotions].[Fantastic Discounts]"
                + "[Promotions].[All Promotions].[Free For All]"
                + "[Promotions].[All Promotions].[Go For It]"
                + "[Promotions].[All Promotions].[Green Light Days]"
                + "[Promotions].[All Promotions].[Green Light Special]"
                + "[Promotions].[All Promotions].[High Roller Savings]"
                + "[Promotions].[All Promotions].[I Cant Believe It Sale]"
                + "[Promotions].[All Promotions].[Money Grabbers]"
                + "[Promotions].[All Promotions].[Money Savers]"
                + "[Promotions].[All Promotions].[Mystery Sale]"
                + "[Promotions].[All Promotions].[No Promotion]"
                + "[Promotions].[All Promotions].[One Day Sale]"
                + "[Promotions].[All Promotions].[Pick Your Savings]"
                + "[Promotions].[All Promotions].[Price Cutters]"
                + "[Promotions].[All Promotions].[Price Destroyers]"
                + "[Promotions].[All Promotions].[Price Savers]"
                + "[Promotions].[All Promotions].[Price Slashers]"
                + "[Promotions].[All Promotions].[Price Smashers]"
                + "[Promotions].[All Promotions].[Price Winners]"
                + "[Promotions].[All Promotions].[Sale Winners]"
                + "[Promotions].[All Promotions].[Sales Days]"
                + "[Promotions].[All Promotions].[Sales Galore]"
                + "[Promotions].[All Promotions].[Save-It Sale]"
                + "[Promotions].[All Promotions].[Saving Days]"
                + "[Promotions].[All Promotions].[Savings Galore]"
                + "[Promotions].[All Promotions].[Shelf Clearing Days]"
                + "[Promotions].[All Promotions].[Shelf Emptiers]";
    private static final String nonEmptyHighCardResults =
                "[Promotions].[All Promotions].[Bag Stuffers]"
                + "[Promotions].[All Promotions].[Best Savings]"
                + "[Promotions].[All Promotions].[Big Promo]"
                + "[Promotions].[All Promotions].[Big Time Discounts]"
                + "[Promotions].[All Promotions].[Big Time Savings]"
                + "[Promotions].[All Promotions].[Bye Bye Baby]"
                + "[Promotions].[All Promotions].[Cash Register Lottery]"
                + "[Promotions].[All Promotions].[Dimes Off]"
                + "[Promotions].[All Promotions].[Dollar Cutters]"
                + "[Promotions].[All Promotions].[Dollar Days]"
                + "[Promotions].[All Promotions].[Double Down Sale]"
                + "[Promotions].[All Promotions].[Double Your Savings]"
                + "[Promotions].[All Promotions].[Free For All]"
                + "[Promotions].[All Promotions].[Go For It]"
                + "[Promotions].[All Promotions].[Green Light Days]"
                + "[Promotions].[All Promotions].[Green Light Special]"
                + "[Promotions].[All Promotions].[High Roller Savings]"
                + "[Promotions].[All Promotions].[I Cant Believe It Sale]"
                + "[Promotions].[All Promotions].[Money Savers]"
                + "[Promotions].[All Promotions].[Mystery Sale]"
                + "[Promotions].[All Promotions].[No Promotion]"
                + "[Promotions].[All Promotions].[One Day Sale]"
                + "[Promotions].[All Promotions].[Pick Your Savings]"
                + "[Promotions].[All Promotions].[Price Cutters]"
                + "[Promotions].[All Promotions].[Price Destroyers]"
                + "[Promotions].[All Promotions].[Price Savers]"
                + "[Promotions].[All Promotions].[Price Slashers]"
                + "[Promotions].[All Promotions].[Price Smashers]"
                + "[Promotions].[All Promotions].[Price Winners]"
                + "[Promotions].[All Promotions].[Sale Winners]"
                + "[Promotions].[All Promotions].[Sales Days]"
                + "[Promotions].[All Promotions].[Sales Galore]"
                + "[Promotions].[All Promotions].[Save-It Sale]"
                + "[Promotions].[All Promotions].[Saving Days]"
                + "[Promotions].[All Promotions].[Savings Galore]"
                + "[Promotions].[All Promotions].[Shelf Clearing Days]"
                + "[Promotions].[All Promotions].[Shelf Emptiers]"
                + "[Promotions].[All Promotions].[Super Duper Savers]"
                + "[Promotions].[All Promotions].[Super Savers]"
                + "[Promotions].[All Promotions].[Super Wallet Savers]"
                + "[Promotions].[All Promotions].[Three for One]"
                + "[Promotions].[All Promotions].[Tip Top Savings]"
                + "[Promotions].[All Promotions].[Two Day Sale]"
                + "[Promotions].[All Promotions].[Two for One]"
                + "[Promotions].[All Promotions].[Unbeatable Price Savers]"
                + "[Promotions].[All Promotions].[Wallet Savers]"
                + "[Promotions].[All Promotions].[Weekend Markdown]"
                + "[Promotions].[All Promotions].[You Save Days]";


    private static final String highCardResults =
                "[Promotions].[All Promotions].[Bag Stuffers]"
                + "[Promotions].[All Promotions].[Best Savings]"
                + "[Promotions].[All Promotions].[Big Promo]"
                + "[Promotions].[All Promotions].[Big Time Discounts]"
                + "[Promotions].[All Promotions].[Big Time Savings]"
                + "[Promotions].[All Promotions].[Bye Bye Baby]"
                + "[Promotions].[All Promotions].[Cash Register Lottery]"
                + "[Promotions].[All Promotions].[Coupon Spectacular]"
                + "[Promotions].[All Promotions].[Dimes Off]"
                + "[Promotions].[All Promotions].[Dollar Cutters]"
                + "[Promotions].[All Promotions].[Dollar Days]"
                + "[Promotions].[All Promotions].[Double Down Sale]"
                + "[Promotions].[All Promotions].[Double Your Savings]"
                + "[Promotions].[All Promotions].[Fantastic Discounts]"
                + "[Promotions].[All Promotions].[Free For All]"
                + "[Promotions].[All Promotions].[Go For It]"
                + "[Promotions].[All Promotions].[Green Light Days]"
                + "[Promotions].[All Promotions].[Green Light Special]"
                + "[Promotions].[All Promotions].[High Roller Savings]"
                + "[Promotions].[All Promotions].[I Cant Believe It Sale]"
                + "[Promotions].[All Promotions].[Money Grabbers]"
                + "[Promotions].[All Promotions].[Money Savers]"
                + "[Promotions].[All Promotions].[Mystery Sale]"
                + "[Promotions].[All Promotions].[No Promotion]"
                + "[Promotions].[All Promotions].[One Day Sale]"
                + "[Promotions].[All Promotions].[Pick Your Savings]"
                + "[Promotions].[All Promotions].[Price Cutters]"
                + "[Promotions].[All Promotions].[Price Destroyers]"
                + "[Promotions].[All Promotions].[Price Savers]"
                + "[Promotions].[All Promotions].[Price Slashers]"
                + "[Promotions].[All Promotions].[Price Smashers]"
                + "[Promotions].[All Promotions].[Price Winners]"
                + "[Promotions].[All Promotions].[Sale Winners]"
                + "[Promotions].[All Promotions].[Sales Days]"
                + "[Promotions].[All Promotions].[Sales Galore]"
                + "[Promotions].[All Promotions].[Save-It Sale]"
                + "[Promotions].[All Promotions].[Saving Days]"
                + "[Promotions].[All Promotions].[Savings Galore]"
                + "[Promotions].[All Promotions].[Shelf Clearing Days]"
                + "[Promotions].[All Promotions].[Shelf Emptiers]"
                + "[Promotions].[All Promotions].[Super Duper Savers]"
                + "[Promotions].[All Promotions].[Super Savers]"
                + "[Promotions].[All Promotions].[Super Wallet Savers]"
                + "[Promotions].[All Promotions].[Three for One]"
                + "[Promotions].[All Promotions].[Tip Top Savings]"
                + "[Promotions].[All Promotions].[Two Day Sale]"
                + "[Promotions].[All Promotions].[Two for One]"
                + "[Promotions].[All Promotions].[Unbeatable Price Savers]"
                + "[Promotions].[All Promotions].[Wallet Savers]"
                + "[Promotions].[All Promotions].[Weekend Markdown]"
                + "[Promotions].[All Promotions].[You Save Days]";

    private static final String moreThan4000highCardResults =
                "[Promotions].[All Promotions].[Cash Register Lottery]"
                + "[Promotions].[All Promotions].[No Promotion]"
                + "[Promotions].[All Promotions].[Price Savers]";


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
