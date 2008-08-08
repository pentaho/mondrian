/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2008 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import mondrian.calc.ResultStyle;
import mondrian.olap.*;
import mondrian.olap.*;
import mondrian.test.FoodMartTestCase;
import mondrian.test.TestContext;
import mondrian.test.DiffRepository;

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
        execHighCardTest("select {[Measures].[Unit Sales]} on columns,\n"
                    + "{[Promotions].[Promotion Name].Members} on rows\n"
                    + "from [Sales Ragged]", 1, "Promotions",
                    highCardResults);
    }


    public void testHead() throws Exception {
        execHighCardTest("select {[Measures].[Unit Sales]} on columns,\n"
                    + "head({[Promotions].[Promotion Name].Members},40) "
                    + "on rows from [Sales Ragged]", 1, "Promotions",
                    first40HighCardResults);
    }


    public void testNonEmpty() throws Exception {
        execHighCardTest("select {[Measures].[Unit Sales]} on columns,\n"
                    + "non empty {[Promotions].[Promotion Name].Members} "
                    + "on rows from [Sales Ragged]", 1, "Promotions",
                    nonEmptyHighCardResults);
    }


    public void testFilter() throws Exception {
         execHighCardTest("select {[Measures].[Unit Sales]} on columns, "
                    + "{filter([Promotions].[Promotion Name].Members, "
                    + "[Measures].[Unit Sales]>0)} "
                    + "on rows from [Sales Ragged]", 1, "Promotions",
                    nonEmptyHighCardResults);
           execHighCardTest("select {[Measures].[Unit Sales]} on columns, "
                    + "{filter([Promotions].[Promotion Name].Members, "
                    + "[Measures].[Unit Sales]>4000)} "
                    + "on rows from [Sales Ragged]", 1, "Promotions",
                    moreThan4000highCardResults);
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
        final String results)
        throws Exception
    {
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

            final List<SoftReference> softReferences =
                    new ArrayList<SoftReference>();
            // Tests results aren't got from database before this point
            int ii = 0;
            for (final Position o : result.getAxes()[axisIndex].getPositions()) {
                assertNotNull(o.get(0));
                ii++;
                softReferences.add(new SoftReference(o.get(0)));
                buffer.append(o.get(0).toString());
            }
            assertEquals(buffer.toString(), results);
            buffer = null;

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

            for (int i = 5; i < 10 && i < ii; i++) {
              try {
                    result.getAxes()[axisIndex].getPositions().get(0).get(0);
                    assert false;
                } catch (RuntimeException nsee) {
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
}

// End HighDimensionsTest.java
