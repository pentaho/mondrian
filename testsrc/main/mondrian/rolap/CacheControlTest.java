/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (c) 2002-2015 Pentaho Corporation..  All rights reserved.
*/
package mondrian.rolap;

import mondrian.olap.*;
import mondrian.olap.CacheControl.CellRegion;
import mondrian.test.*;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

/**
 * Unit-test for cache-flushing functionality.
 *
 * @author jhyde
 * @since Sep 27, 2006
 */
public class CacheControlTest extends FoodMartTestCase {
    /**
     * Creates a CacheControlTest.
     */
    public CacheControlTest() {
    }

    /**
     * Creates a CacheControlTest with the given name.
     */
    public CacheControlTest(String name) {
        super(name);
    }

    /**
     * Returns the repository of result strings.
     * @return repository of result strings
     */
    DiffRepository getDiffRepos() {
        return DiffRepository.lookup(CacheControlTest.class);
    }

    /**
     * Flushes the entire contents of the cache. Utility method used to ensure
     * that cache control tests are starting with a blank page.
     *
     * @param testContext Test context
     */
    public static void flushCache(TestContext testContext) {
        final CacheControl cacheControl = testContext.getCacheControl();

        // Flush the entire cache.
        CacheControl.CellRegion measuresRegion = null;
        for (Cube cube
            : testContext.getConnection().getSchema().getCubes())
        {
            measuresRegion =
                cacheControl.createMeasuresRegion(cube);
          cacheControl.flush(measuresRegion);
        }

        // Check the cache is empty.
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        cacheControl.printCacheState(pw, measuresRegion);
        pw.flush();
        assertEquals("", sw.toString());
    }

    /**
     * Asserts that a cache state string is equal to an expected cache state,
     * after segment ids have been masked out.
     *
     * @param tag Tag of resource in diff repository
     * @param expected Expected state
     * @param actual Actual state
     */
    private void assertCacheStateEquals(
        String tag, String expected, String actual)
    {
        String expected2 = expected.replaceAll("Segment #[0-9]+", "Segment ##");
        String actual2 = actual.replaceAll("Segment #[0-9]+", "Segment ##");
        getDiffRepos().assertEquals(tag, expected2, actual2);
    }

    /**
     * Runs a simple query an asserts that the results are as expected.
     *
     * @param testContext Test context
     */
    private void standardQuery(TestContext testContext) {
        testContext.assertQueryReturns(
            "select {[Time].[Time].Members} on columns,\n"
            + " {[Product].Children} on rows\n"
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Time].[1997]}\n"
            + "{[Time].[1997].[Q1]}\n"
            + "{[Time].[1997].[Q1].[1]}\n"
            + "{[Time].[1997].[Q1].[2]}\n"
            + "{[Time].[1997].[Q1].[3]}\n"
            + "{[Time].[1997].[Q2]}\n"
            + "{[Time].[1997].[Q2].[4]}\n"
            + "{[Time].[1997].[Q2].[5]}\n"
            + "{[Time].[1997].[Q2].[6]}\n"
            + "{[Time].[1997].[Q3]}\n"
            + "{[Time].[1997].[Q3].[7]}\n"
            + "{[Time].[1997].[Q3].[8]}\n"
            + "{[Time].[1997].[Q3].[9]}\n"
            + "{[Time].[1997].[Q4]}\n"
            + "{[Time].[1997].[Q4].[10]}\n"
            + "{[Time].[1997].[Q4].[11]}\n"
            + "{[Time].[1997].[Q4].[12]}\n"
            + "{[Time].[1998]}\n"
            + "{[Time].[1998].[Q1]}\n"
            + "{[Time].[1998].[Q1].[1]}\n"
            + "{[Time].[1998].[Q1].[2]}\n"
            + "{[Time].[1998].[Q1].[3]}\n"
            + "{[Time].[1998].[Q2]}\n"
            + "{[Time].[1998].[Q2].[4]}\n"
            + "{[Time].[1998].[Q2].[5]}\n"
            + "{[Time].[1998].[Q2].[6]}\n"
            + "{[Time].[1998].[Q3]}\n"
            + "{[Time].[1998].[Q3].[7]}\n"
            + "{[Time].[1998].[Q3].[8]}\n"
            + "{[Time].[1998].[Q3].[9]}\n"
            + "{[Time].[1998].[Q4]}\n"
            + "{[Time].[1998].[Q4].[10]}\n"
            + "{[Time].[1998].[Q4].[11]}\n"
            + "{[Time].[1998].[Q4].[12]}\n"
            + "Axis #2:\n"
            + "{[Product].[Drink]}\n"
            + "{[Product].[Food]}\n"
            + "{[Product].[Non-Consumable]}\n"
            + "Row #0: 24,597\n"
            + "Row #0: 5,976\n"
            + "Row #0: 1,910\n"
            + "Row #0: 1,951\n"
            + "Row #0: 2,115\n"
            + "Row #0: 5,895\n"
            + "Row #0: 1,948\n"
            + "Row #0: 2,039\n"
            + "Row #0: 1,908\n"
            + "Row #0: 6,065\n"
            + "Row #0: 2,205\n"
            + "Row #0: 1,921\n"
            + "Row #0: 1,939\n"
            + "Row #0: 6,661\n"
            + "Row #0: 1,898\n"
            + "Row #0: 2,344\n"
            + "Row #0: 2,419\n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #1: 191,940\n"
            + "Row #1: 47,809\n"
            + "Row #1: 15,604\n"
            + "Row #1: 15,142\n"
            + "Row #1: 17,063\n"
            + "Row #1: 44,825\n"
            + "Row #1: 14,393\n"
            + "Row #1: 15,055\n"
            + "Row #1: 15,377\n"
            + "Row #1: 47,440\n"
            + "Row #1: 17,036\n"
            + "Row #1: 15,741\n"
            + "Row #1: 14,663\n"
            + "Row #1: 51,866\n"
            + "Row #1: 14,232\n"
            + "Row #1: 18,278\n"
            + "Row #1: 19,356\n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #2: 50,236\n"
            + "Row #2: 12,506\n"
            + "Row #2: 4,114\n"
            + "Row #2: 3,864\n"
            + "Row #2: 4,528\n"
            + "Row #2: 11,890\n"
            + "Row #2: 3,838\n"
            + "Row #2: 3,987\n"
            + "Row #2: 4,065\n"
            + "Row #2: 12,343\n"
            + "Row #2: 4,522\n"
            + "Row #2: 4,035\n"
            + "Row #2: 3,786\n"
            + "Row #2: 13,497\n"
            + "Row #2: 3,828\n"
            + "Row #2: 4,648\n"
            + "Row #2: 5,021\n"
            + "Row #2: \n"
            + "Row #2: \n"
            + "Row #2: \n"
            + "Row #2: \n"
            + "Row #2: \n"
            + "Row #2: \n"
            + "Row #2: \n"
            + "Row #2: \n"
            + "Row #2: \n"
            + "Row #2: \n"
            + "Row #2: \n"
            + "Row #2: \n"
            + "Row #2: \n"
            + "Row #2: \n"
            + "Row #2: \n"
            + "Row #2: \n"
            + "Row #2: \n");
    }

    // ---------------------
    // Tests

    /**
     * Tests creation of a cell region against an abstract implementation of
     * {@link CacheControl}.
     */
    public void testCreateCellRegion() {
        // Execute a query.
        final TestContext testContext = getTestContext();
        final RolapConnection connection =
            ((RolapConnection) testContext.getConnection());
        final CacheControl cacheControl = new CacheControlImpl(connection);
        final CacheControl.CellRegion region =
            createCellRegion(testContext, cacheControl);
        assertNotNull(region);
    }

    /**
     * Creates a cell region, runs a query, then flushes the cache.
     */
    public void testNormalize2() {
        // Execute a query.
        final TestContext testContext = getTestContext();

        final CacheControl cacheControl = testContext.getCacheControl();

        final CacheControl.CellRegion region =
            createCellRegion(testContext, cacheControl);

        CacheControlImpl.CellRegion normalizedRegion =
            ((CacheControlImpl) cacheControl).normalize(
                (CacheControlImpl.CellRegionImpl) region);
        assertEquals(
            "Union("
            + "Crossjoin("
            + "Member([Product].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer]), "
            + "Member([Time].[1997].[Q1]), "
            + "Member([Measures].[Unit Sales], [Measures].[Store Cost], [Measures].[Store Sales], [Measures].[Sales Count], [Measures].[Customer Count], [Measures].[Promotion Sales])), "
            + "Crossjoin("
            + "Member([Product].[Drink].[Dairy]), "
            + "Member([Time].[1997].[Q1]), "
            + "Member([Measures].[Unit Sales], [Measures].[Store Cost], [Measures].[Store Sales], [Measures].[Sales Count], [Measures].[Customer Count], [Measures].[Promotion Sales])))",
            normalizedRegion.toString());
    }

    /**
     * Creates a cell region, runs a query, then flushes the cache.
     */
    public void testFlush() {
        assertQueryReturns(
            "SELECT {[Product].[Product Department].MEMBERS} ON AXIS(0),\n"
            + "{{[Gender].[Gender].MEMBERS}, {[Gender].[All Gender]}} ON AXIS(1)\n"
            + "FROM [Sales 2] WHERE {[Measures].[Unit Sales]}",
            "Axis #0:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #1:\n"
            + "{[Product].[Drink].[Alcoholic Beverages]}\n"
            + "{[Product].[Drink].[Beverages]}\n"
            + "{[Product].[Drink].[Dairy]}\n"
            + "{[Product].[Food].[Baked Goods]}\n"
            + "{[Product].[Food].[Baking Goods]}\n"
            + "{[Product].[Food].[Breakfast Foods]}\n"
            + "{[Product].[Food].[Canned Foods]}\n"
            + "{[Product].[Food].[Canned Products]}\n"
            + "{[Product].[Food].[Dairy]}\n"
            + "{[Product].[Food].[Deli]}\n"
            + "{[Product].[Food].[Eggs]}\n"
            + "{[Product].[Food].[Frozen Foods]}\n"
            + "{[Product].[Food].[Meat]}\n"
            + "{[Product].[Food].[Produce]}\n"
            + "{[Product].[Food].[Seafood]}\n"
            + "{[Product].[Food].[Snack Foods]}\n"
            + "{[Product].[Food].[Snacks]}\n"
            + "{[Product].[Food].[Starchy Foods]}\n"
            + "{[Product].[Non-Consumable].[Carousel]}\n"
            + "{[Product].[Non-Consumable].[Checkout]}\n"
            + "{[Product].[Non-Consumable].[Health and Hygiene]}\n"
            + "{[Product].[Non-Consumable].[Household]}\n"
            + "{[Product].[Non-Consumable].[Periodicals]}\n"
            + "Axis #2:\n"
            + "{[Gender].[F]}\n"
            + "{[Gender].[M]}\n"
            + "{[Gender].[All Gender]}\n"
            + "Row #0: 3,439\n"
            + "Row #0: 6,776\n"
            + "Row #0: 1,987\n"
            + "Row #0: 3,771\n"
            + "Row #0: 9,841\n"
            + "Row #0: 1,821\n"
            + "Row #0: 9,407\n"
            + "Row #0: 867\n"
            + "Row #0: 6,513\n"
            + "Row #0: 5,990\n"
            + "Row #0: 2,001\n"
            + "Row #0: 13,011\n"
            + "Row #0: 841\n"
            + "Row #0: 18,713\n"
            + "Row #0: 947\n"
            + "Row #0: 14,936\n"
            + "Row #0: 3,459\n"
            + "Row #0: 2,696\n"
            + "Row #0: 368\n"
            + "Row #0: 887\n"
            + "Row #0: 7,841\n"
            + "Row #0: 13,278\n"
            + "Row #0: 2,168\n"
            + "Row #1: 3,399\n"
            + "Row #1: 6,797\n"
            + "Row #1: 2,199\n"
            + "Row #1: 4,099\n"
            + "Row #1: 10,404\n"
            + "Row #1: 1,496\n"
            + "Row #1: 9,619\n"
            + "Row #1: 945\n"
            + "Row #1: 6,372\n"
            + "Row #1: 6,047\n"
            + "Row #1: 2,131\n"
            + "Row #1: 13,644\n"
            + "Row #1: 873\n"
            + "Row #1: 19,079\n"
            + "Row #1: 817\n"
            + "Row #1: 15,609\n"
            + "Row #1: 3,425\n"
            + "Row #1: 2,566\n"
            + "Row #1: 473\n"
            + "Row #1: 892\n"
            + "Row #1: 8,443\n"
            + "Row #1: 13,760\n"
            + "Row #1: 2,126\n"
            + "Row #2: 6,838\n"
            + "Row #2: 13,573\n"
            + "Row #2: 4,186\n"
            + "Row #2: 7,870\n"
            + "Row #2: 20,245\n"
            + "Row #2: 3,317\n"
            + "Row #2: 19,026\n"
            + "Row #2: 1,812\n"
            + "Row #2: 12,885\n"
            + "Row #2: 12,037\n"
            + "Row #2: 4,132\n"
            + "Row #2: 26,655\n"
            + "Row #2: 1,714\n"
            + "Row #2: 37,792\n"
            + "Row #2: 1,764\n"
            + "Row #2: 30,545\n"
            + "Row #2: 6,884\n"
            + "Row #2: 5,262\n"
            + "Row #2: 841\n"
            + "Row #2: 1,779\n"
            + "Row #2: 16,284\n"
            + "Row #2: 27,038\n"
            + "Row #2: 4,294\n");
        if (MondrianProperties.instance().DisableCaching.get()) {
            return;
        }

        final TestContext testContext = getTestContext();
        flushCache(testContext);

        // Make sure MaxConstraint is high enough
        int minConstraints = 3;

        if (MondrianProperties.instance().MaxConstraints.get()
            < minConstraints)
        {
            propSaver.set(
                MondrianProperties.instance().MaxConstraints,
                minConstraints);
        }

        // Execute a query, to bring data into the cache.
        standardQuery(testContext);

        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        final CacheControl cacheControl =
            testContext.getConnection().getCacheControl(pw);

        // Flush the cache. This time, flush is successful.
        final CacheControl.CellRegion region =
            createCellRegion(testContext, cacheControl);
        cacheControl.flush(region);
        pw.flush();
        String tag = "output";
        String expected = "${output}";
        String actual = sw.toString();
        assertCacheStateEquals(tag, expected, actual);

        // Run query again, then inspect the contents of the cache.
        standardQuery(testContext);
        sw.getBuffer().setLength(0);
        cacheControl.printCacheState(pw, region);
        pw.flush();
        assertCacheStateEquals("output2", "${output2}", sw.toString());
    }

    /**
     * Creates a partial cell region, runs a query, then flushes the cache.
     */
    public void testPartialFlush() {
        if (MondrianProperties.instance().DisableCaching.get()) {
            return;
        }

        final TestContext testContext = getTestContext();
        flushCache(testContext);

        // Execute a query.
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        final CacheControl cacheControl =
            testContext.getConnection().getCacheControl(pw);

        // Create a region ([Measures].[Unit Sales], [Time].[1997].[Q1])
        final CacheControl.CellRegion region =
            createCellRegion1997_Q1_UnitSales(testContext, cacheControl);

        // Execute a query, to bring data into the cache.
        standardQuery(testContext);

        // This time, flush is successful.
        // The segment on "year" is entirely flushed.
        // The segment on "year", "quarter" has "Q1" masked out.
        // The segment on "year", "quarter", "month" has "Q1" masked out.
        cacheControl.flush(region);
        pw.flush();
        assertCacheStateEquals("output", "${output}", sw.toString());

        // Flush the same region again. Should be the same result.
        sw.getBuffer().setLength(0);
        cacheControl.flush(region);
        pw.flush();
        assertCacheStateEquals("output2", "${output2}", sw.toString());

        // Create the region ([Time].[1997])
        final CacheControl.CellRegion region2 =
            createCellRegion1997(testContext, cacheControl);

        // Flush a different region. Everything is in 1997, so the entire cache
        // is emptied.
        sw.getBuffer().setLength(0);
        cacheControl.flush(region2);
        pw.flush();
        assertCacheStateEquals("output3", "${output3}", sw.toString());

        // Create the region ([Gender].[F], [Product].[Drink] :
        // [Product].[Food])
        final CacheControl.CellRegion region3 =
            createCellRegionFemaleFoodDrink(testContext, cacheControl);

        // Flush a different region.
        sw.getBuffer().setLength(0);
        cacheControl.flush(region3);
        pw.flush();
        assertCacheStateEquals("output4", "${output4}", sw.toString());

        // Run query again, just to make sure.
        standardQuery(testContext);
    }

    /**
     * This is a test for
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-1120">MONDRIAN-1120</a>
     * <p>SegmentCacheIndexImpl.intersects was not comparing the
     * header column values to those of the cache region.
     */
    public void testPartialFlush_2() throws Exception {
        if (MondrianProperties.instance().DisableCaching.get()) {
            return;
        }

        final TestContext testContext = getTestContext();
        final StringWriter sw = new StringWriter();
        final PrintWriter pw = new PrintWriter(sw);
        final CacheControl cacheControl =
            testContext.getConnection().getCacheControl(pw);

        final CacheControl.CellRegion regionF =
            createCellRegionFemale(testContext, cacheControl);
        final CacheControl.CellRegion regionM =
            createCellRegionMale(testContext, cacheControl);

        flushCache(testContext);

        executeQuery(
            "select {[Measures].[Unit Sales]} on columns, {[Gender].[M]} on rows from [Sales]");

        sw.getBuffer().setLength(0);
        cacheControl.flush(regionF);
        pw.flush();
        assertCacheStateEquals("output", "${output}", sw.toString());

        sw.getBuffer().setLength(0);
        cacheControl.flush(regionM);
        pw.flush();
        assertCacheStateEquals("output2", "${output2}", sw.toString());
    }

    /**
     * Creates a partial cell region over a range, runs a query, then flushes
     * the cache.
     */
    public void testPartialFlushRange() {
        if (MondrianProperties.instance().DisableCaching.get()) {
            return;
        }

        final TestContext testContext = getTestContext();
        flushCache(testContext);

        // Execute a query.
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        final CacheControl cacheControl =
            testContext.getConnection().getCacheControl(pw);

        // Create a region
        //  ([Measures].[Unit Sales],
        //   [Time].[1997].[Q2].[4] .. infinity)
        final CacheControl.CellRegion region =
            createCellRegionAprilOnwards(testContext, cacheControl);

        // Execute a query, to bring data into the cache.
        standardQuery(testContext);

        // This time, flush is successful.
        // The segment on "year" is entirely flushed.
        // The segment on "year", "quarter" has "Q2", "Q3", "Q4" masked out.
        // The segment on "year", "quarter", "month" has "Q3", "Q4" masked out,
        //   and "Q2" masked out if month > 6.
        cacheControl.flush(region);
        pw.flush();
        assertCacheStateEquals("output", "${output}", sw.toString());

        // Flush the same region again. Should be the same result.
        sw.getBuffer().setLength(0);
        cacheControl.flush(region);
        pw.flush();
        assertCacheStateEquals("output2", "${output2}", sw.toString());

        // Run query again, then inspect the contents of the cache.
        standardQuery(testContext);
        sw.getBuffer().setLength(0);
        cacheControl.printCacheState(pw, region);
        pw.flush();
        assertCacheStateEquals("output3", "${output3}", sw.toString());
    }

    /**
     * Creates a cell region using a given {@link CacheControl}, and runs some
     * sanity checks.
     *
     * @param testContext Test context
     * @param cacheControl Cache control
     */
    private CacheControl.CellRegion createCellRegion(
        TestContext testContext,
        CacheControl cacheControl)
    {
        // Flush a region of the cache.
        final Connection connection = testContext.getConnection();
        final Cube salesCube = connection.getSchema().lookupCube("Sales", true);

        // Region consists of [Time].[1997].[Q1] and its children, and products
        // [Beer] and [Dairy].
        final SchemaReader schemaReader =
            salesCube.getSchemaReader(null).withLocus();
        final Member memberQ1 = schemaReader.getMemberByUniqueName(
            Id.Segment.toList("Time", "1997", "Q1"), true);
        final Member memberBeer = schemaReader.getMemberByUniqueName(
            Id.Segment.toList(
                "Product", "Drink", "Alcoholic Beverages", "Beer and Wine",
                "Beer"),
            true);
        final Member memberDairy = schemaReader.getMemberByUniqueName(
            Id.Segment.toList("Product", "Drink", "Dairy"), true);

        final CacheControl.CellRegion regionTimeQ1 =
            cacheControl.createMemberRegion(memberQ1, true);
        assertEquals("Member([Time].[1997].[Q1])", regionTimeQ1.toString());

        final CacheControl.CellRegion regionProductBeer =
            cacheControl.createMemberRegion(memberBeer, false);
        assertEquals(
            "Member([Product].[Drink]."
            + "[Alcoholic Beverages].[Beer and Wine].[Beer])",
            regionProductBeer.toString());

        final CacheControl.CellRegion regionProductDairy =
            cacheControl.createMemberRegion(memberDairy, true);

        final CacheControl.CellRegion regionProductUnion =
            cacheControl.createUnionRegion(
                regionProductBeer,
                regionProductDairy);
        assertEquals(
            "Union(Member([Product].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer]), Member([Product].[Drink].[Dairy]))",
            regionProductUnion.toString());

        final CacheControl.CellRegion regionProductXTime =
            cacheControl.createCrossjoinRegion(
                regionProductUnion, regionTimeQ1);
        assertEquals(
            "Crossjoin(Union(Member([Product].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer]), Member([Product].[Drink].[Dairy])), Member([Time].[1997].[Q1]))",
            regionProductXTime.toString());

        try {
            cacheControl.flush(regionProductXTime);
            fail("expceted error");
        } catch (RuntimeException e) {
            assertContains(
                "Region of cells to be flushed must contain measures.",
                e.getMessage());
        }

        final CacheControl.CellRegion measuresRegion =
            cacheControl.createMeasuresRegion(salesCube);
        return cacheControl.createCrossjoinRegion(
            regionProductXTime,
            measuresRegion);
    }

    /**
     * Creates a cell region using a given {@link CacheControl}, containing
     * only [Time].[1997].[Q1] * {Measures}.
     *
     * @param testContext Test context
     * @param cacheControl Cache control
     */
    private CacheControl.CellRegion createCellRegion1997_Q1_UnitSales(
        TestContext testContext,
        CacheControl cacheControl)
    {
        final Connection connection = testContext.getConnection();
        final Cube salesCube = connection.getSchema().lookupCube("Sales", true);

        // Region consists of [Time].[1997].[Q1] and its children.
        final SchemaReader schemaReader =
            salesCube.getSchemaReader(null).withLocus();
        final Member memberQ1 = schemaReader.getMemberByUniqueName(
            Id.Segment.toList("Time", "1997", "Q1"), true);

        final CacheControl.CellRegion regionTimeQ1 =
            cacheControl.createMemberRegion(memberQ1, true);
        assertEquals("Member([Time].[1997].[Q1])", regionTimeQ1.toString());

        final CacheControl.CellRegion measuresRegion =
            cacheControl.createMeasuresRegion(salesCube);
        return cacheControl.createCrossjoinRegion(
            regionTimeQ1,
            measuresRegion);
    }

    /**
     * Creates a cell region using a given {@link CacheControl}, containing
     * only [Time].[1997].[Q1] * {Measures}.
     *
     * @param testContext Test context
     * @param cacheControl Cache control
     */
    private CacheControl.CellRegion createCellRegionAprilOnwards(
        TestContext testContext,
        CacheControl cacheControl)
    {
        final Connection connection = testContext.getConnection();
        final Cube salesCube = connection.getSchema().lookupCube("Sales", true);

        // Region consists of [Time].[1997].[Q2].[4] and its children.
        final SchemaReader schemaReader =
            salesCube.getSchemaReader(null).withLocus();
        final Member memberApril = schemaReader.getMemberByUniqueName(
            Id.Segment.toList("Time", "1997", "Q2", "4"), true);

        final CacheControl.CellRegion regionTimeApril =
            cacheControl.createMemberRegion(
                true, memberApril, false, null, true);
        assertEquals(
            "Range([Time].[1997].[Q2].[4] inclusive to null)",
            regionTimeApril.toString());

        final CacheControl.CellRegion measuresRegion =
            cacheControl.createMeasuresRegion(salesCube);
        return cacheControl.createCrossjoinRegion(
            regionTimeApril,
            measuresRegion);
    }

    /**
     * Creates a cell region using a given {@link CacheControl}, containing
     * only [Time].[1997] * {Measures}.
     *
     * @param testContext Test context
     * @param cacheControl Cache control
     */
    private CacheControl.CellRegion createCellRegion1997(
        TestContext testContext,
        CacheControl cacheControl)
    {
        final Connection connection = testContext.getConnection();
        final Cube salesCube = connection.getSchema().lookupCube("Sales", true);

        // Region consists of [Time].[1997] and its children.
        final SchemaReader schemaReader = salesCube.getSchemaReader(null);
        final Member member1997 = schemaReader.getMemberByUniqueName(
            Id.Segment.toList("Time", "1997"), true);

        final CacheControl.CellRegion region1997 =
            cacheControl.createMemberRegion(member1997, true);
        assertEquals(
            "Member([Time].[1997])",
            region1997.toString());

        final CacheControl.CellRegion measuresRegion =
            cacheControl.createMeasuresRegion(salesCube);
        return cacheControl.createCrossjoinRegion(
            region1997,
            measuresRegion);
    }

    /**
     * Creates a cell region using a given {@link CacheControl}, containing
     * only [Gender].[F] * {[Product].[Food], [Product].[Drink]} * {Measures}.
     *
     * @param testContext Test context
     * @param cacheControl Cache control
     */
    private CacheControl.CellRegion createCellRegionFemaleFoodDrink(
        TestContext testContext,
        CacheControl cacheControl)
    {
        final Connection connection = testContext.getConnection();
        final Cube salesCube = connection.getSchema().lookupCube("Sales", true);

        // Region consists of [Product].[Food], [Product].[Drink] and their
        // children.
        final SchemaReader schemaReader =
            salesCube.getSchemaReader(null).withLocus();
        final Member memberFood = schemaReader.getMemberByUniqueName(
            Id.Segment.toList("Product", "Food"), true);
        final Member memberDrink = schemaReader.getMemberByUniqueName(
            Id.Segment.toList("Product", "Drink"), true);
        final Member memberFemale = schemaReader.getMemberByUniqueName(
            Id.Segment.toList("Gender", "F"), true);

        final CacheControl.CellRegion regionProductFoodDrink =
            cacheControl.createMemberRegion(
                true, memberDrink, true, memberFood, true);
        assertEquals(
            "Range([Product].[Drink] inclusive to [Product].[Food] inclusive)",
            regionProductFoodDrink.toString());

        final CacheControl.CellRegion regionFemale =
            cacheControl.createMemberRegion(memberFemale, true);

        final CacheControl.CellRegion measuresRegion =
            cacheControl.createMeasuresRegion(salesCube);
        return cacheControl.createCrossjoinRegion(
            regionProductFoodDrink,
            measuresRegion,
            regionFemale);
    }

    private CacheControl.CellRegion createCellRegionFemale(
        TestContext testContext,
        CacheControl cacheControl)
    {
        final Connection connection = testContext.getConnection();
        final Cube salesCube =
            connection.getSchema().lookupCube("Sales", true);

        final SchemaReader schemaReader =
            salesCube.getSchemaReader(null).withLocus();
        final Member memberFemale =
            schemaReader.getMemberByUniqueName(
                Id.Segment.toList("Gender", "F"), true);

        final CacheControl.CellRegion regionFemale =
            cacheControl.createMemberRegion(memberFemale, true);

        final CacheControl.CellRegion measuresRegion =
            cacheControl.createMeasuresRegion(salesCube);
        return cacheControl.createCrossjoinRegion(
            measuresRegion,
            regionFemale);
    }

    private CacheControl.CellRegion createCellRegionMale(
        TestContext testContext,
        CacheControl cacheControl)
    {
        final Connection connection = testContext.getConnection();
        final Cube salesCube =
            connection.getSchema().lookupCube("Sales", true);

        final SchemaReader schemaReader =
            salesCube.getSchemaReader(null).withLocus();
        final Member memberFemale =
            schemaReader.getMemberByUniqueName(
                Id.Segment.toList("Gender", "M"), true);

        final CacheControl.CellRegion regionFemale =
            cacheControl.createMemberRegion(memberFemale, true);

        final CacheControl.CellRegion measuresRegion =
            cacheControl.createMeasuresRegion(salesCube);
        return cacheControl.createCrossjoinRegion(
            measuresRegion,
            regionFemale);
    }

    /**
     * Asserts that a given string contains a given pattern.
     *
     * @param pattern Pattern to find
     * @param message String
     * @throws junit.framework.AssertionFailedError if pattern is not found
     */
    static void assertContains(String pattern, String message) {
        assertTrue(message, message.indexOf(pattern) > -1);
    }

    /**
     * A number of negative tests, trying to do invalid things with cache
     * flushing and getting errors.
     */
    public void testNegative() {
        final TestContext testContext = getTestContext();
        final Connection connection = testContext.getConnection();
        final Cube salesCube = connection.getSchema().lookupCube("Sales", true);
        final SchemaReader schemaReader = salesCube.getSchemaReader(null);
        final CacheControl cacheControl = testContext.getCacheControl();
        final Member memberQ1 =
            schemaReader.withLocus().getMemberByUniqueName(
                Id.Segment.toList("Time", "1997", "Q1"), true);
        final Member memberBeer =
            schemaReader.withLocus().getMemberByUniqueName(
                Id.Segment.toList(
                    "Product", "Drink", "Alcoholic Beverages", "Beer and Wine"),
            true);
        final Member memberDairy =
            schemaReader.withLocus().getMemberByUniqueName(
                Id.Segment.toList("Product", "Drink", "Dairy"), true);

        final CacheControl.CellRegion regionTimeQ1 =
            cacheControl.createMemberRegion(memberQ1, false);
        final CacheControl.CellRegion regionProductBeer =
            cacheControl.createMemberRegion(memberBeer, false);
        final CacheControl.CellRegion regionProductDairy =
            cacheControl.createMemberRegion(memberDairy, true);

        // Try to combine [Time] region with [Product] region.
        // Cannot union regions with different dimensionality.
        try {
            final CacheControl.CellRegion cellRegion =
                cacheControl.createUnionRegion(
                    regionTimeQ1,
                    regionProductBeer);
            fail("expected exception, got " + cellRegion);
        } catch (RuntimeException e) {
            assertContains(
                "Cannot union cell regions of different dimensionalities. "
                + "(Dimensionalities are '[[Time]]', '[[Product]]'.)",
                e.getMessage());
        }

        final CacheControl.CellRegion regionTimeXProduct =
            cacheControl.createCrossjoinRegion(
                regionTimeQ1,
                regionProductBeer);
        assertNotNull(regionTimeXProduct);
        assertEquals(2, regionTimeXProduct.getDimensionality().size());
        assertEquals(
            "Crossjoin(Member([Time].[1997].[Q1]), "
            + "Member([Product].[Drink].[Alcoholic Beverages]."
            + "[Beer and Wine]))",
            regionTimeXProduct.toString());

        // Try to combine ([Time], [Product]) region with ([Time]) region.
        try {
            final CacheControl.CellRegion cellRegion =
                cacheControl.createUnionRegion(
                    regionTimeXProduct,
                    regionTimeQ1);
            fail("expected exception, got " + cellRegion);
        } catch (RuntimeException e) {
            assertContains(
                "Cannot union cell regions of different dimensionalities. "
                + "(Dimensionalities are '[[Time], [Product]]', '[[Time]]'.)",
                e.getMessage());
        }

        // Try to combine ([Time], [Product]) region with ([Product]) region.
        try {
            final CacheControl.CellRegion cellRegion =
                cacheControl.createUnionRegion(
                    regionTimeXProduct,
                    regionProductBeer);
            fail("expected exception, got " + cellRegion);
        } catch (RuntimeException e) {
            assertContains(
                "Cannot union cell regions of different dimensionalities. "
                + "(Dimensionalities are '[[Time], [Product]]', "
                + "'[[Product]]'.)",
                e.getMessage());
        }

        // Try to combine ([Time]) region with ([Time], [Product]) region.
        try {
            final CacheControl.CellRegion cellRegion =
                cacheControl.createUnionRegion(
                    regionTimeQ1,
                    regionTimeXProduct);
            fail("expected exception, got " + cellRegion);
        } catch (RuntimeException e) {
            assertContains(
                "Cannot union cell regions of different dimensionalities. "
                + "(Dimensionalities are '[[Time]]', '[[Time], [Product]]'.)",
                e.getMessage());
        }

        // Union [Time] region with itself -- OK.
        final CacheControl.CellRegion regionTimeUnionTime =
            cacheControl.createUnionRegion(
                regionTimeQ1,
                regionTimeQ1);
        assertNotNull(regionTimeUnionTime);
        assertEquals(1, regionTimeUnionTime.getDimensionality().size());

        // Union [Time] region with itself -- OK.
        final CacheControl.CellRegion regionTimeXProductUnionTimeXProduct =
            cacheControl.createUnionRegion(
                regionTimeXProduct,
                regionTimeXProduct);
        assertNotNull(regionTimeXProductUnionTimeXProduct);
        assertEquals(
            2, regionTimeXProductUnionTimeXProduct.getDimensionality().size());

        // Cartesian product two [Product] regions - not OK.
        try {
            final CacheControl.CellRegion cellRegion =
                cacheControl.createCrossjoinRegion(
                    regionProductBeer,
                    regionProductDairy);
            fail("expected exception, got " + cellRegion);
        } catch (RuntimeException e) {
            assertContains(
                "Cannot crossjoin cell regions which have dimensions in common."
                + " (Dimensionalities are '[[Product]]', '[[Product]]'.)",
                e.getMessage());
        }

        // Cartesian product [Product] and [Time] x [Product] regions - not OK.
        try {
            final CacheControl.CellRegion cellRegion =
                cacheControl.createCrossjoinRegion(
                    regionProductBeer,
                    regionTimeXProduct);
            fail("expected exception, got " + cellRegion);
        } catch (RuntimeException e) {
            assertContains(
                "Cannot crossjoin cell regions which have dimensions in common."
                + " (Dimensionalities are "
                + "'[[Product]]', '[[Time], [Product]]'.)",
                e.getMessage());
        }
    }

    /**
     * Tests crossjoin of regions, {@link CacheControl#createCrossjoinRegion}.
     */
    public void testCrossjoin() {
        final TestContext testContext = getTestContext();
        final Connection connection = testContext.getConnection();
        final Cube salesCube = connection.getSchema().lookupCube("Sales", true);
        final CacheControl cacheControl = testContext.getCacheControl();

        // Region consists of [Time].[1997].[Q1] and its children, and products
        // [Beer] and [Dairy].
        final SchemaReader schemaReader =
            salesCube.getSchemaReader(null).withLocus();
        final Member memberQ1 = schemaReader.getMemberByUniqueName(
            Id.Segment.toList("Time", "1997", "Q1"), true);
        final Member memberBeer = schemaReader.getMemberByUniqueName(
            Id.Segment.toList(
                "Product", "Drink", "Alcoholic Beverages", "Beer and Wine",
                "Beer"),
            true);
        final CacheControl.CellRegion regionProductBeer =
            cacheControl.createMemberRegion(memberBeer, false);

        final Member memberFemale = schemaReader.getMemberByUniqueName(
            Id.Segment.toList("Gender", "F"), true);
        final CacheControl.CellRegion regionGenderFemale =
            cacheControl.createMemberRegion(memberFemale, false);

        final CacheControl.CellRegion regionTimeQ1 =
            cacheControl.createMemberRegion(memberQ1, true);

        final CacheControl.CellRegion regionTimeXProduct =
            cacheControl.createCrossjoinRegion(
                regionTimeQ1,
                regionProductBeer);

        // Compose a crossjoin with a non crossjoin
        final CacheControl.CellRegion regionTimeXProductXGender =
            cacheControl.createCrossjoinRegion(
                regionTimeXProduct,
                regionGenderFemale);
        assertEquals(
            "Crossjoin("
            + "Member([Time].[1997].[Q1]), "
            + "Member([Product].[Drink].[Alcoholic Beverages]."
            + "[Beer and Wine].[Beer]), "
            + "Member([Gender].[F]))",
            regionTimeXProductXGender.toString());
        assertEquals(
            "[[Time], [Product], [Gender]]",
            regionTimeXProductXGender.getDimensionality().toString());

        // Three-way crossjoin, should be same as previous
        final CacheControl.CellRegion regionTimeXProductXGender2 =
            cacheControl.createCrossjoinRegion(
                regionTimeQ1,
                regionProductBeer,
                regionGenderFemale);
        assertEquals(
            "Crossjoin("
            + "Member([Time].[1997].[Q1]), "
            + "Member([Product].[Drink].[Alcoholic Beverages]"
            + ".[Beer and Wine].[Beer]), "
            + "Member([Gender].[F]))",
            regionTimeXProductXGender2.toString());
        assertEquals(
            "[[Time], [Product], [Gender]]",
            regionTimeXProductXGender2.getDimensionality().toString());

        // Compose a non crossjoin with a crossjoin
        final CacheControl.CellRegion regionGenderXTimeXProduct =
            cacheControl.createCrossjoinRegion(
                regionGenderFemale,
                regionTimeXProduct);
        assertEquals(
            "Crossjoin("
            + "Member([Gender].[F]), "
            + "Member([Time].[1997].[Q1]), "
            + "Member([Product].[Drink].[Alcoholic Beverages]"
            + ".[Beer and Wine].[Beer]))",
            regionGenderXTimeXProduct.toString());
        assertEquals(
            "[[Gender], [Time], [Product]]",
            regionGenderXTimeXProduct.getDimensionality().toString());
    }

    /**
     * Helper method, creates a region consisting of a single member, given its
     * unique name (e.g. "[Gender].[F]").
     */
    CacheControl.CellRegion memberRegion(String uniqueName) {
        final String[] names = uniqueName.split("\\.");
        final List<Id.Segment> ids = new ArrayList<Id.Segment>(names.length);
        for (int i = 0; i < names.length; i++) {
            String name = names[i];
            assert name.startsWith("[") && name.endsWith("]");
            names[i] = name.substring(1, name.length() - 1);
            ids.add(new Id.NameSegment(names[i]));
        }
        final TestContext testContext = getTestContext();
        final Connection connection = testContext.getConnection();
        final Cube salesCube = connection.getSchema().lookupCube("Sales", true);
        final CacheControl cacheControl = testContext.getCacheControl();
        final SchemaReader schemaReader =
            salesCube.getSchemaReader(null).withLocus();
        final Member member = schemaReader.getMemberByUniqueName(ids, true);
        return cacheControl.createMemberRegion(member, false);
    }

    /**
     * Tests the algorithm which converts a cache region specification into
     * normal form.
     */
    public void testNormalize() {
        // Create
        // Union(
        //    Crossjoin(
        //       [Marital Status].[S],
        //       Union(
        //          Crossjoin(
        //             [Gender].[F]
        //             [Time].[1997].[Q1])
        //          Crossjoin(
        //             [Gender].[M]
        //             [Time].[1997].[Q2]))
        //    Crossjoin(
        //       Crossjoin(
        //          [Marital Status].[S],
        //          [Gender].[F])
        //       [Time].[1997].[Q1])
        //
        final CacheControl cacheControl =
            new CacheControlImpl(
                (RolapConnection) getTestContext().getConnection());
        final CacheControl.CellRegion region =
            cacheControl.createUnionRegion(
                cacheControl.createCrossjoinRegion(
                    memberRegion("[Marital Status].[S]"),
                    cacheControl.createUnionRegion(
                        cacheControl.createCrossjoinRegion(
                            memberRegion("[Gender].[F]"),
                            memberRegion("[Time].[1997].[Q1]")),
                        cacheControl.createCrossjoinRegion(
                            memberRegion("[Gender].[M]"),
                            memberRegion("[Time].[1997].[Q2]")))),
                cacheControl.createCrossjoinRegion(
                    cacheControl.createCrossjoinRegion(
                        memberRegion("[Marital Status].[S]"),
                        memberRegion("[Gender].[F]")),
                    memberRegion("[Time].[1997].[Q1]")));
        assertEquals(
            "Union("
            + "Crossjoin("
            + "Member([Marital Status].[S]), "
            + "Union("
            + "Crossjoin("
            + "Member([Gender].[F]), "
            + "Member([Time].[1997].[Q1])), "
            + "Crossjoin(Member([Gender].[M]), "
            + "Member([Time].[1997].[Q2])))), "
            + "Crossjoin("
            + "Member([Marital Status].[S]), "
            + "Member([Gender].[F]), "
            + "Member([Time].[1997].[Q1])))",
            region.toString());

        final CacheControl.CellRegion normalizedRegion =
            ((CacheControlImpl) cacheControl).normalize(
                (CacheControlImpl.CellRegionImpl) region);
        assertEquals(
            "Union("
            + "Crossjoin(Member([Marital Status].[S]), Member([Gender].[F]), Member([Time].[1997].[Q1])), "
            + "Crossjoin(Member([Marital Status].[S]), Member([Gender].[M]), Member([Time].[1997].[Q2])), "
            + "Crossjoin(Member([Marital Status].[S]), Member([Gender].[F]), Member([Time].[1997].[Q1])))",
            normalizedRegion.toString());
    }

    /**
     * Test case for bug
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-1077">MONDRIAN-1077,
     * "Cache flush for region that is not necessarily populated results in
     * NullPointerException"</a>.
     */
    public void testFlushNonPrimedContent() throws Exception {
        final TestContext testContext = getTestContext();
        flushCache(testContext);
        final CacheControl cacheControl = testContext.getCacheControl();
        final Cube cube =
            testContext.getConnection()
                .getSchema().lookupCube("Sales", true);
        Hierarchy hier =
            cube.getDimensions()[2].getHierarchies()[0];
        Member hierMember = hier.getAllMember();
        CellRegion measuresRegion = cacheControl.createMeasuresRegion(cube);
        CellRegion hierRegion =
            cacheControl.createMemberRegion(hierMember, true);
        CellRegion flushRegion =
            cacheControl.createCrossjoinRegion(measuresRegion, hierRegion);
        cacheControl.flush(flushRegion);
    }

    public void testMondrian1094() throws Exception {
        final String query =
            "select NON EMPTY {[Measures].[Unit Sales]} ON COLUMNS, \n"
            + "NON EMPTY {[Store].[All Stores].Children} ON ROWS \n"
            + "from [Sales] \n";

        flushCache(getTestContext());

        assertQueryReturns(
            query,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Store].[USA]}\n"
            + "Row #0: 266,773\n");

        if (MondrianProperties.instance().DisableCaching.get()) {
            return;
        }

        // Make sure MaxConstraint is high enough
        int minConstraints = 3;

        if (MondrianProperties.instance().MaxConstraints.get()
            < minConstraints)
        {
            propSaver.set(
                MondrianProperties.instance().MaxConstraints,
                minConstraints);
        }

        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        final CacheControl cacheControl =
            getConnection().getCacheControl(pw);

        // Flush the cache.
        final Cube salesCube =
            getConnection().getSchema().lookupCube("Sales", true);
        final Hierarchy storeHierarchy =
            salesCube.lookupHierarchy(
                new Id.NameSegment(
                    "Store",
                    Id.Quoting.UNQUOTED),
                false);
        final CacheControl.CellRegion measuresRegion =
            cacheControl.createMeasuresRegion(salesCube);
        final CacheControl.CellRegion hierarchyRegion =
            cacheControl.createMemberRegion(
                storeHierarchy.getAllMember(), true);
        final CacheControl.CellRegion region =
            cacheControl.createCrossjoinRegion(
                measuresRegion, hierarchyRegion);
        cacheControl.flush(region);
        pw.flush();

        String tag = "output";
        String expected =
            MondrianProperties.instance().EnableNativeNonEmpty.get()
                ? "${output}" : "${output2}";
        String actual = sw.toString();
        assertCacheStateEquals(tag, expected, actual);
    }

    // todo: Test flushing a segment which is unconstrained

    // todo: Test flushing a segment where 2 or more axes are reduced. E.g.
    // Given segment
    //   (state={CA, OR}, quarter={Q1, Q2, Q3}, year=1997)
    // flush
    //   (state=OR, quarter=Q2)
    // which leaves
    //   (state={CA, OR}, quarter={Q1, Q3}, year=1997)
    //   (state=CA, quarter=Q2, year=1997)
    // For now, we kill the slice of the segment with the fewest values, which
    // is
    //   (quarter=Q2)
    // leaving
    //   (state={CA, OR}, quarter={Q1, Q3}, year=1997)

    // todo: Test flushing values which are not present in a segment. Need to
    // reduce the scope of the segment.

    // todo: Solve the fragmentation problem. Continually ask for later and
    // later times. Two cases: the segment's specification contains the time
    // asked for (and therefore the segment will later need to be pared back),
    // and it doesn't. Either way, end up with a lot of segments which could
    // be merged. Solve by triggering a coalesce or flush, but what should
    // trigger that?

    // todo: test which tries to constraint on calc member. Should get error.

    // todo: test which tries to constrain on member of parent-child hierarchy

    public void testMondrian2366() {
        String mdx =
            "select filter( gender.gender.members, measures.[Unit Sales] > 0) on 0 from sales ";
        mondrian.olap.Result rest = executeQuery(mdx);
        RolapCube cube = (RolapCube) rest.getQuery().getCube();
        RolapConnection con = (RolapConnection) rest.getQuery().getConnection();
        CacheControl cacheControl = con.getCacheControl(null);

        for (RolapHierarchy hier : cube.getHierarchies()) {
            if (hier.hasAll()) {
                cacheControl.flush(
                    cacheControl.createMemberSet(hier.getAllMember(), true));
            }
        }
        executeQuery(mdx);
    }
}

// End CacheControlTest.java
