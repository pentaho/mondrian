/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2009-2010 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.test;

import java.io.InputStream;

import junit.framework.TestCase;

import mondrian.olap.*;
import mondrian.rolap.RolapConnectionProperties;
import mondrian.spi.impl.FilterDynamicSchemaProcessor;

/**
 * Unit test against Pentaho's Steel Wheels sample database.
 *
 * <p>It is not required that the Steel Wheels database be present, so each
 * test should check whether the database exists and trivially succeed if it
 * does not.
 *
 * @author jhyde
 * @since 12 March 2009
 * @version $Id$
 */
public class SteelWheelsTestCase extends TestCase {

    /**
     * Creates a SteelwheelsTestCase.
     *
     * @param name Test case name (usually method name)
     */
    public SteelWheelsTestCase(String name) {
        super(name);
    }

    /**
     * Creates a SteelwheelsTestCase.
     */
    public SteelWheelsTestCase() {
    }

    /**
     * Returns the test context. Override this method if you wish to use a
     * different source for your FoodMart connection.
     */
    public TestContext getTestContext() {
        return new SteelWheelsTestContext(TestContext.instance());
    }

    private static class SteelWheelsTestContext extends DelegatingTestContext
    {
        private SteelWheelsTestContext(TestContext testContext) {
            super(testContext);
        }

        @Override
        public Util.PropertyList getFoodMartConnectionProperties() {
            final Util.PropertyList propertyList =
                Util.parseConnectString(getDefaultConnectString());
            // Assume we are talking to MySQL. Connect to 'sampledata'
            // database, using usual credentials ('foodmart').
            propertyList.put(
                RolapConnectionProperties.Jdbc.name(),
                Util.replace(
                    propertyList.get(RolapConnectionProperties.Jdbc.name()),
                    "/foodmart",
                    "/steelwheels"));
            propertyList.put(
                RolapConnectionProperties.Catalog.name(),
                Util.replace(
                    propertyList.get(RolapConnectionProperties.Catalog.name()),
                    "FoodMart.xml",
                    "SteelWheels.mondrian.xml"));
            return propertyList;
        }

        public String getDefaultCubeName() {
            return "SteelWheelsSales";
        }
    }

    /**
     * Sanity check, that enumerates the Measures dimension.
     */
    public void testMeasures() {
        TestContext testContext = getTestContext();
        if (!testContext.databaseIsValid()) {
            return;
        }
        testContext.assertAxisReturns(
            "Measures.Members",
            "[Measures].[Quantity]\n"
            + "[Measures].[Sales]\n"
            + "[Measures].[Fact Count]");
    }

    /**
     * Test case for Infobright issue where [Markets].[All Markets].[Japan]
     * was not found but [Markets].[All Markets].[JAPAN] was OK.
     * (We've since dropped 'All Xxx' from member unique names.)
     */
    public void testMarkets() {
        TestContext testContext = getTestContext();
        if (!testContext.databaseIsValid()) {
            return;
        }
        testContext.assertQueryReturns(
            "select [Markets].[All Markets].[Japan] on 0 from [SteelWheelsSales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Markets].[Japan]}\n"
            + "Row #0: 4,923\n");

        testContext.assertQueryReturns(
            "select [Markets].Children on 0 from [SteelWheelsSales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Markets].[#null]}\n"
            + "{[Markets].[APAC]}\n"
            + "{[Markets].[EMEA]}\n"
            + "{[Markets].[Japan]}\n"
            + "{[Markets].[NA]}\n"
            + "Row #0: \n"
            + "Row #0: 12,878\n"
            + "Row #0: 49,578\n"
            + "Row #0: 4,923\n"
            + "Row #0: 37,952\n");

        testContext.assertQueryReturns(
            "select Subset([Markets].Members, 130, 8) on 0 from [SteelWheelsSales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Markets].[EMEA].[UK].[Isle of Wight].[Cowes]}\n"
            + "{[Markets].[Japan]}\n"
            + "{[Markets].[Japan].[Hong Kong]}\n"
            + "{[Markets].[Japan].[Hong Kong].[#null]}\n"
            + "{[Markets].[Japan].[Hong Kong].[#null].[Central Hong Kong]}\n"
            + "{[Markets].[Japan].[Japan]}\n"
            + "{[Markets].[Japan].[Japan].[Osaka]}\n"
            + "{[Markets].[Japan].[Japan].[Osaka].[Osaka]}\n"
            + "Row #0: 895\n"
            + "Row #0: 4,923\n"
            + "Row #0: 596\n"
            + "Row #0: 58,396\n"
            + "Row #0: 596\n"
            + "Row #0: 1,842\n"
            + "Row #0: 692\n"
            + "Row #0: 692\n");

        testContext.assertQueryReturns(
            "select [Markets].[Territory].Members on 0 from [SteelWheelsSales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Markets].[#null]}\n"
            + "{[Markets].[APAC]}\n"
            + "{[Markets].[EMEA]}\n"
            + "{[Markets].[Japan]}\n"
            + "{[Markets].[NA]}\n"
            + "Row #0: \n"
            + "Row #0: 12,878\n"
            + "Row #0: 49,578\n"
            + "Row #0: 4,923\n"
            + "Row #0: 37,952\n");
    }

    /**
     * Test case for bug <a href="http://jira.pentaho.com/browse/MONDRIAN-755">
     * MONDRIAN-755, "Getting drillthrough count results in exception"</a>.
     */
    public void testBugMondrian755() {
        TestContext testContext = getTestContext();
        if (!testContext.databaseIsValid()) {
            return;
        }
        // One-dimensional query, using set and trivial calc member.
        checkCellZero(
            testContext,
            "With \n"
            + "Set [*BASE_MEMBERS_Measures] as '{[Measures].[*FORMATTED_MEASURE_0]}'\n"
            + "Member [Measures].[*FORMATTED_MEASURE_0] as '[Measures].[Sales]', FORMAT_STRING = '#,###', SOLVE_ORDER=400\n"
            + "Select\n"
            + "[*BASE_MEMBERS_Measures] on columns\n"
            + "From [SteelWheelsSales]");

        // One-dimensional query, using trivial calc member.
        checkCellZero(
            testContext,
            "With \n"
            + "Member [Measures].[*FORMATTED_MEASURE_0] as '[Measures].[Sales]', FORMAT_STRING = '#,###', SOLVE_ORDER=400\n"
            + "Select\n"
            + " {[Measures].[*FORMATTED_MEASURE_0]} on columns\n"
            + "From [SteelWheelsSales]");

        // One-dimensional query, using simple calc member.
        checkCellZero(
            testContext,
            "With \n"
            + "Member [Measures].[Avg Price] as '[Measures].[Sales] / [Measures].[Quantity]', FORMAT_STRING = '#.##'\n"
            + "Select\n"
            + " {[Measures].[Avg Price]} on columns\n"
            + "From [SteelWheelsSales]");

        // Zero dim query
        checkCellZero(
            testContext,
            "Select\n"
            + "From [SteelWheelsSales]");

        // Zero dim query on calc member
        checkCellZero(
            testContext,
            "With \n"
            + "Member [Measures].[*FORMATTED_MEASURE_0] as '[Measures].[Sales]', FORMAT_STRING = '#,###', SOLVE_ORDER=400\n"
            + "Select\n"
            + "From [SteelWheelsSales]\n"
            + "Where [Measures].[*FORMATTED_MEASURE_0]");

        // Two-dimensional query, using trivial calc member.
        checkCellZero(
            testContext,
            "With \n"
            + "Member [Measures].[*FORMATTED_MEASURE_0] as '[Measures].[Sales]', FORMAT_STRING = '#,###', SOLVE_ORDER=400\n"
            + "Select\n"
            + " {[Measures].[*FORMATTED_MEASURE_0]} on columns,"
            + " [Product].[All Products] * [Customers].[All Customers] on rows\n"
            + "From [SteelWheelsSales]");
    }

    private void checkCellZero(TestContext testContext, String mdx) {
        final Result result = testContext.executeQuery(mdx);
        final Cell cell = result.getCell(new int[result.getAxes().length]);
        assertTrue(cell.canDrillThrough());
        assertEquals(2996, cell.getDrillThroughCount());
    }

    /**
     * Test case for bug <a href="http://jira.pentaho.com/browse/MONDRIAN-756">
     * MONDRIAN-756, "Error in RolapResult.replaceNonAllMembers leads to
     * NPE"</a>.
     */
    public void testBugMondrian756() {
        TestContext testContext =
            new SteelWheelsTestContext(TestContext.instance()) {
                @Override
                public Util.PropertyList getFoodMartConnectionProperties() {
                    Util.PropertyList propertyList =
                        super.getFoodMartConnectionProperties();
                    propertyList.put(
                        RolapConnectionProperties.DynamicSchemaProcessor.name(),
                        Mondrian756SchemaProcessor.class.getName());
                    return propertyList;
                }
            };
        if (!testContext.databaseIsValid()) {
            return;
        }
        testContext.assertQueryReturns(
            "select NON EMPTY {[Measures].[Quantity]} ON COLUMNS,\n"
            + "NON EMPTY {[Markets].[APAC]} ON ROWS\n"
            + "from [SteelWheelsSales]\n"
            + "where [Time].[2004]",
            "Axis #0:\n"
            + "{[Time].[2004]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Quantity]}\n"
            + "Axis #2:\n"
            + "{[Markets].[APAC]}\n"
            + "Row #0: 5,938\n");
    }

    public static class Mondrian756SchemaProcessor
        extends FilterDynamicSchemaProcessor
    {
        @Override
        public String filter(
            String schemaUrl,
            Util.PropertyList connectInfo,
            InputStream stream) throws Exception
        {
            String schema = super.filter(schemaUrl, connectInfo, stream);
            return Util.replace(
                schema, " hasAll=\"true\"", " hasAll=\"false\"");
        }
    }
}

// End SteelWheelsTestCase.java
