/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2009-2009 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.test;

import junit.framework.TestCase;
import mondrian.olap.*;

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
    protected static final String nl = Util.nl;

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
        return new DelegatingTestContext(
            TestContext.instance())
        {
            public Util.PropertyList getFoodMartConnectionProperties() {
                final Util.PropertyList propertyList =
                    Util.parseConnectString(getDefaultConnectString());
                // Assume we are talking to MySQL. Connect to 'sampledata'
                // database, using usual credentials ('foodmart').
                propertyList.put(
                    "Jdbc",
                    Util.replace(
                        propertyList.get("Jdbc"),
                        "/foodmart",
                        "/steelwheels"));
                propertyList.put(
                    "Catalog",
                    Util.replace(
                        propertyList.get("Catalog"),
                        "FoodMart.xml",
                        "SteelWheels.mondrian.xml"));
                return propertyList;
            }

            public String getDefaultCubeName() {
                return "SteelWheelsSales";
            }
        };
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
            TestContext.fold(
                "[Measures].[Quantity]\n" +
                "[Measures].[Sales]"));
    }

    /**
     * Test case for Infobright issue where [Markets].[All Markets].[Japan]
     * was not found but [Markets].[All Markets].[JAPAN] was OK.
     */
    public void testMarkets() {
        TestContext testContext = getTestContext();
        if (!testContext.databaseIsValid()) {
            return;
        }
        testContext.assertQueryReturns(
            "select [Markets].[All Markets].[Japan] on 0 from [SteelWheelsSales]",
            TestContext.fold(
                "Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Markets].[All Markets].[Japan]}\n" +
                "Row #0: 4,923\n"));

        testContext.assertQueryReturns(
            "select [Markets].Children on 0 from [SteelWheelsSales]",
            TestContext.fold(
                "Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Markets].[All Markets].[#null]}\n" +
                "{[Markets].[All Markets].[APAC]}\n" +
                "{[Markets].[All Markets].[EMEA]}\n" +
                "{[Markets].[All Markets].[Japan]}\n" +
                "{[Markets].[All Markets].[NA]}\n" +
                "Row #0: \n" +
                "Row #0: 12,878\n" +
                "Row #0: 49,578\n" +
                "Row #0: 4,923\n" +
                "Row #0: 37,952\n"));

        testContext.assertQueryReturns(
            "select Subset([Markets].Members, 130, 8) on 0 from [SteelWheelsSales]",
            TestContext.fold(
                "Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Markets].[All Markets].[EMEA].[UK].[Isle of Wight].[Cowes]}\n" +
                "{[Markets].[All Markets].[Japan]}\n" +
                "{[Markets].[All Markets].[Japan].[Hong Kong]}\n" +
                "{[Markets].[All Markets].[Japan].[Hong Kong].[#null]}\n" +
                "{[Markets].[All Markets].[Japan].[Hong Kong].[#null].[Central Hong Kong]}\n" +
                "{[Markets].[All Markets].[Japan].[Japan]}\n" +
                "{[Markets].[All Markets].[Japan].[Japan].[Osaka]}\n" +
                "{[Markets].[All Markets].[Japan].[Japan].[Osaka].[Osaka]}\n" +
                "Row #0: 895\n" +
                "Row #0: 4,923\n" +
                "Row #0: 596\n" +
                "Row #0: 58,396\n" +
                "Row #0: 596\n" +
                "Row #0: 1,842\n" +
                "Row #0: 692\n" +
                "Row #0: 692\n"));

        testContext.assertQueryReturns(
            "select [Markets].[Territory].Members on 0 from [SteelWheelsSales]",
            TestContext.fold(
                "Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Markets].[All Markets].[#null]}\n" +
                "{[Markets].[All Markets].[APAC]}\n" +
                "{[Markets].[All Markets].[EMEA]}\n" +
                "{[Markets].[All Markets].[Japan]}\n" +
                "{[Markets].[All Markets].[NA]}\n" +
                "Row #0: \n" +
                "Row #0: 12,878\n" +
                "Row #0: 49,578\n" +
                "Row #0: 4,923\n" +
                "Row #0: 37,952\n"));
    }
}

// End SteelWheelsTestCase.java
