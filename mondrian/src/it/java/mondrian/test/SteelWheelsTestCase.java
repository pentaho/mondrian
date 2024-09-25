/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2017 Hitachi Vantara..  All rights reserved.
*/

package mondrian.test;


import mondrian.olap.Util;
import mondrian.rolap.RolapConnectionProperties;

import junit.framework.TestCase;

/**
 * Unit test against Pentaho's Steel Wheels sample database.
 *
 * <p>It is not required that the Steel Wheels database be present, so each
 * test should check whether the database exists and trivially succeed if it
 * does not.
 *
 * @author jhyde
 * @since 12 March 2009
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
     * Creates a TestContext which contains the given schema text.
     *
     * @param context Base test context
     * @param schema A XML schema, or null
     * Used for testing if the connection is valid.
     * @return TestContext which contains the given schema
     */
    public static TestContext createContext(
        TestContext context,
        final String schema)
    {
        final Util.PropertyList properties =
            context.getConnectionProperties().clone();
        final String jdbc = properties.get(
            RolapConnectionProperties.Jdbc.name());
        properties.put(
            RolapConnectionProperties.Jdbc.name(),
            Util.replace(jdbc, "/foodmart", "/steelwheels"));
        if (schema != null) {
            properties.put(
                RolapConnectionProperties.CatalogContent.name(),
                schema);
            properties.remove(
                RolapConnectionProperties.Catalog.name());
        } else {
            final String catalog =
                properties.get(RolapConnectionProperties.Catalog.name());
            properties.put(
                RolapConnectionProperties.Catalog.name(),
                Util.replace(
                    catalog,
                    "FoodMart.xml",
                    "SteelWheels.xml"));
        }
        return context.withProperties(properties);
    }

    /**
     * Returns the test context. Override this method if you wish to use a
     * different source for your SteelWheels connection.
     */
    public TestContext getTestContext() {
        return createContext(TestContext.instance(), null)
            .withCube("SteelWheelsSales");
    }
}

// End SteelWheelsTestCase.java
