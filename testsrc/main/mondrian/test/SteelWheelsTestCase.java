/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2009-2011 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.test;


import junit.framework.TestCase;

import mondrian.olap.*;
import mondrian.rolap.*;

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
        return new SteelWheelsTestContext();
    }

    static class SteelWheelsTestContext extends TestContext
    {
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

        /**
         * Creates a TestContext which contains the given schema text.
         *
         * @param schema A XML schema.
         * @param baseCubeName The name of a cube present in the schema.
         * Used for testing if the connection is valid.
         * @return TestContext which contains the given schema
         */
        public static TestContext create(
            final String schema,
            final String baseCubeName)
        {
            return new SteelWheelsTestContext() {
                public Util.PropertyList getFoodMartConnectionProperties() {
                    final Util.PropertyList properties =
                        Util.parseConnectString(getDefaultConnectString());
                    properties.put(
                        RolapConnectionProperties.Jdbc.name(),
                        Util.replace(
                            properties.get(
                                RolapConnectionProperties.Jdbc.name()),
                            "/foodmart",
                            "/steelwheels"));
                    properties.put(
                        RolapConnectionProperties.CatalogContent.name(),
                        schema);
                    properties.remove(
                        RolapConnectionProperties.Catalog.name());
                    return properties;
                }
                public String getDefaultCubeName() {
                    return baseCubeName;
                }
            };
        }
    }
}

// End SteelWheelsTestCase.java
