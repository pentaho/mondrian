/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2011-2012 Pentaho and others
// All Rights Reserved.
 */
package mondrian.test;

import mondrian.olap.Connection;
import mondrian.olap.Util;
import mondrian.spi.DynamicSchemaProcessor;

import junit.framework.TestCase;

/**
 * Unit test for {@link DynamicSchemaProcessor}. Tests availability of
 * properties that DSPs are called, and used to modify the resulting Mondrian
 * schema.
 *
 * @author ngoodman
 */
public class DynamicSchemaProcessorTest
    extends TestCase
{
    private static String schema(String schemaName) {
        return "<?xml version=\"1.0\"?>\n"
            + "<Schema name=\""
            + schemaName
            + "\">\n"
            + "<Cube name=\"Sales\">\n"
            + " <Table name=\"sales_fact_1997\">"
            + "   <AggExclude name=\"agg_pl_01_sales_fact_1997\" />"
            + "   <AggExclude name=\"agg_ll_01_sales_fact_1997\" />"
            + "   <AggExclude name=\"agg_lc_100_sales_fact_1997\" />"
            + "   <AggExclude name=\"agg_lc_06_sales_fact_1997\" />"
            + "   <AggExclude name=\"agg_l_04_sales_fact_1997\" />"
            + "   <AggExclude name=\"agg_l_03_sales_fact_1997\" />"
            + "   <AggExclude name=\"agg_g_ms_pcat_sales_fact_1997\" />"
            + "   <AggExclude name=\"agg_c_10_sales_fact_1997\" />"
            + " </Table>"
            + " <Dimension name=\"Fake\">"
            + "   <Hierarchy hasAll=\"true\">"
            + "     <Level name=\"blah\" column=\"store_id\"/>"
            + "   </Hierarchy>"
            + " </Dimension>"
            + " <Measure name=\"c\" column=\"store_id\" aggregator=\"count\"/>"
            + "</Cube>\n"
            + "</Schema>\n";
    }

    /**
     * Tests to make sure that our base DynamicSchemaProcessor works, with no
     * replacement. Does not test Mondrian is able to connect with the schema
     * definition.
     */
    public void testDSPBasics() throws Exception {
        DynamicSchemaProcessor dsp = new BaseDsp();
        Util.PropertyList dummy = new Util.PropertyList();
        String processedSchema = dsp.processSchema("", dummy);
        assertEquals(schema("REPLACEME"), processedSchema);
    }

    /**
     * Tests to make sure that our base DynamicSchemaProcessor works, and
     * Mondrian is able to parse and connect to FoodMart with it
     */
    public void testFoodmartDsp() {
        final Connection monConnection =
            TestContext.instance()
                .withSchemaProcessor(BaseDsp.class)
                .getConnection();
        assertEquals(monConnection.getSchema().getName(), "REPLACEME");
    }

    /**
     * Our base, token replacing schema processor.
     */
    public static class BaseDsp implements DynamicSchemaProcessor {
        // Determines the "cubeName"
        protected String replaceToken = "REPLACEME";

        public BaseDsp() {}

        public String processSchema(
            String schemaUrl,
            Util.PropertyList connectInfo)
            throws Exception
        {
            return getSchema();
        }

        public String getSchema() throws Exception {
            return DynamicSchemaProcessorTest.schema(replaceToken);
        }
    }

    /**
     * Tests to ensure we have access to Connect properies in a DSP
     */
    public void testProviderTestDSP() {
        Connection monConnection =
            TestContext.instance()
                .withSchemaProcessor(ProviderTestDSP.class)
                .getConnection();
        assertEquals(monConnection.getSchema().getName(), "mondrian");
    }

    /**
     * DSP that checks that replaces the Schema Name with the name of the
     * Provider property
     */
    public static class ProviderTestDSP extends BaseDsp {
        public String processSchema(
            String schemaUrl,
            Util.PropertyList connectInfo)
            throws Exception
        {
            this.replaceToken = connectInfo.get("Provider");
            return getSchema();
        }
    }

    /**
     * Tests to ensure we have access to Connect properies in a DSP
     */
    public void testDBInfoDSP() {
        Connection monConnection =
            TestContext.instance()
                .withSchemaProcessor(FoodMartCatalogDsp.class)
                .getConnection();
        assertEquals(
            monConnection.getSchema().getName(),
            "FoodmartFoundInCatalogProperty");
    }

    /**
     * Checks to make sure our Catalog property contains our
     * <code>FoodMart.mondrian.xml</code> VFS URL.
     */
    public static class FoodMartCatalogDsp extends BaseDsp {
        public String processSchema(
            String schemaUrl,
            Util.PropertyList connectInfo)
            throws Exception
        {
            if (connectInfo.get("Catalog").indexOf("FoodMart.mondrian.xml")
                <= 0)
            {
                this.replaceToken = "NoFoodmartFoundInCatalogProperty";
            } else {
                this.replaceToken = "FoodmartFoundInCatalogProperty";
            }

            return getSchema();
        }
    }

    /**
     * Tests to ensure we have access to Connect properties in a DSP
     */
    public void testCheckJdbcPropertyDsp() {
        Connection monConnection =
            TestContext.instance()
                .withSchemaProcessor(CheckJdbcPropertyDsp.class)
                .getConnection();
        assertEquals(
            monConnection.getSchema().getName(),
            CheckJdbcPropertyDsp.RETURNTRUESTRING);
    }

    /**
     * Ensures we have access to the JDBC URL. Note, since Foodmart can run on
     * multiple databases all we check in schema name is the first four
     * characters (JDBC).
     */
    public static class CheckJdbcPropertyDsp extends BaseDsp {
        public static String RETURNTRUESTRING = "true";
        public static String RETURNFALSESTRING = "false";

        public String processSchema(
            String schemaUrl,
            Util.PropertyList connectInfo)
            throws Exception
        {
            String dataSource = connectInfo.get("DataSource");
            String jdbc = connectInfo.get("Jdbc");

            // If we're using a DataSource we might not get a Jdbc= property
            // trivially return true.
            if (dataSource != null && dataSource.length() > 0) {
                this.replaceToken = RETURNTRUESTRING;
                return getSchema();
            }

            // IF we're here, we don't have a DataSource and
            // our JDBC property should have jdbc: in the URL
            if (jdbc == null || !jdbc.startsWith("jdbc")) {
                this.replaceToken = RETURNFALSESTRING;
                return getSchema();
            } else {
                // If we're here, we have a JDBC url
                this.replaceToken = RETURNTRUESTRING;
                return getSchema();
            }
        }
    }
}

// End DynamicSchemaProcessorTest.java
