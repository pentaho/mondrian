/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2017 Hitachi Vantara..  All rights reserved.
*/

package mondrian.test;

import mondrian.olap.Connection;
import mondrian.olap.DriverManager;
import mondrian.olap.MondrianException;
import mondrian.olap.Util;

import junit.framework.Assert;

/**
 * Unit test for automatic detection of schema version.
 */
public class SchemaVersionTest extends FoodMartTestCase {
    public void testSchema3withVersion() {
        TestContext testContext =
            TestContext.instance().withSchema(SCHEMA_3_VHEADER + SCHEMA_3_BODY);
        Util.PropertyList connectInfo =
            testContext.getConnectionProperties();
        Connection conn = DriverManager.getConnection(connectInfo, null);
        assertNotNull(conn);
        conn.close();
    }

    public void testSchema3noVersion() {
        TestContext testContext =
            TestContext.instance().withSchema(SCHEMA_3_HEADER + SCHEMA_3_BODY);
        Util.PropertyList connectInfo =
            testContext.getConnectionProperties();
        Connection conn = DriverManager.getConnection(connectInfo, null);
        assertNotNull(conn);
        conn.close();
    }

    public void testSchema4withVersion() {
        TestContext testContext =
            TestContext.instance().withSchema(SCHEMA_4_HEADER + SCHEMA_4_BODY);
        Util.PropertyList connectInfo =
            testContext.getConnectionProperties();
        try {
            Connection conn = DriverManager.getConnection(connectInfo, null);
            conn.close();
            Assert.fail("No exception thrown for version 4 schema.");
        } catch (MondrianException e) {
            assertTrue(e.getMessage().contains("Schema version"));
        }
    }

    public void testSchema4noVersion() {
        TestContext testContext =
            TestContext.instance().withSchema(
                SCHEMA_4_NVHEADER + SCHEMA_4_BODY);
        Util.PropertyList connectInfo =
            testContext.getConnectionProperties();
        try {
            Connection conn = DriverManager.getConnection(connectInfo, null);
            conn.close();
            Assert.fail("No exception thrown for version 4 schema.");
        } catch (MondrianException e) {
            assertTrue(e.getMessage().contains("Schema version"));
        }
    }

    private static final String SCHEMA_3_HEADER =
        "<?xml version='1.0'?>\n"
        + "<Schema name='MiniFoodMart'>\n";

    private static final String SCHEMA_3_VHEADER =
        "<?xml version='1.0'?>\n"
        + "<Schema name='MiniFoodMart' metamodelVersion='3.0'>\n";

    private static final String SCHEMA_3_BODY =
        "    <Dimension name='Time' type='TimeDimension'>\n"
        + "\n"
        + "      <Hierarchy hasAll='false' primaryKey='time_id'>\n"
        + "        <Table name='time_by_day'/>\n"
        + "        <Level name='Year' column='the_year' type='Numeric' uniqueMembers='true'\n"
        + "            levelType='TimeYears'/>\n"
        + "        <Level name='Quarter' column='quarter' uniqueMembers='false'\n"
        + "            levelType='TimeQuarters'/>\n"
        + "        <Level name='Month' column='month_of_year' uniqueMembers='false' type='Numeric'\n"
        + "            levelType='TimeMonths'/>\n"
        + "      </Hierarchy>\n"
        + "\n"
        + "      <Hierarchy hasAll='true' name='Weekly' primaryKey='time_id'>\n"
        + "        <Table name='time_by_day'/>\n"
        + "        <Level name='Year' column='the_year' type='Numeric' uniqueMembers='true'\n"
        + "            levelType='TimeYears'/>\n"
        + "        <Level name='Week' column='week_of_year' type='Numeric' uniqueMembers='false'\n"
        + "            levelType='TimeWeeks'/>\n"
        + "        <Level name='Day' column='day_of_month' uniqueMembers='false' type='Numeric'\n"
        + "            levelType='TimeDays'/>\n"
        + "      </Hierarchy>\n"
        + "\n"
        + "    </Dimension>\n"
        + "\n"
        + "    <Dimension name='Product'>\n"
        + "      <Hierarchy hasAll='true' "
        + "                 primaryKey='product_id' primaryKeyTable='product'>\n"
        + "        <Join leftKey='product_class_id' rightKey='product_class_id'>\n"
        + "          <Table name='product'/>\n"
        + "          <Table name='product_class'/>\n"
        + "        </Join>\n"
        + "        <Level name='Product Family' table='product_class' column='product_family'\n"
        + "            uniqueMembers='true'/>\n"
        + "        <Level name='Product Category' table='product_class' column='product_category'\n"
        + "            uniqueMembers='false'/>\n"
        + "        <Level name='Product Name' table='product' column='product_name'\n"
        + "            uniqueMembers='true'/>\n"
        + "      </Hierarchy>\n"
        + "    </Dimension>\n"
        + "\n"
        + "    <Cube name='Sales' defaultMeasure='Unit Sales'>\n"
        + "      <Table name='sales_fact_1997'>\n"
        + "        <AggExclude name=\"agg_c_special_sales_fact_1997\" />\n"
        + "        <AggExclude name=\"agg_lc_100_sales_fact_1997\" />\n"
        + "        <AggExclude name=\"agg_lc_10_sales_fact_1997\" />\n"
        + "        <AggExclude name=\"agg_pc_10_sales_fact_1997\" />\n"
        + "      </Table>\n"
        + "      <DimensionUsage name='Time' source='Time' foreignKey='time_id'/>\n"
        + "      <DimensionUsage name='Product' source='Product' foreignKey='product_id'/>\n"
        + "      <Measure name='Unit Sales' column='unit_sales' aggregator='sum' formatString='Standard'/>\n"
        + "      <Measure name='Sales Count' column='product_id' aggregator='count' formatString='#,###'/>\n"
        + "    </Cube>\n"
        + "\n"
        + "</Schema>";

    private static final String SCHEMA_4_HEADER =
        "<?xml version='1.0'?>\n"
        + "<Schema name='MiniFoodMart' metamodelVersion='4.0'>\n";

    private static final String SCHEMA_4_NVHEADER =
        "<?xml version='1.0'?>\n"
        + "<Schema name='MiniFoodMart'>\n";

    private static String SCHEMA_4_BODY =
        "    <PhysicalSchema>\n"
        + "        <Table name='product'>\n"
        + "            <Key>\n"
        + "                <Column name='product_id'/>\n"
        + "            </Key>\n"
        + "        </Table>\n"
        + "        <Table name='product_class'>\n"
        + "            <Key>\n"
        + "                <Column name='product_class_id'/>\n"
        + "            </Key>\n"
        + "        </Table>\n"
        + "        <Link target='product' source='product_class'>\n"
        + "            <ForeignKey>\n"
        + "                <Column name='product_class_id'/>\n"
        + "            </ForeignKey>\n"
        + "        </Link>\n"
        + "        <Table name='time_by_day'>\n"
        + "            <Key>\n"
        + "                <Column name='time_id'/>\n"
        + "            </Key>\n"
        + "        </Table>\n"
        + "        <Table name='sales_fact_1997'>\n"
        + "            <ColumnDefs>\n"
        + "                <CalculatedColumnDef name='promotion_sales'>\n"
        + "                    <ExpressionView>\n"
        + "                        <SQL dialect='access'>\n"
        + "                            Iif(<Column table='sales_fact_1997' name='promotion_id'/> = 0, 0,\n"
        + "                            <Column table='sales_fact_1997' name='store_sales'/>)\n"
        + "                        </SQL>\n"
        + "                    <SQL dialect='generic'>\n"
        + "                        case when <Column table='sales_fact_1997' name='promotion_id'/> = 0 then 0\n"
        + "                        else <Column table='sales_fact_1997' name='store_sales'/> end\n"
        + "                    </SQL>\n"
        + "                    </ExpressionView>\n"
        + "                </CalculatedColumnDef>\n"
        + "            </ColumnDefs>\n"
        + "        </Table>\n"
        + "    </PhysicalSchema>\n"
        + "\n"
        + "    <Dimension name='Time' table='time_by_day' type='TimeDimension' key='Time Id'>\n"
        + "      <Attributes>\n"
        + "          <Attribute name='Year' keyColumn='the_year' levelType='TimeYears'/>\n"
        + "          <Attribute name='Quarter' levelType='TimeQuarters'>\n"
        + "              <Key>\n"
        + "                  <Column name='the_year'/>\n"
        + "                  <Column name='quarter'/>\n"
        + "              </Key>\n"
        + "              <Name>\n"
        + "                  <Column name='quarter'/>\n"
        + "              </Name>\n"
        + "          </Attribute>\n"
        + "      </Attributes>\n"
        + "      <Hierarchies>\n"
        + "        <Hierarchy name='Time' hasAll='false'>\n"
        + "            <Level attribute='Year'/>\n"
        + "            <Level attribute='Quarter'/>\n"
        + "        </Hierarchy>\n"
        + "      </Hierarchies>\n"
        + "    </Dimension>\n"
        + "\n"
        + "    <Dimension name='Product' key='Product Id'>\n"
        + "        <Attributes>\n"
        + "            <Attribute name='Product Family' table='product_class' keyColumn='product_family'/>\n"
        + "            <Attribute name='Product Category' table='product_class'>\n"
        + "                <Key>\n"
        + "                    <Column name='product_family'/>\n"
        + "                    <Column name='product_department'/>\n"
        + "                    <Column name='product_category'/>\n"
        + "                </Key>\n"
        + "                <Name>\n"
        + "                    <Column name='product_category'/>\n"
        + "                </Name>\n"
        + "            </Attribute>\n"
        + "            <Attribute name='Product Name' table='product' keyColumn='product_name'/>\n"
        + "            <Attribute name='Product Id' table='product' keyColumn='product_id'/>\n"
        + "        </Attributes>\n"
        + "        <Hierarchies>\n"
        + "          <Hierarchy name='Products' allMemberName='All Products'>\n"
        + "              <Level attribute='Product Family'/>\n"
        + "              <Level attribute='Product Category'/>\n"
        + "              <Level attribute='Product Name'/>\n"
        + "          </Hierarchy>\n"
        + "        </Hierarchies>\n"
        + "    </Dimension>\n"
        + "\n"
        + "    <Cube name='Sales'>\n"
        + "        <Dimensions>\n"
        + "            <Dimension source='Time'/>\n"
        + "            <Dimension source='Product'/>\n"
        + "        </Dimensions>\n"
        + "        <MeasureGroups>\n"
        + "            <MeasureGroup name='Sales' table='sales_fact_1997'>\n"
        + "                <Measures>\n"
        + "                    <Measure name='Sales Count' column='product_id' aggregator='count' formatString='#,###'>\n"
        + "                        <CalculatedMemberProperty name='MEMBER_ORDINAL' value='1'/>\n"
        + "                    </Measure>\n"
        + "                    <Measure name='Unit Sales' column='unit_sales' aggregator='sum'    formatString='Standard'>\n"
        + "                        <CalculatedMemberProperty name='MEMBER_ORDINAL' value='2'/>\n"
        + "                    </Measure>\n"
        + "                </Measures>\n"
        + "                <DimensionLinks>\n"
        + "                    <ForeignKeyLink dimension='Time' foreignKeyColumn='time_id'/>\n"
        + "                    <ForeignKeyLink dimension='Product' foreignKeyColumn='product_id'/>\n"
        + "                </DimensionLinks>\n"
        + "            </MeasureGroup>\n"
        + "        </MeasureGroups>\n"
        + "    </Cube>\n"
        + "\n"
        + "</Schema>";
}

// End SchemaVersionTest.java
