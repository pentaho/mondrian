/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (c) 2002-2017 Hitachi Vantara..  All rights reserved.
*/
package mondrian.test;

import mondrian.olap.Cell;
import mondrian.olap.Result;

public class DrillThroughExcludeFilterTest extends FoodMartTestCase {

    String schema = "<Schema name=\"MYFoodmart\">\n"
            + "  <Dimension visible=\"true\" highCardinality=\"false\" name=\"Store\">\n"
            + "    <Hierarchy visible=\"true\" hasAll=\"true\" primaryKey=\"store_id\">\n"
            + "      <Table name=\"store\"/>\n"
            + "      <Level name=\"Store ID\" visible=\"true\" column=\"store_id\" type=\"String\" uniqueMembers=\"true\" levelType=\"Regular\" hideMemberIf=\"Never\"/>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n"
            + "  <Dimension type=\"TimeDimension\" visible=\"true\" highCardinality=\"false\" name=\"Time\">\n"
            + "    <Hierarchy name=\"Time Hierarchy\" visible=\"true\" hasAll=\"true\" primaryKey=\"time_id\">\n"
            + "      <Table name=\"time_by_day\"/>\n"
            + "      <Level name=\"Year\" visible=\"true\" column=\"the_year\" type=\"Numeric\" uniqueMembers=\"false\" levelType=\"TimeYears\" hideMemberIf=\"Never\"/>\n"
            + "      <Level name=\"Quarter\" visible=\"true\" column=\"quarter\" type=\"String\" uniqueMembers=\"false\" levelType=\"TimeQuarters\" hideMemberIf=\"Never\"/>\n"
            + "      <Level name=\"Month\" visible=\"true\" column=\"month_of_year\" nameColumn=\"the_month\" type=\"Integer\" uniqueMembers=\"false\" levelType=\"TimeMonths\" hideMemberIf=\"Never\"/>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n"
            + "  <Dimension visible=\"true\" highCardinality=\"false\" name=\"Warehouse\">\n"
            + "    <Hierarchy name=\"Warehouse\" visible=\"true\" hasAll=\"true\" primaryKey=\"warehouse_id\">\n"
            + "      <Table name=\"warehouse\"/>\n"
            + "      <Level name=\"Warehouse Name\" visible=\"true\" column=\"warehouse_name\" type=\"String\" uniqueMembers=\"true\" levelType=\"Regular\" hideMemberIf=\"Never\"/>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n"
            + "  <Cube name=\"Sales\" visible=\"true\" defaultMeasure=\"Unit Sales\" cache=\"true\" enabled=\"true\">\n"
            + "    <Table name=\"sales_fact_1997\"/>\n"
            + "    <DimensionUsage source=\"Store\" name=\"Store\" visible=\"true\" foreignKey=\"store_id\" highCardinality=\"false\"/>\n"
            + "    <DimensionUsage source=\"Time\" name=\"Time\" visible=\"true\" foreignKey=\"time_id\" highCardinality=\"false\"/>\n"
            + "    <Measure name=\"Store Sales\" column=\"store_sales\" formatString=\"#,###.00\" aggregator=\"sum\"/>\n"
            + "  </Cube>\n"
            + "  <Cube name=\"Warehouse\" visible=\"true\" cache=\"true\" enabled=\"true\">\n"
            + "    <Table name=\"inventory_fact_1997\"/>\n"
            + "    <DimensionUsage source=\"Store\" name=\"Store\" visible=\"true\" foreignKey=\"store_id\" highCardinality=\"false\"/>\n"
            + "    <DimensionUsage source=\"Time\" name=\"Time\" visible=\"true\" foreignKey=\"time_id\" highCardinality=\"false\"/>\n"
            + "    <DimensionUsage source=\"Warehouse\" name=\"Warehouse\" visible=\"true\" foreignKey=\"warehouse_id\" highCardinality=\"false\"/>\n"
            + "    <Measure name=\"Warehouse Sales\" column=\"warehouse_sales\" aggregator=\"sum\"/>\n"
            + "  </Cube>\n"
            + "  <VirtualCube enabled=\"true\" name=\"Warehouse and Sales\" defaultMeasure=\"Store Sales\" visible=\"true\">\n"
            + "    <VirtualCubeDimension visible=\"true\" highCardinality=\"false\" name=\"Time\"/>\n"
            + "    <VirtualCubeMeasure cubeName=\"Sales\" name=\"[Measures].[Store Sales]\"/>\n"
            + "    <VirtualCubeMeasure cubeName=\"Warehouse\" name=\"[Measures].[Warehouse Sales]\"/>\n"
            + "  </VirtualCube>  \n"
            + "</Schema>\n";

    // Test for VirtualCube DrillThrough with exclude filter
    // on level not present in report
    public void testDrillThroughExcludeFilter() throws Exception    {
        int expectedDrillThroughCountForCell0 = 3773;
        int expectedDrillThroughCountForCell1 = 78120;

        final TestContext testContext =
                TestContext.instance().withSchema(schema);

        Result result = testContext.executeQuery(
            "WITH"
            + "   SET [*NATIVE_CJ_SET_WITH_SLICER] "
            + "     AS 'FILTER([*BASE_MEMBERS__Time.Time Hierarchy_], NOT ISEMPTY ([Measures].[Warehouse Sales]) OR NOT ISEMPTY ([Measures].[Store Sales]))'"
            + "   SET [*NATIVE_CJ_SET] AS '[*NATIVE_CJ_SET_WITH_SLICER]'"
            + "   SET [*BASE_MEMBERS__Time.Time Hierarchy_] "
            + "     AS 'FILTER([Time.Time Hierarchy].[Month].MEMBERS,[Time.Time Hierarchy].CURRENTMEMBER NOT IN {[Time.Time Hierarchy].[All Time.Time Hierarchys].[1997].[Q4].[December]})'"
            + "   SET [*BASE_MEMBERS__Measures_] AS '{[Measures].[Warehouse Sales],[Measures].[*FORMATTED_MEASURE_0]}'"
            + "   SET [*CJ_SLICER_AXIS] AS 'GENERATE([*NATIVE_CJ_SET_WITH_SLICER], {([Time.Time Hierarchy].CURRENTMEMBER)})'"
            + "   MEMBER [Measures].[*FORMATTED_MEASURE_0] AS '[Measures].[Store Sales]', FORMAT_STRING = '#,###.00', SOLVE_ORDER=500"
            + " SELECT"
            + "   [*BASE_MEMBERS__Measures_] ON COLUMNS"
            + " FROM [Warehouse and Sales]"
            + " WHERE ([*CJ_SLICER_AXIS])");

        assertEquals(
            expectedDrillThroughCountForCell0,
            result.getCell(new int[]{0}).getDrillThroughCount());

        assertEquals(
            expectedDrillThroughCountForCell1,
            result.getCell(new int[]{1}).getDrillThroughCount());
    }
}

// End DrillThroughExcludeFilterTest.java