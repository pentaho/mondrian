/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (c) 2002-2016 Pentaho Corporation..  All rights reserved.
*/
package mondrian.test;

import mondrian.olap.Cell;
import mondrian.olap.Result;

public class DrillThroughExcludeFilterTest extends FoodMartTestCase {
    // Test for VirtualCube DrillThrough with exclude filter
    // on level not present in report
    public void testDrillThroughExcludeFilter() throws Exception {
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

        final TestContext testContext =
            TestContext.instance().withSchema(schema);

        Result result = testContext.executeQuery(
            "with set [*NATIVE_CJ_SET_WITH_SLICER] as 'Filter([*BASE_MEMBERS__Time.Time Hierarchy_], ((NOT IsEmpty([Measures].[Warehouse Sales])) OR (NOT IsEmpty([Measures].[Store Sales]))))'\n"
            + "  set [*NATIVE_CJ_SET] as '[*NATIVE_CJ_SET_WITH_SLICER]'\n"
            + "  set [*BASE_MEMBERS__Time.Time Hierarchy_] as 'Filter([Time.Time Hierarchy].[Month].Members, (NOT ([Time.Time Hierarchy].CurrentMember IN {[Time.Time Hierarchy].[1997].[Q4].[October],[Time.Time Hierarchy].[1997].[Q4].[December]})))'\n"
            + "  set [*BASE_MEMBERS__Measures_] as '{[Measures].[Warehouse Sales], [Measures].[*FORMATTED_MEASURE_0]}'\n"
            + "  set [*CJ_SLICER_AXIS] as 'Generate([*NATIVE_CJ_SET_WITH_SLICER], {[Time.Time Hierarchy].CurrentMember})'\n"
            + "  member [Measures].[*FORMATTED_MEASURE_0] as '[Measures].[Store Sales]', FORMAT_STRING = \"#,###.00\", SOLVE_ORDER = 500\n"
            + "select [*BASE_MEMBERS__Measures_] ON COLUMNS\n"
            + "from [Warehouse and Sales]\n"
            + "where [*CJ_SLICER_AXIS]\n");

        Cell cell = result.getCell(new int[]{0});
        assertTrue(cell.getDrillThroughCount() > 0);
    }
}

// End DrillThroughExcludeFilterTest.java
