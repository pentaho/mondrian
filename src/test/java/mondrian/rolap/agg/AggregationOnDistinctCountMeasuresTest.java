/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2007-2013 Pentaho and others
// All Rights Reserved.
//
// ajogleka, 19 December, 2007
*/
package mondrian.rolap.agg;

import mondrian.calc.TupleList;
import mondrian.calc.impl.UnaryTupleList;
import mondrian.olap.*;
import mondrian.olap.fun.AggregateFunDef;
import mondrian.olap.fun.CrossJoinFunDef;
import mondrian.rolap.BatchTestCase;
import mondrian.rolap.RolapCube;
import mondrian.server.Execution;
import mondrian.server.Locus;
import mondrian.spi.Dialect;
import mondrian.spi.Dialect.DatabaseProduct;
import mondrian.test.SqlPattern;
import mondrian.test.TestContext;
import mondrian.util.Bug;

import java.util.*;

/**
 * <code>AggregationOnDistinctCountMeasureTest</code> tests the
 * Distinct Count functionality with tuples and members.
 *
 * @author ajogleka
 * @since 19 December, 2007
 */
public class AggregationOnDistinctCountMeasuresTest extends BatchTestCase {

    private SchemaReader salesCubeSchemaReader = null;
    private SchemaReader schemaReader = null;
    private RolapCube salesCube;

    protected void setUp() throws Exception {
        schemaReader =
            getTestContext().getConnection().getSchemaReader().withLocus();
        salesCube = (RolapCube) cubeByName(
            getTestContext().getConnection(),
            cubeNameSales);
        salesCubeSchemaReader =
            salesCube.getSchemaReader(
                getTestContext().getConnection().getRole()).withLocus();
    }

    public TestContext getTestContext() {
        return TestContext.instance().legacy().create(
            null,
            null,
            "<VirtualCube name=\"Warehouse and Sales2\" defaultMeasure=\"Store Sales\">\n"
            + "   <VirtualCubeDimension cubeName=\"Sales\" name=\"Gender\"/>\n"
            + "   <VirtualCubeDimension name=\"Store\"/>\n"
            + "   <VirtualCubeDimension name=\"Time\"/>\n"
            + "   <VirtualCubeDimension name=\"Product\"/>\n"
            + "   <VirtualCubeDimension cubeName=\"Warehouse\" name=\"Warehouse\"/>\n"
            + "   <VirtualCubeMeasure cubeName=\"Sales\" name=\"[Measures].[Store Sales]\"/>\n"
            + "   <VirtualCubeMeasure cubeName=\"Sales\" name=\"[Measures].[Customer Count]\"/>\n"
            + "</VirtualCube>"
            + "<VirtualCube name=\"Warehouse and Sales3\" defaultMeasure=\"Store Invoice\">\n"
            + "  <CubeUsages>\n"
            + "       <CubeUsage cubeName=\"Sales\" ignoreUnrelatedDimensions=\"true\"/>"
            + "   </CubeUsages>\n"
            + "   <VirtualCubeDimension cubeName=\"Sales\" name=\"Gender\"/>\n"
            + "   <VirtualCubeDimension name=\"Store\"/>\n"
            + "   <VirtualCubeDimension name=\"Time\"/>\n"
            + "   <VirtualCubeDimension name=\"Product\"/>\n"
            + "   <VirtualCubeDimension cubeName=\"Warehouse\" name=\"Warehouse\"/>\n"
            + "   <VirtualCubeMeasure cubeName=\"Sales\" name=\"[Measures].[Customer Count]\"/>\n"
            + "   <VirtualCubeMeasure cubeName=\"Warehouse\" name=\"[Measures].[Store Invoice]\"/>\n"
            + "</VirtualCube>",
            null,
            null,
            null);
    }

    public void testTupleWithAllLevelMembersOnly() {
        assertQueryReturns(
            "WITH MEMBER GENDER.X AS 'AGGREGATE({([GENDER].DEFAULTMEMBER,\n"
            + "[STORE].DEFAULTMEMBER)})'\n"
            + "SELECT GENDER.X ON 0, [MEASURES].[CUSTOMER COUNT] ON 1 FROM SALES",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Gender].[Gender].[X]}\n"
            + "Axis #2:\n"
            + "{[Measures].[Customer Count]}\n"
            + "Row #0: 5,581\n");
    }

    public void testCrossJoinOfAllMembers() {
        assertQueryReturns(
            "WITH MEMBER GENDER.X AS 'AGGREGATE({CROSSJOIN({[GENDER].DEFAULTMEMBER},\n"
            + "{[STORE].DEFAULTMEMBER})})'\n"
            + "SELECT GENDER.X ON 0, [MEASURES].[CUSTOMER COUNT] ON 1 FROM SALES",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Gender].[Gender].[X]}\n"
            + "Axis #2:\n"
            + "{[Measures].[Customer Count]}\n"
            + "Row #0: 5,581\n");
    }

    public void testCrossJoinMembersWithASingleMember() {
        String query =
            "WITH MEMBER GENDER.X AS 'AGGREGATE({[GENDER].[GENDER].members} * "
            + "{[STORE].[ALL STORES].[USA].[CA]})', solve_order=100 "
            + "SELECT GENDER.X ON 0, [MEASURES].[CUSTOMER COUNT] ON 1 FROM SALES";
        String result =
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Gender].[Gender].[X]}\n"
            + "Axis #2:\n"
            + "{[Measures].[Customer Count]}\n"
            + "Row #0: 2,716\n";

        assertQueryReturns(query, result);

        // Check aggregate loading sql pattern
        String derbySql =
            "select \"time_by_day\".\"the_year\" as \"c0\", "
            + "count(distinct \"sales_fact_1997\".\"customer_id\") as \"m0\" "
            + "from \"time_by_day\" as \"time_by_day\", \"sales_fact_1997\" as \"sales_fact_1997\", \"store\" as \"store\" "
            + "where \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" "
            + "and \"time_by_day\".\"the_year\" = 1997 "
            + "and \"sales_fact_1997\".\"store_id\" = \"store\".\"store_id\" and \"store\".\"store_state\" = 'CA' "
            + "group by \"time_by_day\".\"the_year\"";

        String mysqlSql =
            "select\n"
            + "    `time_by_day`.`the_year` as `c0`,\n"
            + "    count(distinct `sales_fact_1997`.`customer_id`) as `m0`\n"
            + "from\n"
            + "    `sales_fact_1997` as `sales_fact_1997`,\n"
            + "    `time_by_day` as `time_by_day`,\n"
            + "    `store` as `store`\n"
            + "where\n"
            + "    `time_by_day`.`the_year` = 1997\n"
            + "and\n"
            + "    `store`.`store_state` = 'CA'\n"
            + "and\n"
            + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
            + "and\n"
            + "    `sales_fact_1997`.`store_id` = `store`.`store_id`\n"
            + "group by\n"
            + "    `time_by_day`.`the_year`";

        String oraTeraSql =
            "select \"time_by_day\".\"the_year\" as \"c0\", "
            + "count(distinct \"sales_fact_1997\".\"customer_id\") as \"m0\" "
            + "from \"time_by_day\" =as= \"time_by_day\", \"sales_fact_1997\" =as= \"sales_fact_1997\", \"store\" =as= \"store\" "
            + "where \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" and \"time_by_day\".\"the_year\" = 1997 "
            + "and \"sales_fact_1997\".\"store_id\" = \"store\".\"store_id\" and \"store\".\"store_state\" = 'CA' "
            + "group by \"time_by_day\".\"the_year\"";

        SqlPattern[] patterns = {
            new SqlPattern(Dialect.DatabaseProduct.DERBY, derbySql, derbySql),
            new SqlPattern(Dialect.DatabaseProduct.MYSQL, mysqlSql, mysqlSql),
            new SqlPattern(
                Dialect.DatabaseProduct.ORACLE, oraTeraSql, oraTeraSql),
            new SqlPattern(
                Dialect.DatabaseProduct.TERADATA, oraTeraSql, oraTeraSql),
        };

        assertQuerySql(getTestContext(), query, patterns);
    }

    public void testCrossJoinMembersWithSetOfMembers() {
        String query =
            "WITH MEMBER GENDER.X AS 'AGGREGATE({[GENDER].[GENDER].members} * "
            + "{[STORE].[ALL STORES].[USA].[CA], [Store].[All Stores].[Canada]})', solve_order=100 "
            + "SELECT GENDER.X ON 0, [MEASURES].[CUSTOMER COUNT] ON 1 FROM SALES";

        String result =
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Gender].[Gender].[X]}\n"
            + "Axis #2:\n"
            + "{[Measures].[Customer Count]}\n"
            + "Row #0: 2,716\n";

        assertQueryReturns(query, result);

        // Check aggregate loading sql pattern.  Note Derby does not support
        // multicolumn IN list, so the predicates remain in AND/OR form.
        String derbySql =
            "select \"time_by_day\".\"the_year\" as \"c0\", "
            + "count(distinct \"sales_fact_1997\".\"customer_id\") as \"m0\" "
            + "from \"time_by_day\" as \"time_by_day\", \"sales_fact_1997\" as \"sales_fact_1997\", \"store\" as \"store\" "
            + "where \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" "
            + "and \"time_by_day\".\"the_year\" = 1997 "
            + "and \"sales_fact_1997\".\"store_id\" = \"store\".\"store_id\" "
            + "and (\"store\".\"store_state\" = 'CA' or \"store\".\"store_country\" = 'Canada') "
            + "group by \"time_by_day\".\"the_year\"";

        String mysqlSql =
            "select\n"
            + "    `time_by_day`.`the_year` as `c0`,\n"
            + "    count(distinct `sales_fact_1997`.`customer_id`) as `m0`\n"
            + "from\n"
            + "    `sales_fact_1997` as `sales_fact_1997`,\n"
            + "    `time_by_day` as `time_by_day`,\n"
            + "    `store` as `store`\n"
            + "where\n"
            + "    `time_by_day`.`the_year` = 1997\n"
            + "and\n"
            + "    (`store`.`store_state` = 'CA' or `store`.`store_country` = 'Canada')\n"
            + "and\n"
            + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
            + "and\n"
            + "    `sales_fact_1997`.`store_id` = `store`.`store_id`\n"
            + "group by\n"
            + "    `time_by_day`.`the_year`";

        String oraTeraSql =
            "select \"time_by_day\".\"the_year\" as \"c0\", "
            + "count(distinct \"sales_fact_1997\".\"customer_id\") as \"m0\" "
            + "from \"time_by_day\" =as= \"time_by_day\", \"sales_fact_1997\" =as= \"sales_fact_1997\", \"store\" =as= \"store\" "
            + "where \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" and \"time_by_day\".\"the_year\" = 1997 "
            + "and \"sales_fact_1997\".\"store_id\" = \"store\".\"store_id\" "
            + "and (\"store\".\"store_state\" = 'CA' or \"store\".\"store_country\" = 'Canada') "
            + "group by \"time_by_day\".\"the_year\"";

        SqlPattern[] patterns = {
            new SqlPattern(Dialect.DatabaseProduct.DERBY, derbySql, derbySql),
            new SqlPattern(Dialect.DatabaseProduct.MYSQL, mysqlSql, mysqlSql),
            new SqlPattern(
                Dialect.DatabaseProduct.ORACLE, oraTeraSql, oraTeraSql),
            new SqlPattern(
                Dialect.DatabaseProduct.TERADATA, oraTeraSql, oraTeraSql),
        };

        assertQuerySql(getTestContext(), query, patterns);
    }

    public void testCrossJoinParticularMembersFromTwoDimensions() {
        assertQueryReturns(
            "WITH MEMBER GENDER.X AS 'AGGREGATE({[GENDER].M} * "
            + "{[STORE].[ALL STORES].[USA].[CA]})', solve_order=100 "
            + "SELECT GENDER.X ON 0, [MEASURES].[CUSTOMER COUNT] ON 1 FROM SALES",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Gender].[Gender].[X]}\n"
            + "Axis #2:\n"
            + "{[Measures].[Customer Count]}\n"
            + "Row #0: 1,389\n");
    }

    public void testDistinctCountOnSetOfMembersFromOneDimension() {
        assertQueryReturns(
            "WITH MEMBER GENDER.X AS 'AGGREGATE({[GENDER].[GENDER].members})'"
            + "SELECT GENDER.X ON 0, [MEASURES].[CUSTOMER COUNT] ON 1 FROM SALES",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Gender].[Gender].[X]}\n"
            + "Axis #2:\n"
            + "{[Measures].[Customer Count]}\n"
            + "Row #0: 5,581\n");
    }

    public void testDistinctCountWithAMeasureAsPartOfTuple() {
        assertQueryReturns(
            "SELECT [STORE].[ALL STORES].[USA].[CA] ON 0, "
            + "([MEASURES].[CUSTOMER COUNT], [Gender].[m]) ON 1 FROM SALES",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store].[Store].[USA].[CA]}\n"
            + "Axis #2:\n"
            + "{[Measures].[Customer Count], [Gender].[Gender].[M]}\n"
            + "Row #0: 1,389\n");
    }

    public void testDistinctCountOnSetOfMembers() {
        assertQueryReturns(
            "WITH MEMBER STORE.STORE.X as 'Aggregate({[STORE].[ALL STORES].[USA].[CA],"
            + "[STORE].[ALL STORES].[USA].[WA]})'"
            + "SELECT STORE.X  ON ROWS, "
            + "{[MEASURES].[CUSTOMER COUNT]} ON COLUMNS\n"
            + "FROM [SALES]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Customer Count]}\n"
            + "Axis #2:\n"
            + "{[Store].[Store].[X]}\n"
            + "Row #0: 4,544\n");
    }

    public void testDistinctCountOnTuplesWithSomeNonJoiningDimensions() {
        propSaver.set(
            propSaver.props.IgnoreMeasureForNonJoiningDimension, false);
        String mdx =
            "WITH MEMBER WAREHOUSE.X as 'Aggregate({WAREHOUSE.[STATE PROVINCE].MEMBERS}*"
            + "{[Gender].Members})'"
            + "SELECT WAREHOUSE.X  ON ROWS, "
            + "{[MEASURES].[CUSTOMER COUNT]} ON COLUMNS\n"
            + "FROM [WAREHOUSE AND SALES2]";
        String expectedResult =
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Customer Count]}\n"
            + "Axis #2:\n"
            + "{[Warehouse].[Warehouse].[X]}\n"
            + "Row #0: \n";
        assertQueryReturns(mdx, expectedResult);
        propSaver.set(
            propSaver.props.IgnoreMeasureForNonJoiningDimension, true);
        assertQueryReturns(mdx, expectedResult);
    }

    public void testAggregationListOptimizationForChildren() {
        assertQueryReturns(
            "WITH MEMBER GENDER.X AS 'AGGREGATE({[GENDER].[GENDER].members} * "
            + "{[STORE].[ALL STORES].[USA].[CA], [STORE].[ALL STORES].[USA].[OR], "
            + "[STORE].[ALL STORES].[USA].[WA], [Store].[All Stores].[Canada]})' "
            + "SELECT GENDER.X ON 0, [MEASURES].[CUSTOMER COUNT] ON 1 FROM SALES",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Gender].[Gender].[X]}\n"
            + "Axis #2:\n"
            + "{[Measures].[Customer Count]}\n"
            + "Row #0: 5,581\n");
    }

    public void testDistinctCountOnMembersWithNonJoiningDimensionNotAtAllLevel()
    {
        assertQueryReturns(
            "WITH MEMBER WAREHOUSE.X as "
            + "'Aggregate({WAREHOUSE.[STATE PROVINCE].MEMBERS})'"
            + "SELECT WAREHOUSE.X  ON ROWS, "
            + "{[MEASURES].[CUSTOMER COUNT]} ON COLUMNS\n"
            + "FROM [WAREHOUSE AND SALES2]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Customer Count]}\n"
            + "Axis #2:\n"
            + "{[Warehouse].[Warehouse].[X]}\n"
            + "Row #0: \n");
    }

    public void testNonJoiningDimensionWithAllMember() {
        assertQueryReturns(
            "WITH MEMBER WAREHOUSE.X as 'Aggregate({WAREHOUSE.MEMBERS})'"
            + "SELECT WAREHOUSE.X  ON ROWS, "
            + "{[MEASURES].[CUSTOMER COUNT]} ON COLUMNS\n"
            + "FROM [WAREHOUSE AND SALES2]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Customer Count]}\n"
            + "Axis #2:\n"
            + "{[Warehouse].[Warehouse].[X]}\n"
            + "Row #0: 5,581\n");
    }

    public void testCrossJoinOfJoiningAndNonJoiningDimensionWithAllMember() {
        assertQueryReturns(
            "WITH MEMBER WAREHOUSE.X AS "
            + "'AGGREGATE({GENDER.GENDER.MEMBERS} * {WAREHOUSE.MEMBERS})'"
            + "SELECT WAREHOUSE.X  ON ROWS, "
            + "{[MEASURES].[CUSTOMER COUNT]} ON COLUMNS\n"
            + "FROM [WAREHOUSE AND SALES2]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Customer Count]}\n"
            + "Axis #2:\n"
            + "{[Warehouse].[Warehouse].[X]}\n"
            + "Row #0: 5,581\n");

        assertQueryReturns(
            "WITH MEMBER WAREHOUSE.X AS "
            + "'AGGREGATE({GENDER.GENDER.MEMBERS} * {WAREHOUSE.MEMBERS})'"
            + "SELECT WAREHOUSE.X  ON ROWS, "
            + "{[MEASURES].[CUSTOMER COUNT]} ON COLUMNS\n"
            + "FROM [WAREHOUSE AND SALES3]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Customer Count]}\n"
            + "Axis #2:\n"
            + "{[Warehouse].[Warehouse].[X]}\n"
            + "Row #0: 5,581\n");
    }

    public void testCrossJoinOfJoiningAndNonJoiningDimension() {
        assertQueryReturns(
            "WITH MEMBER WAREHOUSE.X AS "
            + "'AGGREGATE({GENDER.GENDER.MEMBERS} * {WAREHOUSE.[STATE PROVINCE].MEMBERS})'"
            + "SELECT WAREHOUSE.X  ON ROWS, "
            + "{[MEASURES].[CUSTOMER COUNT]} ON COLUMNS\n"
            + "FROM [WAREHOUSE AND SALES2]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Customer Count]}\n"
            + "Axis #2:\n"
            + "{[Warehouse].[Warehouse].[X]}\n"
            + "Row #0: \n");

        assertQueryReturns(
            "WITH MEMBER WAREHOUSE.X AS "
            + "'AGGREGATE({GENDER.GENDER.MEMBERS} * {WAREHOUSE.[STATE PROVINCE].MEMBERS})'"
            + "SELECT WAREHOUSE.X  ON ROWS, "
            + "{[MEASURES].[CUSTOMER COUNT]} ON COLUMNS\n"
            + "FROM [WAREHOUSE AND SALES3]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Customer Count]}\n"
            + "Axis #2:\n"
            + "{[Warehouse].[Warehouse].[X]}\n"
            + "Row #0: 5,581\n");
    }

    public void testAggregationOverLargeListGeneratesError() {
        propSaver.set(propSaver.props.MaxConstraints, 7);

        String result;
        final Dialect dialect = getTestContext().getDialect();
        if (dialect.getDatabaseProduct() == Dialect.DatabaseProduct.LUCIDDB) {
            // LucidDB has no limit on the size of IN list
            result =
                "Axis #0:\n"
                + "{}\n"
                + "Axis #1:\n"
                + "{[Measures].[Customer Count]}\n"
                + "Axis #2:\n"
                + "{[Product].[Product].[X]}\n"
                + "Row #0: 1,360\n";
        } else {
            result =
                "Axis #0:\n"
                + "{}\n"
                + "Axis #1:\n"
                + "{[Measures].[Customer Count]}\n"
                + "Axis #2:\n"
                + "{[Product].[Product].[X]}\n"
                + "Row #0: #ERR: mondrian.olap.fun.MondrianEvaluationException: "
                + "Aggregation is not supported over a list with more than 7 predicates (see property mondrian.rolap.maxConstraints)\n";
        }

        assertQueryReturns(
            "WITH MEMBER PRODUCT.X as 'Aggregate({"
            + "[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good],\n"
            + "[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Portsmouth],\n"
            + "[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Top Measure],\n"
            + "[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Walrus],\n"
            + "[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Pearl],\n"
            + "[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Portsmouth],\n"
            + "[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Top Measure],\n"
            + "[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Walrus]})' "
            + "SELECT PRODUCT.X  ON ROWS, "
            + "{[MEASURES].[CUSTOMER COUNT]} ON COLUMNS\n"
            + "FROM [WAREHOUSE AND SALES2]",
            result);
    }

    /**
     * Test case for
     * <a href="http://jira.pentaho.org/browse/MONDRIAN-1122">MONDRIAN-1122,
     * "Aggregation is not supported over a list with more than 1000
     * predicates"</a>.
     *
     * @see #testAggregationOverLargeListGeneratesError
     */
    public void testAggregateMaxConstraints() {
        propSaver.set(propSaver.props.MaxConstraints, 5);
        TestContext.instance().assertQueryReturns(
            "SELECT\n"
            + "  Measures.[Unit Sales] on columns,\n"
            + "  Product.[Product Family].Members on rows\n"
            + "FROM Sales\n"
            + "WHERE {\n"
            + "  [Time].[All Weeklys].[1997].[1].[15],\n"
            + "  [Time].[All Weeklys].[1997].[2].[1],\n"
            + "  [Time].[All Weeklys].[1997].[3].[11],\n"
            + "  [Time].[All Weeklys].[1997].[4].[13],\n"
            + "  [Time].[All Weeklys].[1997].[5].[22],\n"
            + "  [Time].[All Weeklys].[1997].[6].[1]}",
            "Axis #0:\n"
            + "{[Time].[Weekly].[1997].[1].[15]}\n"
            + "{[Time].[Weekly].[1997].[2].[1]}\n"
            + "{[Time].[Weekly].[1997].[3].[11]}\n"
            + "{[Time].[Weekly].[1997].[4].[13]}\n"
            + "{[Time].[Weekly].[1997].[5].[22]}\n"
            + "{[Time].[Weekly].[1997].[6].[1]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[Products].[Drink]}\n"
            + "{[Product].[Products].[Food]}\n"
            + "{[Product].[Products].[Non-Consumable]}\n"
            + "Row #0: 458\n"
            + "Row #1: 3,746\n"
            + "Row #2: 937\n");
    }

    public void testMultiLevelMembersNullParents() {
        if (!isDefaultNullMemberRepresentation()) {
            return;
        }
        String dimension =
            "<Dimension name=\"Warehouse2\">\n"
            + "  <Hierarchy hasAll=\"true\" primaryKey=\"warehouse_id\">\n"
            + "    <Table name=\"warehouse\"/>\n"
            + "    <Level name=\"address3\" column=\"wa_address3\" uniqueMembers=\"true\"/>\n"
            + "    <Level name=\"address2\" column=\"wa_address2\" uniqueMembers=\"true\"/>\n"
            + "    <Level name=\"address1\" column=\"wa_address1\" uniqueMembers=\"false\"/>\n"
            + "    <Level name=\"name\" column=\"warehouse_name\" uniqueMembers=\"false\"/>\n"
            + "  </Hierarchy>\n"
            + "</Dimension>\n";

        String cube =
            "<Cube name=\"Warehouse2\">\n"
            + "  <Table name=\"inventory_fact_1997\"/>\n"
            + "  <DimensionUsage name=\"Product\" source=\"Product\" foreignKey=\"product_id\"/>\n"
            + "  <DimensionUsage name=\"Warehouse2\" source=\"Warehouse2\" foreignKey=\"warehouse_id\"/>\n"
            + "  <Measure name=\"Cost Count\" column=\"warehouse_cost\" aggregator=\"distinct-count\"/>\n"
            + "</Cube>";

        String query =
            "with set [Filtered Warehouse Set] as "
            + "{[Warehouse2].[#null].[#null].[5617 Saclan Terrace].[Arnold and Sons],"
            + " [Warehouse2].[#null].[#null].[3377 Coachman Place].[Jones International]} "
            + "member [Warehouse2].[TwoMembers] as 'AGGREGATE([Filtered Warehouse Set])' "
            + "select {[Measures].[Cost Count]} on columns, {[Warehouse2].[TwoMembers]} on rows "
            + "from [Warehouse2]";

        String sql =
            "select\n"
            + "    count(distinct `inventory_fact_1997`.`warehouse_cost`) as `m0`\n"
            + "from\n"
            + "    `inventory_fact_1997` as `inventory_fact_1997`,\n"
            + "    `warehouse` as `warehouse`\n"
            + "where\n"
            + "    ((`warehouse`.`wa_address2` is null and (`warehouse`.`warehouse_name`, `warehouse`.`wa_address1`) in (('Arnold and Sons', '5617 Saclan Terrace'), ('Jones International', '3377 Coachman Place'))))\n"
            + "and\n"
            + "    `inventory_fact_1997`.`warehouse_id` = `warehouse`.`warehouse_id`";

        TestContext testContext =
            TestContext.instance().legacy().create(
                dimension,
                cube,
                null,
                null,
                null,
                null);

        assertQuerySql(testContext, query, sql);
    }

    public void testMultiLevelMembersMixedNullNonNullParent() {
        if (!isDefaultNullMemberRepresentation()) {
            return;
        }
        String dimension =
            "<Dimension name=\"Warehouse2\">\n"
            + "  <Hierarchy hasAll=\"true\" primaryKey=\"warehouse_id\">\n"
            + "    <Table name=\"warehouse\"/>\n"
            + "    <Level name=\"fax\" column=\"warehouse_fax\" uniqueMembers=\"true\"/>\n"
            + "    <Level name=\"address1\" column=\"wa_address1\" uniqueMembers=\"false\"/>\n"
            + "    <Level name=\"name\" column=\"warehouse_name\" uniqueMembers=\"false\"/>\n"
            + "  </Hierarchy>\n"
            + "</Dimension>\n";

        String cube =
            "<Cube name=\"Warehouse2\">\n"
            + "  <Table name=\"inventory_fact_1997\"/>\n"
            + "  <DimensionUsage name=\"Product\" source=\"Product\" foreignKey=\"product_id\"/>\n"
            + "  <DimensionUsage name=\"Warehouse2\" source=\"Warehouse2\" foreignKey=\"warehouse_id\"/>\n"
            + "  <Measure name=\"Cost Count\" column=\"warehouse_cost\" aggregator=\"distinct-count\"/>\n"
            + "</Cube>";

        String query =
            "with\n"
            + "set [Filtered Warehouse Set] as "
            + "{[Warehouse2].[#null].[234 West Covina Pkwy].[Freeman And Co],"
            + " [Warehouse2].[971-555-6213].[3377 Coachman Place].[Jones International]} "
            + "member [Warehouse2].[TwoMembers] as 'AGGREGATE([Filtered Warehouse Set])' "
            + "select {[Measures].[Cost Count]} on columns, {[Warehouse2].[TwoMembers]} on rows "
            + "from [Warehouse2]";

        String necjSqlMySql2 =
            "select count(distinct `inventory_fact_1997`.`warehouse_cost`) as `m0` from `warehouse` as `warehouse`, `inventory_fact_1997` as `inventory_fact_1997` where `inventory_fact_1997`.`warehouse_id` = `warehouse`.`warehouse_id` and ((`warehouse`.`warehouse_name` = 'Freeman And Co' and `warehouse`.`wa_address1` = '234 West Covina Pkwy' and `warehouse`.`warehouse_fax` is null) or (`warehouse`.`warehouse_name` = 'Jones International' and `warehouse`.`wa_address1` = '3377 Coachman Place' and `warehouse`.`warehouse_fax` = '971-555-6213'))";

        TestContext testContext =
            TestContext.instance().legacy().create(
                dimension,
                cube,
                null,
                null,
                null,
                null);

        String result =
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Cost Count]}\n"
            + "Axis #2:\n"
            + "{[Warehouse2].[Warehouse2].[TwoMembers]}\n"
            + "Row #0: 220\n";

        testContext.assertQueryReturns(query, result);
    }

    public void testMultiLevelsMixedNullNonNullChild() {
        if (!isDefaultNullMemberRepresentation()) {
            return;
        }
        String dimension =
            "<Dimension name=\"Warehouse2\">\n"
            + "  <Hierarchy hasAll=\"true\" primaryKey=\"warehouse_id\">\n"
            + "    <Table name=\"warehouse\"/>\n"
            + "    <Level name=\"address3\" column=\"wa_address3\" uniqueMembers=\"true\"/>\n"
            + "    <Level name=\"address2\" column=\"wa_address2\" uniqueMembers=\"false\"/>\n"
            + "    <Level name=\"fax\" column=\"warehouse_fax\" uniqueMembers=\"false\"/>\n"
            + "  </Hierarchy>\n"
            + "</Dimension>\n";

        String cube =
            "<Cube name=\"Warehouse2\">\n"
            + "  <Table name=\"inventory_fact_1997\"/>\n"
            + "  <DimensionUsage name=\"Product\" source=\"Product\" foreignKey=\"product_id\"/>\n"
            + "  <DimensionUsage name=\"Warehouse2\" source=\"Warehouse2\" foreignKey=\"warehouse_id\"/>\n"
            + "  <Measure name=\"Cost Count\" column=\"warehouse_cost\" aggregator=\"distinct-count\"/>\n"
            + "</Cube>";

        String query =
            "with\n"
            + "set [Filtered Warehouse Set] as "
            + "{[Warehouse2].[#null].[#null].[#null],"
            + " [Warehouse2].[#null].[#null].[971-555-6213]} "
            + "member [Warehouse2].[TwoMembers] as 'AGGREGATE([Filtered Warehouse Set])' "
            + "select {[Measures].[Cost Count]} on columns, {[Warehouse2].[TwoMembers]} on rows "
            + "from [Warehouse2]";

        String necjSqlMySql2 =
            "select count(distinct `inventory_fact_1997`.`warehouse_cost`) as `m0` from `warehouse` as `warehouse`, `inventory_fact_1997` as `inventory_fact_1997` where `inventory_fact_1997`.`warehouse_id` = `warehouse`.`warehouse_id` and ((`warehouse`.`warehouse_fax` is null and `warehouse`.`wa_address2` is null and `warehouse`.`wa_address3` is null) or (`warehouse`.`warehouse_fax` = '971-555-6213' and `warehouse`.`wa_address2` is null and `warehouse`.`wa_address3` is null))";

        TestContext testContext =
            TestContext.instance().legacy().create(
                dimension,
                cube,
                null,
                null,
                null,
                null);

        String result =
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Cost Count]}\n"
            + "Axis #2:\n"
            + "{[Warehouse2].[Warehouse2].[TwoMembers]}\n"
            + "Row #0: 220\n";

        testContext.assertQueryReturns(query, result);
    }

    public void testAggregationOnCJofMembersGeneratesOptimalQuery() {
        // Mondrian does not use GROUPING SETS for distinct-count measures.
        // So, this test should not use GROUPING SETS, even if they are enabled.
        // See change 12310, bug MONDRIAN-470 (aka SF.net 2207515).

        String oraTeraSql =
            "select \"store\".\"store_state\" as \"c0\","
            + " \"time_by_day\".\"the_year\" as \"c1\","
            + " count(distinct \"sales_fact_1997\".\"customer_id\") as \"m0\" "
            + "from \"store\" =as= \"store\","
            + " \"sales_fact_1997\" =as= \"sales_fact_1997\","
            + " \"time_by_day\" =as= \"time_by_day\" "
            + "where \"sales_fact_1997\".\"store_id\" = \"store\".\"store_id\" "
            + "and \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" "
            + "and \"time_by_day\".\"the_year\" = 1997 "
            + "group by \"store\".\"store_state\", \"time_by_day\".\"the_year\"";

        SqlPattern[] patterns = {
            new SqlPattern(
                Dialect.DatabaseProduct.ORACLE, oraTeraSql, oraTeraSql),
            new SqlPattern(
                Dialect.DatabaseProduct.TERADATA, oraTeraSql, oraTeraSql),
        };
        assertQuerySql(
            getTestContext(),
            "WITH \n"
            + "SET [COG_OQP_INT_s2] AS 'CROSSJOIN({[Store].[Stores].MEMBERS}, "
            + "{{[Gender].[Gender].MEMBERS}, "
            + "{([Gender].[COG_OQP_USR_Aggregate(Gender)])}})' \n"
            + "SET [COG_OQP_INT_s1] AS 'CROSSJOIN({[Store].[Stores].MEMBERS}, "
            + "{[Gender].[Gender].MEMBERS})' \n"
            + "\n"
            + "MEMBER [Store].[Stores].[COG_OQP_USR_Aggregate(Store)] AS '\n"
            + "AGGREGATE({COG_OQP_INT_s1})', SOLVE_ORDER = 4 \n"
            + "\n"
            + "MEMBER [Gender].[COG_OQP_USR_Aggregate(Gender)] AS '\n"
            + "AGGREGATE({[Gender].DEFAULTMEMBER})', SOLVE_ORDER = 8 \n"
            + "\n"
            + "\n"
            + "SELECT {[Measures].[Customer Count]} ON AXIS(0), \n"
            + "{[COG_OQP_INT_s2], HEAD({([Store].[COG_OQP_USR_Aggregate(Store)], "
            + "[Customer].[Gender].DEFAULTMEMBER)}, "
            + "IIF(COUNT([COG_OQP_INT_s1], INCLUDEEMPTY) > 0, 1, 0))} ON AXIS(1) \n"
            + "FROM [sales]",
            patterns);
    }

    public void testCanNotBatchForDifferentCompoundPredicate() {
        propSaver.set(propSaver.props.EnableGroupingSets, true);
        String mdxQueryWithFewMembers =
            "WITH "
            + "MEMBER [Store].[Store].[COG_OQP_USR_Aggregate(Store)] AS "
            + "'AGGREGATE({[Store].[All Stores].[USA].[CA], [Store].[All Stores].[USA].[OR],[Store].[All Stores].[USA].[WA]})', SOLVE_ORDER = 8"
            + "SELECT {[Measures].[Customer Count]} ON AXIS(0), "
            + "{[Store].[All Stores].[USA].[CA], [Store].[All Stores].[USA].[OR], [Store].[COG_OQP_USR_Aggregate(Store)]} "
            + "ON AXIS(1) "
            + "FROM [Sales]";

        String desiredResult =
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Customer Count]}\n"
            + "Axis #2:\n"
            + "{[Store].[Store].[USA].[CA]}\n"
            + "{[Store].[Store].[USA].[OR]}\n"
            + "{[Store].[Store].[COG_OQP_USR_Aggregate(Store)]}\n"
            + "Row #0: 2,716\n"
            + "Row #1: 1,037\n"
            + "Row #2: 5,581\n";

        String  oraTeraSqlForAgg =
            "select \"time_by_day\".\"the_year\" as \"c0\", "
            + "count(distinct \"sales_fact_1997\".\"customer_id\") as \"m0\" "
            + "from \"time_by_day\" =as= \"time_by_day\", "
            + "\"sales_fact_1997\" =as= \"sales_fact_1997\", \"store\" =as= \"store\" "
            + "where \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" "
            + "and \"time_by_day\".\"the_year\" = 1997 and "
            + "\"sales_fact_1997\".\"store_id\" = \"store\".\"store_id\" "
            + "and \"store\".\"store_country\" = 'USA' "
            + "group by \"time_by_day\".\"the_year\"";

        String  oraTeraSqlForDetail =
            "select \"store\".\"store_state\" as \"c0\", "
            + "\"time_by_day\".\"the_year\" as \"c1\", "
            + "count(distinct \"sales_fact_1997\".\"customer_id\") as \"m0\" "
            + "from \"store\" =as= \"store\", "
            + "\"sales_fact_1997\" =as= \"sales_fact_1997\", "
            + "\"time_by_day\" =as= \"time_by_day\" "
            + "where \"sales_fact_1997\".\"store_id\" = \"store\".\"store_id\" "
            + "and \"store\".\"store_state\" in ('CA', 'OR') "
            + "and \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" "
            + "and \"time_by_day\".\"the_year\" = 1997 "
            + "group by \"store\".\"store_state\", \"time_by_day\".\"the_year\"";

        SqlPattern[] patterns = {
            new SqlPattern(
                Dialect.DatabaseProduct.ORACLE,
                oraTeraSqlForAgg,
                oraTeraSqlForAgg),
            new SqlPattern(
                Dialect.DatabaseProduct.TERADATA,
                oraTeraSqlForAgg,
                oraTeraSqlForAgg),
            new SqlPattern(
                Dialect.DatabaseProduct.ORACLE,
                oraTeraSqlForDetail,
                oraTeraSqlForDetail),
            new SqlPattern(
                Dialect.DatabaseProduct.TERADATA,
                oraTeraSqlForDetail,
                oraTeraSqlForDetail),
        };

        assertQueryReturns(mdxQueryWithFewMembers, desiredResult);
        assertQuerySql(getTestContext(), mdxQueryWithFewMembers, patterns);
    }


    /**
     * Test distinct count agg happens in non gs query for subset of members
     * with mixed measures.
     */
    public void testDistinctCountInNonGroupingSetsQuery() {
        propSaver.set(propSaver.props.EnableGroupingSets, true);
        String mdxQueryWithFewMembers =
            "WITH "
            + "MEMBER [Store].[Store].[COG_OQP_USR_Aggregate(Store)] AS "
            + "'AGGREGATE({[Store].[All Stores].[USA].[CA], [Store].[All Stores].[USA].[OR]})', SOLVE_ORDER = 8"
            + "SELECT {[Measures].[Customer Count],[Measures].[Unit Sales]} ON AXIS(0), "
            + "{[Store].[All Stores].[USA].[CA], [Store].[All Stores].[USA].[OR], [Store].[COG_OQP_USR_Aggregate(Store)]} "
            + "ON AXIS(1) "
            + "FROM [Sales]";

        String desiredResult =
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Customer Count]}\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Store].[Store].[USA].[CA]}\n"
            + "{[Store].[Store].[USA].[OR]}\n"
            + "{[Store].[Store].[COG_OQP_USR_Aggregate(Store)]}\n"
            + "Row #0: 2,716\n"
            + "Row #0: 74,748\n"
            + "Row #1: 1,037\n"
            + "Row #1: 67,659\n"
            + "Row #2: 3,753\n"
            + "Row #2: 142,407\n";

        String oraTeraSqlForDetail =
            "select \"store\".\"store_state\" as \"c0\", "
            + "\"time_by_day\".\"the_year\" as \"c1\", "
            + "count(distinct \"sales_fact_1997\".\"customer_id\") as \"m0\", "
            + "sum(\"sales_fact_1997\".\"unit_sales\") as \"m1\" "
            + "from \"store\" =as= \"store\", \"sales_fact_1997\" =as= \"sales_fact_1997\", "
            + "\"time_by_day\" =as= \"time_by_day\" "
            + "where \"sales_fact_1997\".\"store_id\" = \"store\".\"store_id\" "
            + "and \"store\".\"store_state\" in ('CA', 'OR') "
            + "and \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" "
            + "and \"time_by_day\".\"the_year\" = 1997 "
            + "group by \"store\".\"store_state\", \"time_by_day\".\"the_year\"";

        String oraTeraSqlForDistinctCountAgg =
            "select \"time_by_day\".\"the_year\" as \"c0\", "
            + "count(distinct \"sales_fact_1997\".\"customer_id\") as \"m0\" "
            + "from \"time_by_day\" =as= \"time_by_day\", "
            + "\"sales_fact_1997\" =as= \"sales_fact_1997\", \"store\" =as= \"store\" "
            + "where \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" "
            + "and \"time_by_day\".\"the_year\" = 1997 "
            + "and \"sales_fact_1997\".\"store_id\" = \"store\".\"store_id\" "
            + "and \"store\".\"store_state\" in ('CA', 'OR') "
            + "group by \"time_by_day\".\"the_year\"";

        SqlPattern[] patterns = {
            new SqlPattern(
                Dialect.DatabaseProduct.ORACLE,
                oraTeraSqlForDetail,
                oraTeraSqlForDetail),
            new SqlPattern(
                Dialect.DatabaseProduct.TERADATA,
                oraTeraSqlForDetail,
                oraTeraSqlForDetail),
            new SqlPattern(
                Dialect.DatabaseProduct.ORACLE,
                oraTeraSqlForDistinctCountAgg,
                oraTeraSqlForDistinctCountAgg),
            new SqlPattern(
                Dialect.DatabaseProduct.TERADATA,
                oraTeraSqlForDistinctCountAgg,
                oraTeraSqlForDistinctCountAgg),
        };

        assertQueryReturns(mdxQueryWithFewMembers, desiredResult);
        assertQuerySql(getTestContext(), mdxQueryWithFewMembers, patterns);
    }

    public void testAggregationOfMembersAndDefaultMemberWithoutGroupingSets() {
        propSaver.set(propSaver.props.EnableGroupingSets, false);
        String mdxQueryWithMembers =
            "WITH "
            + "MEMBER [Gender].[COG_OQP_USR_Aggregate(Gender)] AS "
            + "'AGGREGATE({[Gender].MEMBERS})', SOLVE_ORDER = 8"
            + "SELECT {[Measures].[Customer Count]} ON AXIS(0), "
            + "{[Gender].MEMBERS, [Gender].[COG_OQP_USR_Aggregate(Gender)]} "
            + "ON AXIS(1) "
            + "FROM [Sales]";

        String mdxQueryWithDefaultMember =
            "WITH "
            + "MEMBER [Gender].[COG_OQP_USR_Aggregate(Gender)] AS "
            + "'AGGREGATE({[Gender].DEFAULTMEMBER})', SOLVE_ORDER = 8"
            + "SELECT {[Measures].[Customer Count]} ON AXIS(0), \n"
            + "{[Gender].MEMBERS, [Gender].[COG_OQP_USR_Aggregate(Gender)]} "
            + "ON AXIS(1) \n"
            + "FROM [sales]";

        String desiredResult =
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Customer Count]}\n"
            + "Axis #2:\n"
            + "{[Gender].[Gender].[All Gender]}\n"
            + "{[Gender].[Gender].[F]}\n"
            + "{[Gender].[Gender].[M]}\n"
            + "{[Gender].[Gender].[COG_OQP_USR_Aggregate(Gender)]}\n"
            + "Row #0: 5,581\n"
            + "Row #1: 2,755\n"
            + "Row #2: 2,826\n"
            + "Row #3: 5,581\n";

        String  oraTeraSql =
            "select \"time_by_day\".\"the_year\" as \"c0\", "
            + "\"customer\".\"gender\" as \"c1\", "
            + "count(distinct \"sales_fact_1997\".\"customer_id\") as \"m0\" "
            + "from \"time_by_day\" =as= \"time_by_day\", "
            + "\"sales_fact_1997\" =as= \"sales_fact_1997\", \"customer\" =as= \"customer\" "
            + "where \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" "
            + "and \"time_by_day\".\"the_year\" = 1997 "
            + "and \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\" "
            + "group by \"time_by_day\".\"the_year\", \"customer\".\"gender\"";

        SqlPattern[] patterns = {
            new SqlPattern(
                Dialect.DatabaseProduct.ORACLE, oraTeraSql, oraTeraSql),
            new SqlPattern(
                Dialect.DatabaseProduct.TERADATA, oraTeraSql, oraTeraSql),
        };

        assertQueryReturns(mdxQueryWithMembers, desiredResult);
        assertQuerySql(getTestContext(), mdxQueryWithMembers, patterns);
        assertQueryReturns(mdxQueryWithDefaultMember, desiredResult);
        assertQuerySql(getTestContext(), mdxQueryWithDefaultMember, patterns);
    }

    public void testOptimizeChildren() {
        String query =
            "with member gender.x as "
            + "'aggregate("
            + "{gender.gender.members * Store.[all stores].[usa].children})' "
            + "select {gender.x} on 0, measures.[customer count] on 1 from sales";
        String expected =
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Gender].[Gender].[x]}\n"
            + "Axis #2:\n"
            + "{[Measures].[Customer Count]}\n"
            + "Row #0: 5,581\n";
        assertQueryReturns(query, expected);

        String derbySql =
            "select \"time_by_day\".\"the_year\" as \"c0\", "
            + "count(distinct \"sales_fact_1997\".\"customer_id\") as \"m0\" "
            + "from \"time_by_day\" as \"time_by_day\", \"sales_fact_1997\" as \"sales_fact_1997\", \"store\" as \"store\" "
            + "where \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" and \"time_by_day\".\"the_year\" = 1997 "
            + "and \"sales_fact_1997\".\"store_id\" = \"store\".\"store_id\" "
            + "and \"store\".\"store_country\" = 'USA' group by \"time_by_day\".\"the_year\"";

        String accessSql =
            "select `d0` as `c0`, count(`m0`) as `c1` "
            + "from (select distinct `time_by_day`.`the_year` as `d0`, `sales_fact_1997`.`customer_id` as `m0` "
            + "from `time_by_day` as `time_by_day`, `sales_fact_1997` as `sales_fact_1997`, `store` as `store` "
            + "where `sales_fact_1997`.`time_id` = `time_by_day`.`time_id` and `time_by_day`.`the_year` = 1997 "
            + "and `sales_fact_1997`.`store_id` = `store`.`store_id` "
            + "and `store`.`store_country` = 'USA') as `dummyname` group by `d0`";

        // For LucidDB, we don't optimize since it supports
        // unlimited IN list.
        String luciddbSql =
            "select \"time_by_day\".\"the_year\" as \"c0\", "
            + "count(distinct \"sales_fact_1997\".\"customer_id\") as \"m0\" "
            + "from \"time_by_day\" as \"time_by_day\", \"sales_fact_1997\" as \"sales_fact_1997\", \"customer\" as \"customer\", \"store\" as \"store\" "
            + "where \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" and \"time_by_day\".\"the_year\" = 1997 and \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\" "
            + "and \"sales_fact_1997\".\"store_id\" = \"store\".\"store_id\" "
            + "and (((\"store\".\"store_state\", \"customer\".\"gender\") in (('CA', 'F'), ('OR', 'F'), ('WA', 'F'), ('CA', 'M'), ('OR', 'M'), ('WA', 'M')))) group by \"time_by_day\".\"the_year\"";

        SqlPattern[] patterns = {
            new SqlPattern(Dialect.DatabaseProduct.DERBY, derbySql, derbySql),
            new SqlPattern(
                Dialect.DatabaseProduct.ACCESS, accessSql, accessSql),
            new SqlPattern(
                Dialect.DatabaseProduct.LUCIDDB, luciddbSql, luciddbSql),
        };

        assertQuerySql(getTestContext(), query, patterns);
    }

    public void testOptimizeListWhenTuplesAreFormedWithDifferentLevels() {
        String query =
            "WITH\n"
            + "MEMBER Product.Agg AS \n"
            + "'Aggregate({[Product].[All Products].[Non-Consumable].[Household].[Kitchen Products].[Pot Scrubbers].[Cormorant],\n"
            + "[Product].[All Products].[Non-Consumable].[Household].[Kitchen Products].[Pot Scrubbers].[Denny],\n"
            + "[Product].[All Products].[Non-Consumable].[Household].[Kitchen Products].[Pot Scrubbers].[High Quality],\n"
            + "[Product].[All Products].[Non-Consumable].[Household].[Kitchen Products].[Pot Scrubbers].[Red Wing],\n"
            + "[Product].[All Products].[Non-Consumable].[Household].[Kitchen Products].[Pots and Pans].[Cormorant],\n"
            + "[Product].[All Products].[Non-Consumable].[Household].[Kitchen Products].[Pots and Pans].[Denny],\n"
            + "[Product].[All Products].[Non-Consumable].[Household].[Kitchen Products].[Pots and Pans].[High Quality],\n"
            + "[Product].[All Products].[Non-Consumable].[Household].[Kitchen Products].[Pots and Pans].[Red Wing],\n"
            + "[Product].[All Products].[Non-Consumable].[Household].[Kitchen Products].[Pots and Pans].[Sunset]} *\n"
            + "{[Gender].[Gender].[Gender].Members})'\n"
            + "SELECT {Product.Agg} on 0, {[Measures].[Customer Count]} on 1\n"
            + "from Sales\n"
            + "where [Time.Weekly].[1997]";
        String expected =
            "Axis #0:\n"
            + "{[Time].[Weekly].[1997]}\n"
            + "Axis #1:\n"
            + "{[Product].[Product].[Agg]}\n"
            + "Axis #2:\n"
            + "{[Measures].[Customer Count]}\n"
            + "Row #0: 421\n";
        assertQueryReturns(query, expected);

        String mysqlSql =
            "select\n"
            + "    `time_by_day`.`the_year` as `c0`,\n"
            + "    count(distinct `sales_fact_1997`.`customer_id`) as `m0`\n"
            + "from\n"
            + "    `sales_fact_1997` as `sales_fact_1997`,\n"
            + "    `time_by_day` as `time_by_day`,\n"
            + "    `product` as `product`,\n"
            + "    `product_class` as `product_class`\n"
            + "where\n"
            + "    `time_by_day`.`the_year` = 1997\n"
            + "and\n"
            + "    ((((`product`.`brand_name`, `product_class`.`product_category`, `product_class`.`product_department`, `product_class`.`product_family`, `product_class`.`product_subcategory`) in"
            + " (('Cormorant', 'Kitchen Products', 'Household', 'Non-Consumable', 'Pot Scrubbers'),"
            + " ('Denny', 'Kitchen Products', 'Household', 'Non-Consumable', 'Pot Scrubbers'),"
            + " ('High Quality', 'Kitchen Products', 'Household', 'Non-Consumable', 'Pot Scrubbers'),"
            + " ('Red Wing', 'Kitchen Products', 'Household', 'Non-Consumable', 'Pot Scrubbers')))) "
            + "or"
            + " (`product_class`.`product_family` = 'Non-Consumable'"
            + " and `product_class`.`product_department` = 'Household'"
            + " and `product_class`.`product_category` = 'Kitchen Products'"
            + " and `product_class`.`product_subcategory` = 'Pots and Pans'))\n"
            + "and\n"
            + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
            + "and\n"
            + "    `sales_fact_1997`.`product_id` = `product`.`product_id`\n"
            + "and\n"
            + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
            + "group by\n"
            + "    `time_by_day`.`the_year`";

        assertQuerySql(getTestContext(), query, mysqlSql);
    }

    public void testOptimizeListWithTuplesOfLength3() {
        String query =
            "WITH\n"
            + "MEMBER Product.Agg AS \n"
            + "'Aggregate"
            + "({[Product].[All Products].[Non-Consumable].[Household].[Kitchen Products].[Pot Scrubbers].[Cormorant],\n"
            + "[Product].[All Products].[Non-Consumable].[Household].[Kitchen Products].[Pot Scrubbers].[Denny],\n"
            + "[Product].[All Products].[Non-Consumable].[Household].[Kitchen Products].[Pot Scrubbers].[High Quality],\n"
            + "[Product].[All Products].[Non-Consumable].[Household].[Kitchen Products].[Pot Scrubbers].[Red Wing],\n"
            + "[Product].[All Products].[Non-Consumable].[Household].[Kitchen Products].[Pots and Pans].[Cormorant],\n"
            + "[Product].[All Products].[Non-Consumable].[Household].[Kitchen Products].[Pots and Pans].[Denny],\n"
            + "[Product].[All Products].[Non-Consumable].[Household].[Kitchen Products].[Pots and Pans].[High Quality],\n"
            + "[Product].[All Products].[Non-Consumable].[Household].[Kitchen Products].[Pots and Pans].[Red Wing],\n"
            + "[Product].[All Products].[Non-Consumable].[Household].[Kitchen Products].[Pots and Pans].[Sunset]} *\n"
            + "{[Gender].[Gender].[Gender].Members}*"
            + "{[Store].[All Stores].[USA].[CA].[Alameda],\n"
            + "[Store].[All Stores].[USA].[CA].[Alameda].[HQ],\n"
            + "[Store].[All Stores].[USA].[CA].[Beverly Hills],\n"
            + "[Store].[All Stores].[USA].[CA].[Beverly Hills].[Store 6],\n"
            + "[Store].[All Stores].[USA].[CA].[Los Angeles],\n"
            + "[Store].[All Stores].[USA].[OR].[Portland],\n"
            + "[Store].[All Stores].[USA].[OR].[Portland].[Store 11],\n"
            + "[Store].[All Stores].[USA].[OR].[Salem],\n"
            + "[Store].[All Stores].[USA].[OR].[Salem].[Store 13]})'\n"
            + "SELECT {Product.Agg} on 0, {[Measures].[Customer Count]} on 1 from Sales";
        String expected =
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Product].[Product].[Agg]}\n"
            + "Axis #2:\n"
            + "{[Measures].[Customer Count]}\n"
            + "Row #0: 189\n";
        assertQueryReturns(query, expected);
    }

    public void testOptimizeChildrenForTuplesWithLength1() {
        TupleList memberList =
            productMembersPotScrubbersPotsAndPans(
                salesCubeSchemaReader);

        TupleList tuples = optimizeChildren(memberList);
        assertTrue(
            tuppleListContains(
                tuples,
                member(
                    Id.Segment.toList(
                        "Product", "All Products", "Non-Consumable",
                        "Household", "Kitchen Products", "Pot Scrubbers",
                        "Cormorant"),
                    salesCubeSchemaReader)));
        assertFalse(
            tuppleListContains(
                tuples,
                member(
                    Id.Segment.toList(
                        "Product", "All Products", "Non-Consumable",
                        "Household", "Kitchen Products", "Pot Scrubbers"),
                    salesCubeSchemaReader)));
        assertFalse(
            tuppleListContains(
                tuples,
                member(
                    Id.Segment.toList(
                        "Product", "All Products", "Non-Consumable",
                        "Household", "Kitchen Products", "Pots and Pans",
                        "Cormorant"),
                    salesCubeSchemaReader)));
        assertTrue(
            tuppleListContains(
                tuples,
                member(
                    Id.Segment.toList(
                        "Product", "All Products", "Non-Consumable",
                        "Household", "Kitchen Products", "Pots and Pans"),
                    salesCubeSchemaReader)));
        assertEquals(4, tuples.size());
    }

    public void testOptimizeChildrenForTuplesWithLength3() {
        TupleList memberList =
            CrossJoinFunDef.mutableCrossJoin(
                genderMembersIncludingAll(
                    false, salesCubeSchemaReader, salesCube),
                productMembersPotScrubbersPotsAndPans(salesCubeSchemaReader));
        memberList =
            CrossJoinFunDef.mutableCrossJoin(
                memberList, storeMembersCAAndOR(salesCubeSchemaReader));
        TupleList tuples = optimizeChildren(memberList);
        assertFalse(
            tuppleListContains(
                tuples,
                member(
                    Id.Segment.toList(
                        "Store", "All Stores", "USA", "OR", "Portland"),
                    salesCubeSchemaReader)));
        assertTrue(
            tuppleListContains(
                tuples,
                member(
                    Id.Segment.toList("Store", "All Stores", "USA", "OR"),
                    salesCubeSchemaReader)));
        assertEquals(16, tuples.size());
    }

    public void testOptimizeChildrenWhenTuplesAreFormedWithDifferentLevels() {
        TupleList memberList =
            CrossJoinFunDef.mutableCrossJoin(
                genderMembersIncludingAll(
                    false, salesCubeSchemaReader, salesCube),
                productMembersPotScrubbersPotsAndPans(salesCubeSchemaReader));
        TupleList tuples = optimizeChildren(memberList);
        assertEquals(4, tuples.size());

        assertFalse(
            tuppleListContains(
                tuples,
                member(
                    Id.Segment.toList(
                        "Product", "All Products", "Non-Consumable",
                        "Household", "Kitchen Products", "Pots and Pans",
                        "Cormorant"),
                salesCubeSchemaReader)));
        assertTrue(
            tuppleListContains(
                tuples,
                member(
                    Id.Segment.toList(
                        "Product", "All Products", "Non-Consumable",
                        "Household", "Kitchen Products", "Pots and Pans"),
                salesCubeSchemaReader)));
        assertTrue(
            tuppleListContains(
                tuples,
                member(
                    Id.Segment.toList(
                        "Product", "All Products", "Non-Consumable",
                        "Household", "Kitchen Products", "Pot Scrubbers",
                        "Cormorant"),
                salesCubeSchemaReader)));
    }

    public void testWhetherCJOfChildren() {
        TupleList memberList =
            CrossJoinFunDef.mutableCrossJoin(
                genderMembersIncludingAll(
                    false, salesCubeSchemaReader, salesCube),
                storeMembersUsaAndCanada(
                    false, salesCubeSchemaReader, salesCube));

        List tuples = optimizeChildren(memberList);
        assertEquals(2, tuples.size());
    }

    public void testShouldNotRemoveDuplicateTuples() {
        Member maleChildMember = member(
            Id.Segment.toList("Gender", "All Gender", "M"),
            salesCubeSchemaReader);
        Member femaleChildMember = member(
            Id.Segment.toList("Gender", "All Gender", "F"),
            salesCubeSchemaReader);

        List<Member> memberList = new ArrayList<Member>();
        memberList.add(maleChildMember);
        memberList.add(maleChildMember);
        memberList.add(femaleChildMember);
        TupleList tuples = new UnaryTupleList(memberList);
        tuples = optimizeChildren(tuples);
        assertEquals(3, tuples.size());
    }

    public void testMemberCountIsSameForAllMembersInTuple() {
        TupleList memberList =
            CrossJoinFunDef.mutableCrossJoin(
                genderMembersIncludingAll(
                    false, salesCubeSchemaReader, salesCube),
                storeMembersUsaAndCanada(
                    false, salesCubeSchemaReader, salesCube));
        Map<Member, Integer>[] memberCounterMap =
            AggregateFunDef.AggregateCalc.membersVersusOccurencesInTuple(
                memberList);

        assertTrue(
            Util.areOccurencesEqual(
                memberCounterMap[0].values()));
        assertTrue(
            Util.areOccurencesEqual(
                memberCounterMap[1].values()));
    }

    public void testMemberCountIsNotSameForAllMembersInTuple() {
        Member maleChild =
            member(
                Id.Segment.toList("Gender", "All Gender", "M"),
                salesCubeSchemaReader);
        Member femaleChild =
            member(
                Id.Segment.toList("Gender", "All Gender", "F"),
                salesCubeSchemaReader);
        Member mexicoMember =
            member(
                Id.Segment.toList("Store", "All Stores", "Mexico"),
                salesCubeSchemaReader);

        TupleList memberList =
            new UnaryTupleList(
                Collections.singletonList(maleChild));

        memberList = CrossJoinFunDef.mutableCrossJoin(
            memberList,
            storeMembersUsaAndCanada(
                false, salesCubeSchemaReader, salesCube));

        memberList.addTuple(femaleChild, mexicoMember);

        Map<Member, Integer>[] memberCounterMap =
            AggregateFunDef.AggregateCalc.membersVersusOccurencesInTuple(
                memberList);

        assertFalse(
            Util.areOccurencesEqual(
                memberCounterMap[0].values()));
        assertTrue(
            Util.areOccurencesEqual(
                memberCounterMap[1].values()));
    }

    public void testAggregatesAtTheSameLevelForNormalAndDistinctCountMeasure() {
        propSaver.set(propSaver.props.EnableGroupingSets, true);
        assertQueryReturns(
            "WITH "
            + "MEMBER GENDER.AGG AS 'AGGREGATE({ GENDER.[F] })' "
            + "MEMBER GENDER.AGG2 AS 'AGGREGATE({ GENDER.[M] })' "
            + "SELECT "
            + "{ MEASURES.[CUSTOMER COUNT], MEASURES.[UNIT SALES] } ON 0, "
            + "{ GENDER.AGG, GENDER.AGG2 } ON 1 \n"
            + "FROM SALES",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Customer Count]}\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Gender].[Gender].[AGG]}\n"
            + "{[Gender].[Gender].[AGG2]}\n"
            + "Row #0: 2,755\n"
            + "Row #0: 131,558\n"
            + "Row #1: 2,826\n"
            + "Row #1: 135,215\n");
    }

    public void testDistinctCountForAggregatesAtTheSameLevel() {
        propSaver.set(propSaver.props.EnableGroupingSets, true);
        assertQueryReturns(
            "WITH "
            + "MEMBER GENDER.AGG AS 'AGGREGATE({ GENDER.[F], GENDER.[M] })' "
            + "SELECT "
            + "{MEASURES.[CUSTOMER COUNT]} ON 0, "
            + "{GENDER.AGG } ON 1 "
            + "FROM SALES",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Customer Count]}\n"
            + "Axis #2:\n"
            + "{[Gender].[Gender].[AGG]}\n"
            + "Row #0: 5,581\n");
    }

    /**
     * This test makes sure that the AggregateFunDef will not optimize a tuples
     * list when the rollup policy is set to something else than FULL, as it
     * results in wrong data for a distinct count operation when using roles to
     * narrow down the members access.
     */
    public void testMondrian906() {
        final TestContext context =
            TestContext.instance().create(
                null, null, null, null, null,
                "<Role name=\"Role1\">\n"
                + "  <SchemaGrant access=\"all\">\n"
                + "    <CubeGrant cube=\"Sales\" access=\"all\">\n"
                + "      <HierarchyGrant hierarchy=\"[Customers]\" access=\"custom\" rollupPolicy=\"partial\">\n"
                + "        <MemberGrant member=\"[Customers].[USA].[OR]\" access=\"all\"/>\n"
                + "        <MemberGrant member=\"[Customers].[USA].[WA]\" access=\"all\"/>\n"
                + "      </HierarchyGrant>\n"
                + "    </CubeGrant>\n"
                + "  </SchemaGrant>\n"
                + "</Role>\n");
        final String mdx =
            "select {[Customers].[USA], [Customers].[USA].[OR], [Customers].[USA].[WA]} on columns, {[Measures].[Customer Count]} on rows from [Sales]";
        context
            .withRole("Role1")
                .assertQueryReturns(
                    mdx,
                    "Axis #0:\n"
                    + "{}\n"
                    + "Axis #1:\n"
                    + "{[Customer].[Customers].[USA]}\n"
                    + "{[Customer].[Customers].[USA].[OR]}\n"
                    + "{[Customer].[Customers].[USA].[WA]}\n"
                    + "Axis #2:\n"
                    + "{[Measures].[Customer Count]}\n"
                    + "Row #0: 2,865\n"
                    + "Row #0: 1,037\n"
                    + "Row #0: 1,828\n");
    }

    private boolean tuppleListContains(
        TupleList tuples,
        Member memberByUniqueName)
    {
        if (tuples.getArity() == 1) {
            return tuples.contains(
                Collections.singletonList(memberByUniqueName));
        }
        for (List<Member> tuple : tuples) {
            if (tuple.contains(memberByUniqueName)) {
                return true;
            }
        }
        return false;
    }

    private TupleList optimizeChildren(final TupleList memberList) {
        return Locus.execute(
            Execution.NONE,
            "AggregationOnDistinctCountMeasuresTest",
            new Locus.Action<TupleList>() {
                public TupleList execute() {
                    return AggregateFunDef.AggregateCalc.optimizeChildren(
                        memberList,
                        schemaReader,
                        salesCube.getMeasureGroups().get(0));
                }
            }
        );
    }

    /**
     * Test case for
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-1370">MONDRIAN-1370</a>
     * <br> Wrong results for aggregate with distinct count measure.
     */
    public void testDistinctCountAggMeasure() {
        final String dimension =
            "<Dimension name='Time' table='time_by_day' type='TIME' key='Time Id'> \n"
            + "  <Attributes>\n"
            + "      <Attribute name='Year' keyColumn='the_year' levelType='TimeYears' hasHierarchy='false'/>\n"
            + "      <Attribute name='Quarter' levelType='TimeQuarters' hasHierarchy='false'>\n"
            + "   <Key>\n"
            + "       <Column name='the_year'/>\n"
            + "       <Column name='quarter'/>\n"
            + "   </Key>\n"
            + "   <Name>\n"
            + "       <Column name='quarter'/>\n"
            + "   </Name>\n"
            + "      </Attribute>\n"
            + "      <Attribute name='Month' levelType='TimeMonths' hasHierarchy='false'>\n"
            + " <Key>\n"
            + "     <Column name='the_year'/>\n"
            + "     <Column name='month_of_year'/>\n"
            + " </Key>\n"
            + " <Name>\n"
            + "     <Column name='month_of_year'/>\n"
            + " </Name>\n"
            + "      </Attribute>\n"
            + "      <Attribute name='Time Id' keyColumn='time_id' hasHierarchy='false'/>\n"
            + "  </Attributes>\n"
            + "  <Hierarchies>\n"
            + "    <Hierarchy name='Time' hasAll='false'>\n"
            + "      <Level attribute='Year'/>\n"
            + "      <Level attribute='Quarter'/>\n"
            + "      <Level attribute='Month'/>\n"
            + "    </Hierarchy>\n"
            + "  </Hierarchies>\n"
            + "</Dimension>\n";
        final String cube =
            "<Cube name='Sales'>\n"
            + "  <Dimensions>\n"
            + "    <Dimension source='Time'/>\n"
            + "  </Dimensions>\n"
            + "  <MeasureGroups>\n"
            + "   <MeasureGroup name='Sales' table='sales_fact_1997'>\n"
            + "      <Measures>\n"
            + "        <Measure name='Customer Count' column='customer_id' aggregator='distinct-count' formatString='#,###'/>\n"
            + "     </Measures>\n"
            + "     <DimensionLinks>\n"
            + "       <ForeignKeyLink dimension='Time' foreignKeyColumn='time_id'/>\n"
            + "     </DimensionLinks>\n"
            + "   </MeasureGroup>\n"
            + "   <MeasureGroup table='agg_c_10_sales_fact_1997' type='aggregate'>\n"
            + "     <Measures>\n"
            + "       <MeasureRef name='Customer Count' aggColumn='customer_count'/>\n"
            + "     </Measures>\n"
            + "     <DimensionLinks>\n"
            + "       <CopyLink dimension='Time' attribute='Month'>\n"
            + "         <Column aggColumn='the_year' table='time_by_day' name='the_year'/>\n"
            + "         <Column aggColumn='quarter' table='time_by_day' name='quarter'/>\n"
            + "         <Column aggColumn='month_of_year' table='time_by_day' name='month_of_year'/>\n"
            + "        </CopyLink>\n"
            + "     </DimensionLinks>\n"
            + "   </MeasureGroup>\n"
            + "  </MeasureGroups>\n"
            + "</Cube>\n";
        final String schema = "<?xml version='1.0'?>\n"
            + "<Schema name='FoodMart' metamodelVersion='4.0'>\n"
            + "<PhysicalSchema>\n"
            + "  <Table name='sales_fact_1997'/>\n"
            + "  <Table name='time_by_day'/>\n"
            + "  <Table name='agg_c_10_sales_fact_1997'/>\n"
            + "</PhysicalSchema>\n"
            + dimension
            + cube
            + "</Schema>";
        final String query =
            "select "
            + "  NON EMPTY {[Measures].[Customer Count]} ON COLUMNS, "
            + "  NON EMPTY {[Time].[Year].Members} ON ROWS "
            + "from [Sales]";
        final String monthsQuery =
            "select "
            + "  NON EMPTY {[Measures].[Customer Count]} ON COLUMNS, "
            + "  NON EMPTY {[Time].[1997].[Q1].Children} ON ROWS "
            + "from [Sales]";
        // should skip aggregate table, cannot aggregate
        propSaver.set(propSaver.props.UseAggregates, true);
        propSaver.set(propSaver.props.ReadAggregates, true);

        TestContext withAggDistinctCount =
            TestContext.instance().withSchema(schema);
        withAggDistinctCount.assertQueryReturns(
            query,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Customer Count]}\n"
            + "Axis #2:\n"
            + "{[Time].[Time].[1997]}\n"
            + "Row #0: 5,581\n");

        if (!Bug.BugMondrian1375Fixed) {
            return;
        }
        // aggregate table has count for months, make sure it is used
        propSaver.set(propSaver.props.UseAggregates, true);
        propSaver.set(propSaver.props.ReadAggregates, true);
        propSaver.set(propSaver.props.GenerateFormattedSql, true);
        final String expectedSql =
            "select\n"
            + "    `agg_c_10_sales_fact_1997`.`the_year` as `c0`,\n"
            + "    `agg_c_10_sales_fact_1997`.`quarter` as `c1`,\n"
            + "    `agg_c_10_sales_fact_1997`.`month_of_year` as `c2`,\n"
            + "    `agg_c_10_sales_fact_1997`.`customer_count` as `m0`\n"
            + "from\n"
            + "    `agg_c_10_sales_fact_1997` as `agg_c_10_sales_fact_1997`\n"
            + "where\n"
            + "    `agg_c_10_sales_fact_1997`.`the_year` = 1997\n"
            + "and\n"
            + "    `agg_c_10_sales_fact_1997`.`quarter` = 'Q1'\n"
            + "and\n"
            + "    `agg_c_10_sales_fact_1997`.`month_of_year` in (1, 2, 3)";
        assertQuerySqlOrNot(
            withAggDistinctCount,
            monthsQuery,
            new SqlPattern[]{
                new SqlPattern(
                    DatabaseProduct.MYSQL,
                    expectedSql,
                    expectedSql.indexOf("from"))},
            false,
            true,
            true);
    }
}

// End AggregationOnDistinctCountMeasuresTest.java
