/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2001-2008 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// ajogleka, 19 December, 2007
*/
package mondrian.rolap.agg;

import mondrian.olap.*;
import mondrian.olap.fun.AggregateFunDef;
import mondrian.olap.fun.CrossJoinFunDef;
import mondrian.rolap.BatchTestCase;
import mondrian.rolap.RolapCube;
import mondrian.test.SqlPattern;
import mondrian.test.TestContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * <code>AggregationOnDistinctCountMeasureTest</code> tests the
 * Distinct Count functionality with tuples and members.
 *
 * @author ajogleka
 * @version $Id$
 * @since 19 December, 2007
 */
public class AggregationOnDistinctCountMeasuresTest extends BatchTestCase {
    private final MondrianProperties props = MondrianProperties.instance();

    private SchemaReader salesCubeSchemaReader = null;
    private SchemaReader schemaReader = null;
    private RolapCube salesCube;

    protected void setUp() throws Exception {
        schemaReader = getTestContext().getConnection().getSchemaReader();
        salesCube = (RolapCube) cubeByName(getTestContext().getConnection(),
            cubeNameSales);
        salesCubeSchemaReader = salesCube.
            getSchemaReader(getTestContext().getConnection().getRole());
    }

    public TestContext getTestContext() {

        final TestContext testContext =
            TestContext.create(null, null,
                "<VirtualCube name=\"Warehouse and Sales2\" defaultMeasure=\"Store Sales\">\n" +
                "   <VirtualCubeDimension cubeName=\"Sales\" name=\"Gender\"/>\n" +
                "   <VirtualCubeDimension name=\"Store\"/>\n" +
                "   <VirtualCubeDimension name=\"Product\"/>\n" +
                "   <VirtualCubeDimension cubeName=\"Warehouse\" name=\"Warehouse\"/>\n" +
                "   <VirtualCubeMeasure cubeName=\"Sales\" name=\"[Measures].[Store Sales]\"/>\n" +
                "   <VirtualCubeMeasure cubeName=\"Sales\" name=\"[Measures].[Customer Count]\"/>\n" +
                "</VirtualCube>" +
                "<VirtualCube name=\"Warehouse and Sales3\" defaultMeasure=\"Store Invoice\">\n" +
                "  <CubeUsages>\n" +
                "       <CubeUsage cubeName=\"Sales\" ignoreUnrelatedDimensions=\"true\"/>" +
                "   </CubeUsages>\n" +
                "   <VirtualCubeDimension cubeName=\"Sales\" name=\"Gender\"/>\n" +
                "   <VirtualCubeDimension name=\"Store\"/>\n" +
                "   <VirtualCubeDimension name=\"Product\"/>\n" +
                "   <VirtualCubeDimension cubeName=\"Warehouse\" name=\"Warehouse\"/>\n" +
                "   <VirtualCubeMeasure cubeName=\"Sales\" name=\"[Measures].[Customer Count]\"/>\n" +
                "</VirtualCube>",
                null, null, null);
        return testContext;
    }

    public void testTupleWithAllLevelMembersOnly() {
        assertQueryReturns(
            "WITH MEMBER GENDER.X AS 'AGGREGATE({([GENDER].DEFAULTMEMBER,\n" +
            "[STORE].DEFAULTMEMBER)})'\n" +
            "SELECT GENDER.X ON 0, [MEASURES].[CUSTOMER COUNT] ON 1 FROM SALES",
            fold(
                "Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Gender].[X]}\n" +
                "Axis #2:\n" +
                "{[Measures].[Customer Count]}\n" +
                "Row #0: 5,581\n"));

    }

    public void testCrossJoinOfAllMembers() {
        assertQueryReturns(
            "WITH MEMBER GENDER.X AS 'AGGREGATE({CROSSJOIN({[GENDER].DEFAULTMEMBER},\n" +
            "{[STORE].DEFAULTMEMBER})})'\n" +
            "SELECT GENDER.X ON 0, [MEASURES].[CUSTOMER COUNT] ON 1 FROM SALES",
            fold(
                "Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Gender].[X]}\n" +
                "Axis #2:\n" +
                "{[Measures].[Customer Count]}\n" +
                "Row #0: 5,581\n"));

    }

    public void testCrossJoinMembersWithASingleMember() {
        String query =
            "WITH MEMBER GENDER.X AS 'AGGREGATE({[GENDER].[GENDER].members} * " +
            "{[STORE].[ALL STORES].[USA].[CA]})', solve_order=100 " +
            "SELECT GENDER.X ON 0, [MEASURES].[CUSTOMER COUNT] ON 1 FROM SALES";
        String result =
            "Axis #0:\n" +
            "{}\n" +
            "Axis #1:\n" +
            "{[Gender].[X]}\n" +
            "Axis #2:\n" +
            "{[Measures].[Customer Count]}\n" +
            "Row #0: 2,716\n";

        assertQueryReturns(query, fold(result));

        // Check aggregate loading sql pattern
        String derbySql =
            "select \"time_by_day\".\"the_year\" as \"c0\", " +
            "count(distinct \"sales_fact_1997\".\"customer_id\") as \"m0\" " +
            "from \"time_by_day\" as \"time_by_day\", \"sales_fact_1997\" as \"sales_fact_1997\", \"store\" as \"store\" " +
            "where \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" " +
            "and \"time_by_day\".\"the_year\" = 1997 " +
            "and \"sales_fact_1997\".\"store_id\" = \"store\".\"store_id\" and \"store\".\"store_state\" = 'CA' " +
            "group by \"time_by_day\".\"the_year\"";

        String mysqlSql =
            "select `time_by_day`.`the_year` as `c0`, " +
            "count(distinct `sales_fact_1997`.`customer_id`) as `m0` " +
            "from `time_by_day` as `time_by_day`, `sales_fact_1997` as `sales_fact_1997`, `store` as `store` " +
            "where `sales_fact_1997`.`time_id` = `time_by_day`.`time_id` " +
            "and `time_by_day`.`the_year` = 1997 " +
            "and `sales_fact_1997`.`store_id` = `store`.`store_id` and `store`.`store_state` = 'CA' " +
            "group by `time_by_day`.`the_year`";

        String oraTeraSql =
            "select \"time_by_day\".\"the_year\" as \"c0\", " +
            "count(distinct \"sales_fact_1997\".\"customer_id\") as \"m0\" " +
            "from \"time_by_day\" =as= \"time_by_day\", \"sales_fact_1997\" =as= \"sales_fact_1997\", \"store\" =as= \"store\" " +
            "where \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" and \"time_by_day\".\"the_year\" = 1997 " +
            "and \"sales_fact_1997\".\"store_id\" = \"store\".\"store_id\" and \"store\".\"store_state\" = 'CA' " +
            "group by \"time_by_day\".\"the_year\"";

        SqlPattern[] patterns = {
            new SqlPattern(SqlPattern.Dialect.DERBY, derbySql, derbySql),
            new SqlPattern(SqlPattern.Dialect.MYSQL, mysqlSql, mysqlSql),
            new SqlPattern(SqlPattern.Dialect.ORACLE, oraTeraSql, oraTeraSql),
            new SqlPattern(SqlPattern.Dialect.TERADATA, oraTeraSql, oraTeraSql),
        };

        assertQuerySql(query, patterns);

    }

    public void testCrossJoinMembersWithSetOfMembers() {
        String query =
            "WITH MEMBER GENDER.X AS 'AGGREGATE({[GENDER].[GENDER].members} * " +
            "{[STORE].[ALL STORES].[USA].[CA], [Store].[All Stores].[Canada]})', solve_order=100 " +
            "SELECT GENDER.X ON 0, [MEASURES].[CUSTOMER COUNT] ON 1 FROM SALES";

        String result =
            "Axis #0:\n" +
            "{}\n" +
            "Axis #1:\n" +
            "{[Gender].[X]}\n" +
            "Axis #2:\n" +
            "{[Measures].[Customer Count]}\n" +
            "Row #0: 2,716\n";

        assertQueryReturns(query, fold(result));

        // Check aggregate loading sql pattern
        // Note Derby does not support multicolumn IN list, so the predicates remain in AND/OR form.
        String derbySql =
            "select \"time_by_day\".\"the_year\" as \"c0\", " +
            "count(distinct \"sales_fact_1997\".\"customer_id\") as \"m0\" " +
            "from \"time_by_day\" as \"time_by_day\", \"sales_fact_1997\" as \"sales_fact_1997\", \"store\" as \"store\" " +
            "where \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" " +
            "and \"time_by_day\".\"the_year\" = 1997 " +
            "and \"sales_fact_1997\".\"store_id\" = \"store\".\"store_id\" " +
            "and (\"store\".\"store_country\" = 'Canada' or \"store\".\"store_state\" = 'CA') " +
            "group by \"time_by_day\".\"the_year\"";

        String mysqlSql =
            "select `time_by_day`.`the_year` as `c0`, " +
            "count(distinct `sales_fact_1997`.`customer_id`) as `m0` " +
            "from `time_by_day` as `time_by_day`, `sales_fact_1997` as `sales_fact_1997`, `store` as `store` " +
            "where `sales_fact_1997`.`time_id` = `time_by_day`.`time_id` and `time_by_day`.`the_year` = 1997 " +
            "and `sales_fact_1997`.`store_id` = `store`.`store_id` " +
            "and (`store`.`store_country` = 'Canada' or `store`.`store_state` = 'CA') " +
            "group by `time_by_day`.`the_year`";

        String oraTeraSql=
            "select \"time_by_day\".\"the_year\" as \"c0\", " +
            "count(distinct \"sales_fact_1997\".\"customer_id\") as \"m0\" " +
            "from \"time_by_day\" =as= \"time_by_day\", \"sales_fact_1997\" =as= \"sales_fact_1997\", \"store\" =as= \"store\" " +
            "where \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" and \"time_by_day\".\"the_year\" = 1997 " +
            "and \"sales_fact_1997\".\"store_id\" = \"store\".\"store_id\" " +
            "and (\"store\".\"store_country\" = 'Canada' or \"store\".\"store_state\" = 'CA') " +
            "group by \"time_by_day\".\"the_year\"";

        SqlPattern[] patterns = {
            new SqlPattern(SqlPattern.Dialect.DERBY, derbySql, derbySql),
            new SqlPattern(SqlPattern.Dialect.MYSQL, mysqlSql, mysqlSql),
            new SqlPattern(SqlPattern.Dialect.ORACLE, oraTeraSql, oraTeraSql),
            new SqlPattern(SqlPattern.Dialect.TERADATA, oraTeraSql, oraTeraSql),
        };

        assertQuerySql(query, patterns);

    }

    public void testCrossJoinParticularMembersFromTwoDimensions() {
        assertQueryReturns(
            "WITH MEMBER GENDER.X AS 'AGGREGATE({[GENDER].M} * " +
            "{[STORE].[ALL STORES].[USA].[CA]})', solve_order=100 " +
            "SELECT GENDER.X ON 0, [MEASURES].[CUSTOMER COUNT] ON 1 FROM SALES",
            fold(
                "Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Gender].[X]}\n" +
                "Axis #2:\n" +
                "{[Measures].[Customer Count]}\n" +
                "Row #0: 1,389\n"));

    }

    public void testDistinctCountOnSetOfMembersFromOneDimension() {
        assertQueryReturns(
            "WITH MEMBER GENDER.X AS 'AGGREGATE({[GENDER].[GENDER].members})'" +
            "SELECT GENDER.X ON 0, [MEASURES].[CUSTOMER COUNT] ON 1 FROM SALES",
            fold(
                "Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Gender].[X]}\n" +
                "Axis #2:\n" +
                "{[Measures].[Customer Count]}\n" +
                "Row #0: 5,581\n"));

    }

    public void testDistinctCountWithAMeasureAsPartOfTuple() {
        assertQueryReturns("SELECT [STORE].[ALL STORES].[USA].[CA] ON 0, " +
            "([MEASURES].[CUSTOMER COUNT], [Gender].[m]) ON 1 FROM SALES",
            fold(
                "Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Store].[All Stores].[USA].[CA]}\n" +
                "Axis #2:\n" +
                "{[Measures].[Customer Count], [Gender].[All Gender].[M]}\n" +
                "Row #0: 1,389\n"));
    }

    public void testDistinctCountOnSetOfMembers() {
        assertQueryReturns(
            "WITH MEMBER STORE.X as 'Aggregate({[STORE].[ALL STORES].[USA].[CA]," +
            "[STORE].[ALL STORES].[USA].[WA]})'" +
            "SELECT STORE.X  ON ROWS, " +
            "{[MEASURES].[CUSTOMER COUNT]} ON COLUMNS\n" +
            "FROM [SALES]",
            fold(
                "Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Measures].[Customer Count]}\n" +
                "Axis #2:\n" +
                "{[Store].[X]}\n" +
                "Row #0: 4,544\n"));
    }

    public void testDistinctCountOnTuplesWithSomeNonJoiningDimensions() {

        boolean orginalPropertyValue = props.IgnoreMeasureForNonJoiningDimension.get();
        props.IgnoreMeasureForNonJoiningDimension.set(false);
        String mdx = "WITH MEMBER WAREHOUSE.X as 'Aggregate({WAREHOUSE.[STATE PROVINCE].MEMBERS}*" +
            "{[Gender].Members})'" +
            "SELECT WAREHOUSE.X  ON ROWS, " +
            "{[MEASURES].[CUSTOMER COUNT]} ON COLUMNS\n" +
            "FROM [WAREHOUSE AND SALES2]";
        String expectedResult = fold(
            "Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Measures].[Customer Count]}\n" +
                "Axis #2:\n" +
                "{[Warehouse].[X]}\n" +
                "Row #0: \n");
        assertQueryReturns(mdx, expectedResult);
        props.IgnoreMeasureForNonJoiningDimension.set(true);
        assertQueryReturns(mdx, expectedResult);
        props.IgnoreMeasureForNonJoiningDimension.set(orginalPropertyValue);
    }

    public void testAggregationListOptimizationForChildren() {
        String query =
            "WITH MEMBER GENDER.X AS 'AGGREGATE({[GENDER].[GENDER].members} * " +
            "{[STORE].[ALL STORES].[USA].[CA], [STORE].[ALL STORES].[USA].[OR], " +
            "[STORE].[ALL STORES].[USA].[WA], [Store].[All Stores].[Canada]})' " +
            "SELECT GENDER.X ON 0, [MEASURES].[CUSTOMER COUNT] ON 1 FROM SALES";

        String result =
            "Axis #0:\n" +
            "{}\n" +
            "Axis #1:\n" +
            "{[Gender].[X]}\n" +
            "Axis #2:\n" +
            "{[Measures].[Customer Count]}\n" +
            "Row #0: 5,581\n";

        assertQueryReturns(query, fold(result));
    }


    public void testDistinctCountOnMembersWithNonJoiningDimensionNotAtAllLevel() {
        assertQueryReturns(
            "WITH MEMBER WAREHOUSE.X as " +
            "'Aggregate({WAREHOUSE.[STATE PROVINCE].MEMBERS})'" +
            "SELECT WAREHOUSE.X  ON ROWS, " +
            "{[MEASURES].[CUSTOMER COUNT]} ON COLUMNS\n" +
            "FROM [WAREHOUSE AND SALES2]",
            fold(
                "Axis #0:\n" +
                    "{}\n" +
                    "Axis #1:\n" +
                    "{[Measures].[Customer Count]}\n" +
                    "Axis #2:\n" +
                    "{[Warehouse].[X]}\n" +
                    "Row #0: \n"));

    }

    public void testNonJoiningDimensionWithAllMember() {
        assertQueryReturns(
            "WITH MEMBER WAREHOUSE.X as 'Aggregate({WAREHOUSE.MEMBERS})'" +
            "SELECT WAREHOUSE.X  ON ROWS, " +
            "{[MEASURES].[CUSTOMER COUNT]} ON COLUMNS\n" +
            "FROM [WAREHOUSE AND SALES2]",
            fold(
                "Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Measures].[Customer Count]}\n" +
                "Axis #2:\n" +
                "{[Warehouse].[X]}\n" +
                "Row #0: 5,581\n"));
    }

    public void testCrossJoinOfJoiningAndNonJoiningDimensionWithAllMember() {
        assertQueryReturns(
            "WITH MEMBER WAREHOUSE.X AS " +
            "'AGGREGATE({GENDER.GENDER.MEMBERS} * {WAREHOUSE.MEMBERS})'" +
            "SELECT WAREHOUSE.X  ON ROWS, " +
            "{[MEASURES].[CUSTOMER COUNT]} ON COLUMNS\n" +
            "FROM [WAREHOUSE AND SALES2]",
            fold(
                "Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Measures].[Customer Count]}\n" +
                "Axis #2:\n" +
                "{[Warehouse].[X]}\n" +
                "Row #0: 5,581\n"));

        assertQueryReturns(
            "WITH MEMBER WAREHOUSE.X AS " +
            "'AGGREGATE({GENDER.GENDER.MEMBERS} * {WAREHOUSE.MEMBERS})'" +
            "SELECT WAREHOUSE.X  ON ROWS, " +
            "{[MEASURES].[CUSTOMER COUNT]} ON COLUMNS\n" +
            "FROM [WAREHOUSE AND SALES3]",
            fold(
                "Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Measures].[Customer Count]}\n" +
                "Axis #2:\n" +
                "{[Warehouse].[X]}\n" +
                "Row #0: 5,581\n"));
    }

    public void testCrossJoinOfJoiningAndNonJoiningDimension() {
        assertQueryReturns(
            "WITH MEMBER WAREHOUSE.X AS " +
            "'AGGREGATE({GENDER.GENDER.MEMBERS} * {WAREHOUSE.[STATE PROVINCE].MEMBERS})'" +
            "SELECT WAREHOUSE.X  ON ROWS, " +
            "{[MEASURES].[CUSTOMER COUNT]} ON COLUMNS\n" +
            "FROM [WAREHOUSE AND SALES2]",
            fold(
                "Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Measures].[Customer Count]}\n" +
                "Axis #2:\n" +
                "{[Warehouse].[X]}\n" +
                "Row #0: \n"));

        assertQueryReturns(
            "WITH MEMBER WAREHOUSE.X AS " +
            "'AGGREGATE({GENDER.GENDER.MEMBERS} * {WAREHOUSE.[STATE PROVINCE].MEMBERS})'" +
            "SELECT WAREHOUSE.X  ON ROWS, " +
            "{[MEASURES].[CUSTOMER COUNT]} ON COLUMNS\n" +
            "FROM [WAREHOUSE AND SALES3]",
            fold(
                "Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Measures].[Customer Count]}\n" +
                "Axis #2:\n" +
                "{[Warehouse].[X]}\n" +
                "Row #0: 5,581\n"));
    }

    public void testAggregationOverLargeListGeneratesError() {
//        assertQueryReturns("select {[PRODUCT].[BRAND NAME].MEMBERS} on 0 from sales", "");
        int origMaxConstraint = props.MaxConstraints.get();
        props.MaxConstraints.set(7);

        String query =
            "WITH MEMBER PRODUCT.X as 'Aggregate({" +
            "[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good],\n" +
            "[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Portsmouth],\n" +
            "[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Top Measure],\n" +
            "[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Walrus],\n" +
            "[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Pearl],\n" +
            "[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Portsmouth],\n" +
            "[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Top Measure],\n" +
            "[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Walrus]})' " +
            "SELECT PRODUCT.X  ON ROWS, " +
            "{[MEASURES].[CUSTOMER COUNT]} ON COLUMNS\n" +
            "FROM [WAREHOUSE AND SALES2]";

        String result;
        if (getTestContext().getDialect().isLucidDB()) {
            // LucidDB has no limit on the size of IN list
            result =
                "Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Measures].[Customer Count]}\n" +
                "Axis #2:\n" +
                "{[Product].[X]}\n" +
                "Row #0: 1,360\n";
        } else {
            result =
                "Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Measures].[Customer Count]}\n" +
                "Axis #2:\n" +
                "{[Product].[X]}\n" +
                "Row #0: #ERR: mondrian.olap.fun.MondrianEvaluationException: " +
                "Distinct Count aggregation is not supported over a list with more than 7 predicates (see property mondrian.rolap.maxConstraints)\n";
        }

        assertQueryReturns(query, fold(result));

        props.MaxConstraints.set(origMaxConstraint);
    }

    public void testMultiLevelMembersNullParents() {
        if (!isDefaultNullMemberRepresentation()) {
            return;
        }
        String dimension =
            "<Dimension name=\"Warehouse2\">\n" +
            "  <Hierarchy hasAll=\"true\" primaryKey=\"warehouse_id\">\n" +
            "    <Table name=\"warehouse\"/>\n" +
            "    <Level name=\"address3\" column=\"wa_address3\" uniqueMembers=\"true\"/>\n" +
            "    <Level name=\"address2\" column=\"wa_address2\" uniqueMembers=\"true\"/>\n" +
            "    <Level name=\"address1\" column=\"wa_address1\" uniqueMembers=\"false\"/>\n" +
            "    <Level name=\"name\" column=\"warehouse_name\" uniqueMembers=\"false\"/>\n" +
            "  </Hierarchy>\n" +
            "</Dimension>\n";

        String cube =
            "<Cube name=\"Warehouse2\">\n" +
            "  <Table name=\"inventory_fact_1997\"/>\n" +
            "  <DimensionUsage name=\"Product\" source=\"Product\" foreignKey=\"product_id\"/>\n" +
            "  <DimensionUsage name=\"Warehouse2\" source=\"Warehouse2\" foreignKey=\"warehouse_id\"/>\n" +
            "  <Measure name=\"Cost Count\" column=\"warehouse_cost\" aggregator=\"distinct-count\"/>\n" +
            "</Cube>";

        String query =
            "with set [Filtered Warehouse Set] as " +
            "{[Warehouse2].[#null].[#null].[5617 Saclan Terrace].[Arnold and Sons]," +
            " [Warehouse2].[#null].[#null].[3377 Coachman Place].[Jones International]} " +
            "member [Warehouse2].[TwoMembers] as 'AGGREGATE([Filtered Warehouse Set])' " +
            "select {[Measures].[Cost Count]} on columns, {[Warehouse2].[TwoMembers]} on rows " +
            "from [Warehouse2]";

        String necjSqlDerby =
            "select count(distinct \"inventory_fact_1997\".\"warehouse_cost\") as \"m0\" " +
            "from \"warehouse\" as \"warehouse\", " +
            "\"inventory_fact_1997\" as \"inventory_fact_1997\" " +
            "where \"inventory_fact_1997\".\"warehouse_id\" = \"warehouse\".\"warehouse_id\" " +
            "and ((\"warehouse\".\"warehouse_name\" = 'Arnold and Sons' " +
            "and \"warehouse\".\"wa_address1\" = '5617 Saclan Terrace' " +
            "and \"warehouse\".\"wa_address2\" is null) " +
            "or (\"warehouse\".\"warehouse_name\" = 'Jones International' " +
            "and \"warehouse\".\"wa_address1\" = '3377 Coachman Place' " +
            "and \"warehouse\".\"wa_address2\" is null))";

        String necjSqlMySql =
            "select count(distinct `inventory_fact_1997`.`warehouse_cost`) as `m0` " +
            "from `warehouse` as `warehouse`, `inventory_fact_1997` as `inventory_fact_1997` " +
            "where `inventory_fact_1997`.`warehouse_id` = `warehouse`.`warehouse_id` " +
            "and ((`warehouse`.`wa_address2` is null " +
            "and (`warehouse`.`wa_address1`, `warehouse`.`warehouse_name`) " +
            "in (('5617 Saclan Terrace', 'Arnold and Sons'), " +
            "('3377 Coachman Place', 'Jones International'))))";

        TestContext testContext =
            TestContext.create(
                dimension,
                cube,
                null,
                null,
                null,
                null);

        SqlPattern[] patterns =
            new SqlPattern[] {
                new SqlPattern(SqlPattern.Dialect.DERBY, necjSqlDerby, necjSqlDerby),
                new SqlPattern(SqlPattern.Dialect.MYSQL, necjSqlMySql, necjSqlMySql)
            };

        assertQuerySql(testContext, query, patterns);
    }

    public void testMultiLevelMembersMixedNullNonNullParent() {
        if (!isDefaultNullMemberRepresentation()) {
            return;
        }
        String dimension =
            "<Dimension name=\"Warehouse2\">\n" +
            "  <Hierarchy hasAll=\"true\" primaryKey=\"warehouse_id\">\n" +
            "    <Table name=\"warehouse\"/>\n" +
            "    <Level name=\"fax\" column=\"warehouse_fax\" uniqueMembers=\"true\"/>\n" +
            "    <Level name=\"address1\" column=\"wa_address1\" uniqueMembers=\"false\"/>\n" +
            "    <Level name=\"name\" column=\"warehouse_name\" uniqueMembers=\"false\"/>\n" +
            "  </Hierarchy>\n" +
            "</Dimension>\n";

        String cube =
            "<Cube name=\"Warehouse2\">\n" +
            "  <Table name=\"inventory_fact_1997\"/>\n" +
            "  <DimensionUsage name=\"Product\" source=\"Product\" foreignKey=\"product_id\"/>\n" +
            "  <DimensionUsage name=\"Warehouse2\" source=\"Warehouse2\" foreignKey=\"warehouse_id\"/>\n" +
            "  <Measure name=\"Cost Count\" column=\"warehouse_cost\" aggregator=\"distinct-count\"/>\n" +
            "</Cube>";

        String query =
            "with\n" +
            "set [Filtered Warehouse Set] as " +
            "{[Warehouse2].[#null].[234 West Covina Pkwy].[Freeman And Co]," +
            " [Warehouse2].[971-555-6213].[3377 Coachman Place].[Jones International]} " +
            "member [Warehouse2].[TwoMembers] as 'AGGREGATE([Filtered Warehouse Set])' " +
            "select {[Measures].[Cost Count]} on columns, {[Warehouse2].[TwoMembers]} on rows " +
            "from [Warehouse2]";

        String necjSqlDerby =
            "select count(distinct \"inventory_fact_1997\".\"warehouse_cost\") as \"m0\" " +
            "from \"warehouse\" as \"warehouse\", \"inventory_fact_1997\" as \"inventory_fact_1997\" " +
            "where \"inventory_fact_1997\".\"warehouse_id\" = \"warehouse\".\"warehouse_id\" and " +
            "((\"warehouse\".\"warehouse_name\" = 'Freeman And Co' " +
            "and \"warehouse\".\"wa_address1\" = '234 West Covina Pkwy' " +
            "and \"warehouse\".\"warehouse_fax\" is null) " +
            "or (\"warehouse\".\"warehouse_name\" = 'Jones International' " +
            "and \"warehouse\".\"wa_address1\" = '3377 Coachman Place' " +
            "and \"warehouse\".\"warehouse_fax\" = '971-555-6213'))";

        String necjSqlMySql =
            "select count(distinct `inventory_fact_1997`.`warehouse_cost`) as `m0` " +
            "from `warehouse` as `warehouse`, `inventory_fact_1997` as `inventory_fact_1997` " +
            "where `inventory_fact_1997`.`warehouse_id` = `warehouse`.`warehouse_id` and " +
            "((`warehouse`.`warehouse_name` = 'Jones International' and `warehouse`.`wa_address1` = '3377 Coachman Place' and `warehouse`.`warehouse_fax` = '971-555-6213') " +
            "or (`warehouse`.`warehouse_name` = 'Freeman And Co' and `warehouse`.`wa_address1` = '234 West Covina Pkwy' and `warehouse`.`warehouse_fax` is null))";

        String necjSqlMySql2 =
            "select count(distinct `inventory_fact_1997`.`warehouse_cost`) as `m0` from `warehouse` as `warehouse`, `inventory_fact_1997` as `inventory_fact_1997` where `inventory_fact_1997`.`warehouse_id` = `warehouse`.`warehouse_id` and ((`warehouse`.`warehouse_name` = 'Freeman And Co' and `warehouse`.`wa_address1` = '234 West Covina Pkwy' and `warehouse`.`warehouse_fax` is null) or (`warehouse`.`warehouse_name` = 'Jones International' and `warehouse`.`wa_address1` = '3377 Coachman Place' and `warehouse`.`warehouse_fax` = '971-555-6213'))";


        TestContext testContext =
            TestContext.create(
                dimension,
                cube,
                null,
                null,
                null,
                null);

        SqlPattern[] patterns =
            new SqlPattern[] {
                new SqlPattern(SqlPattern.Dialect.DERBY, necjSqlDerby, necjSqlDerby),
                new SqlPattern(SqlPattern.Dialect.MYSQL, necjSqlMySql, necjSqlMySql)
            };

        try {
            assertQuerySql(testContext, query, patterns);
        } catch(Throwable t) {
            patterns =
                new SqlPattern[] {
                    new SqlPattern(SqlPattern.Dialect.DERBY, necjSqlDerby, necjSqlDerby),
                    new SqlPattern(SqlPattern.Dialect.MYSQL, necjSqlMySql2, necjSqlMySql2)
                };
 
            assertQuerySql(testContext, query, patterns);
        }
    }

    public void testMultiLevelsMixedNullNonNullChild() {
        if (!isDefaultNullMemberRepresentation()) {
            return;
        }
        String dimension =
            "<Dimension name=\"Warehouse2\">\n" +
            "  <Hierarchy hasAll=\"true\" primaryKey=\"warehouse_id\">\n" +
            "    <Table name=\"warehouse\"/>\n" +
            "    <Level name=\"address3\" column=\"wa_address3\" uniqueMembers=\"true\"/>\n" +
            "    <Level name=\"address2\" column=\"wa_address2\" uniqueMembers=\"false\"/>\n" +
            "    <Level name=\"fax\" column=\"warehouse_fax\" uniqueMembers=\"false\"/>\n" +
            "  </Hierarchy>\n" +
            "</Dimension>\n";

        String cube =
            "<Cube name=\"Warehouse2\">\n" +
            "  <Table name=\"inventory_fact_1997\"/>\n" +
            "  <DimensionUsage name=\"Product\" source=\"Product\" foreignKey=\"product_id\"/>\n" +
            "  <DimensionUsage name=\"Warehouse2\" source=\"Warehouse2\" foreignKey=\"warehouse_id\"/>\n" +
            "  <Measure name=\"Cost Count\" column=\"warehouse_cost\" aggregator=\"distinct-count\"/>\n" +
            "</Cube>";

        String query =
            "with\n" +
            "set [Filtered Warehouse Set] as " +
            "{[Warehouse2].[#null].[#null].[#null]," +
            " [Warehouse2].[#null].[#null].[971-555-6213]} " +
            "member [Warehouse2].[TwoMembers] as 'AGGREGATE([Filtered Warehouse Set])' " +
            "select {[Measures].[Cost Count]} on columns, {[Warehouse2].[TwoMembers]} on rows " +
            "from [Warehouse2]";

        String necjSqlDerby =
            "select count(distinct \"inventory_fact_1997\".\"warehouse_cost\") as \"m0\" " +
            "from \"warehouse\" as \"warehouse\", \"inventory_fact_1997\" as \"inventory_fact_1997\" " +
            "where \"inventory_fact_1997\".\"warehouse_id\" = \"warehouse\".\"warehouse_id\" and " +
            "((\"warehouse\".\"warehouse_fax\" is null and \"warehouse\".\"wa_address2\" is null and \"warehouse\".\"wa_address3\" is null) " +
            "or (\"warehouse\".\"warehouse_fax\" = '971-555-6213' and \"warehouse\".\"wa_address2\" is null and \"warehouse\".\"wa_address3\" is null))";

        String necjSqlMySql =
            "select count(distinct `inventory_fact_1997`.`warehouse_cost`) as `m0` " +
            "from `warehouse` as `warehouse`, `inventory_fact_1997` as `inventory_fact_1997` " +
            "where `inventory_fact_1997`.`warehouse_id` = `warehouse`.`warehouse_id` and " +
            "((`warehouse`.`warehouse_fax` = '971-555-6213' and `warehouse`.`wa_address2` is null and `warehouse`.`wa_address3` is null) " +
            "or (`warehouse`.`warehouse_fax` is null and `warehouse`.`wa_address2` is null and `warehouse`.`wa_address3` is null))";

        String necjSqlMySql2 =
            "select count(distinct `inventory_fact_1997`.`warehouse_cost`) as `m0` from `warehouse` as `warehouse`, `inventory_fact_1997` as `inventory_fact_1997` where `inventory_fact_1997`.`warehouse_id` = `warehouse`.`warehouse_id` and ((`warehouse`.`warehouse_fax` is null and `warehouse`.`wa_address2` is null and `warehouse`.`wa_address3` is null) or (`warehouse`.`warehouse_fax` = '971-555-6213' and `warehouse`.`wa_address2` is null and `warehouse`.`wa_address3` is null))";

        TestContext testContext =
            TestContext.create(
                dimension,
                cube,
                null,
                null,
                null,
                null);

        SqlPattern[] patterns =
            new SqlPattern[] {
                new SqlPattern(SqlPattern.Dialect.DERBY, necjSqlDerby, necjSqlDerby),
                new SqlPattern(SqlPattern.Dialect.MYSQL, necjSqlMySql, necjSqlMySql)
            };

        try {
            assertQuerySql(testContext, query, patterns);
        } catch(Throwable t) {
            patterns =
                new SqlPattern[] {
                    new SqlPattern(SqlPattern.Dialect.DERBY, necjSqlDerby, necjSqlDerby),
                    new SqlPattern(SqlPattern.Dialect.MYSQL, necjSqlMySql2, necjSqlMySql2)
            };
            
            assertQuerySql(testContext, query, patterns);
        }
    }

    public void testAggregationOnCJofMembersGeneratesOptimalQuery() {

        String mdxQuery = "WITH \n" +
            "SET [COG_OQP_INT_s2] AS 'CROSSJOIN({[Store].[Store].MEMBERS}, " +
            "{{[Gender].[Gender].MEMBERS}, " +
            "{([Gender].[COG_OQP_USR_Aggregate(Gender)])}})' \n" +
            "SET [COG_OQP_INT_s1] AS 'CROSSJOIN({[Store].[Store].MEMBERS}, " +
            "{[Gender].[Gender].MEMBERS})' \n" +
            "\n" +
            "MEMBER [Store].[COG_OQP_USR_Aggregate(Store)] AS '\n" +
            "AGGREGATE({COG_OQP_INT_s1})', SOLVE_ORDER = 4 \n" +
            "\n" +
            "MEMBER [Gender].[COG_OQP_USR_Aggregate(Gender)] AS '\n" +
            "AGGREGATE({[Gender].DEFAULTMEMBER})', SOLVE_ORDER = 8 \n" +
            "\n" +
            "\n" +
            "SELECT {[Measures].[Customer Count]} ON AXIS(0), \n" +
            "{[COG_OQP_INT_s2], HEAD({([Store].[COG_OQP_USR_Aggregate(Store)], " +
            "[Gender].DEFAULTMEMBER)}, " +
            "IIF(COUNT([COG_OQP_INT_s1], INCLUDEEMPTY) > 0, 1, 0))} ON AXIS(1) \n" +
            "FROM [sales]";

        final String oraTeraSql;
        if (MondrianProperties.instance().EnableGroupingSets.get()) {
            oraTeraSql = "select \"store\".\"store_state\" as \"c0\", " +
                "\"store\".\"store_city\" as \"c1\", " +
                "\"time_by_day\".\"the_year\" as \"c2\", " +
                "\"customer\".\"gender\" as \"c3\", " +
                "count(distinct \"sales_fact_1997\".\"customer_id\") as \"m0\", " +
                "grouping(\"customer\".\"gender\") as \"g0\", " +
                "grouping(\"store\".\"store_city\") as \"g1\" " +
                "from \"store\" =as= \"store\"," +
                " \"sales_fact_1997\" =as= \"sales_fact_1997\"," +
                " \"time_by_day\" =as= \"time_by_day\"," +
                " \"customer\" =as= \"customer\" " +
                "where \"sales_fact_1997\".\"store_id\" = \"store\".\"store_id\" " +
                "and \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" " +
                "and \"time_by_day\".\"the_year\" = 1997 " +
                "and \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\" " +
                "group by grouping sets ((\"store\".\"store_state\",\"store\".\"store_city\",\"time_by_day\".\"the_year\",\"customer\".\"gender\")," +
                "(\"store\".\"store_state\",\"store\".\"store_city\",\"time_by_day\".\"the_year\")," +
                "(\"store\".\"store_state\",\"time_by_day\".\"the_year\")," +
                "(\"store\".\"store_state\",\"time_by_day\".\"the_year\",\"customer\".\"gender\"))";
        } else {
            oraTeraSql = "select \"store\".\"store_state\" as \"c0\", " +
                "\"time_by_day\".\"the_year\" as \"c1\", " +
                "count(distinct \"sales_fact_1997\".\"customer_id\") as \"m0\" " +
                "from \"store\" =as= \"store\"," +
                " \"sales_fact_1997\" =as= \"sales_fact_1997\"," +
                " \"time_by_day\" =as= \"time_by_day\" " +
                "where \"sales_fact_1997\".\"store_id\" = \"store\".\"store_id\" " +
                "and \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" " +
                "and \"time_by_day\".\"the_year\" = 1997 " +
                "group by \"store\".\"store_state\", \"time_by_day\".\"the_year\"";
        }
        SqlPattern[] patterns = {
            new SqlPattern(SqlPattern.Dialect.ORACLE, oraTeraSql, oraTeraSql),
            new SqlPattern(SqlPattern.Dialect.TERADATA, oraTeraSql, oraTeraSql),
        };
        assertQuerySql(mdxQuery,patterns);
    }

    public void testCanNotBatchForDifferentCompoundPredicate() {
        boolean originalGroupingSetsPropertyValue = props.EnableGroupingSets.get();
        props.EnableGroupingSets.set(true);
        String mdxQueryWithFewMembers = "WITH " +
            "MEMBER [Store].[COG_OQP_USR_Aggregate(Store)] AS " +
            "'AGGREGATE({[Store].[All Stores].[USA].[CA], [Store].[All Stores].[USA].[OR],[Store].[All Stores].[USA].[WA]})', SOLVE_ORDER = 8" +
            "SELECT {[Measures].[Customer Count]} ON AXIS(0), " +
            "{[Store].[All Stores].[USA].[CA], [Store].[All Stores].[USA].[OR], [Store].[COG_OQP_USR_Aggregate(Store)]} " +
            "ON AXIS(1) " +
            "FROM [Sales]";

        String desiredResult =
                        fold(
                            "Axis #0:\n" +
                            "{}\n" +
                            "Axis #1:\n" +
                            "{[Measures].[Customer Count]}\n" +
                            "Axis #2:\n" +
                            "{[Store].[All Stores].[USA].[CA]}\n" +
                            "{[Store].[All Stores].[USA].[OR]}\n" +
                            "{[Store].[COG_OQP_USR_Aggregate(Store)]}\n" +
                            "Row #0: 2,716\n" +
                            "Row #1: 1,037\n" +
                            "Row #2: 5,581\n");

        String  oraTeraSqlForAgg =
            "select \"time_by_day\".\"the_year\" as \"c0\", " +
            "count(distinct \"sales_fact_1997\".\"customer_id\") as \"m0\" " +
            "from \"time_by_day\" =as= \"time_by_day\", " +
            "\"sales_fact_1997\" =as= \"sales_fact_1997\", \"store\" =as= \"store\" " +
            "where \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" " +
            "and \"time_by_day\".\"the_year\" = 1997 and " +
            "\"sales_fact_1997\".\"store_id\" = \"store\".\"store_id\" " +
            "and \"store\".\"store_country\" = 'USA' " +
            "group by \"time_by_day\".\"the_year\"";

        String  oraTeraSqlForDetail =
            "select \"store\".\"store_state\" as \"c0\", " +
            "\"time_by_day\".\"the_year\" as \"c1\", " +
            "count(distinct \"sales_fact_1997\".\"customer_id\") as \"m0\" " +
            "from \"store\" =as= \"store\", " +
            "\"sales_fact_1997\" =as= \"sales_fact_1997\", " +
            "\"time_by_day\" =as= \"time_by_day\" " +
            "where \"sales_fact_1997\".\"store_id\" = \"store\".\"store_id\" " +
            "and \"store\".\"store_state\" in ('CA', 'OR') " +
            "and \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" " +
            "and \"time_by_day\".\"the_year\" = 1997 " +
            "group by \"store\".\"store_state\", \"time_by_day\".\"the_year\"";

        SqlPattern[] patterns = {
            new SqlPattern(SqlPattern.Dialect.ORACLE, oraTeraSqlForAgg, oraTeraSqlForAgg),
            new SqlPattern(SqlPattern.Dialect.TERADATA, oraTeraSqlForAgg, oraTeraSqlForAgg),
            new SqlPattern(SqlPattern.Dialect.ORACLE, oraTeraSqlForDetail, oraTeraSqlForDetail),
            new SqlPattern(SqlPattern.Dialect.TERADATA, oraTeraSqlForDetail, oraTeraSqlForDetail),
        };

        assertQueryReturns(mdxQueryWithFewMembers, desiredResult);
        assertQuerySql(mdxQueryWithFewMembers, patterns);
        props.EnableGroupingSets.set(originalGroupingSetsPropertyValue);
    }

    public void testDistinctCountAggHappensInGSQueryForSubsetOfMembers() {
        boolean originalGroupingSetsPropertyValue = props.EnableGroupingSets.get();
        props.EnableGroupingSets.set(true);
        String mdxQueryWithFewMembers = "WITH " +
            "MEMBER [Store].[COG_OQP_USR_Aggregate(Store)] AS " +
            "'AGGREGATE({[Store].[All Stores].[USA].[CA], [Store].[All Stores].[USA].[OR]})', SOLVE_ORDER = 8" +
            "SELECT {[Measures].[Customer Count]} ON AXIS(0), " +
            "{[Store].[All Stores].[USA].[CA], [Store].[All Stores].[USA].[OR], [Store].[COG_OQP_USR_Aggregate(Store)]} " +
            "ON AXIS(1) " +
            "FROM [Sales]";

        String desiredResult =
            fold(
                "Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Measures].[Customer Count]}\n" +
                "Axis #2:\n" +
                "{[Store].[All Stores].[USA].[CA]}\n" +
                "{[Store].[All Stores].[USA].[OR]}\n" +
                "{[Store].[COG_OQP_USR_Aggregate(Store)]}\n" +
                "Row #0: 2,716\n" +
                "Row #1: 1,037\n" +
                "Row #2: 3,753\n");

        String  oraTeraSql = "select \"store\".\"store_state\" as \"c0\", " +
            "\"time_by_day\".\"the_year\" as \"c1\", " +
            "count(distinct \"sales_fact_1997\".\"customer_id\") as \"m0\", " +
            "grouping(\"store\".\"store_state\") as \"g0\" " +
            "from \"store\" =as= \"store\", \"sales_fact_1997\" =as= \"sales_fact_1997\", " +
            "\"time_by_day\" =as= \"time_by_day\" " +
            "where \"sales_fact_1997\".\"store_id\" = \"store\".\"store_id\" " +
            "and \"store\".\"store_state\" in ('CA', 'OR') " +
            "and \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" " +
            "and \"time_by_day\".\"the_year\" = 1997 " +
            "group by grouping sets " +
            "((\"store\".\"store_state\",\"time_by_day\".\"the_year\")," +
            "(\"time_by_day\".\"the_year\"))";

        SqlPattern[] patterns = {
            new SqlPattern(SqlPattern.Dialect.ORACLE, oraTeraSql, oraTeraSql),
            new SqlPattern(SqlPattern.Dialect.TERADATA, oraTeraSql, oraTeraSql),
        };

        assertQueryReturns(mdxQueryWithFewMembers, desiredResult);
        assertQuerySql(mdxQueryWithFewMembers, patterns);
        props.EnableGroupingSets.set(originalGroupingSetsPropertyValue);
    }

    public void testDistinctCountAggHappensInNonGSQueryForSubsetOfMembersWithMixedMeasures() {
        boolean originalGroupingSetsPropertyValue = props.EnableGroupingSets.get();
        props.EnableGroupingSets.set(true);
        String mdxQueryWithFewMembers = "WITH " +
            "MEMBER [Store].[COG_OQP_USR_Aggregate(Store)] AS " +
            "'AGGREGATE({[Store].[All Stores].[USA].[CA], [Store].[All Stores].[USA].[OR]})', SOLVE_ORDER = 8" +
            "SELECT {[Measures].[Customer Count],[Measures].[Unit Sales]} ON AXIS(0), " +
            "{[Store].[All Stores].[USA].[CA], [Store].[All Stores].[USA].[OR], [Store].[COG_OQP_USR_Aggregate(Store)]} " +
            "ON AXIS(1) " +
            "FROM [Sales]";

        String desiredResult =
            fold(
                "Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Measures].[Customer Count]}\n" +
                "{[Measures].[Unit Sales]}\n" +
                "Axis #2:\n" +
                "{[Store].[All Stores].[USA].[CA]}\n" +
                "{[Store].[All Stores].[USA].[OR]}\n" +
                "{[Store].[COG_OQP_USR_Aggregate(Store)]}\n" +
                "Row #0: 2,716\n" +
                "Row #0: 74,748\n" +
                "Row #1: 1,037\n" +
                "Row #1: 67,659\n" +
                "Row #2: 3,753\n" +
                "Row #2: 142,407\n");

        String  oraTeraSqlForDetail =
            "select \"store\".\"store_state\" as \"c0\", " +
            "\"time_by_day\".\"the_year\" as \"c1\", " +
            "count(distinct \"sales_fact_1997\".\"customer_id\") as \"m0\", " +
            "sum(\"sales_fact_1997\".\"unit_sales\") as \"m1\" " +
            "from \"store\" =as= \"store\", \"sales_fact_1997\" =as= \"sales_fact_1997\", " +
            "\"time_by_day\" =as= \"time_by_day\" " +
            "where \"sales_fact_1997\".\"store_id\" = \"store\".\"store_id\" " +
            "and \"store\".\"store_state\" in ('CA', 'OR') " +
            "and \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" " +
            "and \"time_by_day\".\"the_year\" = 1997 " +
            "group by \"store\".\"store_state\", \"time_by_day\".\"the_year\"";
        String  oraTeraSqlForDistinctCountAgg =
            "select \"time_by_day\".\"the_year\" as \"c0\", " +
            "count(distinct \"sales_fact_1997\".\"customer_id\") as \"m0\" " +
            "from \"time_by_day\" =as= \"time_by_day\", " +
            "\"sales_fact_1997\" =as= \"sales_fact_1997\", \"store\" =as= \"store\" " +
            "where \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" " +
            "and \"time_by_day\".\"the_year\" = 1997 " +
            "and \"sales_fact_1997\".\"store_id\" = \"store\".\"store_id\" " +
            "and \"store\".\"store_state\" in ('CA', 'OR') " +
            "group by \"time_by_day\".\"the_year\"";

        SqlPattern[] patterns = {
            new SqlPattern(SqlPattern.Dialect.ORACLE, oraTeraSqlForDetail, oraTeraSqlForDetail),
            new SqlPattern(SqlPattern.Dialect.TERADATA, oraTeraSqlForDetail, oraTeraSqlForDetail),
            new SqlPattern(SqlPattern.Dialect.ORACLE, oraTeraSqlForDistinctCountAgg, oraTeraSqlForDistinctCountAgg),
            new SqlPattern(SqlPattern.Dialect.TERADATA, oraTeraSqlForDistinctCountAgg, oraTeraSqlForDistinctCountAgg),
        };

        assertQueryReturns(mdxQueryWithFewMembers, desiredResult);
        assertQuerySql(mdxQueryWithFewMembers, patterns);
        props.EnableGroupingSets.set(originalGroupingSetsPropertyValue);
    }

    public void testAggregationOfMembersAndDefaultMemberWithoutGroupingSets() {

        boolean originalGroupingSetsPropertyValue = props.EnableGroupingSets.get();
        props.EnableGroupingSets.set(false);
        String mdxQueryWithMembers = "WITH " +
            "MEMBER [Gender].[COG_OQP_USR_Aggregate(Gender)] AS " +
            "'AGGREGATE({[Gender].MEMBERS})', SOLVE_ORDER = 8" +
            "SELECT {[Measures].[Customer Count]} ON AXIS(0), " +
            "{[Gender].MEMBERS, [Gender].[COG_OQP_USR_Aggregate(Gender)]} " +
            "ON AXIS(1) " +
            "FROM [Sales]";

        String mdxQueryWithDefaultMember = "WITH " +
            "MEMBER [Gender].[COG_OQP_USR_Aggregate(Gender)] AS " +
            "'AGGREGATE({[Gender].DEFAULTMEMBER})', SOLVE_ORDER = 8" +
            "SELECT {[Measures].[Customer Count]} ON AXIS(0), \n" +
            "{[Gender].MEMBERS, [Gender].[COG_OQP_USR_Aggregate(Gender)]} " +
            "ON AXIS(1) \n" +
            "FROM [sales]";

        String desiredResult = fold(
            "Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Measures].[Customer Count]}\n" +
                "Axis #2:\n" +
                "{[Gender].[All Gender]}\n" +
                "{[Gender].[All Gender].[F]}\n" +
                "{[Gender].[All Gender].[M]}\n" +
                "{[Gender].[COG_OQP_USR_Aggregate(Gender)]}\n" +
                "Row #0: 5,581\n" +
                "Row #1: 2,755\n" +
                "Row #2: 2,826\n" +
                "Row #3: 5,581\n");

        String  oraTeraSql = "select \"time_by_day\".\"the_year\" as \"c0\", " +
            "\"customer\".\"gender\" as \"c1\", " +
            "count(distinct \"sales_fact_1997\".\"customer_id\") as \"m0\" " +
            "from \"time_by_day\" =as= \"time_by_day\", " +
            "\"sales_fact_1997\" =as= \"sales_fact_1997\", \"customer\" =as= \"customer\" " +
            "where \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" " +
            "and \"time_by_day\".\"the_year\" = 1997 " +
            "and \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\" " +
            "group by \"time_by_day\".\"the_year\", \"customer\".\"gender\"";

        SqlPattern[] patterns = {
            new SqlPattern(SqlPattern.Dialect.ORACLE, oraTeraSql, oraTeraSql),
            new SqlPattern(SqlPattern.Dialect.TERADATA, oraTeraSql, oraTeraSql),
        };

        assertQueryReturns(mdxQueryWithMembers, desiredResult);
        assertQuerySql(mdxQueryWithMembers, patterns);
        assertQueryReturns(mdxQueryWithDefaultMember, desiredResult);
        assertQuerySql(mdxQueryWithDefaultMember, patterns);
        props.EnableGroupingSets.set(originalGroupingSetsPropertyValue);
    }

    public void testShouldConvertListOfMembersToTuples() {
        List <Member[]> tuples = tupleList(
            genderMembersIncludingAll(true, salesCubeSchemaReader, salesCube));
        assertEquals(3, tuples.size());
        assertEquals(allMember("Gender", salesCube).getUniqueName(),
                tuples.get(0)[0].getUniqueName());
    }


    public void testOptimizeChildren() {
        String query =
            "with member gender.x as " +
            "'aggregate(" +
            "{gender.gender.members * Store.[all stores].[usa].children})' " +
            "select {gender.x} on 0, measures.[customer count] on 1 from sales";
        String expected = fold(
            "Axis #0:\n" +
            "{}\n" +
            "Axis #1:\n" +
            "{[Gender].[x]}\n" +
            "Axis #2:\n" +
            "{[Measures].[Customer Count]}\n" +
            "Row #0: 5,581\n");
        assertQueryReturns(query, expected);

        String derbySql =
            "select \"time_by_day\".\"the_year\" as \"c0\", " +
            "count(distinct \"sales_fact_1997\".\"customer_id\") as \"m0\" " +
            "from \"time_by_day\" as \"time_by_day\", \"sales_fact_1997\" as \"sales_fact_1997\", \"store\" as \"store\" " +
            "where \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" and \"time_by_day\".\"the_year\" = 1997 " +
            "and \"sales_fact_1997\".\"store_id\" = \"store\".\"store_id\" " +
            "and \"store\".\"store_country\" = 'USA' group by \"time_by_day\".\"the_year\"";

        String accessSql =
            "select `d0` as `c0`, count(`m0`) as `c1` " +
            "from (select distinct `time_by_day`.`the_year` as `d0`, `sales_fact_1997`.`customer_id` as `m0` " +
            "from `time_by_day` as `time_by_day`, `sales_fact_1997` as `sales_fact_1997`, `store` as `store` " +
            "where `sales_fact_1997`.`time_id` = `time_by_day`.`time_id` and `time_by_day`.`the_year` = 1997 " +
            "and `sales_fact_1997`.`store_id` = `store`.`store_id` " +
            "and `store`.`store_country` = 'USA') as `dummyname` group by `d0`";

        // For LucidDB, we don't optimize since it supports
        // unlimited IN list.
        String luciddbSql =
            "select \"time_by_day\".\"the_year\" as \"c0\", " +
            "count(distinct \"sales_fact_1997\".\"customer_id\") as \"m0\" " +
            "from \"time_by_day\" as \"time_by_day\", \"sales_fact_1997\" as \"sales_fact_1997\", \"customer\" as \"customer\", \"store\" as \"store\" " +
            "where \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" and \"time_by_day\".\"the_year\" = 1997 and \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\" " +
            "and \"sales_fact_1997\".\"store_id\" = \"store\".\"store_id\" " +
            "and (((\"store\".\"store_state\", \"customer\".\"gender\") in (('CA', 'F'), ('OR', 'F'), ('WA', 'F'), ('CA', 'M'), ('OR', 'M'), ('WA', 'M')))) group by \"time_by_day\".\"the_year\"";

        SqlPattern[] patterns = {
            new SqlPattern(SqlPattern.Dialect.DERBY, derbySql, derbySql),
            new SqlPattern(SqlPattern.Dialect.ACCESS, accessSql, accessSql),
            new SqlPattern(SqlPattern.Dialect.LUCIDDB, luciddbSql, luciddbSql),
        };

        assertQuerySql(query, patterns);

    }

    public void testOptimizeListWhenTuplesAreFormedWithDifferentLevels() {
        String query =
            "WITH\n" +
            "MEMBER Product.Agg AS \n" +
            "'Aggregate({[Product].[All Products].[Non-Consumable].[Household].[Kitchen Products].[Pot Scrubbers].[Cormorant],\n" +
            "[Product].[All Products].[Non-Consumable].[Household].[Kitchen Products].[Pot Scrubbers].[Denny],\n" +
            "[Product].[All Products].[Non-Consumable].[Household].[Kitchen Products].[Pot Scrubbers].[High Quality],\n" +
            "[Product].[All Products].[Non-Consumable].[Household].[Kitchen Products].[Pot Scrubbers].[Red Wing],\n" +
            "[Product].[All Products].[Non-Consumable].[Household].[Kitchen Products].[Pots and Pans].[Cormorant],\n" +
            "[Product].[All Products].[Non-Consumable].[Household].[Kitchen Products].[Pots and Pans].[Denny],\n" +
            "[Product].[All Products].[Non-Consumable].[Household].[Kitchen Products].[Pots and Pans].[High Quality],\n" +
            "[Product].[All Products].[Non-Consumable].[Household].[Kitchen Products].[Pots and Pans].[Red Wing],\n" +
            "[Product].[All Products].[Non-Consumable].[Household].[Kitchen Products].[Pots and Pans].[Sunset]} *\n" +
            "{[Gender].[Gender].Members})'\n" +
            "SELECT {Product.Agg} on 0, {[Measures].[Customer Count]} on 1 from Sales";
        String expected = fold(
            "Axis #0:\n" +
            "{}\n" +
            "Axis #1:\n" +
            "{[Product].[Agg]}\n" +
            "Axis #2:\n" +
            "{[Measures].[Customer Count]}\n" +
            "Row #0: 421\n");
        assertQueryReturns(query, expected);
        String derbySql =
            "select \"time_by_day\".\"the_year\" as \"c0\", " +
            "count(distinct \"sales_fact_1997\".\"customer_id\") as \"m0\" " +
            "from \"time_by_day\" as \"time_by_day\", \"sales_fact_1997\" as \"sales_fact_1997\", " +
            "\"product\" as \"product\", \"product_class\" as \"product_class\" " +
            "where \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" " +
            "and \"time_by_day\".\"the_year\" = 1997 and \"sales_fact_1997\".\"product_id\" = \"product\".\"product_id\" " +
            "and \"product\".\"product_claid\" = \"product_class\".\"product_claid\" " +
            "and (((\"product\".\"brand_name\" = 'Red Wing' and \"product_class\".\"product_subcategory\" = 'Pot Scrubbers' " +
            "and \"product_class\".\"product_category\" = 'Kitchen Products' " +
            "and \"product_class\".\"product_department\" = 'Household' " +
            "and \"product_class\".\"product_family\" = 'Non-Consumable') " +
            "or (\"product\".\"brand_name\" = 'Cormorant' and \"product_class\".\"product_subcategory\" = 'Pot Scrubbers' " +
            "and \"product_class\".\"product_category\" = 'Kitchen Products' " +
            "and \"product_class\".\"product_department\" = 'Household' " +
            "and \"product_class\".\"product_family\" = 'Non-Consumable') " +
            "or (\"product\".\"brand_name\" = 'Denny' and \"product_class\".\"product_subcategory\" = 'Pot Scrubbers' " +
            "and \"product_class\".\"product_category\" = 'Kitchen Products' " +
            "and \"product_class\".\"product_department\" = 'Household' " +
            "and \"product_class\".\"product_family\" = 'Non-Consumable') or (\"product\".\"brand_name\" = 'High Quality' " +
            "and \"product_class\".\"product_subcategory\" = 'Pot Scrubbers' " +
            "and \"product_class\".\"product_category\" = 'Kitchen Products' " +
            "and \"product_class\".\"product_department\" = 'Household' and \"product_class\".\"product_family\" = 'Non-Consumable')) " +
            "or (\"product_class\".\"product_subcategory\" = 'Pots and Pans' " +
            "and \"product_class\".\"product_category\" = 'Kitchen Products' and \"product_class\".\"product_department\" = 'Household' " +
            "and \"product_class\".\"product_family\" = 'Non-Consumable')) " +
            "group by \"time_by_day\".\"the_year\"";

        String accessSql =
            "select `d0` as `c0`, count(`m0`) as `c1` from (select distinct `time_by_day`.`the_year` as `d0`, `sales_fact_1997`.`customer_id` as `m0` " +
            "from `time_by_day` as `time_by_day`, `sales_fact_1997` as `sales_fact_1997`, `product` as `product`, `product_class` as `product_class` " +
            "where `sales_fact_1997`.`time_id` = `time_by_day`.`time_id` and `time_by_day`.`the_year` = 1997 " +
            "and `sales_fact_1997`.`product_id` = `product`.`product_id` and `product`.`product_claid` = `product_class`.`product_claid` " +
            "and (((`product`.`brand_name` = 'High Quality' and `product_class`.`product_subcategory` = 'Pot Scrubbers' " +
            "and `product_class`.`product_category` = 'Kitchen Products' and `product_class`.`product_department` = 'Household' " +
            "and `product_class`.`product_family` = 'Non-Consumable') or (`product`.`brand_name` = 'Denny' " +
            "and `product_class`.`product_subcategory` = 'Pot Scrubbers' and `product_class`.`product_category` = 'Kitchen Products' " +
            "and `product_class`.`product_department` = 'Household' " +
            "and `product_class`.`product_family` = 'Non-Consumable') or (`product`.`brand_name` = 'Red Wing' " +
            "and `product_class`.`product_subcategory` = 'Pot Scrubbers' and `product_class`.`product_category` = 'Kitchen Products' " +
            "and `product_class`.`product_department` = 'Household' " +
            "and `product_class`.`product_family` = 'Non-Consumable') or (`product`.`brand_name` = 'Cormorant' " +
            "and `product_class`.`product_subcategory` = 'Pot Scrubbers' and `product_class`.`product_category` = 'Kitchen Products' " +
            "and `product_class`.`product_department` = 'Household' " +
            "and `product_class`.`product_family` = 'Non-Consumable')) or (`product_class`.`product_subcategory` = 'Pots and Pans' " +
            "and `product_class`.`product_category` = 'Kitchen Products' and `product_class`.`product_department` = 'Household' " +
            "and `product_class`.`product_family` = 'Non-Consumable'))) as `dummyname` group by `d0`";

        SqlPattern[] patterns = {
            new SqlPattern(SqlPattern.Dialect.DERBY, derbySql, derbySql),
            new SqlPattern(SqlPattern.Dialect.ACCESS, accessSql, accessSql)};

        assertQuerySql(query, patterns);

    }

    public void testOptimizeListWithTuplesOfLength3() {

        String query =
            "WITH\n" +
            "MEMBER Product.Agg AS \n" +
            "'Aggregate" +
            "({[Product].[All Products].[Non-Consumable].[Household].[Kitchen Products].[Pot Scrubbers].[Cormorant],\n" +
            "[Product].[All Products].[Non-Consumable].[Household].[Kitchen Products].[Pot Scrubbers].[Denny],\n" +
            "[Product].[All Products].[Non-Consumable].[Household].[Kitchen Products].[Pot Scrubbers].[High Quality],\n" +
            "[Product].[All Products].[Non-Consumable].[Household].[Kitchen Products].[Pot Scrubbers].[Red Wing],\n" +
            "[Product].[All Products].[Non-Consumable].[Household].[Kitchen Products].[Pots and Pans].[Cormorant],\n" +
            "[Product].[All Products].[Non-Consumable].[Household].[Kitchen Products].[Pots and Pans].[Denny],\n" +
            "[Product].[All Products].[Non-Consumable].[Household].[Kitchen Products].[Pots and Pans].[High Quality],\n" +
            "[Product].[All Products].[Non-Consumable].[Household].[Kitchen Products].[Pots and Pans].[Red Wing],\n" +
            "[Product].[All Products].[Non-Consumable].[Household].[Kitchen Products].[Pots and Pans].[Sunset]} *\n" +
            "{[Gender].[Gender].Members}*" +
            "{[Store].[All Stores].[USA].[CA].[Alameda],\n" +
            "[Store].[All Stores].[USA].[CA].[Alameda].[HQ],\n" +
            "[Store].[All Stores].[USA].[CA].[Beverly Hills],\n" +
            "[Store].[All Stores].[USA].[CA].[Beverly Hills].[Store 6],\n" +
            "[Store].[All Stores].[USA].[CA].[Los Angeles],\n" +
            "[Store].[All Stores].[USA].[OR].[Portland],\n" +
            "[Store].[All Stores].[USA].[OR].[Portland].[Store 11],\n" +
            "[Store].[All Stores].[USA].[OR].[Salem],\n" +
            "[Store].[All Stores].[USA].[OR].[Salem].[Store 13]})'\n" +
            "SELECT {Product.Agg} on 0, {[Measures].[Customer Count]} on 1 from Sales";
        String expected = fold(
            "Axis #0:\n" +
            "{}\n" +
            "Axis #1:\n" +
            "{[Product].[Agg]}\n" +
            "Axis #2:\n" +
            "{[Measures].[Customer Count]}\n" +
            "Row #0: 189\n");
        assertQueryReturns(query, expected);
    }

    public void testOptimizeChildrenForTuplesWithLength1() {
        List<Member[]> memberList =
            AggregateFunDef.AggregateCalc
                .makeTupleList(
                    productMembersPotScrubbersPotsAndPans(salesCubeSchemaReader));

        List tuples = optimizeChildren(memberList);
        assertTrue(tuppleListContains(tuples,
            member(Id.Segment.toList("Product", "All Products", "Non-Consumable",
                "Household", "Kitchen Products", "Pot Scrubbers", "Cormorant"),
                salesCubeSchemaReader)));
        assertFalse(tuppleListContains(tuples,
            member(Id.Segment.toList("Product", "All Products", "Non-Consumable",
                "Household", "Kitchen Products", "Pot Scrubbers"),
                salesCubeSchemaReader)));
        assertFalse(tuppleListContains(tuples,
            member(Id.Segment.toList("Product", "All Products", "Non-Consumable",
                "Household", "Kitchen Products", "Pots and Pans", "Cormorant"),
                salesCubeSchemaReader)));
        assertTrue(tuppleListContains(tuples,
            member(Id.Segment.toList("Product", "All Products", "Non-Consumable",
                "Household", "Kitchen Products", "Pots and Pans"),
                salesCubeSchemaReader)));
        assertEquals(4, tuples.size());
    }

    public void testOptimizeChildrenForTuplesWithLength3() {
        List<Member[]> memberList =
            CrossJoinFunDef.crossJoin(
                genderMembersIncludingAll(
                    false, salesCubeSchemaReader, salesCube),
                productMembersPotScrubbersPotsAndPans(salesCubeSchemaReader));
        memberList =
            CrossJoinFunDef.crossJoin(
                memberList,
                storeMembersCAAndOR(salesCubeSchemaReader));
        List tuples = optimizeChildren(memberList);
        assertFalse(tuppleListContains(tuples,
            member(
                Id.Segment.toList("Store","All Stores","USA","OR","Portland"),
                salesCubeSchemaReader)));
        assertTrue(tuppleListContains(tuples,
            member(
                Id.Segment.toList("Store","All Stores","USA","OR"),
                salesCubeSchemaReader)));
        assertEquals(16, tuples.size());
    }

    public void testOptimizeChildrenWhenTuplesAreFormedWithDifferentLevels() {
        List<Member[]> memberList =
            CrossJoinFunDef.crossJoin(
                genderMembersIncludingAll(
                    false, salesCubeSchemaReader, salesCube),
                productMembersPotScrubbersPotsAndPans(salesCubeSchemaReader));
        List tuples = optimizeChildren(memberList);
        assertEquals(4, tuples.size());

        assertFalse(tuppleListContains(tuples,
            member(
                Id.Segment.toList("Product","All Products","Non-Consumable",
                    "Household","Kitchen Products","Pots and Pans","Cormorant"),
                salesCubeSchemaReader)));
        assertTrue(tuppleListContains(tuples,
            member(
                Id.Segment.toList("Product","All Products","Non-Consumable",
                    "Household","Kitchen Products","Pots and Pans"),
                salesCubeSchemaReader)));
        assertTrue(tuppleListContains(tuples,
            member(
                Id.Segment.toList("Product","All Products","Non-Consumable",
                    "Household","Kitchen Products","Pot Scrubbers","Cormorant"),
                salesCubeSchemaReader)));
    }

    public void testWhetherCJOfChildren() {
        List<Member[]> memberList =
            CrossJoinFunDef.crossJoin(
                genderMembersIncludingAll(
                    false, salesCubeSchemaReader, salesCube),
                storeMembersUsaAndCanada(
                    false, salesCubeSchemaReader, salesCube));

        List tuples = optimizeChildren(memberList);
        assertEquals(2, tuples.size());
    }

    public void testShouldNotRemoveDuplicateTuples() {

        Member maleChildMember = member(
            Id.Segment.toList("Gender","All Gender","M"), salesCubeSchemaReader);
        Member femaleChildMember = member(
            Id.Segment.toList("Gender","All Gender","F"), salesCubeSchemaReader);

        List<Member> memberList = new ArrayList<Member>();
        memberList.add(maleChildMember);
        memberList.add(maleChildMember);
        memberList.add(femaleChildMember);
        List<Member[]> tuples = tupleList(memberList);
        tuples = optimizeChildren(tuples);
        assertEquals(3, tuples.size());
    }

    public void testMemberCountIsSameForAllMembersInTuple() {

        List <Member[]>memberList =
            CrossJoinFunDef.crossJoin(
                genderMembersIncludingAll(false, salesCubeSchemaReader, salesCube),
                storeMembersUsaAndCanada(false, salesCubeSchemaReader, salesCube));
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
                Id.Segment.toList("Gender","All Gender","M"),
                salesCubeSchemaReader);
        Member femaleChild =
            member(
                Id.Segment.toList("Gender","All Gender","F"),
                salesCubeSchemaReader);
        Member mexicoMember =
            member(
                Id.Segment.toList("Store","All Stores","Mexico"),
                salesCubeSchemaReader);

        List<Member[]> memberList = new ArrayList<Member[]>();
        memberList.add(new Member[]{maleChild});

        memberList = CrossJoinFunDef.crossJoin(
            memberList,
            storeMembersUsaAndCanada(false, salesCubeSchemaReader, salesCube));

        memberList.add(new Member[]{femaleChild, mexicoMember});

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
        boolean useGroupingSets = props.EnableGroupingSets.get();
        props.EnableGroupingSets.set(true);
        try {
            assertQueryReturns("WITH " +
            "MEMBER GENDER.AGG AS 'AGGREGATE( { GENDER.[F] } )' " +
            "MEMBER GENDER.AGG2 AS 'AGGREGATE( { GENDER.[M] } )' " +
            "SELECT " +
            "{ MEASURES.[CUSTOMER COUNT], MEASURES.[UNIT SALES] } ON 0, " +
            "{ GENDER.AGG, GENDER.AGG2 } ON 1 \n" +
            "FROM SALES",
                fold(
                    "Axis #0:\n" +
                    "{}\n" +
                    "Axis #1:\n" +
                    "{[Measures].[Customer Count]}\n" +
                    "{[Measures].[Unit Sales]}\n" +
                    "Axis #2:\n" +
                    "{[Gender].[AGG]}\n" +
                    "{[Gender].[AGG2]}\n" +
                    "Row #0: 2,755\n" +
                    "Row #0: 131,558\n" +
                    "Row #1: 2,826\n" +
                    "Row #1: 135,215\n"));
        } finally {
            props.EnableGroupingSets.set(useGroupingSets);
        }
    }

    public void testDistinctCountForAggregatesAtTheSameLevel() {
        boolean useGroupingSets = props.EnableGroupingSets.get();
        props.EnableGroupingSets.set(true);
        try {
            assertQueryReturns("WITH " +
            "MEMBER GENDER.AGG AS 'AGGREGATE( { GENDER.[F], GENDER.[M] } )' " +
            "SELECT " +
            "{MEASURES.[CUSTOMER COUNT]} ON 0, " +
            "{GENDER.AGG } ON 1 " +
            "FROM SALES",
                fold(
                    "Axis #0:\n" +
                    "{}\n" +
                    "Axis #1:\n" +
                    "{[Measures].[Customer Count]}\n" +
                    "Axis #2:\n" +
                    "{[Gender].[AGG]}\n" +
                    "Row #0: 5,581\n"));
        } finally {
            props.EnableGroupingSets.set(useGroupingSets);
        }
    }

    private boolean tuppleListContains(
        List tuples,
        Member memberByUniqueName)
    {
        if (tuples.get(0) instanceof Member) {
            return tuples.contains(memberByUniqueName);
        }
        for (Object o : tuples) {
            Member[] members = (Member[]) o;
            if (Arrays.asList(members).contains(memberByUniqueName)) {
                return true;
            }
        }
        return false;
    }

    private List<Member[]> optimizeChildren(List<Member[]> memberList) {
        return AggregateFunDef.AggregateCalc.
            optimizeChildren(memberList, schemaReader,salesCube);
    }

    private List<Member[]> tupleList(List<Member> members) {
        return AggregateFunDef.AggregateCalc.makeTupleList(members);
    }
}
