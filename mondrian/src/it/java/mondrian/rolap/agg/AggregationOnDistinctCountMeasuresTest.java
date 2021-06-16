/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (c) 2002-2021 Hitachi Vantara..  All rights reserved.
*/
package mondrian.rolap.agg;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import mondrian.calc.TupleList;
import mondrian.calc.impl.ArrayTupleList;
import mondrian.calc.impl.UnaryTupleList;
import mondrian.olap.Id;
import mondrian.olap.Member;
import mondrian.olap.MondrianProperties;
import mondrian.olap.Result;
import mondrian.olap.ResultBase;
import mondrian.olap.SchemaReader;
import mondrian.olap.Util;
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

/**
 * <code>AggregationOnDistinctCountMeasureTest</code> tests the
 * Distinct Count functionality with tuples and members.
 *
 * @author ajogleka
 * @since 19 December, 2007
 */
public class AggregationOnDistinctCountMeasuresTest extends BatchTestCase {
    private final MondrianProperties props = MondrianProperties.instance();

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
        return TestContext.instance().create(
            null,
            null,
            "<VirtualCube name=\"Warehouse and Sales2\" defaultMeasure=\"Store Sales\">\n"
            + "   <VirtualCubeDimension cubeName=\"Sales\" name=\"Gender\"/>\n"
            + "   <VirtualCubeDimension name=\"Store\"/>\n"
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
            + "   <VirtualCubeDimension name=\"Product\"/>\n"
            + "   <VirtualCubeDimension cubeName=\"Warehouse\" name=\"Warehouse\"/>\n"
            + "   <VirtualCubeMeasure cubeName=\"Sales\" name=\"[Measures].[Customer Count]\"/>\n"
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
            + "{[Gender].[X]}\n"
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
            + "{[Gender].[X]}\n"
            + "Axis #2:\n"
            + "{[Measures].[Customer Count]}\n"
            + "Row #0: 5,581\n");
    }

    public void testCrossJoinMembersWithASingleMember() {
        // make sure tuple optimization will be used
        propSaver.set(propSaver.properties.MaxConstraints, 1);

        String query =
            "WITH MEMBER GENDER.X AS 'AGGREGATE({[GENDER].[GENDER].members} * "
            + "{[STORE].[ALL STORES].[USA].[CA]})', solve_order=100 "
            + "SELECT GENDER.X ON 0, [MEASURES].[CUSTOMER COUNT] ON 1 FROM SALES";
        String result =
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Gender].[X]}\n"
            + "Axis #2:\n"
            + "{[Measures].[Customer Count]}\n"
            + "Row #0: 2,716\n";

        assertQueryReturns(query, result);

        // Check aggregate loading sql pattern
        String mysqlSql =
            "select `time_by_day`.`the_year` as `c0`, "
            + "count(distinct `sales_fact_1997`.`customer_id`) as `m0` "
            + "from `time_by_day` as `time_by_day`, `sales_fact_1997` as `sales_fact_1997`, `store` as `store` "
            + "where `sales_fact_1997`.`time_id` = `time_by_day`.`time_id` "
            + "and `time_by_day`.`the_year` = 1997 "
            + "and `sales_fact_1997`.`store_id` = `store`.`store_id` and `store`.`store_state` = 'CA' "
            + "group by `time_by_day`.`the_year`";

        String oraTeraSql =
            "select \"time_by_day\".\"the_year\" as \"c0\", "
            + "count(distinct \"sales_fact_1997\".\"customer_id\") as \"m0\" "
            + "from \"time_by_day\" =as= \"time_by_day\", \"sales_fact_1997\" =as= \"sales_fact_1997\", \"store\" =as= \"store\" "
            + "where \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" and \"time_by_day\".\"the_year\" = 1997 "
            + "and \"sales_fact_1997\".\"store_id\" = \"store\".\"store_id\" and \"store\".\"store_state\" = 'CA' "
            + "group by \"time_by_day\".\"the_year\"";

        SqlPattern[] patterns = {
            new SqlPattern(Dialect.DatabaseProduct.MYSQL, mysqlSql, mysqlSql),
            new SqlPattern(
                Dialect.DatabaseProduct.ORACLE, oraTeraSql, oraTeraSql),
            new SqlPattern(
                Dialect.DatabaseProduct.TERADATA, oraTeraSql, oraTeraSql),
        };

        assertQuerySql(query, patterns);
    }

    public void testCrossJoinMembersWithSetOfMembers() {
        // make sure tuple optimization will be used
        propSaver.set(propSaver.properties.MaxConstraints, 2);

        String query =
            "WITH MEMBER GENDER.X AS 'AGGREGATE({[GENDER].[GENDER].members} * "
            + "{[STORE].[ALL STORES].[USA].[CA], [Store].[All Stores].[Canada]})', solve_order=100 "
            + "SELECT GENDER.X ON 0, [MEASURES].[CUSTOMER COUNT] ON 1 FROM SALES";

        String result =
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Gender].[X]}\n"
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
            "select `time_by_day`.`the_year` as `c0`, "
            + "count(distinct `sales_fact_1997`.`customer_id`) as `m0` "
            + "from `time_by_day` as `time_by_day`, `sales_fact_1997` as `sales_fact_1997`, `store` as `store` "
            + "where `sales_fact_1997`.`time_id` = `time_by_day`.`time_id` and `time_by_day`.`the_year` = 1997 "
            + "and `sales_fact_1997`.`store_id` = `store`.`store_id` "
            + "and (`store`.`store_state` = 'CA' or `store`.`store_country` = 'Canada') "
            + "group by `time_by_day`.`the_year`";

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

        assertQuerySql(query, patterns);
    }

    public void testCrossJoinParticularMembersFromTwoDimensions() {
        assertQueryReturns(
            "WITH MEMBER GENDER.X AS 'AGGREGATE({[GENDER].M} * "
            + "{[STORE].[ALL STORES].[USA].[CA]})', solve_order=100 "
            + "SELECT GENDER.X ON 0, [MEASURES].[CUSTOMER COUNT] ON 1 FROM SALES",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Gender].[X]}\n"
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
            + "{[Gender].[X]}\n"
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
            + "{[Store].[USA].[CA]}\n"
            + "Axis #2:\n"
            + "{[Measures].[Customer Count], [Gender].[M]}\n"
            + "Row #0: 1,389\n");
    }

    public void testDistinctCountOnSetOfMembers() {
        assertQueryReturns(
            "WITH MEMBER STORE.X as 'Aggregate({[STORE].[ALL STORES].[USA].[CA],"
            + "[STORE].[ALL STORES].[USA].[WA]})'"
            + "SELECT STORE.X  ON ROWS, "
            + "{[MEASURES].[CUSTOMER COUNT]} ON COLUMNS\n"
            + "FROM [SALES]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Customer Count]}\n"
            + "Axis #2:\n"
            + "{[Store].[X]}\n"
            + "Row #0: 4,544\n");
    }

    public void testDistinctCountOnTuplesWithSomeNonJoiningDimensions() {
        propSaver.set(
            props.IgnoreMeasureForNonJoiningDimension, false);
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
            + "{[Warehouse].[X]}\n"
            + "Row #0: \n";
        assertQueryReturns(mdx, expectedResult);
        propSaver.set(
            props.IgnoreMeasureForNonJoiningDimension, true);
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
            + "{[Gender].[X]}\n"
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
            + "{[Warehouse].[X]}\n"
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
            + "{[Warehouse].[X]}\n"
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
            + "{[Warehouse].[X]}\n"
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
            + "{[Warehouse].[X]}\n"
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
            + "{[Warehouse].[X]}\n"
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
            + "{[Warehouse].[X]}\n"
            + "Row #0: 5,581\n");
    }

    public void testAggregationOverLargeListGeneratesError() {
        propSaver.set(props.MaxConstraints, 7);

        // LucidDB has no limit on the size of IN list
        final boolean isLuciddb =
            getTestContext().getDialect().getDatabaseProduct()
            == Dialect.DatabaseProduct.LUCIDDB;

        assertQueryReturns(
            makeQuery("[MEASURES].[CUSTOMER COUNT]"),
            isLuciddb
            ? "Axis #0:\n"
              + "{}\n"
              + "Axis #1:\n"
              + "{[Measures].[Customer Count]}\n"
              + "Axis #2:\n"
              + "{[Product].[X]}\n"
              + "Row #0: 1,360\n"
            : "Axis #0:\n"
              + "{}\n"
              + "Axis #1:\n"
              + "{[Measures].[Customer Count]}\n"
              + "Axis #2:\n"
              + "{[Product].[X]}\n"
              + "Row #0: #ERR: mondrian.olap.fun.MondrianEvaluationException: "
              + "Aggregation is not supported over a list with more than 7 predicates (see property mondrian.rolap.maxConstraints)\n");

        // aggregation over a non-distinct-count measure is OK
        assertQueryReturns(
            makeQuery("[Measures].[Store Sales]"),
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Store Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[X]}\n"
            + "Row #0: 11,257.28\n");

        // aggregation over a non-distinct-count measure in slicer should be
        // OK. Before bug MONDRIAN-1122 was fixed, a large set in the slicer
        // would cause an error even if there was not a distinct-count measure.
        assertQueryReturns(
            "SELECT {[Measures].[Store Sales]} ON COLUMNS\n"
            + "FROM [WAREHOUSE AND SALES2]\n"
            + "WHERE {\n"
            + "[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good],\n"
            + "[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Portsmouth],\n"
            + "[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Top Measure],\n"
            + "[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Walrus],\n"
            + "[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Pearl],\n"
            + "[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Portsmouth],\n"
            + "[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Top Measure],\n"
            + "[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Walrus]}",
            "Axis #0:\n"
            + "{[Product].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good]}\n"
            + "{[Product].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Portsmouth]}\n"
            + "{[Product].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Top Measure]}\n"
            + "{[Product].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Walrus]}\n"
            + "{[Product].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Pearl]}\n"
            + "{[Product].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Portsmouth]}\n"
            + "{[Product].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Top Measure]}\n"
            + "{[Product].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Walrus]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Store Sales]}\n"
            + "Row #0: 11,257.28\n");
    }

    private String makeQuery(String measureName) {
        return "WITH MEMBER PRODUCT.X as 'Aggregate({"
            + "[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good],\n"
            + "[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Portsmouth],\n"
            + "[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Top Measure],\n"
            + "[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Walrus],\n"
            + "[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Pearl],\n"
            + "[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Portsmouth],\n"
            + "[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Top Measure],\n"
            + "[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Walrus]})' "
            + "SELECT PRODUCT.X  ON ROWS,\n"
            + " {" + measureName + "} ON COLUMNS\n"
            + "FROM [WAREHOUSE AND SALES2]";
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
        if (!MondrianProperties.instance().SsasCompatibleNaming.get()) {
            return;
        }
        propSaver.set(MondrianProperties.instance().MaxConstraints, 5);
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
            + "{[Product].[Drink]}\n"
            + "{[Product].[Food]}\n"
            + "{[Product].[Non-Consumable]}\n"
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

        String necjSqlDerby =
            "select count(distinct \"inventory_fact_1997\".\"warehouse_cost\") as \"m0\" "
            + "from \"warehouse\" as \"warehouse\", "
            + "\"inventory_fact_1997\" as \"inventory_fact_1997\" "
            + "where \"inventory_fact_1997\".\"warehouse_id\" = \"warehouse\".\"warehouse_id\" "
            + "and ((\"warehouse\".\"warehouse_name\" = 'Arnold and Sons' "
            + "and \"warehouse\".\"wa_address1\" = '5617 Saclan Terrace' "
            + "and \"warehouse\".\"wa_address2\" is null) "
            + "or (\"warehouse\".\"warehouse_name\" = 'Jones International' "
            + "and \"warehouse\".\"wa_address1\" = '3377 Coachman Place' "
            + "and \"warehouse\".\"wa_address2\" is null))";

        String necjSqlMySql =
            "select count(distinct `inventory_fact_1997`.`warehouse_cost`) as `m0` "
            + "from `warehouse` as `warehouse`, `inventory_fact_1997` as `inventory_fact_1997` "
            + "where `inventory_fact_1997`.`warehouse_id` = `warehouse`.`warehouse_id` "
            + "and ((`warehouse`.`wa_address2` is null "
            + "and (`warehouse`.`wa_address1`, `warehouse`.`warehouse_name`) "
            + "in (('5617 Saclan Terrace', 'Arnold and Sons'), "
            + "('3377 Coachman Place', 'Jones International'))))";

        TestContext testContext =
            TestContext.instance().create(
                dimension,
                cube,
                null,
                null,
                null,
                null);

        SqlPattern[] patterns = {
            new SqlPattern(
                Dialect.DatabaseProduct.DERBY, necjSqlDerby, necjSqlDerby),
            new SqlPattern(
                Dialect.DatabaseProduct.MYSQL, necjSqlMySql, necjSqlMySql)
        };

        assertQuerySql(testContext, query, patterns);
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
            TestContext.instance().create(
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
            + "{[Warehouse2].[TwoMembers]}\n"
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
            TestContext.instance().create(
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
            + "{[Warehouse2].[TwoMembers]}\n"
            + "Row #0: 220\n";

        testContext.assertQueryReturns(query, result);
    }

    public void testAggregationOnCJofMembersGeneratesOptimalQuery() {
        // Mondrian does not use GROUPING SETS for distinct-count measures.
        // So, this test should not use GROUPING SETS, even if they are enabled.
        // See change 12310, bug MONDRIAN-470 (aka SF.net 2207515).
        Util.discard(props.EnableGroupingSets);

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
            "WITH \n"
            + "SET [COG_OQP_INT_s2] AS 'CROSSJOIN({[Store].[Store].MEMBERS}, "
            + "{{[Gender].[Gender].MEMBERS}, "
            + "{([Gender].[COG_OQP_USR_Aggregate(Gender)])}})' \n"
            + "SET [COG_OQP_INT_s1] AS 'CROSSJOIN({[Store].[Store].MEMBERS}, "
            + "{[Gender].[Gender].MEMBERS})' \n"
            + "\n"
            + "MEMBER [Store].[COG_OQP_USR_Aggregate(Store)] AS '\n"
            + "AGGREGATE({COG_OQP_INT_s1})', SOLVE_ORDER = 4 \n"
            + "\n"
            + "MEMBER [Gender].[COG_OQP_USR_Aggregate(Gender)] AS '\n"
            + "AGGREGATE({[Gender].DEFAULTMEMBER})', SOLVE_ORDER = 8 \n"
            + "\n"
            + "\n"
            + "SELECT {[Measures].[Customer Count]} ON AXIS(0), \n"
            + "{[COG_OQP_INT_s2], HEAD({([Store].[COG_OQP_USR_Aggregate(Store)], "
            + "[Gender].DEFAULTMEMBER)}, "
            + "IIF(COUNT([COG_OQP_INT_s1], INCLUDEEMPTY) > 0, 1, 0))} ON AXIS(1) \n"
            + "FROM [sales]",
            patterns);
    }

    public void testCanNotBatchForDifferentCompoundPredicate() {
        propSaver.set(props.EnableGroupingSets, true);
        String mdxQueryWithFewMembers =
            "WITH "
            + "MEMBER [Store].[COG_OQP_USR_Aggregate(Store)] AS "
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
            + "{[Store].[USA].[CA]}\n"
            + "{[Store].[USA].[OR]}\n"
            + "{[Store].[COG_OQP_USR_Aggregate(Store)]}\n"
            + "Row #0: 2,716\n"
            + "Row #1: 1,037\n"
            + "Row #2: 5,581\n";

        String oraTeraSqlForAgg =
            "select \"time_by_day\".\"the_year\" as \"c0\", "
            + "count(distinct \"sales_fact_1997\".\"customer_id\") as \"m0\" "
            + "from \"time_by_day\" =as= \"time_by_day\", "
            + "\"sales_fact_1997\" =as= \"sales_fact_1997\", \"store\" =as= \"store\" "
            + "where \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" "
            + "and \"time_by_day\".\"the_year\" = 1997 and "
            + "\"sales_fact_1997\".\"store_id\" = \"store\".\"store_id\" "
            + "and \"store\".\"store_state\" in (\"CA\", \"OR\", \"WA\") "
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
        assertQuerySql(mdxQueryWithFewMembers, patterns);
    }


    /**
     * Test distinct count agg happens in non gs query for subset of members
     * with mixed measures.
     */
    public void testDistinctCountInNonGroupingSetsQuery() {
        propSaver.set(props.EnableGroupingSets, true);

        String mdxQueryWithFewMembers =
            "WITH "
            + "MEMBER [Store].[COG_OQP_USR_Aggregate(Store)] AS "
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
            + "{[Store].[USA].[CA]}\n"
            + "{[Store].[USA].[OR]}\n"
            + "{[Store].[COG_OQP_USR_Aggregate(Store)]}\n"
            + "Row #0: 2,716\n"
            + "Row #0: 74,748\n"
            + "Row #1: 1,037\n"
            + "Row #1: 67,659\n"
            + "Row #2: 3,753\n"
            + "Row #2: 142,407\n";

        String oraTeraSqlForDetail =
            "select \"store\".\"store_state\" as \"c0\", "
            + "\"time_by_day\".\"the_year\" as \"c1\", "
            + "sum(\"sales_fact_1997\".\"unit_sales\") as \"m0\", "
            + "count(distinct \"sales_fact_1997\".\"customer_id\") as \"m1\" "
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
        assertQuerySql(mdxQueryWithFewMembers, patterns);
    }

    public void testAggregationOfMembersAndDefaultMemberWithoutGroupingSets() {
        propSaver.set(props.EnableGroupingSets, false);

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
            + "{[Gender].[All Gender]}\n"
            + "{[Gender].[F]}\n"
            + "{[Gender].[M]}\n"
            + "{[Gender].[COG_OQP_USR_Aggregate(Gender)]}\n"
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
        assertQuerySql(mdxQueryWithMembers, patterns);
        assertQueryReturns(mdxQueryWithDefaultMember, desiredResult);
        assertQuerySql(mdxQueryWithDefaultMember, patterns);
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
            + "{[Gender].[x]}\n"
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

        assertQuerySql(query, patterns);
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
            + "{[Gender].[Gender].Members})'\n"
            + "SELECT {Product.Agg} on 0, {[Measures].[Customer Count]} on 1\n"
            + "from Sales\n"
            + "where [Time.Weekly].[1997]";
        String expected =
            "Axis #0:\n"
            + "{[Time].[Weekly].[1997]}\n"
            + "Axis #1:\n"
            + "{[Product].[Agg]}\n"
            + "Axis #2:\n"
            + "{[Measures].[Customer Count]}\n"
            + "Row #0: 421\n";
        assertQueryReturns(query, expected);
        String derbySql =
            "select \"time_by_day\".\"the_year\" as \"c0\", "
            + "count(distinct \"sales_fact_1997\".\"customer_id\") as \"m0\" "
            + "from \"time_by_day\" as \"time_by_day\", \"sales_fact_1997\" as \"sales_fact_1997\", "
            + "\"product\" as \"product\", \"product_class\" as \"product_class\" "
            + "where \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" "
            + "and \"time_by_day\".\"the_year\" = 1997 and \"sales_fact_1997\".\"product_id\" = \"product\".\"product_id\" "
            + "and \"product\".\"product_class_id\" = \"product_class\".\"product_class_id\" "
            + "and (((\"product\".\"brand_name\" = 'Red Wing' and \"product_class\".\"product_subcategory\" = 'Pot Scrubbers' "
            + "and \"product_class\".\"product_category\" = 'Kitchen Products' "
            + "and \"product_class\".\"product_department\" = 'Household' "
            + "and \"product_class\".\"product_family\" = 'Non-Consumable') "
            + "or (\"product\".\"brand_name\" = 'Cormorant' and \"product_class\".\"product_subcategory\" = 'Pot Scrubbers' "
            + "and \"product_class\".\"product_category\" = 'Kitchen Products' "
            + "and \"product_class\".\"product_department\" = 'Household' "
            + "and \"product_class\".\"product_family\" = 'Non-Consumable') "
            + "or (\"product\".\"brand_name\" = 'Denny' and \"product_class\".\"product_subcategory\" = 'Pot Scrubbers' "
            + "and \"product_class\".\"product_category\" = 'Kitchen Products' "
            + "and \"product_class\".\"product_department\" = 'Household' "
            + "and \"product_class\".\"product_family\" = 'Non-Consumable') or (\"product\".\"brand_name\" = 'High Quality' "
            + "and \"product_class\".\"product_subcategory\" = 'Pot Scrubbers' "
            + "and \"product_class\".\"product_category\" = 'Kitchen Products' "
            + "and \"product_class\".\"product_department\" = 'Household' and \"product_class\".\"product_family\" = 'Non-Consumable')) "
            + "or (\"product_class\".\"product_subcategory\" = 'Pots and Pans' "
            + "and \"product_class\".\"product_category\" = 'Kitchen Products' and \"product_class\".\"product_department\" = 'Household' "
            + "and \"product_class\".\"product_family\" = 'Non-Consumable')) "
            + "group by \"time_by_day\".\"the_year\"";

        String accessSql =
            "select `d0` as `c0`, count(`m0`) as `c1` from (select distinct `time_by_day`.`the_year` as `d0`, `sales_fact_1997`.`customer_id` as `m0` "
            + "from `time_by_day` as `time_by_day`, `sales_fact_1997` as `sales_fact_1997`, `product` as `product`, `product_class` as `product_class` "
            + "where `sales_fact_1997`.`time_id` = `time_by_day`.`time_id` and `time_by_day`.`the_year` = 1997 "
            + "and `sales_fact_1997`.`product_id` = `product`.`product_id` and `product`.`product_class_id` = `product_class`.`product_class_id` "
            + "and (((`product`.`brand_name` = 'High Quality' and `product_class`.`product_subcategory` = 'Pot Scrubbers' "
            + "and `product_class`.`product_category` = 'Kitchen Products' and `product_class`.`product_department` = 'Household' "
            + "and `product_class`.`product_family` = 'Non-Consumable') or (`product`.`brand_name` = 'Denny' "
            + "and `product_class`.`product_subcategory` = 'Pot Scrubbers' and `product_class`.`product_category` = 'Kitchen Products' "
            + "and `product_class`.`product_department` = 'Household' "
            + "and `product_class`.`product_family` = 'Non-Consumable') or (`product`.`brand_name` = 'Red Wing' "
            + "and `product_class`.`product_subcategory` = 'Pot Scrubbers' and `product_class`.`product_category` = 'Kitchen Products' "
            + "and `product_class`.`product_department` = 'Household' "
            + "and `product_class`.`product_family` = 'Non-Consumable') or (`product`.`brand_name` = 'Cormorant' "
            + "and `product_class`.`product_subcategory` = 'Pot Scrubbers' and `product_class`.`product_category` = 'Kitchen Products' "
            + "and `product_class`.`product_department` = 'Household' "
            + "and `product_class`.`product_family` = 'Non-Consumable')) or (`product_class`.`product_subcategory` = 'Pots and Pans' "
            + "and `product_class`.`product_category` = 'Kitchen Products' and `product_class`.`product_department` = 'Household' "
            + "and `product_class`.`product_family` = 'Non-Consumable'))) as `dummyname` group by `d0`";

        // FIXME jvs 20-Sept-2008: The Derby pattern fails, probably due to
        // usage of non-order-deterministic Hash data structures in
        // AggregateFunDef.  (Access may be failing too; I haven't tried it.)
        // So it is disabled for now.  Perhaps this test should be calling
        // directly into optimizeChildren like some of the tests below rather
        // than using SQL pattern verification.
        SqlPattern[] patterns = {
            new SqlPattern(
                Dialect.DatabaseProduct.ACCESS, accessSql, accessSql)};

        assertQuerySql(query, patterns);
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
            + "{[Gender].[Gender].Members}*"
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
            + "{[Product].[Agg]}\n"
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
        TupleList genderMembers =
            genderMembersIncludingAll(false, salesCubeSchemaReader, salesCube);
        TupleList productMembers =
            productMembersPotScrubbersPotsAndPans(salesCubeSchemaReader);
        TupleList crossJoinResult = mutableCrossJoin(
            genderMembers, productMembers);
        TupleList storeMembers = storeMembersCAAndOR(salesCubeSchemaReader);
        crossJoinResult = mutableCrossJoin(crossJoinResult, storeMembers);
        TupleList tuples = optimizeChildren(crossJoinResult);
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
        TupleList genderMembers =
            genderMembersIncludingAll(false, salesCubeSchemaReader, salesCube);
        TupleList productMembers =
            productMembersPotScrubbersPotsAndPans(salesCubeSchemaReader);
        TupleList memberList = mutableCrossJoin(genderMembers, productMembers);
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
        TupleList genderMembers =
            genderMembersIncludingAll(false, salesCubeSchemaReader, salesCube);
        TupleList storeMembers =
            storeMembersUsaAndCanada(false, salesCubeSchemaReader, salesCube);
        TupleList memberList = mutableCrossJoin(genderMembers, storeMembers);

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
        TupleList genderMembers =
            genderMembersIncludingAll(false, salesCubeSchemaReader, salesCube);
        TupleList storeMembers =
            storeMembersUsaAndCanada(false, salesCubeSchemaReader, salesCube);
        TupleList memberList = mutableCrossJoin(genderMembers, storeMembers);
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

        TupleList list2 =
            storeMembersUsaAndCanada(
                false, salesCubeSchemaReader, salesCube);
        memberList = mutableCrossJoin(memberList, list2);

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
        propSaver.set(props.EnableGroupingSets, true);

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
            + "{[Gender].[AGG]}\n"
            + "{[Gender].[AGG2]}\n"
            + "Row #0: 2,755\n"
            + "Row #0: 131,558\n"
            + "Row #1: 2,826\n"
            + "Row #1: 135,215\n");
    }

    public void testDistinctCountForAggregatesAtTheSameLevel() {
        propSaver.set(props.EnableGroupingSets, true);
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
            + "{[Gender].[AGG]}\n"
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
                    + "{[Customers].[USA]}\n"
                    + "{[Customers].[USA].[OR]}\n"
                    + "{[Customers].[USA].[WA]}\n"
                    + "Axis #2:\n"
                    + "{[Measures].[Customer Count]}\n"
                    + "Row #0: 2,865\n"
                    + "Row #0: 1,037\n"
                    + "Row #0: 1,828\n");
    }

    /**
     * This is a test for
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-1125">MONDRIAN-1225</a>
     *
     * <p>The optimization routine for tuple lists was implementing a single
     * side of an IF conditional, which resulted in an NPE.
     */
    public void testTupleOptimizationBug1225() {
        Member caMember =
            member(
                Id.Segment.toList(
                    "Store", "All Stores", "USA", "CA"),
                salesCubeSchemaReader);
        Member orMember =
            member(
                Id.Segment.toList(
                    "Store", "All Stores", "USA", "OR"),
                salesCubeSchemaReader);
        Member waMember =
            member(
                Id.Segment.toList(
                    "Store", "All Stores", "USA", "WA"),
                salesCubeSchemaReader);
        Member femaleMember =
            member(
                Id.Segment.toList("Gender", "All Gender", "F"),
                salesCubeSchemaReader);
        Member [] tupleMembersArity1 =
            new Member[] {
                caMember,
                allMember("Gender", salesCube)};
        Member [] tupleMembersArity2 =
            new Member[] {
                orMember,
                allMember("Gender", salesCube)};
        Member [] tupleMembersArity3 =
            new Member[] {
                waMember,
                femaleMember};

        TupleList tl = new ArrayTupleList(2);
        tl.add(Arrays.asList(tupleMembersArity1));
        tl.add(Arrays.asList(tupleMembersArity2));
        tl.add(Arrays.asList(tupleMembersArity3));

        TupleList optimized =
            optimizeChildren(tl);
        assertEquals(
            "[[[Store].[USA], [Gender].[All Gender]], [[Store].[USA], [Gender].[F]]]",
            optimized.toString());
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
                        memberList, schemaReader, salesCube);
                }
            }
        );
    }

    private TupleList mutableCrossJoin(
        final TupleList list1, final TupleList list2)
    {
        return Locus.execute(
            Execution.NONE,
            "AggregationOnDistinctCountMeasuresTest",
            new Locus.Action<TupleList>() {
                public TupleList execute()
                {
                    return CrossJoinFunDef.mutableCrossJoin(
                        list1, list2);
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
        String dimension =
            "<Dimension name=\"Time\" type=\"TimeDimension\"> "
            + "  <Hierarchy hasAll=\"false\" primaryKey=\"time_id\"> "
            + "    <Table name=\"time_by_day\"/> "
            + "    <Level name=\"Year\" column=\"the_year\" type=\"Numeric\" uniqueMembers=\"true\" levelType=\"TimeYears\"/> "
            + "    <Level name=\"Quarter\" column=\"quarter\" uniqueMembers=\"false\" levelType=\"TimeQuarters\"/> "
            + "    <Level name=\"Month\" column=\"month_of_year\" uniqueMembers=\"false\" type=\"Numeric\" levelType=\"TimeMonths\"/> "
            + "  </Hierarchy> "
            + "</Dimension>";
        String cube =
            "<Cube name=\"Sales\" defaultMeasure=\"Unit Sales\"> "
            + "  <Table name=\"sales_fact_1997\"> "
            + "      <AggExclude name=\"agg_c_special_sales_fact_1997\"/>"
            + "      <AggExclude name=\"agg_g_ms_pcat_sales_fact_1997\"/>"
            + "      <AggExclude name=\"agg_c_14_sales_fact_1997\"/>"
            + "      <AggExclude name=\"agg_l_05_sales_fact_1997\"/>"
            + "      <AggExclude name=\"agg_lc_06_sales_fact_1997\"/>"
            + "      <AggExclude name=\"agg_l_04_sales_fact_1997\"/>"
            + "      <AggExclude name=\"agg_ll_01_sales_fact_1997\"/>"
            + "      <AggExclude name=\"agg_lc_100_sales_fact_1997\"/>"
            + "      <AggExclude name=\"agg_l_03_sales_fact_1997\"/>"
            + "      <AggExclude name=\"agg_pl_01_sales_fact_1997\"/>"
            + "      <AggName name=\"agg_c_10_sales_fact_1997\">"
            + "           <AggFactCount column=\"FACT_COUNT\"/>"
            + "           <AggMeasure name=\"[Measures].[Store Sales]\" column=\"store_sales\"/>"
            + "           <AggMeasure name=\"[Measures].[Store Cost]\" column=\"store_cost\"/>"
            + "           <AggMeasure name=\"[Measures].[Unit Sales]\" column=\"unit_sales\"/>"
            + "           <AggMeasure name=\"[Measures].[Customer Count]\" column=\"customer_count\" />"
            + "           <AggLevel name=\"[Time].[Year]\" column=\"the_year\" />"
            + "           <AggLevel name=\"[Time].[Quarter]\" column=\"quarter\" />"
            + "           <AggLevel name=\"[Time].[Month]\" column=\"month_of_year\" />"
            + "      </AggName>"
            + "  </Table>"
            + "  <DimensionUsage name=\"Time\" source=\"Time\" foreignKey=\"time_id\"/> "
            + "<Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\"\n"
            + "      formatString=\"Standard\"/>\n"
            + "  <Measure name=\"Store Cost\" column=\"store_cost\" aggregator=\"sum\"\n"
            + "      formatString=\"#,###.00\"/>\n"
            + "  <Measure name=\"Store Sales\" column=\"store_sales\" aggregator=\"sum\"\n"
            + "      formatString=\"#,###.00\"/>"
            + "  <Measure name=\"Customer Count\" column=\"customer_id\" aggregator=\"distinct-count\" formatString=\"#,###\" />"
            + "</Cube>";
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
        String simpleSchema = "<Schema name=\"FoodMart\">" + dimension + cube
            + "</Schema>";
        // should skip aggregate table, cannot aggregate
        propSaver.set(propSaver.properties.UseAggregates, true);
        propSaver.set(propSaver.properties.ReadAggregates, true);
        TestContext withAggDistinctCount =
            TestContext.instance().withSchema(simpleSchema);
        withAggDistinctCount.assertQueryReturns(
            query,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Customer Count]}\n"
            + "Axis #2:\n"
            + "{[Time].[1997]}\n"
            + "Row #0: 5,581\n");
        // aggregate table has count for months, make sure it is used
        propSaver.set(propSaver.properties.UseAggregates, true);
        propSaver.set(propSaver.properties.ReadAggregates, true);
        propSaver.set(propSaver.properties.GenerateFormattedSql, true);
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
    
  /**
   * Verify that the CACHE MDX function includes aggregation lists in the current evaluation context. In this test, the
   * CM with solve order 20 will set an aggregation list for the distinct count measure. The cache key on the CM with
   * solve order 10 needs to include the aggregation list or else the cache generated for [Gender].[F], [Store
   * Type].[*TOTAL_MEMBER_SEL~AGG] would be re-used for [Gender].[M], [Store Type].[*TOTAL_MEMBER_SEL~AGG]
   * 
   */
  public void testCachedAggregate() {

    Result result =
        executeQuery( " WITH\r\n"
            + " SET [*NATIVE_CJ_SET_WITH_SLICER] AS 'NONEMPTYCROSSJOIN([*BASE_MEMBERS__Gender_],NONEMPTYCROSSJOIN([*BASE_MEMBERS__Store Type_],[*BASE_MEMBERS__Product_]))'\r\n"
            + " SET [*NATIVE_CJ_SET] AS 'GENERATE([*NATIVE_CJ_SET_WITH_SLICER], {([Gender].CURRENTMEMBER,[Store Type].CURRENTMEMBER)})'\r\n"
            + " SET [*BASE_MEMBERS__Store Type_] AS '{[Store Type].[All Store Types].[Gourmet Supermarket],[Store Type].[All Store Types].[Supermarket]}'\r\n"
            + " SET [*BASE_MEMBERS__Gender_] AS '[Gender].[Gender].MEMBERS'\r\n"
            + " SET [*CJ_SLICER_AXIS] AS 'GENERATE([*NATIVE_CJ_SET_WITH_SLICER], {([Product].CURRENTMEMBER)})'\r\n"
            + " SET [*BASE_MEMBERS__Product_] AS '{[Product].[All Products].[Food],[Product].[All Products].[Drink]}'\r\n"
            + " SET [*CJ_ROW_AXIS] AS 'GENERATE([*NATIVE_CJ_SET], {([Gender].CURRENTMEMBER,[Store Type].CURRENTMEMBER)})'\r\n"
            + " MEMBER [Store Type].[*TOTAL_MEMBER_SEL~AGG] AS '([Education Level].[*TOTAL_MEMBER_SEL~AGG], [Time].[*TOTAL_MEMBER_SEL~AGG])'\r\n"
            + " MEMBER [Education Level].[*TOTAL_MEMBER_SEL~AGG] AS 'CACHE(AGGREGATE([*CJ_SLICER_AXIS]))', SOLVE_ORDER=10\r\n"
            + " MEMBER [Time].[*TOTAL_MEMBER_SEL~AGG] AS 'AGGREGATE(EXISTS([*CJ_ROW_AXIS],([Gender].CURRENTMEMBER)))', SOLVE_ORDER=20\r\n"
            + " SELECT\r\n" + " {[Measures].[Customer Count]} ON COLUMNS\r\n" + " , NON EMPTY\r\n"
            + " UNION(CROSSJOIN(GENERATE([*CJ_ROW_AXIS], {([Gender].CURRENTMEMBER)}),{[Store Type].[*TOTAL_MEMBER_SEL~AGG]}),[*CJ_ROW_AXIS]) ON ROWS\r\n"
            + " FROM [Sales]\r\n" );
    String resultString = TestContext.toString( result );
    TestContext.assertEqualsVerbose( "Axis #0:\n" + "{}\n" + "Axis #1:\n" + "{[Measures].[Customer Count]}\n"
        + "Axis #2:\n" + "{[Gender].[F], [Store Type].[*TOTAL_MEMBER_SEL~AGG]}\n"
        + "{[Gender].[M], [Store Type].[*TOTAL_MEMBER_SEL~AGG]}\n"
        + "{[Gender].[F], [Store Type].[Gourmet Supermarket]}\n" + "{[Gender].[F], [Store Type].[Supermarket]}\n"
        + "{[Gender].[M], [Store Type].[Gourmet Supermarket]}\n" + "{[Gender].[M], [Store Type].[Supermarket]}\n"
        + "Row #0: 2,044\n" + "Row #1: 2,084\n" + "Row #2: 519\n" + "Row #3: 1,896\n" + "Row #4: 540\n"
        + "Row #5: 1,945\n", getTestContext().upgradeActual( resultString ) );
    Execution e = ( (ResultBase) result ).getExecution();
    assertEquals( 2, e.getExpCacheHitCount() );
    assertEquals( 10, e.getExpCacheMissCount() );
  }

  /**
   * Similar to above test except now we verify the cache key is correct when generated for the slicer compound member.
   */
  public void testCachedCompoundSlicer() {

    Result result =
        executeQuery( " WITH\r\n"
            + " SET [*NATIVE_CJ_SET_WITH_SLICER] AS 'NONEMPTYCROSSJOIN([*BASE_MEMBERS__Gender_],NONEMPTYCROSSJOIN([*BASE_MEMBERS__Store Type_],[*BASE_MEMBERS__Product_]))'\r\n"
            + " SET [*NATIVE_CJ_SET] AS 'GENERATE([*NATIVE_CJ_SET_WITH_SLICER], {([Gender].CURRENTMEMBER,[Store Type].CURRENTMEMBER)})'\r\n"
            + " SET [*BASE_MEMBERS__Store Type_] AS '{[Store Type].[All Store Types].[Gourmet Supermarket],[Store Type].[All Store Types].[Supermarket]}'\r\n"
            + " SET [*SORTED_ROW_AXIS] AS 'ORDER([*CJ_ROW_AXIS],[Gender].CURRENTMEMBER.ORDERKEY,BASC,[Store Type].CURRENTMEMBER.ORDERKEY,BASC)'\r\n"
            + " SET [*BASE_MEMBERS__Measures_] AS '{[Measures].[*FORMATTED_MEASURE_0]}'\r\n"
            + " SET [*BASE_MEMBERS__Gender_] AS '[Gender].[Gender].MEMBERS'\r\n"
            + " SET [*CJ_SLICER_AXIS] AS 'GENERATE([*NATIVE_CJ_SET_WITH_SLICER], {([Product].CURRENTMEMBER)})'\r\n"
            + " SET [*BASE_MEMBERS__Product_] AS '{[Product].[All Products].[Food],[Product].[All Products].[Drink]}'\r\n"
            + " SET [*CJ_ROW_AXIS] AS 'GENERATE([*NATIVE_CJ_SET], {([Gender].CURRENTMEMBER,[Store Type].CURRENTMEMBER)})'\r\n"
            + " MEMBER [Measures].[*FORMATTED_MEASURE_0] AS '[Measures].[Customer Count]', FORMAT_STRING = '#,###', SOLVE_ORDER=500\r\n"
            + " MEMBER [Store Type].[*TOTAL_MEMBER_SEL~AGG] AS 'AGGREGATE(CACHEDEXISTS([*CJ_ROW_AXIS],([Gender].CURRENTMEMBER),\"[*CJ_ROW_AXIS]\"))', SOLVE_ORDER=-101\r\n"
            + " SELECT\r\n" + " [*BASE_MEMBERS__Measures_] ON COLUMNS\r\n" + " , NON EMPTY\r\n"
            + " UNION(CROSSJOIN(GENERATE([*CJ_ROW_AXIS], {([Gender].CURRENTMEMBER)}),{[Store Type].[*TOTAL_MEMBER_SEL~AGG]}),[*SORTED_ROW_AXIS]) ON ROWS\r\n"
            + " FROM [Sales]\r\n" + " WHERE ([*CJ_SLICER_AXIS])\r\n" );
    String resultString = TestContext.toString( result );
    TestContext.assertEqualsVerbose( "Axis #0:\n" + "{[Product].[Drink]}\n" + "{[Product].[Food]}\n" + "Axis #1:\n"
        + "{[Measures].[*FORMATTED_MEASURE_0]}\n" + "Axis #2:\n"
        + "{[Gender].[F], [Store Type].[*TOTAL_MEMBER_SEL~AGG]}\n"
        + "{[Gender].[M], [Store Type].[*TOTAL_MEMBER_SEL~AGG]}\n"
        + "{[Gender].[F], [Store Type].[Gourmet Supermarket]}\n" + "{[Gender].[F], [Store Type].[Supermarket]}\n"
        + "{[Gender].[M], [Store Type].[Gourmet Supermarket]}\n" + "{[Gender].[M], [Store Type].[Supermarket]}\n"
        + "Row #0: 2,044\n" + "Row #1: 2,084\n" + "Row #2: 512\n" // Less than 519 above because slicer was applied
        + "Row #3: 1,884\n" + "Row #4: 531\n" + "Row #5: 1,929\n", getTestContext().upgradeActual( resultString ) );
    Execution e = ( (ResultBase) result ).getExecution();
    assertEquals( 1, e.getExpCacheHitCount() );
    assertEquals( 15, e.getExpCacheMissCount() );

  }

  /**
   * Verifies that expression cache entries generated with aggregation lists can be re-used.
   */
  public void testExpCacheHit() {

    Result result =
        executeQuery( "WITH\r\n"
            + " SET [*NATIVE_CJ_SET_WITH_SLICER] AS 'NONEMPTYCROSSJOIN([*BASE_MEMBERS__Gender_],NONEMPTYCROSSJOIN([*BASE_MEMBERS__Store Type_],[*BASE_MEMBERS__Product_]))'\r\n"
            + " SET [*NATIVE_CJ_SET] AS 'GENERATE([*NATIVE_CJ_SET_WITH_SLICER], {([Gender].CURRENTMEMBER,[Store Type].CURRENTMEMBER)})'\r\n"
            + " SET [*METRIC_CJ_SET] AS 'FILTER([*NATIVE_CJ_SET],[Gender].CURRENTMEMBER IN [*METRIC_CACHE_SET])'\r\n"
            + " SET [*BASE_MEMBERS__Store Type_] AS '{[Store Type].[All Store Types].[Gourmet Supermarket],[Store Type].[All Store Types].[Supermarket]}'\r\n"
            + " SET [*SORTED_ROW_AXIS] AS 'ORDER([*CJ_ROW_AXIS],[Gender].CURRENTMEMBER.ORDERKEY,BASC,[Store Type].CURRENTMEMBER.ORDERKEY,BASC)'\r\n"
            + " SET [*BASE_MEMBERS__Measures_] AS '{[Measures].[Customer Count]}'\r\n"
            + " SET [*BASE_MEMBERS__Gender_] AS '[Gender].[Gender].MEMBERS'\r\n"
            + " SET [*METRIC_CACHE_SET] AS 'FILTER(GENERATE([*NATIVE_CJ_SET],{([Gender].CURRENTMEMBER)}),2044 <= [Measures].[*Customer Count_SEL~AGG] and [Measures].[*Customer Count_SEL~AGG] <= 2084)'\r\n"
            + " SET [*CJ_SLICER_AXIS] AS 'GENERATE([*NATIVE_CJ_SET_WITH_SLICER], {([Product].CURRENTMEMBER)})'\r\n"
            + " SET [*BASE_MEMBERS__Product_] AS '{[Product].[All Products].[Drink],[Product].[All Products].[Food]}'\r\n"
            + " SET [*CJ_ROW_AXIS] AS 'GENERATE([*METRIC_CJ_SET], {([Gender].CURRENTMEMBER,[Store Type].CURRENTMEMBER)})'\r\n"
            + " MEMBER [Education Level].[*METRIC_CTX_SET_AGG] AS 'CACHE(AGGREGATE(CACHEDEXISTS([*NATIVE_CJ_SET],([Gender].CURRENTMEMBER),\"[*NATIVE_CJ_SET]\")))', SOLVE_ORDER=-100\r\n"
            + " MEMBER [Measures].[*Customer Count_SEL~AGG] AS '([Measures].[Customer Count], [Education Level].[*METRIC_CTX_SET_AGG])', SOLVE_ORDER=400\r\n"
            + " MEMBER [Store Type].[*TOTAL_MEMBER_SEL~AGG] AS 'AGGREGATE(CACHEDEXISTS([*CJ_ROW_AXIS],([Gender].CURRENTMEMBER),\"[*CJ_ROW_AXIS]\"))', SOLVE_ORDER=-101\r\n"
            + " SELECT\r\n" + " [*BASE_MEMBERS__Measures_] ON COLUMNS\r\n" + " , NON EMPTY\r\n"
            + " UNION(CROSSJOIN(GENERATE([*CJ_ROW_AXIS], {([Gender].CURRENTMEMBER)}),{[Store Type].[*TOTAL_MEMBER_SEL~AGG]}),[*SORTED_ROW_AXIS]) ON ROWS\r\n"
            + " FROM [Sales]\r\n" + " WHERE ([*CJ_SLICER_AXIS])" );
    String resultString = TestContext.toString( result );
    TestContext.assertEqualsVerbose( "Axis #0:\n" + "{[Product].[Drink]}\n" + "{[Product].[Food]}\n" + "Axis #1:\n"
        + "{[Measures].[Customer Count]}\n" + "Axis #2:\n" + "{[Gender].[F], [Store Type].[*TOTAL_MEMBER_SEL~AGG]}\n"
        + "{[Gender].[M], [Store Type].[*TOTAL_MEMBER_SEL~AGG]}\n"
        + "{[Gender].[F], [Store Type].[Gourmet Supermarket]}\n" + "{[Gender].[F], [Store Type].[Supermarket]}\n"
        + "{[Gender].[M], [Store Type].[Gourmet Supermarket]}\n" + "{[Gender].[M], [Store Type].[Supermarket]}\n"
        + "Row #0: 2,044\n" + "Row #1: 2,084\n" + "Row #2: 512\n" + "Row #3: 1,884\n" + "Row #4: 531\n"
        + "Row #5: 1,929\n", getTestContext().upgradeActual( resultString ) );
    Execution e = ( (ResultBase) result ).getExecution();
    assertEquals( 13, e.getExpCacheHitCount() );
    assertEquals( 23, e.getExpCacheMissCount() );
  }
  
  public void testExpCacheHit2() {

    Result result =
        executeQuery( "WITH\r\n" + 
            " SET [*NATIVE_CJ_SET_WITH_SLICER] AS 'NONEMPTYCROSSJOIN([*BASE_MEMBERS__Customers_],NONEMPTYCROSSJOIN([*BASE_MEMBERS__Education Level_],NONEMPTYCROSSJOIN([*BASE_MEMBERS__Time_],NONEMPTYCROSSJOIN([*BASE_MEMBERS__Product_],[*BASE_MEMBERS__Promotion Media_]))))'\r\n" + 
            " SET [*NATIVE_CJ_SET] AS 'GENERATE([*NATIVE_CJ_SET_WITH_SLICER], {([Customers].CURRENTMEMBER,[Education Level].CURRENTMEMBER,[Time].CURRENTMEMBER)})'\r\n" + 
            " SET [*METRIC_CJ_SET] AS 'FILTER([*NATIVE_CJ_SET],[Customers].CURRENTMEMBER IN [*METRIC_CACHE_SET])'\r\n" + 
            " SET [*SORTED_ROW_AXIS] AS 'ORDER([*CJ_ROW_AXIS],[Customers].CURRENTMEMBER.ORDERKEY,BASC,ANCESTOR([Customers].CURRENTMEMBER,[Customers].[City]).ORDERKEY,BASC,[Measures].[*SORTED_MEASURE],BASC)'\r\n" + 
            " SET [*BASE_MEMBERS__Education Level_] AS '{[Education Level].[All Education Levels].[Graduate Degree],[Education Level].[All Education Levels].[High School Degree],[Education Level].[All Education Levels].[Partial College],[Education Level].[All Education Levels].[Partial High School]}'\r\n" + 
            " SET [*BASE_MEMBERS__Customers_] AS '[Customers].[Name].MEMBERS'\r\n" + 
            " SET [*METRIC_CACHE_SET] AS 'FILTER(GENERATE([*NATIVE_CJ_SET],{([Customers].CURRENTMEMBER)}),[Measures].[**CALCULATED_MEASURE_3_SEL~SUM] > 0)'\r\n" + 
            " SET [*METRIC_MEMBERS__Time_] AS 'GENERATE([*METRIC_CJ_SET], {[Time].CURRENTMEMBER})'\r\n" + 
            " SET [*SORTED_COL_AXIS] AS 'ORDER([*CJ_COL_AXIS],[Time].CURRENTMEMBER.ORDERKEY,BASC,ANCESTOR([Time].CURRENTMEMBER,[Time].[Quarter]).ORDERKEY,BASC,[Measures].CURRENTMEMBER.ORDERKEY,BASC)'\r\n" + 
            " SET [*BASE_MEMBERS__Measures_] AS '{[Measures].[*FORMATTED_MEASURE_0],[Measures].[*CALCULATED_MEASURE_2],[Measures].[*CALCULATED_MEASURE_1],[Measures].[*CALCULATED_MEASURE_4],[Measures].[*CALCULATED_MEASURE_3]}'\r\n" + 
            " SET [*CJ_SLICER_AXIS] AS 'GENERATE([*NATIVE_CJ_SET_WITH_SLICER], {([Product].CURRENTMEMBER,[Promotion Media].CURRENTMEMBER)})'\r\n" + 
            " SET [*CJ_COL_AXIS] AS 'GENERATE([*METRIC_CJ_SET], {([Time].CURRENTMEMBER)})'\r\n" + 
            " SET [*BASE_MEMBERS__Product_] AS '{[Product].[All Products].[Drink]}'\r\n" + 
            " SET [*CJ_ROW_AXIS] AS 'GENERATE([*METRIC_CJ_SET], {([Customers].CURRENTMEMBER,[Education Level].CURRENTMEMBER)})'\r\n" + 
            " SET [*BASE_MEMBERS__Time_] AS '{[Time].[1997].[Q4].[12]}'\r\n" + 
            " SET [*BASE_MEMBERS__Promotion Media_] AS '{[Promotion Media].[All Media].[Bulk Mail],[Promotion Media].[All Media].[Cash Register Handout]}'\r\n" + 
            " MEMBER [Store].[*METRIC_CTX_SET_SUM] AS 'CACHE(SUM(CACHEDEXISTS([*NATIVE_CJ_SET],([Customers].CURRENTMEMBER),\"[*NATIVE_CJ_SET]\")))', SOLVE_ORDER=100\r\n" + 
            " MEMBER [Measures].[**CALCULATED_MEASURE_3_SEL~SUM] AS '([Measures].[*CALCULATED_MEASURE_3], [Store].[*METRIC_CTX_SET_SUM])', SOLVE_ORDER=400\r\n" + 
            " MEMBER [Measures].[*CALCULATED_MEASURE_1] AS 'CACHE(SUM(\r\n" + 
            "\r\n" + 
            "PERIODSTODATE([Time].[Year], \r\n" + 
            "\r\n" + 
            "ParallelPeriod(\r\n" + 
            "[Time].[Quarter], 1,\r\n" + 
            "[Time].CurrentMember)\r\n" + 
            "\r\n" + 
            ")\r\n" + 
            ", [Measures].[Unit Sales]))', SOLVE_ORDER=200\r\n" + 
            " MEMBER [Measures].[*CALCULATED_MEASURE_2] AS 'CACHE(SUM(\r\n" + 
            "\r\n" + 
            "PERIODSTODATE([Time].[Year], [Time].CurrentMember), [Measures].[Unit Sales]))', SOLVE_ORDER=0\r\n" + 
            " MEMBER [Measures].[*CALCULATED_MEASURE_3] AS '([Measures].[*CALCULATED_MEASURE_2]-[Measures].[*CALCULATED_MEASURE_1])/[Measures].[*CALCULATED_MEASURE_1]', FORMAT_STRING = '###0.00%', SOLVE_ORDER=0\r\n" + 
            " MEMBER [Measures].[*CALCULATED_MEASURE_4] AS '[Measures].[*CALCULATED_MEASURE_2]-[Measures].[*CALCULATED_MEASURE_1]', SOLVE_ORDER=0\r\n" + 
            " MEMBER [Measures].[*FORMATTED_MEASURE_0] AS '[Measures].[Unit Sales]', FORMAT_STRING = 'Standard', SOLVE_ORDER=500\r\n" + 
            " MEMBER [Measures].[*SORTED_MEASURE] AS '([Measures].[*CALCULATED_MEASURE_3],[Time].[*CTX_MEMBER_SEL~SUM])', SOLVE_ORDER=400\r\n" + 
            " MEMBER [Time].[*CTX_MEMBER_SEL~SUM] AS 'SUM([*METRIC_MEMBERS__Time_])', SOLVE_ORDER=98\r\n" + 
            " SELECT\r\n" + 
            " CROSSJOIN([*SORTED_COL_AXIS],[*BASE_MEMBERS__Measures_]) ON COLUMNS\r\n" + 
            " , NON EMPTY\r\n" + 
            " [*SORTED_ROW_AXIS] ON ROWS\r\n" + 
            " FROM [Sales]\r\n" + 
            " WHERE ([*CJ_SLICER_AXIS])" );
    Execution e = ( (ResultBase) result ).getExecution();
    assertEquals( 3581, e.getExpCacheHitCount() );
    assertEquals( 8300, e.getExpCacheMissCount() );
  }
  
}

// End AggregationOnDistinctCountMeasuresTest.java
