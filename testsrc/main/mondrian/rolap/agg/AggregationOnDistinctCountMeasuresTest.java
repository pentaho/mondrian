/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2001-2007 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// ajogleka, 19 December, 2007
*/
package mondrian.rolap.agg;

import mondrian.olap.*;
import mondrian.olap.fun.AggregateFunDef;
import mondrian.rolap.BatchTestCase;
import mondrian.test.SqlPattern;
import mondrian.test.TestContext;
import java.util.List;
import java.util.ArrayList;

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

    public TestContext getTestContext() {
        final TestContext testContext =
            TestContext.create(null, null,
                "<VirtualCube name=\"Warehouse and Sales2\" defaultMeasure=\"Store Sales\">\n" +
                "<VirtualCubeDimension cubeName=\"Sales\" name=\"Gender\"/>\n" +
                "<VirtualCubeDimension name=\"Store\"/>\n" +
                "<VirtualCubeDimension name=\"Product\"/>\n" +
                "<VirtualCubeDimension cubeName=\"Warehouse\" name=\"Warehouse\"/>\n" +
                "<VirtualCubeMeasure cubeName=\"Sales\" name=\"[Measures].[Store Sales]\"/>\n" +
                "<VirtualCubeMeasure cubeName=\"Sales\" name=\"[Measures].[Customer Count]\"/>\n" +
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
            "from \"time_by_day\" as \"time_by_day\", \"sales_fact_1997\" as \"sales_fact_1997\", " +
            "\"customer\" as \"customer\", " +
            "\"store\" as \"store\" " +
            "where \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" " +
            "and \"time_by_day\".\"the_year\" = 1997 " +
            "and \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\" " +
            "and \"sales_fact_1997\".\"store_id\" = \"store\".\"store_id\" " +
            "and ((\"customer\".\"gender\" = 'F' and \"store\".\"store_state\" = 'CA') " +
            "or (\"customer\".\"gender\" = 'M' and \"store\".\"store_state\" = 'CA')) " +
            "group by \"time_by_day\".\"the_year\"";
        
        String mysqlSql =
            "select `time_by_day`.`the_year` as `c0`, " +
            "count(distinct `sales_fact_1997`.`customer_id`) as `m0` " +
            "from `time_by_day` as `time_by_day`, `sales_fact_1997` as `sales_fact_1997`, " +
            "`customer` as `customer`, `store` as `store` " +
            "where `sales_fact_1997`.`time_id` = `time_by_day`.`time_id` " +
            "and `time_by_day`.`the_year` = 1997 " +
            "and `sales_fact_1997`.`customer_id` = `customer`.`customer_id` " +
            "and `sales_fact_1997`.`store_id` = `store`.`store_id` " +
            "and (((`store`.`store_state`, `customer`.`gender`) " +
            "in (('CA', 'F'), ('CA', 'M')))) group by `time_by_day`.`the_year`";
        
        String oracleSql =
            "select \"time_by_day\".\"the_year\" as \"c0\", " +
            "count(distinct \"sales_fact_1997\".\"customer_id\") as \"m0\" " +
            "from \"time_by_day\" \"time_by_day\", " +
            "\"sales_fact_1997\" \"sales_fact_1997\", " +
            "\"customer\" \"customer\", \"store\" \"store\" " +
            "where \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" " +
            "and \"time_by_day\".\"the_year\" = 1997 " +
            "and \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\" " +
            "and \"sales_fact_1997\".\"store_id\" = \"store\".\"store_id\" " +
            "and ((\"customer\".\"gender\" = 'F' " +
            "and \"store\".\"store_state\" = 'CA') " +
            "or (\"customer\".\"gender\" = 'M' " +
            "and \"store\".\"store_state\" = 'CA')) " +
            "group by \"time_by_day\".\"the_year\"";

        SqlPattern[] patterns = {
            new SqlPattern(SqlPattern.Dialect.DERBY, derbySql, derbySql),
            new SqlPattern(SqlPattern.Dialect.MYSQL, mysqlSql, mysqlSql),
            new SqlPattern(SqlPattern.Dialect.ORACLE, oracleSql, oracleSql)};

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
            "from \"time_by_day\" as \"time_by_day\", \"sales_fact_1997\" as \"sales_fact_1997\", " +
            "\"customer\" as \"customer\", \"store\" as \"store\" " +
            "where \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" " +
            "and \"time_by_day\".\"the_year\" = 1997 and \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\" " +
            "and \"sales_fact_1997\".\"store_id\" = \"store\".\"store_id\" " +
            "and (((\"customer\".\"gender\" = 'F' and \"store\".\"store_state\" = 'CA') " +
            "or (\"customer\".\"gender\" = 'M' and \"store\".\"store_state\" = 'CA')) " +
            "or ((\"customer\".\"gender\" = 'F' and \"store\".\"store_country\" = 'Canada') " +
            "or (\"customer\".\"gender\" = 'M' and \"store\".\"store_country\" = 'Canada'))) " +
            "group by \"time_by_day\".\"the_year\"";
        
        String mysqlSql =
            "select `time_by_day`.`the_year` as `c0`, " +
            "count(distinct `sales_fact_1997`.`customer_id`) as `m0` " +
            "from `time_by_day` as `time_by_day`, `sales_fact_1997` as `sales_fact_1997`, " +
            "`customer` as `customer`, `store` as `store` " +
            "where `sales_fact_1997`.`time_id` = `time_by_day`.`time_id` " +
            "and `time_by_day`.`the_year` = 1997 " +
            "and `sales_fact_1997`.`customer_id` = `customer`.`customer_id` " +
            "and `sales_fact_1997`.`store_id` = `store`.`store_id` " +
            "and ((((`store`.`store_state`, `customer`.`gender`) " +
            "in (('CA', 'F'), ('CA', 'M')))) or " +
            "(((`store`.`store_country`, `customer`.`gender`) " +
            "in (('Canada', 'F'), ('Canada', 'M'))))) " +
            "group by `time_by_day`.`the_year`";
        
        String oracleSql="select \"time_by_day\".\"the_year\" as \"c0\", " +
            "count(distinct \"sales_fact_1997\".\"customer_id\") as \"m0\" " +
            "from \"time_by_day\" \"time_by_day\", " +
            "\"sales_fact_1997\" \"sales_fact_1997\", " +
            "\"customer\" \"customer\", " +
            "\"store\" \"store\" " +
            "where \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" " +
            "and \"time_by_day\".\"the_year\" = 1997 " +
            "and \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\" " +
            "and \"sales_fact_1997\".\"store_id\" = \"store\".\"store_id\" " +
            "and (((\"customer\".\"gender\" = 'F' and \"store\".\"store_state\" = 'CA') or " +
            "(\"customer\".\"gender\" = 'M' and \"store\".\"store_state\" = 'CA')) or " +
            "((\"customer\".\"gender\" = 'F' and \"store\".\"store_country\" = 'Canada') or " +
            "(\"customer\".\"gender\" = 'M' and \"store\".\"store_country\" = 'Canada'))) " +
            "group by \"time_by_day\".\"the_year\"";

        SqlPattern[] patterns = {
            new SqlPattern(SqlPattern.Dialect.DERBY, derbySql, derbySql),
            new SqlPattern(SqlPattern.Dialect.MYSQL, mysqlSql, mysqlSql),
            new SqlPattern(SqlPattern.Dialect.ORACLE, oracleSql, oracleSql)};

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
        assertQueryReturns(
            "WITH MEMBER WAREHOUSE.X as 'Aggregate({WAREHOUSE.[STATE PROVINCE].MEMBERS}*" +
            "{[Gender].Members})'" +
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

    public void testDistinctCountOnMembersWithNonJoiningDimension() {
        assertQueryReturns(
            "WITH MEMBER WAREHOUSE.X as 'Aggregate({WAREHOUSE.[STATE PROVINCE].MEMBERS})'" +
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

    public void testDistinctCountOnTuplesWithLargeNumberOfDimensionMembers() {
        int origMaxConstraint = props.MaxConstraints.get();
        if (origMaxConstraint > 500) {
            props.MaxConstraints.set(500);
        }
        
        assertQueryReturns(
            "WITH MEMBER PRODUCT.X as 'Aggregate({[PRODUCT].[BRAND NAME].MEMBERS})' " +
            "SELECT PRODUCT.X  ON ROWS, " +
            "{[MEASURES].[CUSTOMER COUNT]} ON COLUMNS\n" +
            "FROM [WAREHOUSE AND SALES2]",
            fold(
                "Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Measures].[Customer Count]}\n" +
                "Axis #2:\n" +
                "{[Product].[X]}\n" +
                "Row #0: #ERR: mondrian.olap.fun.MondrianEvaluationException: " +
                "Distinct Count aggregation is not supported over a large list\n"));
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

        String oracleSql = "select \"time_by_day\".\"the_year\" as \"c0\", " +
            "count(distinct \"sales_fact_1997\".\"customer_id\") as \"m0\" " +
            "from \"time_by_day\" \"time_by_day\", " +
            "\"sales_fact_1997\" \"sales_fact_1997\", \"customer\" \"customer\" " +
            "where \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" " +
            "and \"time_by_day\".\"the_year\" = 1997 " +
            "and \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\" " +
            "and \"customer\".\"gender\" " +
            "in ('F', 'M') group by \"time_by_day\".\"the_year\"";
        SqlPattern[] patterns = {new SqlPattern(SqlPattern.Dialect.ORACLE, oracleSql, oracleSql)};
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

        String  oracleSqlForAgg =
            "select \"time_by_day\".\"the_year\" as \"c0\", " +
            "count(distinct \"sales_fact_1997\".\"customer_id\") as \"m0\" " +
            "from \"time_by_day\" \"time_by_day\", " +
            "\"sales_fact_1997\" \"sales_fact_1997\", " +
            "\"store\" \"store\" " +
            "where \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" " +
            "and \"time_by_day\".\"the_year\" = 1997 " +
            "and \"sales_fact_1997\".\"store_id\" = \"store\".\"store_id\" " +
            "and \"store\".\"store_state\" in ('CA', 'OR', 'WA') " +
            "group by \"time_by_day\".\"the_year\"";

        String  oracleSqlForDetail =
            "select \"store\".\"store_state\" as \"c0\", " +
            "\"time_by_day\".\"the_year\" as \"c1\", " +
            "count(distinct \"sales_fact_1997\".\"customer_id\") as \"m0\" " +
            "from \"store\" \"store\", " +
            "\"sales_fact_1997\" \"sales_fact_1997\", " +
            "\"time_by_day\" \"time_by_day\" " +
            "where \"sales_fact_1997\".\"store_id\" = \"store\".\"store_id\" " +
            "and \"store\".\"store_state\" in ('CA', 'OR') " +
            "and \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" " +
            "and \"time_by_day\".\"the_year\" = 1997 " +
            "group by \"store\".\"store_state\", \"time_by_day\".\"the_year\"";

        SqlPattern[] patterns =
            {new SqlPattern(SqlPattern.Dialect.ORACLE, oracleSqlForAgg, oracleSqlForAgg),
            new SqlPattern(SqlPattern.Dialect.ORACLE, oracleSqlForDetail, oracleSqlForDetail)};

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

        String  oracleSql = "select \"store\".\"store_state\" as \"c0\", " +
            "\"time_by_day\".\"the_year\" as \"c1\", " +
            "count(distinct \"sales_fact_1997\".\"customer_id\") as \"m0\", " +
            "grouping(\"store\".\"store_state\") as \"g0\" " +
            "from \"store\" \"store\", \"sales_fact_1997\" \"sales_fact_1997\", " +
            "\"time_by_day\" \"time_by_day\" " +
            "where \"sales_fact_1997\".\"store_id\" = \"store\".\"store_id\" " +
            "and \"store\".\"store_state\" in ('CA', 'OR') " +
            "and \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" " +
            "and \"time_by_day\".\"the_year\" = 1997 " +
            "group by grouping sets " +
            "((\"store\".\"store_state\",\"time_by_day\".\"the_year\")," +
            "(\"time_by_day\".\"the_year\"))";

        SqlPattern[] patterns =
            {new SqlPattern(SqlPattern.Dialect.ORACLE, oracleSql, oracleSql)};

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

        String  oracleSqlForDetail =
            "select \"store\".\"store_state\" as \"c0\", " +
            "\"time_by_day\".\"the_year\" as \"c1\", " +
            "count(distinct \"sales_fact_1997\".\"customer_id\") as \"m0\", " +
            "sum(\"sales_fact_1997\".\"unit_sales\") as \"m1\" " +
            "from \"store\" \"store\", \"sales_fact_1997\" \"sales_fact_1997\", " +
            "\"time_by_day\" \"time_by_day\" " +
            "where \"sales_fact_1997\".\"store_id\" = \"store\".\"store_id\" " +
            "and \"store\".\"store_state\" in ('CA', 'OR') " +
            "and \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" " +
            "and \"time_by_day\".\"the_year\" = 1997 " +
            "group by \"store\".\"store_state\", \"time_by_day\".\"the_year\"";
        String  oracleSqlForDistinctCountAgg =
            "select \"time_by_day\".\"the_year\" as \"c0\", " +
            "count(distinct \"sales_fact_1997\".\"customer_id\") as \"m0\" " +
            "from \"time_by_day\" \"time_by_day\", " +
            "\"sales_fact_1997\" \"sales_fact_1997\", \"store\" \"store\" " +
            "where \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" " +
            "and \"time_by_day\".\"the_year\" = 1997 " +
            "and \"sales_fact_1997\".\"store_id\" = \"store\".\"store_id\" " +
            "and \"store\".\"store_state\" in ('CA', 'OR') " +
            "group by \"time_by_day\".\"the_year\"";

        SqlPattern[] patterns =
            {new SqlPattern(SqlPattern.Dialect.ORACLE,
                oracleSqlForDetail, oracleSqlForDetail),
            new SqlPattern(SqlPattern.Dialect.ORACLE,
                oracleSqlForDistinctCountAgg, oracleSqlForDistinctCountAgg)};

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

        String  oracleSql = "select \"time_by_day\".\"the_year\" as \"c0\", " +
            "\"customer\".\"gender\" as \"c1\", " +
            "count(distinct \"sales_fact_1997\".\"customer_id\") as \"m0\" " +
            "from \"time_by_day\" \"time_by_day\", " +
            "\"sales_fact_1997\" \"sales_fact_1997\", \"customer\" \"customer\" " +
            "where \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" " +
            "and \"time_by_day\".\"the_year\" = 1997 " +
            "and \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\" " +
            "group by \"time_by_day\".\"the_year\", \"customer\".\"gender\"";

        SqlPattern[] patterns =
            {new SqlPattern(SqlPattern.Dialect.ORACLE, oracleSql, oracleSql)};

        assertQueryReturns(mdxQueryWithMembers, desiredResult);
        assertQuerySql(mdxQueryWithMembers, patterns);
        assertQueryReturns(mdxQueryWithDefaultMember, desiredResult);
        assertQuerySql(mdxQueryWithDefaultMember, patterns);
        props.EnableGroupingSets.set(originalGroupingSetsPropertyValue);
    }

    public void testRemoveOverlappingTuplesForSameDimension() {
        SchemaReader schemaReader = getSalesCubeSchemaReader();

        Member allMember = allMember("Gender");
        Member firstChildMember = child(schemaReader, allMember, "M");
        Member secondChildMember = child(schemaReader, allMember, "F");
        Member[] members = new Member[]{allMember, firstChildMember, secondChildMember};
        List<Member[]> memberList = new ArrayList();
        memberList.add(members);

        List filteredList =
            AggregateFunDef.AggregateCalc.removeOverlappingTupleEntries(memberList);
        assertEquals(1, filteredList.size());
        assertEquals(allMember.getUniqueName(), ((Member[]) filteredList.get(0))[0].getUniqueName());
    }

    public void testShouldConvertListOfMembersToTuples() {
        SchemaReader schemaReader = getSalesCubeSchemaReader();
        Member allMember = allMember("Gender");
        Member firstChildMember = child(schemaReader, allMember, "M");
        Member secondChildMember = child(schemaReader, allMember, "F");

        List<Member> memberList = new ArrayList<Member>();
        memberList.add(allMember);
        memberList.add(firstChildMember);
        memberList.add(secondChildMember);
        List<Member[]> tuples =
            AggregateFunDef.AggregateCalc.makeTupleList(memberList);

        assertEquals(3, tuples.size());
        assertEquals(allMember.getUniqueName(), tuples.get(0)[0].getUniqueName());
    }

    public void testMemberIsSuperSetOfAnotherMember() {
        SchemaReader schemaReader = getSalesCubeSchemaReader();
        Member allMember = allMember("Gender");
        Member firstChildMember = child(schemaReader, allMember, "M");
        Member secondChildMember = child(schemaReader, allMember, "F");

        List<Member> memberList = new ArrayList<Member>();
        memberList.add(allMember);
        memberList.add(firstChildMember);
        memberList.add(secondChildMember);
        List<Member[]> tuples =
            AggregateFunDef.AggregateCalc.makeTupleList(memberList);

        assertTrue(AggregateFunDef.AggregateCalc.isSuperSet(tuples.get(0), tuples.get(1)));
        assertFalse(AggregateFunDef.AggregateCalc.isSuperSet(tuples.get(1), tuples.get(2)));
    }

    public void testShouldRemoveOverlappingTuplesFromDifferentDimensions() {
        SchemaReader schemaReader = getSalesCubeSchemaReader();

        Member genderAllMember = allMember("Gender");
        Member genderMaleChild = child(schemaReader, genderAllMember, "M");
        Member genderFemaleChild = child(schemaReader, genderAllMember, "F");
        Member storeAllMember = allMember("Store");
        Member storeUsaChild = child(schemaReader, storeAllMember, "USA");
        Member storeCanadaChild = child(schemaReader, storeAllMember, "CANADA");
        Member[] genders = new Member[]{genderAllMember, genderMaleChild, genderFemaleChild};
        Member[] stores = new Member[]{storeAllMember, storeUsaChild, storeCanadaChild};
        List<Member[]> memberList = crossProduct(genders, stores);
        List filteredList =
            AggregateFunDef.AggregateCalc.removeOverlappingTupleEntries(memberList);
        assertEquals(1, filteredList.size());
        assertEquals(genderAllMember, ((Member[]) filteredList.get(0))[0]);
        assertEquals(storeAllMember, ((Member[]) filteredList.get(0))[1]);
    }

    public void testShouldRemoveOverlappingTuplesWithoutAllLevelTuple() {
        SchemaReader schemaReader = getSalesCubeSchemaReader();

        Member genderAllMember = allMember("Gender");
        Member genderMaleChild = child(schemaReader, genderAllMember, "M");
        Member genderFemaleChild = child(schemaReader, genderAllMember, "F");
        Member storeAllMember = allMember("Store");
        Member storeUsaChild = child(schemaReader, storeAllMember, "USA");
        Member storeCanadaChild = child(schemaReader, storeAllMember, "CANADA");
        Member[] genders = new Member[]{genderAllMember, genderMaleChild, genderFemaleChild};
        Member[] stores = new Member[]{storeUsaChild, storeCanadaChild};
        List<Member[]> memberList = crossProduct(genders, stores);
        memberList.add(new Member[]{genderMaleChild, storeAllMember});
        memberList.add(new Member[]{genderFemaleChild, storeAllMember});
        List filteredList =
            AggregateFunDef.AggregateCalc.removeOverlappingTupleEntries(memberList);
        assertEquals(4, filteredList.size());
    }


    public void testShouldNotRemoveNonOverlappingTuplesAtSameLevels() {
        SchemaReader schemaReader = getSalesCubeSchemaReader();

        Member genderAllMember = allMember("Gender");
        Member genderMaleChild = child(schemaReader, genderAllMember, "M");
        Member genderFemaleChild = child(schemaReader, genderAllMember, "F");
        Member storeAllMember = allMember("Store");
        Member storeUsaChild = child(schemaReader, storeAllMember, "USA");
        Member storeCanadaChild = child(schemaReader, storeAllMember, "CANADA");
        Member[] genders = new Member[]{genderMaleChild, genderFemaleChild};
        Member[] stores = new Member[]{storeUsaChild, storeCanadaChild};
        List<Member[]> memberList = crossProduct(genders, stores);
        List filteredList =
            AggregateFunDef.AggregateCalc.removeOverlappingTupleEntries(memberList);
        assertEquals(4, filteredList.size());
    }

    public void testShouldNotRemoveNonOverlappingTuplesAtDifferentLevels() {
        SchemaReader schemaReader = getSalesCubeSchemaReader();

        Member genderAllMember = allMember("Gender");
        Member genderMaleChild = child(schemaReader, genderAllMember, "M");
        Member storeAllMember = allMember("Store");
        Member storeUsaChild = child(schemaReader, storeAllMember, "USA");
        List<Member[]> memberList = new ArrayList<Member[]>();
        memberList.add(new Member[]{genderMaleChild, storeAllMember});
        memberList.add(new Member[]{genderAllMember,storeUsaChild});
        List filteredList =
            AggregateFunDef.AggregateCalc.removeOverlappingTupleEntries(memberList);
        assertEquals(2, filteredList.size());
    }

    public void testShouldRemoveDuplicateTuples() {
        SchemaReader schemaReader = getSalesCubeSchemaReader();
        Member allMember = allMember("Gender");
        Member firstChildMember = child(schemaReader, allMember, "M");
        Member secondChildMember = child(schemaReader, allMember, "F");

        List<Member> memberList = new ArrayList<Member>();
        memberList.add(firstChildMember);
        memberList.add(firstChildMember);
        memberList.add(secondChildMember);
        List<Member[]> tuples =
            AggregateFunDef.AggregateCalc.makeTupleList(memberList);

       List filteredList =
            AggregateFunDef.AggregateCalc.removeOverlappingTupleEntries(tuples);
        assertEquals(2, filteredList.size());
    }

    private List<Member[]> crossProduct(Member[] genders, Member[] stores) {
        List<Member[]> tuples = new ArrayList<Member[]>();
        for (Member gender : genders) {
            for (Member store : stores) {
                tuples.add(new Member[]{gender, store});
            }
        }
        return tuples;
    }

    private Member allMember(String dimensionName) {
        Dimension genderDimension = getDimension(dimensionName);
        Member allMember = genderDimension.getHierarchy().getAllMember();
        return allMember;
    }

    private Member child(SchemaReader schemaReader, Member allMember, String name) {
        Member secondChildMember =
            schemaReader.lookupMemberChildByName(
                allMember,
                new Id.Segment(name, Id.Quoting.UNQUOTED));
        return secondChildMember;
    }

    private Dimension getDimension(String dimensionName) {
        return getDimensionWithName(dimensionName, getCubeWithName(
            "Sales", getSchemaReader().getCubes()).getDimensions());
    }

    private SchemaReader getSalesCubeSchemaReader() {
        SchemaReader schemaReader =
            getCubeWithName("Sales", getSchemaReader().getCubes()).
                getSchemaReader(getTestContext().getConnection().getRole());
        return schemaReader;
    }

    private SchemaReader getSchemaReader() {
        SchemaReader reader = getTestContext().getConnection().getSchemaReader();
        return reader;
    }

    private Cube getCubeWithName(String cubeName, Cube[] cubes) {
        Cube resultCube = null;
        for (Cube cube : cubes) {
            if (cubeName.equals(cube.getName())) {
                resultCube = cube;
                break;
            }
        }
        return resultCube;
    }

    private Dimension getDimensionWithName(String name, Dimension[] dimensions) {
        Dimension resultDimension = null;
        for (Dimension dimension : dimensions) {
            if (dimension.getName().equals(name)) {
                resultDimension = dimension;
                break;
            }
        }
        return resultDimension;
    }
}
