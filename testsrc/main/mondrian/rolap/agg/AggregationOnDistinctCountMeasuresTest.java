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

import mondrian.rolap.BatchTestCase;
import mondrian.test.SqlPattern;
import mondrian.test.TestContext;

/**
 * <code>AggregationOnDistinctCountMeasureTest</code> tests the
 * Distinct Count functionality with tuples and members.
 *
 * @author ajogleka
 * @version $Id$
 * @since 19 December, 2007
 */
public class AggregationOnDistinctCountMeasuresTest extends BatchTestCase {

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
            "\"customer\" as \"customer\", \"store\" as \"store\" " +
            "where \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" and \"time_by_day\".\"the_year\" = 1997 and " +
            "\"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\" and \"sales_fact_1997\".\"store_id\" = \"store\".\"store_id\" and " +
            "((\"customer\".\"gender\" = 'F' and \"store\".\"store_state\" = 'CA' and \"store\".\"store_country\" = 'USA') or " +
            "(\"customer\".\"gender\" = 'M' and \"store\".\"store_state\" = 'CA' and \"store\".\"store_country\" = 'USA')) " +
            "group by \"time_by_day\".\"the_year\"";
        
        String mysqlSql =
            "select `time_by_day`.`the_year` as `c0`, " +
            "count(distinct `sales_fact_1997`.`customer_id`) as `m0` " +
            "from `time_by_day` as `time_by_day`, `sales_fact_1997` as `sales_fact_1997`, `customer` as `customer`, `store` as `store` " +
            "where `sales_fact_1997`.`time_id` = `time_by_day`.`time_id` and `time_by_day`.`the_year` = 1997 and " +
            "`sales_fact_1997`.`customer_id` = `customer`.`customer_id` and `sales_fact_1997`.`store_id` = `store`.`store_id` and " +
            "(((`store`.`store_country`, `store`.`store_state`, `customer`.`gender`) in (('USA', 'CA', 'F'), ('USA', 'CA', 'M')))) " +
            "group by `time_by_day`.`the_year`";        
        
        SqlPattern[] patterns = {
            new SqlPattern(SqlPattern.Dialect.DERBY, derbySql, derbySql),
            new SqlPattern(SqlPattern.Dialect.MYSQL, mysqlSql, mysqlSql)};

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
            "where \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" and \"time_by_day\".\"the_year\" = 1997 and " +
            "\"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\" and \"sales_fact_1997\".\"store_id\" = \"store\".\"store_id\" and " +
            "(((\"customer\".\"gender\" = 'F' and \"store\".\"store_state\" = 'CA' and \"store\".\"store_country\" = 'USA') or " +
            "(\"customer\".\"gender\" = 'M' and \"store\".\"store_state\" = 'CA' and \"store\".\"store_country\" = 'USA')) or " +
            "((\"customer\".\"gender\" = 'F' and \"store\".\"store_country\" = 'Canada') or " +
            "(\"customer\".\"gender\" = 'M' and \"store\".\"store_country\" = 'Canada'))) " +
            "group by \"time_by_day\".\"the_year\"";
        
        String mysqlSql =
            "select `time_by_day`.`the_year` as `c0`, " +
            "count(distinct `sales_fact_1997`.`customer_id`) as `m0` " +
            "from `time_by_day` as `time_by_day`, `sales_fact_1997` as `sales_fact_1997`, " +
            "`customer` as `customer`, `store` as `store` " +
            "where `sales_fact_1997`.`time_id` = `time_by_day`.`time_id` and `time_by_day`.`the_year` = 1997 and " +
            "`sales_fact_1997`.`customer_id` = `customer`.`customer_id` and `sales_fact_1997`.`store_id` = `store`.`store_id` and " +
            "((((`store`.`store_country`, `store`.`store_state`, `customer`.`gender`) in (('USA', 'CA', 'F'), ('USA', 'CA', 'M')))) or " +
            "(((`store`.`store_country`, `customer`.`gender`) in (('Canada', 'F'), ('Canada', 'M'))))) " +
            "group by `time_by_day`.`the_year`";
        
        SqlPattern[] patterns = {
            new SqlPattern(SqlPattern.Dialect.DERBY, derbySql, derbySql),
            new SqlPattern(SqlPattern.Dialect.MYSQL, mysqlSql, mysqlSql)};

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
            "from \"warehouse\" as \"warehouse\", \"inventory_fact_1997\" as \"inventory_fact_1997\" " +
            "where \"inventory_fact_1997\".\"warehouse_id\" = \"warehouse\".\"warehouse_id\" and " +
            "((\"warehouse\".\"warehouse_name\" = 'Arnold and Sons' " +
            "and \"warehouse\".\"wa_address1\" = '5617 Saclan Terrace' " +
            "and \"warehouse\".\"wa_address2\" is null " +
            "and \"warehouse\".\"wa_address3\" is null) " +
            "or (\"warehouse\".\"warehouse_name\" = 'Jones International' " +
            "and \"warehouse\".\"wa_address1\" = '3377 Coachman Place' " +
            "and \"warehouse\".\"wa_address2\" is null " +
            "and \"warehouse\".\"wa_address3\" is null))";
        
        String necjSqlMySql =
            "select count(distinct `inventory_fact_1997`.`warehouse_cost`) as `m0` " +
            "from `warehouse` as `warehouse`, `inventory_fact_1997` as `inventory_fact_1997` " +
            "where `inventory_fact_1997`.`warehouse_id` = `warehouse`.`warehouse_id` and " +
            "((`warehouse`.`wa_address3` is null and `warehouse`.`wa_address2` is null and " +
            "(`warehouse`.`wa_address1`, `warehouse`.`warehouse_name`) in " +
            "(('5617 Saclan Terrace', 'Arnold and Sons'), ('3377 Coachman Place', 'Jones International'))))";
        
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
}
