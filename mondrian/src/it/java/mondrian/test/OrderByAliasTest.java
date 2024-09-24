/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (c) 2002-2017 Hitachi Vantara..  All rights reserved.
*/
package mondrian.test;

import mondrian.olap.MondrianProperties;
import mondrian.rolap.BatchTestCase;
import mondrian.spi.Dialect;

/**
 * Test cases to verify requiresOrderByAlias()=true for the MySQL 5.7+
 *
 * MySQL 5.7+ sets SQL_MODE=ONLY_FULL_GROUP_BY, which makes it more strict
 * about SQL with fields in the SELECT and ORDER BY clauses that aren't present
 * in the GROUP BY. See Jira-case for background:
 * http://jira.pentaho.com/browse/MONDRIAN-2451
 *
 * @author Aleksandr Kozlov
 */
public class OrderByAliasTest extends BatchTestCase {

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    propSaver.set(MondrianProperties.instance().GenerateFormattedSql, true);
  }

  @Override
  protected void tearDown() throws Exception {
    propSaver.reset();
    super.tearDown();
  }

  public void testSqlInKeyExpression() {
    if (TestContext.instance().getDialect().getDatabaseProduct()
        != Dialect.DatabaseProduct.MYSQL
        || !TestContext.instance().getDialect().requiresOrderByAlias())
    {
      return; // For MySQL 5.7+ only!
    }
    final String colName = TestContext.instance().getDialect()
        .quoteIdentifier("promotion_name");
    TestContext context = TestContext.instance().createSubstitutingCube(
        "Sales",
        "<Dimension name=\"Promotions\" foreignKey=\"promotion_id\">\n"
        + "  <Hierarchy hasAll=\"true\" allMemberName=\"All Promotions\" primaryKey=\"promotion_id\" defaultMember=\"[All Promotions]\">\n"
        + "    <Table name=\"promotion\"/>\n"
        + "    <Level name=\"Promotion Name\" column=\"promotion_name\" uniqueMembers=\"true\">\n"
        + "      <KeyExpression><SQL>RTRIM("
        + colName + ")</SQL></KeyExpression>\n"
        + "    </Level>\n"
        + "  </Hierarchy>\n"
        + "</Dimension>");
    assertQuerySql(
        context,
        "select non empty{[Promotions].[All Promotions].Children} ON rows, "
        + "non empty {[Store].[All Stores]} ON columns "
        + "from [Sales] "
        + "where {[Measures].[Unit Sales]}",
        mysqlPattern(
            "select\n"
            + "    RTRIM(\"promotion_name\") as \"c0\"\n"
            + "from\n"
            + "    \"promotion\" as \"promotion\",\n"
            + "    \"sales_fact_1997\" as \"sales_fact_1997\"\n"
            + "where\n"
            + "    \"sales_fact_1997\".\"promotion_id\" = \"promotion\".\"promotion_id\"\n"
            + "group by\n"
            + "    RTRIM(\"promotion_name\")\n"
            + "order by\n"
            + "    ISNULL(\"c0\") ASC, \"c0\" ASC"));
  }

  public void testSqlInNameExpression() {
    if (TestContext.instance().getDialect().getDatabaseProduct()
        != Dialect.DatabaseProduct.MYSQL
        || !TestContext.instance().getDialect().requiresOrderByAlias())
    {
      return; // For MySQL 5.7+ only!
    }
    final String colName = TestContext.instance().getDialect()
        .quoteIdentifier("promotion_name");
    TestContext context = TestContext.instance().createSubstitutingCube(
        "Sales",
        "<Dimension name=\"Promotions\" foreignKey=\"promotion_id\">\n"
        + "  <Hierarchy hasAll=\"true\" allMemberName=\"All Promotions\" primaryKey=\"promotion_id\" defaultMember=\"[All Promotions]\">\n"
        + "    <Table name=\"promotion\"/>\n"
        + "    <Level name=\"Promotion Name\" column=\"promotion_name\" uniqueMembers=\"true\">\n"
        + "      <NameExpression><SQL>RTRIM("
        + colName + ")</SQL></NameExpression>\n"
        + "    </Level>\n"
        + "  </Hierarchy>\n"
        + "</Dimension>");
    assertQuerySql(
        context,
        "select non empty{[Promotions].[All Promotions].Children} ON rows, "
        + "non empty {[Store].[All Stores]} ON columns "
        + "from [Sales] "
        + "where {[Measures].[Unit Sales]}",
        mysqlPattern(
            "select\n"
            + "    \"promotion\".\"promotion_name\" as \"c0\",\n"
            + "    RTRIM(\"promotion_name\") as \"c1\"\n"
            + "from\n"
            + "    \"promotion\" as \"promotion\",\n"
            + "    \"sales_fact_1997\" as \"sales_fact_1997\"\n"
            + "where\n"
            + "    \"sales_fact_1997\".\"promotion_id\" = \"promotion\".\"promotion_id\"\n"
            + "group by\n"
            + "    \"promotion\".\"promotion_name\",\n"
            + "    RTRIM(\"promotion_name\")\n"
            + "order by\n"
            + "    ISNULL(\"c0\") ASC, \"c0\" ASC"));
  }

  public void testSqlInCaptionExpression() {
    if (TestContext.instance().getDialect().getDatabaseProduct()
        != Dialect.DatabaseProduct.MYSQL
        || !TestContext.instance().getDialect().requiresOrderByAlias())
    {
      return; // For MySQL 5.7+ only!
    }
    final String colName = TestContext.instance().getDialect()
        .quoteIdentifier("promotion_name");
    TestContext context = TestContext.instance().createSubstitutingCube(
        "Sales",
        "<Dimension name=\"Promotions\" foreignKey=\"promotion_id\">\n"
        + "  <Hierarchy hasAll=\"true\" allMemberName=\"All Promotions\" primaryKey=\"promotion_id\" defaultMember=\"[All Promotions]\">\n"
        + "    <Table name=\"promotion\"/>\n"
        + "    <Level name=\"Promotion Name\" column=\"promotion_name\" uniqueMembers=\"true\">\n"
        + "      <CaptionExpression><SQL>RTRIM("
        + colName + ")</SQL></CaptionExpression>\n"
        + "    </Level>\n"
        + "  </Hierarchy>\n"
        + "</Dimension>");
    assertQuerySql(
        context,
        "select non empty{[Promotions].[All Promotions].Children} ON rows, "
        + "non empty {[Store].[All Stores]} ON columns "
        + "from [Sales] "
        + "where {[Measures].[Unit Sales]}",
        mysqlPattern(
            "select\n"
            + "    \"promotion\".\"promotion_name\" as \"c0\",\n"
            + "    RTRIM(\"promotion_name\") as \"c1\"\n"
            + "from\n"
            + "    \"promotion\" as \"promotion\",\n"
            + "    \"sales_fact_1997\" as \"sales_fact_1997\"\n"
            + "where\n"
            + "    \"sales_fact_1997\".\"promotion_id\" = \"promotion\".\"promotion_id\"\n"
            + "group by\n"
            + "    \"promotion\".\"promotion_name\",\n"
            + "    RTRIM(\"promotion_name\")\n"
            + "order by\n"
            + "    ISNULL(\"c0\") ASC, \"c0\" ASC"));
  }

  public void testSqlInOrdinalExpression() {
    if (TestContext.instance().getDialect().getDatabaseProduct()
        != Dialect.DatabaseProduct.MYSQL
        || !TestContext.instance().getDialect().requiresOrderByAlias())
    {
      return; // For MySQL 5.7+ only!
    }
    final String colName = TestContext.instance().getDialect()
        .quoteIdentifier("promotion_name");
    TestContext context = TestContext.instance().createSubstitutingCube(
        "Sales",
        "<Dimension name=\"Promotions\" foreignKey=\"promotion_id\">\n"
        + "  <Hierarchy hasAll=\"true\" allMemberName=\"All Promotions\" primaryKey=\"promotion_id\" defaultMember=\"[All Promotions]\">\n"
        + "    <Table name=\"promotion\"/>\n"
        + "    <Level name=\"Promotion Name\" column=\"promotion_name\" uniqueMembers=\"true\">\n"
        + "      <OrdinalExpression><SQL>RTRIM("
        + colName + ")</SQL></OrdinalExpression>\n"
        + "    </Level>\n"
        + "  </Hierarchy>\n"
        + "</Dimension>");
    assertQuerySql(
        context,
        "select non empty{[Promotions].[All Promotions].Children} ON rows, "
        + "non empty {[Store].[All Stores]} ON columns "
        + "from [Sales] "
        + "where {[Measures].[Unit Sales]}",
        mysqlPattern(
            "select\n"
            + "    \"promotion\".\"promotion_name\" as \"c0\",\n"
            + "    RTRIM(\"promotion_name\") as \"c1\"\n"
            + "from\n"
            + "    \"promotion\" as \"promotion\",\n"
            + "    \"sales_fact_1997\" as \"sales_fact_1997\"\n"
            + "where\n"
            + "    \"sales_fact_1997\".\"promotion_id\" = \"promotion\".\"promotion_id\"\n"
            + "group by\n"
            + "    \"promotion\".\"promotion_name\",\n"
            + "    RTRIM(\"promotion_name\")\n"
            + "order by\n"
            + "    ISNULL(\"c1\") ASC, \"c1\" ASC"));
  }

  public void testSqlInParentExpression() {
    if (TestContext.instance().getDialect().getDatabaseProduct()
        != Dialect.DatabaseProduct.MYSQL
        || !TestContext.instance().getDialect().requiresOrderByAlias())
    {
      return; // For MySQL 5.7+ only!
    }
    final String colName = TestContext.instance().getDialect()
        .quoteIdentifier("supervisor_id");
    TestContext context = TestContext.instance().createSubstitutingCube(
        "HR",
        "<Dimension name=\"Employees\" foreignKey=\"employee_id\">\n"
        + "  <Hierarchy hasAll=\"true\" allMemberName=\"All Employees\"\n"
        + "      primaryKey=\"employee_id\">\n"
        + "    <Table name=\"employee\"/>\n"
        + "    <Level name=\"Employee Id\" type=\"Numeric\" uniqueMembers=\"true\"\n"
        + "        column=\"employee_id\" parentColumn=\"supervisor_id\"\n"
        + "        nameColumn=\"full_name\" nullParentValue=\"0\">\n"
        + "      <ParentExpression><SQL>RTRIM("
        + colName + ")</SQL></ParentExpression>\n"
        + "      <Closure parentColumn=\"supervisor_id\" childColumn=\"employee_id\">\n"
        + "        <Table name=\"employee_closure\"/>\n"
        + "      </Closure>\n"
        + "      <Property name=\"Marital Status\" column=\"marital_status\"/>\n"
        + "      <Property name=\"Position Title\" column=\"position_title\"/>\n"
        + "      <Property name=\"Gender\" column=\"gender\"/>\n"
        + "      <Property name=\"Salary\" column=\"salary\"/>\n"
        + "      <Property name=\"Education Level\" column=\"education_level\"/>\n"
        + "      <Property name=\"Management Role\" column=\"management_role\"/>\n"
        + "    </Level>\n"
        + "  </Hierarchy>\n"
        + "</Dimension>");
    assertQuerySql(
        context,
        "select non empty{[Employees].[All Employees].Children} ON rows, "
        + "non empty {[Store].[All Stores]} ON columns "
        + "from [HR] "
        + "where {[Measures].[Avg Salary]}",
        mysqlPattern(
            "select\n"
            + "    \"employee\".\"employee_id\" as \"c0\",\n"
            + "    \"employee\".\"full_name\" as \"c1\",\n"
            + "    \"employee\".\"marital_status\" as \"c2\",\n"
            + "    \"employee\".\"position_title\" as \"c3\",\n"
            + "    \"employee\".\"gender\" as \"c4\",\n"
            + "    \"employee\".\"salary\" as \"c5\",\n"
            + "    \"employee\".\"education_level\" as \"c6\",\n"
            + "    \"employee\".\"management_role\" as \"c7\"\n"
            + "from\n"
            + "    \"employee\" as \"employee\"\n"
            + "where\n"
            + "    RTRIM(\"supervisor_id\") = 0\n"
            + "group by\n"
            + "    \"employee\".\"employee_id\",\n"
            + "    \"employee\".\"full_name\",\n"
            + "    \"employee\".\"marital_status\",\n"
            + "    \"employee\".\"position_title\",\n"
            + "    \"employee\".\"gender\",\n"
            + "    \"employee\".\"salary\",\n"
            + "    \"employee\".\"education_level\",\n"
            + "    \"employee\".\"management_role\"\n"
            + "order by\n"
            + "    ISNULL(\"c0\") ASC, \"c0\" ASC"));
  }

  public void testSqlInPropertyExpression() {
    if (TestContext.instance().getDialect().getDatabaseProduct()
        != Dialect.DatabaseProduct.MYSQL
        || !TestContext.instance().getDialect().requiresOrderByAlias())
    {
      return; // For MySQL 5.7+ only!
    }
    final String colName = TestContext.instance().getDialect()
        .quoteIdentifier("promotion_name");
    TestContext context = TestContext.instance().createSubstitutingCube(
        "Sales",
        "<Dimension name=\"Promotions\" foreignKey=\"promotion_id\">\n"
        + "  <Hierarchy hasAll=\"true\" allMemberName=\"All Promotions\" primaryKey=\"promotion_id\" defaultMember=\"[All Promotions]\">\n"
        + "    <Table name=\"promotion\"/>\n"
        + "    <Level name=\"Promotion Name\" column=\"promotion_name\" uniqueMembers=\"true\">\n"
        + "      <PropertyExpression name=\"Rtrim Name\"><SQL>RTRIM("
        + colName + ")</SQL></PropertyExpression>\n"
        + "    </Level>\n"
        + "  </Hierarchy>\n"
        + "</Dimension>");
    assertQuerySql(
        context,
        "select non empty{[Promotions].[All Promotions].Children} ON rows, "
        + "non empty {[Store].[All Stores]} ON columns "
        + "from [Sales] "
        + "where {[Measures].[Unit Sales]}",
        mysqlPattern(
            "select\n"
            + "    \"promotion\".\"promotion_name\" as \"c0\"\n"
            + "from\n"
            + "    \"promotion\" as \"promotion\",\n"
            + "    \"sales_fact_1997\" as \"sales_fact_1997\"\n"
            + "where\n"
            + "    \"sales_fact_1997\".\"promotion_id\" = \"promotion\".\"promotion_id\"\n"
            + "group by\n"
            + "    \"promotion\".\"promotion_name\"\n"
            + "order by\n"
            + "    ISNULL(\"c0\") ASC, \"c0\" ASC"));
  }

  public void testSqlInMeasureExpression() {
    if (TestContext.instance().getDialect().getDatabaseProduct()
        != Dialect.DatabaseProduct.MYSQL
        || !TestContext.instance().getDialect().requiresOrderByAlias())
    {
      return; // For MySQL 5.7+ only!
    }
    final String colName = TestContext.instance().getDialect()
        .quoteIdentifier("promotion_name");
    TestContext context = TestContext.instance().createSubstitutingCube(
        "Sales",
        "<Dimension name=\"Promotions\" foreignKey=\"promotion_id\">\n"
        + "  <Hierarchy hasAll=\"true\" allMemberName=\"All Promotions\" primaryKey=\"promotion_id\" defaultMember=\"[All Promotions]\">\n"
        + "    <Table name=\"promotion\"/>\n"
        + "    <Level name=\"Promotion Name\" column=\"promotion_name\" uniqueMembers=\"true\">\n"
        + "      <MeasureExpression><SQL>RTRIM("
        + colName + ")</SQL></MeasureExpression>\n"
        + "    </Level>\n"
        + "  </Hierarchy>\n"
        + "</Dimension>");
    assertQuerySql(
        context,
        "select non empty{[Promotions].[All Promotions].Children} ON rows, "
        + "non empty {[Store].[All Stores]} ON columns "
        + "from [Sales] "
        + "where {[Measures].[Unit Sales]}",
        mysqlPattern(
            "select\n"
            + "    \"promotion\".\"promotion_name\" as \"c0\"\n"
            + "from\n"
            + "    \"promotion\" as \"promotion\",\n"
            + "    \"sales_fact_1997\" as \"sales_fact_1997\"\n"
            + "where\n"
            + "    \"sales_fact_1997\".\"promotion_id\" = \"promotion\".\"promotion_id\"\n"
            + "group by\n"
            + "    \"promotion\".\"promotion_name\"\n"
            + "order by\n"
            + "    ISNULL(\"c0\") ASC, \"c0\" ASC"));
  }

  public void testNonEmptyCrossJoin() {
    if (TestContext.instance().getDialect().getDatabaseProduct()
        != Dialect.DatabaseProduct.MYSQL
        || !TestContext.instance().getDialect().requiresOrderByAlias())
    {
      return; // For MySQL 5.7+ only!
    }
    TestContext context = TestContext.instance();
    assertQuerySql(
        context,
        "with set necj as\n"
        + "NonEmptyCrossJoin([Customers].[Name].members,[Store].[Store Name].members)\n"
        + "select\n"
        + "{[Measures].[Unit Sales]} on columns,\n"
        + "Tail(hierarchize(necj),5) on rows\n"
        + "from sales",
        mysqlPattern(
            "select\n"
            + "    \"customer\".\"country\" as \"c0\",\n"
            + "    \"customer\".\"state_province\" as \"c1\",\n"
            + "    \"customer\".\"city\" as \"c2\",\n"
            + "    \"customer\".\"customer_id\" as \"c3\",\n"
            + "    CONCAT(\"customer\".\"fname\", \" \", \"customer\".\"lname\") as \"c4\",\n"
            + "    CONCAT(\"customer\".\"fname\", \" \", \"customer\".\"lname\") as \"c5\",\n"
            + "    \"customer\".\"gender\" as \"c6\",\n"
            + "    \"customer\".\"marital_status\" as \"c7\",\n"
            + "    \"customer\".\"education\" as \"c8\",\n"
            + "    \"customer\".\"yearly_income\" as \"c9\",\n"
            + "    \"store\".\"store_country\" as \"c10\",\n"
            + "    \"store\".\"store_state\" as \"c11\",\n"
            + "    \"store\".\"store_city\" as \"c12\",\n"
            + "    \"store\".\"store_name\" as \"c13\",\n"
            + "    \"store\".\"store_type\" as \"c14\",\n"
            + "    \"store\".\"store_manager\" as \"c15\",\n"
            + "    \"store\".\"store_sqft\" as \"c16\",\n"
            + "    \"store\".\"grocery_sqft\" as \"c17\",\n"
            + "    \"store\".\"frozen_sqft\" as \"c18\",\n"
            + "    \"store\".\"meat_sqft\" as \"c19\",\n"
            + "    \"store\".\"coffee_bar\" as \"c20\",\n"
            + "    \"store\".\"store_street_address\" as \"c21\"\n"
            + "from\n"
            + "    \"customer\" as \"customer\",\n"
            + "    \"sales_fact_1997\" as \"sales_fact_1997\",\n"
            + "    \"store\" as \"store\"\n"
            + "where\n"
            + "    \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\"\n"
            + "and\n"
            + "    \"sales_fact_1997\".\"store_id\" = \"store\".\"store_id\"\n"
            + "group by\n"
            + "    \"customer\".\"country\",\n"
            + "    \"customer\".\"state_province\",\n"
            + "    \"customer\".\"city\",\n"
            + "    \"customer\".\"customer_id\",\n"
            + "    CONCAT(\"customer\".\"fname\", \" \", \"customer\".\"lname\"),\n"
            + "    \"customer\".\"gender\",\n"
            + "    \"customer\".\"marital_status\",\n"
            + "    \"customer\".\"education\",\n"
            + "    \"customer\".\"yearly_income\",\n"
            + "    \"store\".\"store_country\",\n"
            + "    \"store\".\"store_state\",\n"
            + "    \"store\".\"store_city\",\n"
            + "    \"store\".\"store_name\",\n"
            + "    \"store\".\"store_type\",\n"
            + "    \"store\".\"store_manager\",\n"
            + "    \"store\".\"store_sqft\",\n"
            + "    \"store\".\"grocery_sqft\",\n"
            + "    \"store\".\"frozen_sqft\",\n"
            + "    \"store\".\"meat_sqft\",\n"
            + "    \"store\".\"coffee_bar\",\n"
            + "    \"store\".\"store_street_address\"\n"
            + "order by\n"
            + "    ISNULL(\"c0\") ASC, \"c0\" ASC,\n"
            + "    ISNULL(\"c1\") ASC, \"c1\" ASC,\n"
            + "    ISNULL(\"c2\") ASC, \"c2\" ASC,\n"
            + "    ISNULL(\"c4\") ASC, \"c4\" ASC,\n"
            + "    ISNULL(\"c10\") ASC, \"c10\" ASC,\n"
            + "    ISNULL(\"c11\") ASC, \"c11\" ASC,\n"
            + "    ISNULL(\"c12\") ASC, \"c12\" ASC,\n"
            + "    ISNULL(\"c13\") ASC, \"c13\" ASC"));
  }

  public void testVirtualCube() {
    if (TestContext.instance().getDialect().getDatabaseProduct()
        != Dialect.DatabaseProduct.MYSQL
        || !TestContext.instance().getDialect().requiresOrderByAlias())
    {
      return; // For MySQL 5.7+ only!
    }
    TestContext context = TestContext.instance().withSchema(
        "<?xml version=\"1.0\"?>\n"
        + "<Schema name=\"FoodMart\">\n"
        + "  <Dimension name=\"Time\" type=\"TimeDimension\">\n"
        + "    <Hierarchy hasAll=\"false\" primaryKey=\"time_id\">\n"
        + "      <Table name=\"time_by_day\" />\n"
        + "      <Level name=\"Year\" column=\"the_year\" type=\"Numeric\" uniqueMembers=\"true\" "
        + "levelType=\"TimeYears\" />\n"
        + "      <Level name=\"Quarter\" uniqueMembers=\"false\" levelType=\"TimeQuarters\" >\n"
        + "        <KeyExpression><SQL>RTRIM(quarter)</SQL></KeyExpression>\n"
        + "      </Level>\n"
        + "    </Hierarchy>\n"
        + "  </Dimension>\n"
        + "  <Dimension name=\"Product\">\n"
        + "    <Hierarchy hasAll=\"true\" primaryKey=\"product_id\" primaryKeyTable=\"product\">\n"
        + "      <Join leftKey=\"product_class_id\" rightKey=\"product_class_id\">\n"
        + "        <Table name=\"product\"/>\n"
        + "        <Table name=\"product_class\"/>\n"
        + "      </Join>\n"
        + "      <Level name=\"Product Family\" table=\"product_class\" column=\"product_family\" "
        + "uniqueMembers=\"true\" />\n"
        + "    </Hierarchy>\n"
        + "  </Dimension>\n"
        + "  <Cube name=\"Sales\">\n"
        + "    <Table name=\"sales_fact_1997\" />\n"
        + "    <DimensionUsage name=\"Time\" source=\"Time\" foreignKey=\"time_id\" />\n"
        + "    <DimensionUsage name=\"Product\" source=\"Product\" foreignKey=\"product_id\" />\n"
        + "    <Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\" formatString=\"Standard\" />\n"
        + "  </Cube>\n"
        + "  <Cube name=\"Warehouse\">\n"
        + "    <Table name=\"inventory_fact_1997\" />\n"
        + "    <DimensionUsage name=\"Time\" source=\"Time\" foreignKey=\"time_id\" />\n"
        + "    <DimensionUsage name=\"Product\" source=\"Product\" foreignKey=\"product_id\" />\n"
        + "    <Measure name=\"Warehouse Sales\" column=\"warehouse_sales\" aggregator=\"sum\" "
        + "formatString=\"Standard\" />\n"
        + "  </Cube>\n"
        + "  <VirtualCube name=\"Warehouse and Sales\">\n"
        + "    <VirtualCubeDimension name=\"Time\" />\n"
        + "    <VirtualCubeDimension name=\"Product\" />\n"
        + "    <VirtualCubeMeasure cubeName=\"Sales\" name=\"[Measures].[Unit Sales]\" />\n"
        + "    <VirtualCubeMeasure cubeName=\"Warehouse\" name=\"[Measures].[Warehouse Sales]\" />\n"
        + "  </VirtualCube>\n"
        + "</Schema>");
    assertQuerySql(
        context,
        "select non empty crossjoin( product.[product family].members, time.quarter.members) on 0 "
        + "from [warehouse and sales]",
        mysqlPattern(
            "select\n"
            + "    *\n"
            + "from\n"
            + "    (select\n"
            + "    `product_class`.`product_family` as `c0`,\n"
            + "    `time_by_day`.`the_year` as `c1`,\n"
            + "    RTRIM(quarter) as `c2`\n"
            + "from\n"
            + "    `product` as `product`,\n"
            + "    `product_class` as `product_class`,\n"
            + "    `sales_fact_1997` as `sales_fact_1997`,\n"
            + "    `time_by_day` as `time_by_day`\n"
            + "where\n"
            + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
            + "and\n"
            + "    `sales_fact_1997`.`product_id` = `product`.`product_id`\n"
            + "and\n"
            + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
            + "group by\n"
            + "    `product_class`.`product_family`,\n"
            + "    `time_by_day`.`the_year`,\n"
            + "    RTRIM(quarter)\n"
            + "union\n"
            + "select\n"
            + "    `product_class`.`product_family` as `c0`,\n"
            + "    `time_by_day`.`the_year` as `c1`,\n"
            + "    RTRIM(quarter) as `c2`\n"
            + "from\n"
            + "    `product` as `product`,\n"
            + "    `product_class` as `product_class`,\n"
            + "    `inventory_fact_1997` as `inventory_fact_1997`,\n"
            + "    `time_by_day` as `time_by_day`\n"
            + "where\n"
            + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
            + "and\n"
            + "    `inventory_fact_1997`.`product_id` = `product`.`product_id`\n"
            + "and\n"
            + "    `inventory_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
            + "group by\n"
            + "    `product_class`.`product_family`,\n"
            + "    `time_by_day`.`the_year`,\n"
            + "    RTRIM(quarter)) as `unionQuery`\n"
            + "order by\n"
            + "    ISNULL(1) ASC, 1 ASC,\n"
            + "    ISNULL(2) ASC, 2 ASC,\n"
            + "    ISNULL(3) ASC, 3 ASC"));
  }

}
// End OrderByAliasTest.java