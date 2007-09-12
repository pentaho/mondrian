/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2004-2007 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap.sql;

import mondrian.test.SqlPattern;
import mondrian.test.TestContext;
import mondrian.olap.MondrianProperties;
import mondrian.olap.Util;
import mondrian.rolap.BatchTestCase;

import java.util.ArrayList;

/**
 * <p>Test for <code>SqlQuery</code></p>
 *
 * @author Thiyagu
 * @version $Id$
 * @since 06-Jun-2007
 */
public class SqlQueryTest extends BatchTestCase {


    public void testToStringForSingleGroupingSetSql() {
        if (isGroupingSetsSupported()) {
            MondrianProperties.instance().GenerateFormattedSql.set(true);
            SqlQuery sqlQuery = new SqlQuery(getTestContext().getDialect());
            sqlQuery.addSelect("c1");
            sqlQuery.addSelect("c2");
            sqlQuery.addGroupingFunction("gf0");
            sqlQuery.addFromTable("s", "t1", "t1alias", null, true);
            sqlQuery.addWhere("a=b");
            ArrayList<String> groupingsetsList = new ArrayList<String>();
            groupingsetsList.add("gs1");
            groupingsetsList.add("gs2");
            groupingsetsList.add("gs3");
            sqlQuery.addGroupingSet(groupingsetsList);
            MondrianProperties.instance().GenerateFormattedSql.set(false);
            assertEquals(
                "select c1 as \"c0\", c2 as \"c1\", grouping(gf0) as \"g0\" " +
                    "from \"s\".\"t1\" \"t1alias\" where a=b group by grouping sets ((gs1,gs2,gs3))",
                sqlQuery.toString());
            MondrianProperties.instance().GenerateFormattedSql.set(true);
            String expectedString = "select " + Util.nl +
                "    c1 as \"c0\", " + Util.nl +
                "    c2 as \"c1\"" + Util.nl +
                "    , grouping(gf0) as \"g0\"" + Util.nl +
                "from " + Util.nl +
                "    \"s\".\"t1\" \"t1alias\"" + Util.nl +
                "where " + Util.nl +
                "    a=b" + Util.nl +
                " group by grouping sets ((" + Util.nl +
                "    gs1," + Util.nl +
                "    gs2," + Util.nl +
                "    gs3" + Util.nl +
                "))";
            assertEquals(expectedString, sqlQuery.toString());
        }
    }

    public void testToStringForGroupingSetSqlWithEmptyGroup() {
        if (isGroupingSetsSupported()) {
            MondrianProperties.instance().GenerateFormattedSql.set(true);
            SqlQuery sqlQuery = new SqlQuery(getTestContext().getDialect());
            sqlQuery.addSelect("c1");
            sqlQuery.addSelect("c2");
            sqlQuery.addFromTable("s", "t1", "t1alias", null, true);
            sqlQuery.addWhere("a=b");
            sqlQuery.addGroupingFunction("g1");
            sqlQuery.addGroupingFunction("g2");
            ArrayList<String> groupingsetsList = new ArrayList<String>();
            groupingsetsList.add("gs1");
            groupingsetsList.add("gs2");
            groupingsetsList.add("gs3");
            sqlQuery.addGroupingSet(new ArrayList<String>());
            sqlQuery.addGroupingSet(groupingsetsList);
            MondrianProperties.instance().GenerateFormattedSql.set(false);
            assertEquals(
                "select c1 as \"c0\", c2 as \"c1\", grouping(g1) as \"g0\", " +
                    "grouping(g2) as \"g1\" from \"s\".\"t1\" \"t1alias\" where a=b " +
                    "group by grouping sets ((),(gs1,gs2,gs3))",
                sqlQuery.toString());
            MondrianProperties.instance().GenerateFormattedSql.set(true);
            String expectedString = "select " + Util.nl +
                "    c1 as \"c0\", " + Util.nl +
                "    c2 as \"c1\"" + Util.nl +
                "    , grouping(g1) as \"g0\"" + Util.nl +
                "    , grouping(g2) as \"g1\"" + Util.nl +
                "from " + Util.nl +
                "    \"s\".\"t1\" \"t1alias\"" + Util.nl +
                "where " + Util.nl +
                "    a=b" + Util.nl +
                " group by grouping sets ((),(" + Util.nl +
                "    gs1," + Util.nl +
                "    gs2," + Util.nl +
                "    gs3" + Util.nl +
                "))";
            assertEquals(expectedString, sqlQuery.toString());
        }
    }

    public void testToStringForMultipleGroupingSetsSql() {
        if (isGroupingSetsSupported()) {
            MondrianProperties.instance().GenerateFormattedSql.set(true);
            SqlQuery sqlQuery = new SqlQuery(getTestContext().getDialect());
            sqlQuery.addSelect("c0");
            sqlQuery.addSelect("c1");
            sqlQuery.addSelect("c2");
            sqlQuery.addSelect("m1", "m1");
            sqlQuery.addFromTable("s", "t1", "t1alias", null, true);
            sqlQuery.addWhere("a=b");
            sqlQuery.addGroupingFunction("c0");
            sqlQuery.addGroupingFunction("c1");
            sqlQuery.addGroupingFunction("c2");
            ArrayList<String> groupingSetlist1 = new ArrayList<String>();
            groupingSetlist1.add("c0");
            groupingSetlist1.add("c1");
            groupingSetlist1.add("c2");
            sqlQuery.addGroupingSet(groupingSetlist1);
            ArrayList<String> groupingsetsList2 = new ArrayList<String>();
            groupingsetsList2.add("c1");
            groupingsetsList2.add("c2");
            sqlQuery.addGroupingSet(groupingsetsList2);
            MondrianProperties.instance().GenerateFormattedSql.set(false);
            assertEquals(
                "select c0 as \"c0\", c1 as \"c1\", c2 as \"c2\", m1 as \"m1\", " +
                    "grouping(c0) as \"g0\", grouping(c1) as \"g1\", grouping(c2) as \"g2\" " +
                    "from \"s\".\"t1\" \"t1alias\" where a=b " +
                    "group by grouping sets ((c0,c1,c2),(c1,c2))",
                sqlQuery.toString());
            MondrianProperties.instance().GenerateFormattedSql.set(true);
            String expectedString = "select " + Util.nl +
                "    c0 as \"c0\", " + Util.nl +
                "    c1 as \"c1\", " + Util.nl +
                "    c2 as \"c2\", " + Util.nl +
                "    m1 as \"m1\"" + Util.nl +
                "    , grouping(c0) as \"g0\"" + Util.nl +
                "    , grouping(c1) as \"g1\"" + Util.nl +
                "    , grouping(c2) as \"g2\"" + Util.nl +
                "from " + Util.nl +
                "    \"s\".\"t1\" \"t1alias\"" + Util.nl +
                "where " + Util.nl +
                "    a=b" + Util.nl +
                " group by grouping sets ((" + Util.nl +
                "    c0," + Util.nl +
                "    c1," + Util.nl +
                "    c2" + Util.nl +
                "),(" + Util.nl +
                "    c1," + Util.nl +
                "    c2" + Util.nl +
                "))";
            assertEquals(expectedString, sqlQuery.toString());
        }
    }
    
    /**
     * This test verifies that the correct SQL string is generated for literals of
     * SQL type "double".
     *
     */
    public void testDoubleInList() {
        
        final SqlQuery.Dialect dialect = getTestContext().getDialect();
        if (SqlPattern.Dialect.get(dialect) != SqlPattern.Dialect.LUCIDDB) {
            return;
        }
        boolean origIgnoreInvalidMembers =
            MondrianProperties.instance().IgnoreInvalidMembers.get();
        boolean origIgnoreInvalidMembersDuringQuery =
            MondrianProperties.instance().IgnoreInvalidMembersDuringQuery.get();
        
        MondrianProperties.instance().IgnoreInvalidMembers.set(true);
        MondrianProperties.instance().IgnoreInvalidMembersDuringQuery.set(true);

        String dimensionSqlExpression =
            "cast(cast(\"salary\" as double)/cast(3.1234567890123456 as double) as double)\n";
        
        String cubeFirstPart =
            "<Cube name=\"Sales 3\">\n" +
            "  <Table name=\"sales_fact_1997\"/>\n" +
            "  <Dimension name=\"StoreEmpSalary\" foreignKey=\"store_id\">\n" +
            "    <Hierarchy hasAll=\"true\" allMemberName=\"All Salary\" primaryKey=\"store_id\">\n" +
            "      <Table name=\"employee\"/>\n" +
            "      <Level name=\"Salary\" column=\"salary\" type=\"Numeric\" uniqueMembers=\"true\" approxRowCount=\"10000000\">\n" +
            "        <KeyExpression>\n" +
            "          <SQL dialect=\"luciddb\">\n";
        
        String cubeSecondPart =
            "          </SQL>\n" +
            "        </KeyExpression>\n" +
            "      </Level>\n" +
            "    </Hierarchy>\n" +
            "  </Dimension>" +
            "  <Measure name=\"Store Cost\" column=\"store_cost\" aggregator=\"sum\"/>\n" +
            "</Cube>";
            
        String cube =
            cubeFirstPart +
            dimensionSqlExpression +
            cubeSecondPart;
        
        String query =
            "select " +
            "{[StoreEmpSalary].[All Salary].[6.403162057613773], [StoreEmpSalary].[All Salary].[1184.584980658548], [StoreEmpSalary].[All Salary].[1344.6640320988924], " +
            " [StoreEmpSalary].[All Salary].[1376.6798423869611],[StoreEmpSalary].[All Salary].[1408.69565267503],  [StoreEmpSalary].[All Salary].[1440.711462963099], " +
            " [StoreEmpSalary].[All Salary].[1456.7193681071333],[StoreEmpSalary].[All Salary].[1472.7272732511678],[StoreEmpSalary].[All Salary].[1488.7351783952022], " +
            " [StoreEmpSalary].[All Salary].[1504.7430835392367],[StoreEmpSalary].[All Salary].[1536.7588938273054],[StoreEmpSalary].[All Salary].[1600.7905144034432], " +
            " [StoreEmpSalary].[All Salary].[1664.822134979581], [StoreEmpSalary].[All Salary].[1888.932806996063], [StoreEmpSalary].[All Salary].[1952.9644275722007], " +
            " [StoreEmpSalary].[All Salary].[1984.9802378602697],[StoreEmpSalary].[All Salary].[2049.0118584364072],[StoreEmpSalary].[All Salary].[2081.0276687244764], " +
            " [StoreEmpSalary].[All Salary].[2113.043479012545], [StoreEmpSalary].[All Salary].[2145.059289300614], [StoreEmpSalary].[All Salary].[25612.648230455092]} " +
            " on rows, {[Measures].[Store Cost]} on columns from [Sales 3]";
        
        String loadSqlLucidDB =
            "select cast(cast(\"salary\" as double)/cast(3.1234567890123456 as double) as double) as \"c0\", " +
            "sum(\"sales_fact_1997\".\"store_cost\") as \"m0\" " +
            "from \"employee\" as \"employee\", \"sales_fact_1997\" as \"sales_fact_1997\" " +
            "where \"sales_fact_1997\".\"store_id\" = \"employee\".\"store_id\" and " +
            "cast(cast(\"salary\" as double)/cast(3.1234567890123456 as double) as double) in " +
            "(6.403162057613773E0, 1184.584980658548E0, 1344.6640320988924E0, " +
            "1376.6798423869611E0, 1408.69565267503E0, 1440.711462963099E0, " +
            "1456.7193681071333E0, 1472.7272732511678E0, 1488.7351783952022E0, " +
            "1504.7430835392367E0, 1536.7588938273054E0, 1600.7905144034432E0, " +
            "1664.822134979581E0, 1888.932806996063E0, 1952.9644275722007E0, " +
            "1984.9802378602697E0, 2049.0118584364072E0, 2081.0276687244764E0, " +
            "2113.043479012545E0, 2145.059289300614E0, 25612.648230455092E0) " +
            "group by cast(cast(\"salary\" as double)/cast(3.1234567890123456 as double) as double)";

        TestContext testContext =
            TestContext.create(
             null,
             cube,
             null,
             null,
             null);

        SqlPattern[] patterns =
            new SqlPattern[] {
                new SqlPattern(SqlPattern.Dialect.LUCIDDB, loadSqlLucidDB, loadSqlLucidDB)};

        // assertQuerySql(testContext, query, patterns);

        // Test when the double value itself cotnains "E".
        dimensionSqlExpression =
            "cast(cast(\"salary\" as double)*cast(1000.0 as double)/cast(3.1234567890123456 as double) as double)\n";
        
        cubeFirstPart =
            "<Cube name=\"Sales 3\">\n" +
            "  <Table name=\"sales_fact_1997\"/>\n" +
            "  <Dimension name=\"StoreEmpSalary\" foreignKey=\"store_id\">\n" +
            "    <Hierarchy hasAll=\"true\" allMemberName=\"All Salary\" primaryKey=\"store_id\">\n" +
            "      <Table name=\"employee\"/>\n" +
            "      <Level name=\"Salary\" column=\"salary\" type=\"Numeric\" uniqueMembers=\"true\" approxRowCount=\"10000000\">\n" +
            "        <KeyExpression>\n" +
            "          <SQL dialect=\"luciddb\">\n";
        
        cubeSecondPart =
            "          </SQL>\n" +
            "        </KeyExpression>\n" +
            "      </Level>\n" +
            "    </Hierarchy>\n" +
            "  </Dimension>" +
            "  <Measure name=\"Store Cost\" column=\"store_cost\" aggregator=\"sum\"/>\n" +
            "</Cube>";
            
        cube =
            cubeFirstPart +
            dimensionSqlExpression +
            cubeSecondPart;
        
        query =
            "select " +
            "{[StoreEmpSalary].[All Salary].[6403.162057613773],[StoreEmpSalary].[All Salary].[1184584.980658548],[StoreEmpSalary].[All Salary].[1344664.0320988924], " +
            " [StoreEmpSalary].[All Salary].[1376679.8423869612],[StoreEmpSalary].[All Salary].[1408695.65267503],[StoreEmpSalary].[All Salary].[1440711.462963099], " +
            " [StoreEmpSalary].[All Salary].[1456719.3681071333],[StoreEmpSalary].[All Salary].[1472727.2732511677],[StoreEmpSalary].[All Salary].[1488735.1783952022], " +
            " [StoreEmpSalary].[All Salary].[1504743.0835392366],[StoreEmpSalary].[All Salary].[1536758.8938273056],[StoreEmpSalary].[All Salary].[1600790.5144034433], " +
            " [StoreEmpSalary].[All Salary].[1664822.134979581],[StoreEmpSalary].[All Salary].[1888932.806996063],[StoreEmpSalary].[All Salary].[1952964.4275722008], " +
            " [StoreEmpSalary].[All Salary].[1984980.2378602696],[StoreEmpSalary].[All Salary].[2049011.8584364073],[StoreEmpSalary].[All Salary].[2081027.6687244761], " +
            " [StoreEmpSalary].[All Salary].[2113043.479012545],[StoreEmpSalary].[All Salary].[2145059.289300614],[StoreEmpSalary].[All Salary].[2.5612648230455093E7]} " +
            " on rows, {[Measures].[Store Cost]} on columns from [Sales 3]";

        // Notice there are a few members missing in this sql. This is a LucidDB bug wrt comparison involving "approximate number literals".
        // Mondrian properties "IgnoreInvalidMembers" and "IgnoreInvalidMembersDuringQuery" are required for this MDX to finished, even though the
        // the generated sql(below) and the final result are both incorrect.
        loadSqlLucidDB =
            "select cast(cast(\"salary\" as double)*cast(1000.0 as double)/cast(3.1234567890123456 as double) as double) as \"c0\", " + 
            "sum(\"sales_fact_1997\".\"store_cost\") as \"m0\" " + 
            "from \"employee\" as \"employee\", \"sales_fact_1997\" as \"sales_fact_1997\" " + 
            "where \"sales_fact_1997\".\"store_id\" = \"employee\".\"store_id\" and " + 
            "cast(cast(\"salary\" as double)*cast(1000.0 as double)/cast(3.1234567890123456 as double) as double) in " + 
            "(6403.162057613773E0, 1184584.980658548E0, 1344664.0320988924E0, " + 
            "1376679.8423869612E0, 1408695.65267503E0, 1440711.462963099E0, " +
            "1456719.3681071333E0, 1488735.1783952022E0, " + 
            "1504743.0835392366E0, 1536758.8938273056E0, " + 
            "1664822.134979581E0, 1888932.806996063E0, 1952964.4275722008E0, " + 
            "1984980.2378602696E0, 2049011.8584364073E0, " +
            "2113043.479012545E0, 2145059.289300614E0, 2.5612648230455093E7) " + 
            "group by cast(cast(\"salary\" as double)*cast(1000.0 as double)/cast(3.1234567890123456 as double) as double)";
        
        patterns =
            new SqlPattern[] {
                new SqlPattern(SqlPattern.Dialect.LUCIDDB, loadSqlLucidDB, loadSqlLucidDB)};
        testContext =
            TestContext.create(
             null,
             cube,
             null,
             null,
             null);
        
        assertQuerySql(testContext, query, patterns);
        
        MondrianProperties.instance().IgnoreInvalidMembers.set(origIgnoreInvalidMembers);
        MondrianProperties.instance().IgnoreInvalidMembersDuringQuery.set(origIgnoreInvalidMembersDuringQuery);
    }
}

// End SqlQueryTest.java