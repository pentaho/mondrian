/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2007-2014 Pentaho
// All Rights Reserved.
*/
package mondrian.test;

import mondrian.olap.Result;
import mondrian.olap.Util;
import mondrian.rolap.RolapMember;
import mondrian.rolap.SqlStatement;
import mondrian.spi.Dialect;
import mondrian.spi.DialectManager;
import mondrian.spi.impl.*;
import mondrian.util.DelegatingInvocationHandler;

import junit.framework.AssertionFailedError;
import junit.framework.TestCase;

import java.lang.reflect.*;
import java.sql.*;
import java.util.*;
import javax.sql.DataSource;

/**
 * Unit test which checks that {@link mondrian.spi.Dialect}
 * accurately represents the capabilities of the underlying database.
 *
 * <p>The existing mondrian tests, when run on various databases and drivers,
 * make sure that Dialect never over-states the capabilities of a particular
 * database. But sometimes they under-state a database's capabilities: for
 * example, MySQL version 3 did not allow subqueries in the FROM clause, but
 * version 4 does. This test helps ensure that mondrian is using the full
 * capabilities of each database.
 *
 * <p><strong>NOTE: If you see failures in this test, let the mondrian
 * developers know!</strong>
 * You may be running a version of a database which no one has
 * tried before, and which has more capabilities than we expect. If you tell us
 * about them, we can change mondrian to use those features.</p>
 *
 * @author jhyde
 * @since May 18, 2007
 */
public class DialectTest extends TestCase {
    private Connection connection;
    private Dialect dialect;
    private static final String INFOBRIGHT_UNSUPPORTED =
        "The query includes syntax that is not supported by the Infobright"
        + " Optimizer. Either restructure the query with supported syntax, or"
        + " enable the MySQL Query Path in the brighthouse.ini file to execute"
        + " the query with reduced performance.";
    private static final String NEOVIEW_SYNTAX_ERROR =
        "(?s).* ERROR\\[15001\\] A syntax error occurred at or before: .*";

    /**
     * Creates a DialectTest.
     *
     * @param name Test case name
     */
    public DialectTest(String name) {
        super(name);
    }

    protected DataSource getDataSource() {
        return TestContext.instance().getConnection().getDataSource();
    }

    protected void tearDown() throws Exception {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                // ignore
            } finally {
                connection = null;
            }
        }
        super.tearDown();
    }

    protected Dialect getDialect() {
        if (dialect == null) {
            dialect = DialectManager.createDialect(getDataSource(), null);
        }
        return dialect;
    }

    protected Connection getConnection() {
        if (connection == null) {
            try {
                connection = getDataSource().getConnection();
            } catch (SQLException e) {
                throw Util.newInternal(e, "while creating connection");
            }
        }
        return connection;
    }

    public void testDialectVsDatabaseProduct() throws SQLException {
        final Dialect dialect = getDialect();
        final Dialect.DatabaseProduct databaseProduct =
            dialect.getDatabaseProduct();
        final DatabaseMetaData databaseMetaData = getConnection().getMetaData();
        switch (databaseProduct) {
        case MYSQL:
            // Dialect has identified that it is MySQL.
            assertTrue(dialect instanceof MySqlDialect);
            assertFalse(dialect instanceof InfobrightDialect);
            assertFalse(MySqlDialect.isInfobright(databaseMetaData));
            assertEquals("MySQL", databaseMetaData.getDatabaseProductName());
            break;
        case HIVE:
            // Dialect has identified that it is Hive.
            assertTrue(dialect instanceof HiveDialect);
            break;
        case INFOBRIGHT:
            // Dialect has identified that it is MySQL.
            assertTrue(dialect instanceof MySqlDialect);
            assertTrue(dialect instanceof InfobrightDialect);
            assertTrue(MySqlDialect.isInfobright(databaseMetaData));
            assertEquals("MySQL", databaseMetaData.getDatabaseProductName());
            break;
        case POSTGRESQL:
            // Dialect has identified that it is PostgreSQL.
            assertTrue(dialect instanceof PostgreSqlDialect);
            assertFalse(dialect instanceof NetezzaDialect);
            assertTrue(
                databaseMetaData.getDatabaseProductName()
                    .indexOf("PostgreSQL") >= 0);
            break;
        case MSSQL:
            // Dialect has identified that it is MSSQL.
            assertTrue(dialect instanceof MicrosoftSqlServerDialect);
            assertTrue(
                databaseMetaData.getDatabaseProductName()
                    .contains("Microsoft"));
            break;
        case NETEZZA:
            // Dialect has identified that it is Netezza and a sub class of
            // PostgreSql.
            assertTrue(dialect instanceof PostgreSqlDialect);
            assertTrue(dialect instanceof NetezzaDialect);
            assertTrue(
                databaseMetaData.getDatabaseProductName()
                    .indexOf("Netezza") >= 0);
            break;
        case NUODB:
            // Dialect has identified that it is NUODB.
            assertTrue(dialect instanceof NuoDbDialect);
            assertTrue(
                databaseMetaData.getDatabaseProductName()
                    .contains("NuoDB"));
            break;
        default:
            // Neither MySQL nor Infobright.
            assertFalse(dialect instanceof MySqlDialect);
            assertFalse(dialect instanceof InfobrightDialect);
            assertNotSame("MySQL", databaseMetaData.getDatabaseProductName());
            break;
        }
    }

    public void testAllowsCompoundCountDistinct() {
        String sql =
            dialectize(
                "select count(distinct [customer_id], [product_id])\n"
                + "from [sales_fact_1997]");
        if (getDialect().allowsCompoundCountDistinct()) {
            assertQuerySucceeds(sql);
        } else {
            String[] errs = {
                // oracle
                "(?s)ORA-00909: invalid number of arguments.*",
                // derby
                "Syntax error: Encountered \",\" at line 1, column 36.",
                // access
                "\\[Microsoft\\]\\[ODBC Microsoft Access Driver\\] Syntax error \\(missing operator\\) in query expression '.*'.",
                // hsqldb
                "Unexpected token in statement \\[select count\\(distinct \"customer_id\",\\]",
                // infobright
                INFOBRIGHT_UNSUPPORTED,
                // neoview
                ".* ERROR\\[3129\\] Function COUNT DISTINCT accepts exactly one operand\\. .*",
                // postgres
                "ERROR: function count\\(integer, integer\\) does not exist.*",
                // LucidDb
                ".*Invalid number of arguments to function 'COUNT'. Was expecting 1 arguments",
                // teradata
                ".*Syntax error: expected something between the word 'customer_id' and ','\\..*",
                // netezza
                "(?s).*ERROR:  Function 'COUNT', number of parameters greater than the maximum \\(1\\).*",
                // Vertica
                "(?s).*ERROR: [Ff]unction count\\(int, int\\) does not exist, or permission is denied for count\\(int, int\\).*",
                // postgres
                "(?s).*ERROR: function count\\(integer, integer\\) does not exist.*",
                // monetdb
                "syntax error, unexpected ',', expecting '\\)' in: \"select count\\(distinct \"customer_id\",\"",
                // SQL server 2008
                "Incorrect syntax near ','.",
                // NuoDB
                "(?s).*expected closing parenthesis got ,.*"
            };
            assertQueryFails(sql, errs);
        }
    }

    public void testAllowsCountDistinct() {
        String sql1 =
            dialectize(
                "select count(distinct [customer_id]) from [sales_fact_1997]");
        // one distinct-count and one nondistinct-agg
        String sql2 =
            dialectize(
                "select count(distinct [customer_id]),\n"
                + " sum([time_id])\n"
                + "from [sales_fact_1997]");
        if (getDialect().allowsCountDistinct()) {
            assertQuerySucceeds(sql1);
            assertQuerySucceeds(sql2);
        } else {
            String[] errs = {
                // access
                "\\[Microsoft\\]\\[ODBC Microsoft Access Driver\\] Syntax error \\(missing operator\\) in query expression '.*'."
            };
            assertQueryFails(sql1, errs);
            assertQueryFails(sql2, errs);
        }
    }

    public void testAllowsMultipleCountDistinct() {
        // multiple distinct-counts
        String sql1 =
            dialectize(
                "select count(distinct [customer_id]),\n"
                + " count(distinct [time_id])\n"
                + "from [sales_fact_1997]");
        // multiple distinct-counts with group by and other aggs
        String sql3 =
            dialectize(
                "select [unit_sales],\n"
                + " count(distinct [customer_id]),\n"
                + " count(distinct [product_id])\n"
                + "from [sales_fact_1997]\n"
                + "where [time_id] in (371, 372)\n"
                + "group by [unit_sales]");
        if (getDialect().allowsMultipleCountDistinct()) {
            assertQuerySucceeds(sql1);
            assertQuerySucceeds(sql3);
            assertTrue(getDialect().allowsCountDistinct());
        } else {
            String[] errs = {
                // derby
                "Multiple DISTINCT aggregates are not supported at this time.",
                // access
                "\\[Microsoft\\]\\[ODBC Microsoft Access Driver\\] Syntax error \\(missing operator\\) in query expression '.*'.",
                // impala -- Returns a whole stack trace in its message
                // requires (?s) to set single line mode
                "(?s).*all DISTINCT aggregate functions need to have the same set of parameters as COUNT\\(DISTINCT customer_id\\)\\; deviating function\\: COUNT\\(DISTINCT time_id\\).*",
                // impala
                "(?s).*GROUP BY expression must have a discrete \\(non-floating point\\) type.*"
            };
            assertQueryFails(sql1, errs);
            assertQueryFails(sql3, errs);
        }
    }

    public void testAllowsDdl() {
        int phase = 0;
        SQLException e = null;
        Statement stmt = null;
        try {
            String dropSql = dialectize("drop table [foo]");
            String createSql = dialectize("create table [foo] ([i] integer)");
            stmt = getConnection().createStatement();

            // drop previously existing table, and ignore any errors
            try {
                stmt.execute(dropSql);
            } catch (SQLException e3) {
                // ignore
            }
            // now create and drop a dummy table
            phase = 1;
            assertFalse(stmt.execute(createSql));
            phase = 2;
            assertFalse(stmt.execute(dropSql));
            phase = 3;
        } catch (SQLException e2) {
            e = e2;
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e1) {
                    // ignore
                }
            }
        }
        if (getDialect().allowsDdl()) {
            assertNull(e == null ? null : e.getMessage(), e);
            assertEquals(3, phase);
        } else {
            assertEquals(1, phase);
            assertNotNull(e);
        }
    }

    public void testAllowsFromQuery() {
        String sql =
            dialectize(
                "select * from (select * from [sales_fact_1997]) as [x]");
        if (getDialect().allowsFromQuery()) {
            assertQuerySucceeds(sql);
        } else {
            assertQueryFails(sql, new String[] {});
        }
    }

    public void testRequiresFromQueryAlias() {
        if (getDialect().requiresAliasForFromQuery()) {
            assertTrue(getDialect().allowsFromQuery());
        }
        if (!getDialect().allowsFromQuery()) {
            return;
        }

        String sql =
            dialectize("select * from (select * from [sales_fact_1997])");
        if (getDialect().requiresAliasForFromQuery()) {
            String[] errs = {
                // mysql
                "Every derived table must have its own alias",
                // derby
                "Syntax error: Encountered \"<EOF>\" at line 1, column 47.",
                // hive
                "(?s).*mismatched input \'<EOF>\' expecting Identifier in subquery source.*",
                // postgres
                "(?s)ERROR: subquery in FROM must have an alias.*",
                // teradata
                ".*Syntax error, expected something like a name or a Unicode "
                + "delimited identifier or an 'UDFCALLNAME' keyword between "
                + "'\\)' and ';'\\.",
                // neoview
                NEOVIEW_SYNTAX_ERROR,
                // netezza
                "(?s).*ERROR:  sub-SELECT in FROM must have an alias.*",
                // monetdb
                "subquery table reference needs alias, use AS xxx in:.*",
                // SQL server 2008
                "Incorrect syntax near \\'\\)\\'\\.",
                // Impala
                "(?s).*Encountered: EOF.*Expected: IDENTIFIER.*"
            };
            assertQueryFails(sql, errs);
        } else {
            assertQuerySucceeds(sql);
        }
    }

    public void testRequiresOrderByAlias() {
        String sql =
            dialectize(
                "SELECT [unit_sales]\n"
                + "FROM [sales_fact_1997]\n"
                + "ORDER BY [unit_sales] + [store_id]");
        if (getDialect().requiresOrderByAlias()) {
            final String[] errs = {
                // infobright
                INFOBRIGHT_UNSUPPORTED,
                // hive
                "(?s).*Invalid Table Alias or Column Reference.*",
                // neoview
                NEOVIEW_SYNTAX_ERROR,
            };
            assertQueryFails(sql, errs);
        } else {
            assertQuerySucceeds(sql);
        }
    }

    public void testAllowsOrderByAlias() {
        String sql =
            dialectize(
                "SELECT [unit_sales] as [x],\n"
                + " [unit_sales] + [store_id] as [y]\n"
                + "FROM [sales_fact_1997]\n"
                + "ORDER BY [y]");
        if (getDialect().allowsOrderByAlias()) {
            assertQuerySucceeds(sql);
        } else {
            String[] errs = {
                // oracle
                "(?s)ORA-03001: unimplemented feature.*",
                // access
                "\\[Microsoft\\]\\[ODBC Microsoft Access Driver\\] Too few parameters. Expected 1.",
                // infobright
                INFOBRIGHT_UNSUPPORTED,
            };
            assertQueryFails(sql, errs);
        }
    }

    public void testRequiresUnionOrderByExprToBeInSelectClause() {
        String sql =
            dialectize(
                "SELECT [unit_sales], [store_sales]\n"
                + "FROM [sales_fact_1997]\n"
                + "UNION ALL\n"
                + "SELECT [unit_sales], [store_sales]\n"
                + "FROM [sales_fact_1997]\n"
                + "ORDER BY [unit_sales] + [store_sales]");

        if (!getDialect().requiresUnionOrderByExprToBeInSelectClause()) {
            assertQuerySucceeds(sql);
        } else {
            String[] errs = {
                // access
                "\\[Microsoft\\]\\[ODBC Microsoft Access Driver\\] The ORDER "
                + "BY expression \\(\\[unit_sales\\]\\+\\[store_sales\\]\\) "
                + "includes fields that are not selected by the query\\.  "
                + "Only those fields requested in the first query can be "
                + "included in an ORDER BY expression\\.",
                // derby (yes, lame message)
                "Java exception: ': java.lang.NullPointerException'.",
                // hsqldb
                "(?s)Cannot be in ORDER BY clause in statement .*",
                // neoview
                NEOVIEW_SYNTAX_ERROR,
                // oracle
                "ORA-01785: ORDER BY item must be the number of a SELECT-list "
                + "expression\n",
                // teradata
                ".*The ORDER BY clause must contain only integer constants.",
                // Greenplum / Postgres
                "ERROR: ORDER BY on a UNION/INTERSECT/EXCEPT result must be on "
                + "one of the result columns.*",
                // Postgres 9
                "(?s)ERROR: invalid UNION/INTERSECT/EXCEPT ORDER BY clause.*",
                // Vectorwise
                "Parse error in StringBuffer at line 0, column 525\\: \\<missing\\>\\.",
                // SQL server 2008
                "ORDER BY items must appear in the select list if the statement contains a UNION, INTERSECT or EXCEPT operator."
            };
            assertQueryFails(sql, errs);
        }
    }

    public void testSupportsGroupByExpressions() {
        String sql =
            dialectize(
                "SELECT sum([unit_sales] + 3) + 8\n"
                + "FROM [sales_fact_1997]\n"
                + "GROUP BY [unit_sales] + [store_id]");
        if (getDialect().supportsGroupByExpressions()) {
            assertQuerySucceeds(sql);
        } else {
            final String[] errs = {
                // mysql
                "'sum\\(`unit_sales` \\+ 3\\) \\+ 8' isn't in GROUP BY",
                // neoview
                ".* ERROR\\[4197\\] This expression cannot be used in the GROUP BY clause\\. .*",
                // monetdb
                "syntax error, unexpected '\\+', expecting SCOLON in: \"select sum\\(\"unit_sales\" \\+ 3\\) \\+ 8",
                // impala
                "(?s).*GROUP BY expression must have a discrete.*"
            };
            assertQueryFails(sql, errs);
        }
    }

    /**
     * Tests that the
     * {@link mondrian.spi.Dialect#supportsGroupingSets()}
     * dialect property is accurate.
     */
    public void testAllowsGroupingSets() {
        String sql =
            dialectize(
                "SELECT [customer_id],\n"
                + " SUM([store_sales]),\n"
                + " GROUPING([unit_sales]),\n"
                + " GROUPING([customer_id])\n"
                + "FROM [sales_fact_1997]\n"
                + "GROUP BY GROUPING SETS(\n"
                + " ([customer_id], [unit_sales]),\n"
                + " [customer_id],\n"
                + " ())");
        if (getDialect().supportsGroupingSets()) {
            assertQuerySucceeds(sql);
        } else {
            String[] errs = {
                // derby
                "Syntax error: Encountered \"SETS\" at line 6, column 19.",
                // hive
                "(?s).*line 6:18 mismatched input 'SETS' expecting EOF.*",
                // hsqldb
                "(?s)Unexpected token: GROUPING in statement .*",
                // mysql
                "(?s)You have an error in your SQL syntax; check .*",
                // access
                "(?s)\\[Microsoft\\]\\[ODBC Microsoft Access Driver\\] Syntax error \\(missing operator\\) in query expression 'GROUPING SETS.*",
                // luciddb
                "(?s).*Encountered \"GROUPING\" at line 3, column 2\\..*",
                // postgres
                "(?s)ERROR: syntax error at or near \"SETS\".*",
                // neoview
                NEOVIEW_SYNTAX_ERROR,
                // netezza
                "(?s).*found \"SETS\" \\(at char 135\\) expecting `EXCEPT' or `FOR' or `INTERSECT' or `ORDER' or `UNION'.*",
                // Vertica
                "line 3, There is no such function as \\'grouping\\'\\.",
                // monetdb
                "syntax error, unexpected IDENT, expecting SCOLON in: \"select \"customer_id\",",
                // impala
                "(?s).*Encountered: IDENTIFIER.*Expected: DIV, HAVING, LIMIT, ORDER, UNION, COMMA.*",
                // NuoDB
                "(?s).*expected end of statement got SETS.*"
            };
            assertQueryFails(sql, errs);
        }
    }

    public void testSupportsMultiValueInExpr() {
        String sql =
            dialectize(
                "SELECT [unit_sales]\n"
                + "FROM [sales_fact_1997]\n"
                + "WHERE ([unit_sales], [time_id]) IN ((1, 371), (2, 394))");

        if (getDialect().supportsMultiValueInExpr()) {
            assertQuerySucceeds(sql);
        } else {
            String[] errs = {
                // derby
                "Syntax error: Encountered \",\" at line 3, column 20.",
                // access
                "\\[Microsoft\\]\\[ODBC Microsoft Access Driver\\] Syntax error \\(comma\\) in query expression '.*'.",
                // hive
                "(?s).*line 3:19 mismatched input ','.*",
                // hsqldb
                "(?s)Unexpected token: , in statement .*",
                // infobright
                INFOBRIGHT_UNSUPPORTED,
                // neoview
                NEOVIEW_SYNTAX_ERROR,
                // teradata
                ".*Syntax error, expected something like a 'SELECT' keyword or '\\(' between '\\(' and the integer '1'\\.",
                // netezza
                "(?s).*found \"1\" \\(at char 81\\) expecting `SELECT' or `'\\(''.*",
                // monetdb
                "syntax error, unexpected ',', expecting '\\)' in: \"select \"unit_sales\"",
                // SQL server 2008
                "An expression of non-boolean type specified in a context where a condition is expected, near ','.",
                // impala
                "(?s).*Encountered: COMMA.*Expected: BETWEEN, DIV, IS, IN, LIKE, NOT, REGEXP, RLIKE.*",
                // NuoDB
                "(?s).*Operator in list does not support multi-column operands.*"
            };
            assertQueryFails(sql, errs);
        }
    }

    public void testDateLiteralString() {
        // verify correct construction of the date literal string.
        // With Oracle this can get interesting, because depending on the
        // driver version the string may be a DATE or a TIMESTAMP.
        // We need to construct a valid date literal in either case.
        // See http://jira.pentaho.com/browse/MONDRIAN-1819 and
        // http://jira.pentaho.com/browse/MONDRIAN-626
        if (getDialect().getDatabaseProduct()
            != Dialect.DatabaseProduct.ORACLE)
        {
            // the following test is specifically for Oracle.
            return;
        }
        final TestContext context = TestContext.instance().withSchema(
            "<?xml version=\"1.0\"?>\n"
            + "<Schema name=\"FoodMart\" metamodelVersion=\"4.00\">\n"
            + "  <PhysicalSchema>\n"
            + "    <Table name='time_by_day'>\n"
            + "      <Key>\n"
            + "        <Column name='time_id'/>\n"
            + "      </Key>\n"
            + "      <ColumnDefs>\n"
            + "        <CalculatedColumnDef name='date_datatype' type='Date'>\n"
            + "          <ExpressionView>\n"
            + "            <SQL dialect='oracle'>\n"
            + "              CAST(\"the_date\" as DATE)\n"
            + "            </SQL>\n"
            + "          </ExpressionView>\n"
            + "        </CalculatedColumnDef>\n"
            + "      </ColumnDefs>\n"
            + "    </Table>\n"
            + "    <Table name=\"sales_fact_1997\" alias=\"sales_fact_1997\"/>\n"
            + "  </PhysicalSchema>\n"
            + "  <Cube name=\"Sales\" >\n"
            + "    <Dimensions>\n"
            + "      <Dimension name=\"Time\" table='time_by_day'  key=\"Time Id\">\n"
            + "        <Attributes>\n"
            + "          <Attribute name='Time Id' keyColumn='time_id' hasHierarchy='false'/>\n"
            + "          <Attribute name=\"Day\" keyColumn='date_datatype' hasHierarchy=\"true\">\n"
            + "          </Attribute>\n"
            + "        </Attributes>\n"
            + "      </Dimension>\n"
            + "    </Dimensions>\n"
            + "    <MeasureGroups>\n"
            + "      <MeasureGroup name=\"Sales\" type=\"fact\"  table=\"sales_fact_1997\">\n"
            + "        <Measures>\n"
            + "          <Measure name='Unit Sales' column='unit_sales' aggregator='sum' formatString='Standard'/>\n"
            + "        </Measures>\n"
            + "        <DimensionLinks>\n"
            + "          <ForeignKeyLink dimension=\"Time\" foreignKeyColumn='time_id' />\n"
            + "        </DimensionLinks>\n"
            + "      </MeasureGroup>\n"
            + "    </MeasureGroups>\n"
            + "  </Cube>\n"
            + "</Schema>\n");
        // if date literal is incorrect the following query will give the error
        // ORA-01861: literal does not match format string
        Result result = context.executeQuery(
            "select [Time].[Day].[All Day].FirstChild on 0 from Sales");
        String firstChild =
            result.getAxes()[0].getPositions().get(0).get(0)
                .getName().toString();
        // the member name may have timestamp info, for example if using
        // Oracle with ojdbc5+.  Make sure it starts w/ the expected date.
        assertTrue(firstChild.startsWith("1997-01-01"));
    }

    public void testBigInt() {
        if (getDialect().getDatabaseProduct()
            != Dialect.DatabaseProduct.VERTICA)
        {
            // currently only checks VERTICA
            // Once MONDRIAN-1890 is fixed this test should minimally cover
            // Oracle and MySQL as well.
            return;
        }
        final TestContext context = TestContext.instance().withSchema(
            "<?xml version=\"1.0\"?>\n"
            + "<Schema name=\"FoodMart\" metamodelVersion=\"4.00\">\n"
            + "  <PhysicalSchema>\n"
            + "    <Table name='store'>\n"
            + "      <Key>\n"
            + "        <Column name='store_id'/>\n"
            + "      </Key>\n"
            + "      <ColumnDefs>\n"
            + "        <CalculatedColumnDef name='long_sqft' >\n"
            + "          <ExpressionView>\n"
            + "            <SQL dialect='vertica'>\n"
            + "            coalesce(cast(\"store_sqft\" as BIGINT) + "
            + Integer.MAX_VALUE + ", 0)"
            + "            </SQL>\n"
            + "          </ExpressionView>\n"
            + "        </CalculatedColumnDef>\n"
            + "      </ColumnDefs>\n"
            + "    </Table>\n"
            + "    <Table name=\"sales_fact_1997\" alias=\"sales_fact_1997\">\n"
            + "      <ColumnDefs>\n"
            + "        <CalculatedColumnDef name='big_unit_sales' >\n"
            + "          <ExpressionView>\n"
            + "            <SQL dialect='vertica'>\n"
            + "   CAST(\"unit_sales\" + 2147483647 AS NUMBER(22)) \n"
            + "            </SQL>\n"
            + "          </ExpressionView>\n"
            + "        </CalculatedColumnDef>\n"
            + "      </ColumnDefs>\n"
            + "    </Table>\n"
            + "  </PhysicalSchema>\n"
            + "  <Cube name=\"BigIntTest\" >\n"
            + "    <Dimensions>\n"
            + "  <Dimension name=\"StoreSqft\" table='store' key='Store Id'>\n"
            + "        <Attributes>\n"
            + "          <Attribute name='Store Id'  keyColumn='store_id' hasHierarchy='false'/>\n"
            + "          <Attribute name=\"StoreSqft\" keyColumn='long_sqft' hasHierarchy=\"true\">\n"
            + "          </Attribute>\n"
            + "        </Attributes>\n"
            + "      </Dimension>\n"
            + "    </Dimensions>\n"
            + "    <MeasureGroups>\n"
            + "      <MeasureGroup name=\"Sales\" type=\"fact\"  table=\"sales_fact_1997\">\n"
            + "        <Measures>\n"
            + "          <Measure name='Big Unit Sales' column='big_unit_sales' aggregator='sum' formatString='Standard'/>\n"
            + "        </Measures>\n"
            + "        <DimensionLinks>\n"
            + "          <ForeignKeyLink dimension=\"StoreSqft\" foreignKeyColumn='store_id' />\n"
            + "        </DimensionLinks>\n"
            + "      </MeasureGroup>\n"
            + "    </MeasureGroups>\n"
            + "  </Cube>\n"
            + "</Schema>\n");
        Result result = context.executeQuery(
            "select [StoreSqft].[StoreSqft].[All StoreSqft].children on 0 from BigIntTest");
        RolapMember secondChild =
            (RolapMember) result.getAxes()[0].getPositions().get(1).get(0);

        assertTrue(secondChild.getKey() instanceof Long);
        assertEquals(2147503966L, ((Long) secondChild.getKey()).longValue());

        context.assertQueryReturns(
            "select [StoreSqft].[StoreSqft].[All StoreSqft].children on 0, "
            + "{measures.[Big Unit Sales]} on 1 from BigIntTest",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[StoreSqft].[StoreSqft].[0]}\n"
            + "{[StoreSqft].[StoreSqft].[2147503966]}\n"
            + "{[StoreSqft].[StoreSqft].[2147504862]}\n"
            + "{[StoreSqft].[StoreSqft].[2147506125]}\n"
            + "{[StoreSqft].[StoreSqft].[2147506759]}\n"
            + "{[StoreSqft].[StoreSqft].[2147507240]}\n"
            + "{[StoreSqft].[StoreSqft].[2147507245]}\n"
            + "{[StoreSqft].[StoreSqft].[2147507335]}\n"
            + "{[StoreSqft].[StoreSqft].[2147507406]}\n"
            + "{[StoreSqft].[StoreSqft].[2147508244]}\n"
            + "{[StoreSqft].[StoreSqft].[2147511341]}\n"
            + "{[StoreSqft].[StoreSqft].[2147511853]}\n"
            + "{[StoreSqft].[StoreSqft].[2147513915]}\n"
            + "{[StoreSqft].[StoreSqft].[2147514231]}\n"
            + "{[StoreSqft].[StoreSqft].[2147514444]}\n"
            + "{[StoreSqft].[StoreSqft].[2147517505]}\n"
            + "{[StoreSqft].[StoreSqft].[2147518099]}\n"
            + "{[StoreSqft].[StoreSqft].[2147518438]}\n"
            + "{[StoreSqft].[StoreSqft].[2147520156]}\n"
            + "{[StoreSqft].[StoreSqft].[2147522029]}\n"
            + "{[StoreSqft].[StoreSqft].[2147523343]}\n"
            + "Axis #2:\n"
            + "{[Measures].[Big Unit Sales]}\n"
            + "Row #0: 28,101,971,043,971\n"
            + "Row #0: 17,746,804,884,887\n"
            + "Row #0: 17,085,379,920,543\n"
            + "Row #0: 2,845,415,834,392\n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: 17,624,398,316,592\n"
            + "Row #0: 14,635,101,075,638\n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: 28,662,464,278,089\n"
            + "Row #0: 2,963,527,435,097\n"
            + "Row #0: 15,884,936,560,450\n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: 24,017,457,143,305\n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: 16,913,581,228,348\n");
    }



    public void testResultSetConcurrency() {
        int[] Types = {
            ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.TYPE_SCROLL_SENSITIVE
        };
        int[] Concurs = {
            ResultSet.CONCUR_READ_ONLY,
            ResultSet.CONCUR_UPDATABLE
        };
        String sql =
            dialectize("SELECT [unit_sales] FROM [sales_fact_1997]");
        for (int type : Types) {
            for (int concur : Concurs) {
                boolean b =
                    getDialect().supportsResultSetConcurrency(type, concur);
                Statement stmt = null;
                try {
                    stmt = getConnection().createStatement(type, concur);
                    ResultSet resultSet = stmt.executeQuery(sql);
                    assertTrue(resultSet.next());
                    Object col1 = resultSet.getObject(1);
                    Util.discard(col1);
                    if (!b) {
                        // It's a little surprising that the driver said it
                        // didn't support this type/concurrency combination,
                        // but allowed the statement to be executed anyway.
                        // But don't fail.
                        Util.discard(
                            "expected to fail for type=" + type
                            + ", concur=" + concur);
                    }
                } catch (SQLException e) {
                    if (b) {
                        fail(
                            "expected to succeed for type=" + type
                            + ", concur=" + concur);
                        throw Util.newInternal(e, "query [" + sql + "] failed");
                    }
                } finally {
                    if (stmt != null) {
                        try {
                            stmt.close();
                        } catch (SQLException e) {
                            // ignore
                        }
                    }
                }
            }
        }
    }

    public void testGenerateInline() throws SQLException {
        final List<String> typeList = Arrays.asList("String", "Numeric");
        final List<String> nameList = Arrays.asList("x", "y");
        assertInline(
            nameList, typeList,
            new String[]{"a", "1"});

        assertInline(
            nameList, typeList,
            new String[]{"a", "1"}, new String[]{"bb", "2"});

        // Make sure the handling of the single quote doesn't interfere
        // with double quotes
        assertInline(
            nameList, typeList,
            new String[]{"can't \"stop\"", "1"});

        // string containing single quote (problem for all database) and a
        // backslash (problem for mysql; appears as a double backslash for
        // java's benefit, but is a single backslash by the time it gets to SQL)
        assertInline(
            nameList, typeList,
            new String[]{"can't stop", "1"}, new String[]{"back\\slash", "2"});

        // date value
        final List<String> typeList2 = Arrays.asList("String", "Date");
        assertInline(
            nameList, typeList2,
            new String[]{"a", "2008-04-29"}, new String[]{"b", "2007-01-02"});

        // empty list
        final List<String> typeList3 =
            Arrays.asList("String", "Date", "Numeric");
        final List<String> nameList3 = Arrays.asList("x", "y", "z");
        assertInline(nameList3, typeList3);
    }

    /**
     * Tests that the dialect can generate a valid query to sort ascending and
     * descending, with NULL values appearing last in both cases.
     */
    public void testForceNullCollation() throws SQLException {
        checkForceNullCollation(true, true);
        checkForceNullCollation(false, true);
        checkForceNullCollation(true, false);
        checkForceNullCollation(false, false);
    }

    /**
     * Checks that the dialect can generate a valid query to sort in a given
     * direction, with NULL values appearing last.
     *
     * @param ascending Whether ascending
     * @param nullsLast Force nulls last or not.
     */
    private void checkForceNullCollation(
        boolean ascending,
        boolean nullsLast) throws SQLException
    {
        Dialect dialect = getDialect();
        String query =
            "select "
            + dialect.quoteIdentifier("store_manager")
            + " from "
            + dialect.quoteIdentifier("store")
            + " order by "
            + dialect.generateOrderItem(
                dialect.quoteIdentifier("store_manager"),
                true, ascending, nullsLast);
        if (ascending) {
            if (nullsLast) {
                assertFirstLast(query, "Brown", null);
            } else {
                assertFirstLast(query, null, "Williams");
            }
        } else {
            // Largest value comes first, null comes last.
            switch (dialect.getDatabaseProduct()) {
            case GREENPLUM:
                // Current version cannot force null order, introduced in
                // Postgres 8.3
                return;
            case HIVE:
                // Hive cannot force nulls to appear last
                return;
            case NEOVIEW:
                // Neoview cannot force nulls to appear last
                return;
            }
            if (nullsLast) {
                assertFirstLast(query, "Williams", null);
            } else {
                assertFirstLast(query, null, "Brown");
            }
        }
    }

    private void assertFirstLast(
        String query,
        String expectedFirst,
        String expectedLast) throws SQLException
    {
        ResultSet resultSet =
            getConnection().createStatement().executeQuery(query);
        List<String> values = new ArrayList<String>();
        while (resultSet.next()) {
            values.add(resultSet.getString(1));
            if (resultSet.wasNull()) {
                values.set(values.size() - 1, null);
            }
        }
        resultSet.close();
        String actualFirst = values.get(0);
        String actualLast = values.get(values.size() - 1);
        assertEquals(query, expectedFirst, actualFirst);
        assertEquals(query, expectedLast, actualLast);
    }

    private void assertInline(
        List<String> nameList,
        List<String> typeList,
        String[]... valueList) throws SQLException
    {
        String sql =
            getDialect().generateInline(
                nameList,
                typeList,
                Arrays.asList(valueList));
        Statement stmt = null;
        try {
            stmt = getConnection().createStatement();
            ResultSet resultSet = stmt.executeQuery(sql);
            Set<List<String>> actualValues = new HashSet<List<String>>();
            while (resultSet.next()) {
                final List<String> row = new ArrayList<String>();
                for (int i = 0; i < typeList.size(); i++) {
                    final String s;
                    final String type = typeList.get(i);
                    if (type.equals("String")) {
                        s = resultSet.getString(i + 1);
                    } else if (type.equals("Date")) {
                        s = String.valueOf(resultSet.getDate(i + 1));
                    } else if (type.equals("Numeric")) {
                        s = String.valueOf(resultSet.getInt(i + 1));
                    } else {
                        throw new RuntimeException("unknown type " + type);
                    }
                    row.add(s);
                }
                actualValues.add(row);
            }
            Set<List<String>> expectedRows = new HashSet<List<String>>();
            for (String[] strings : valueList) {
                expectedRows.add(Arrays.asList(strings));
            }
            assertEquals(expectedRows, actualValues);
            stmt.close();
            stmt = null;
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    // ignore
                }
            }
        }
    }

    /**
     * Converts query or DDL statement into this dialect.
     *
     * @param s SQL query or DDL statement
     * @return Query or DDL statement translated into this dialect
     */
    private String dialectize(String s) {
        if (dialect == null) {
            dialect = getDialect();
        }
        final Dialect.DatabaseProduct databaseProduct =
            dialect.getDatabaseProduct();
        switch (databaseProduct) {
        case ACCESS:
            break;
        case HIVE:
            if (s.contains("UNION ALL")) {
                s = "SELECT * FROM (" + s + ") x";
            }
            s = s.replace('[', '`');
            s = s.replace(']', '`');
            s = s.replaceAll(" as ", "");
            break;
        case IMPALA:
            s = s.replace("[", "");
            s = s.replace("]", "");
            s = s.replaceAll(" as ", " ");
            break;
        case MYSQL:
        case INFOBRIGHT:
            s = s.replace('[', '`');
            s = s.replace(']', '`');
            break;
        case ORACLE:
            s = s.replace('[', '"');
            s = s.replace(']', '"');
            s = s.replaceAll(" as ", "");
            break;
        case INFORMIX:
            s = s.replace("[", "");
            s = s.replace("]", "");
            break;
        default:
            s = s.replace('[', '"');
            s = s.replace(']', '"');
            break;
        }
        return s;
    }

    /**
     * Asserts that a query succeeds and produces at least one row.
     *
     * @param sql SQL query in current dialect
     */
    protected void assertQuerySucceeds(String sql) {
        Statement stmt = null;
        try {
            stmt = getConnection().createStatement();
            ResultSet resultSet = stmt.executeQuery(sql);
            assertTrue(resultSet.next());
            Object col1 = resultSet.getObject(1);
            Util.discard(col1);
        } catch (SQLException e) {
            throw Util.newInternal(e, "query [" + sql + "] failed");
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    // ignore
                }
            }
        }
    }

    /**
     * Asserts that a query fails.
     *
     * @param sql SQL query
     * @param patterns Array of expected patterns, generally one for each
     *   SQL dialect for which the test is expected to fail
     */
    protected void assertQueryFails(String sql, String[] patterns) {
        Statement stmt = null;
        try {
            stmt = getConnection().createStatement();
            ResultSet resultSet;
            try {
            resultSet = stmt.executeQuery(sql);
            } catch (SQLException e2) {
                // execution failed - good
                String message = e2.getMessage();
                for (String pattern : patterns) {
                    if (message.matches(pattern)) {
                        return;
                    }
                }
                throw new AssertionFailedError(
                    "error [" + message
                    + "] did not match any of the supplied patterns");
            }
            assertTrue(resultSet.next());
            Object col1 = resultSet.getObject(1);
            Util.discard(col1);
        } catch (SQLException e) {
            throw Util.newInternal(e, "failed in wrong place");
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    // ignore
                }
            }
        }
    }

    /**
     * Unit test for {@link Dialect#allowsSelectNotInGroupBy}.
     */
    public void testAllowsSelectNotInGroupBy() throws SQLException {
        Dialect dialect = getDialect();
        String sql =
            "select "
            + dialect.quoteIdentifier("time_id")
            + ", "
            + dialect.quoteIdentifier("the_month")
            + " from "
            + dialect.quoteIdentifier("time_by_day")
            + " group by "
            + dialect.quoteIdentifier("time_id");
        if (dialect.allowsSelectNotInGroupBy()) {
            final ResultSet resultSet =
                getConnection().createStatement().executeQuery(sql);
            assertTrue(resultSet.next());
            resultSet.close();
        } else {
            String[] errs = {
                // oracle
                "ORA-00979: not a GROUP BY expression\n",
                // derby
                "The SELECT list of a grouped query contains at least one "
                + "invalid expression. If a SELECT list has a GROUP BY, the "
                + "list may only contain valid grouping expressions and valid "
                + "aggregate expressions.  ",
                // hive
                "(?s).*line 1:18 Expression Not In Group By Key `the_month`.*",
                // hsqldb
                "(?s)Not in aggregate function or group by clause: .*",
                // mysql (if sql_mode contains ONLY_FULL_GROUP_BY)
                "ERROR 1055 (42000): 'foodmart.time_by_day.the_month' isn't in "
                + "GROUP BY",
                // access
                "\\[Microsoft\\]\\[ODBC Microsoft Access Driver\\] You tried "
                + "to execute a query that does not include the specified "
                + "expression 'the_month' as part of an aggregate function.",
                // luciddb
                "From line 1, column 19 to line 1, column 29: Expression "
                + "'the_month' is not being grouped",
                // neoview
                ".* ERROR\\[4005\\] Column reference \"the_month\" must be a "
                + "grouping column or be specified within an aggregate. .*",
                // teradata
                ".*Selected non-aggregate values must be part of the "
                + "associated group.",
                // Greenplum & postgresql
                "(?s).*ERROR: column \"time_by_day.the_month\" must appear in "
                + "the GROUP BY clause or be used in an aggregate function.*",
                // Vectorwise
                "line 1, The columns in the SELECT clause must be contained in the GROUP BY clause\\.",
                // MonetDB
                "SELECT: cannot use non GROUP BY column 'the_month' in query results without an aggregate function",
                // SQL Server 2008
                "Column 'time_by_day.the_month' is invalid in the select list because it is not contained in either an aggregate function or the GROUP BY clause.",
                // impala
                "(?s).*select list expression not produced by aggregation output.*missing from GROUP BY clause.*",
                // NuoDB
                "(?s).*scolumn mondrian.time_by_day.the_month must appear in the GROUP BY clause or be used in an aggregate function.*"
            };
            assertQueryFails(sql, errs);
        }
    }

    public void testHavingRequiresAlias() throws Exception {
        Dialect dialect = getDialect();
        StringBuilder sb =
            new StringBuilder(
                "select upper("
                + dialect.quoteIdentifier("customer", "fname")
                + ") as c from "
                + dialect.quoteIdentifier("customer")
                + " group by "
                + dialect.quoteIdentifier("customer", "fname")
                + " having "
                + dialect.quoteIdentifier("customer", "fname")
                + " LIKE ");
        dialect.quoteStringLiteral(sb, "%");
        if (!dialect.requiresHavingAlias()) {
            final ResultSet resultSet =
                getConnection().createStatement().executeQuery(sb.toString());
            assertTrue(resultSet.next());
            resultSet.close();
        } else {
            String[] errs = {
                // mysql
                "Unknown column 'customer\\.fname' in 'having clause'",
                // vectorwise
                "No conversion defined for result data type\\.",
            };
            assertQueryFails(sb.toString(), errs);
        }
    }

    public void testAllowsRegularExpressionInWhereClause() throws Exception {
        Dialect dialect = getDialect();
        if (dialect.allowsRegularExpressionInWhereClause()) {
            assertNotNull(
                dialect.generateRegularExpression(
                    dialect.quoteIdentifier("customer", "fname"),
                    "(?i).*\\QJeanne\\E.*"));
            StringBuilder sb =
                new StringBuilder(
                    "select "
                    + dialect.quoteIdentifier("customer", "fname")
                    + " from "
                    + dialect.quoteIdentifier("customer")
                    + " group by "
                    + dialect.quoteIdentifier("customer", "fname")
                    + " having "
                    + dialect.generateRegularExpression(
                        dialect.quoteIdentifier("customer", "fname"),
                        "(?i).*\\QJeanne\\E.*"));
            final ResultSet resultSet =
                getConnection().createStatement().executeQuery(sb.toString());
            assertTrue(resultSet.next());
            resultSet.close();
        } else {
            assertNull(
                dialect.generateRegularExpression(
                    "Foo",
                    "(?i).*\\QBar\\E.*"));
        }
    }

    /**
     * This is a test for
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-1057">
     * http://jira.pentaho.com/browse/MONDRIAN-1057</a>
     * Some dialects are not removing the \Q and \E markers if they
     * are in the middle of the regexp.
     */
    public void testComplexRegularExpression() throws Exception {
        final String regexp =
            "(?i).*\\QJeanne\\E.*|.*\\QSheri\\E.*|.*\\QJonathan\\E.*|.*\\QJewel\\E.*";
        Dialect dialect = getDialect();
        if (dialect.allowsRegularExpressionInWhereClause()) {
            assertNotNull(
                dialect.generateRegularExpression(
                    dialect.quoteIdentifier("customer", "fname"),
                    regexp));
            StringBuilder sb =
                new StringBuilder(
                    "select "
                    + dialect.quoteIdentifier("customer", "fname")
                    + " from "
                    + dialect.quoteIdentifier("customer")
                    + " group by "
                    + dialect.quoteIdentifier("customer", "fname")
                    + " having "
                    + dialect.generateRegularExpression(
                        dialect.quoteIdentifier("customer", "fname"),
                        regexp));
            final ResultSet resultSet =
                getConnection().createStatement().executeQuery(sb.toString());
            int i = 0;
            while (resultSet.next()) {
                i++;
            }
            assertEquals(7, i);
            resultSet.close();
        } else {
            assertNull(
                dialect.generateRegularExpression(
                    "Foo",
                    "(?i).*\\QBar\\E.*"));
        }
    }

    public void testRegularExpressionSqlInjection() throws SQLException {
        // bug mondrian-983
        // We know that mysql's dialect can handle this regex
        boolean couldTranslate =
            checkRegex("(?i).*\\Qa\"\"\\); window.alert(\"\"woot');\\E.*");
        switch (getDialect().getDatabaseProduct()) {
        case MYSQL:
            assertTrue(couldTranslate);
            break;
        }

        // On mysql, this gives error:
        //   Got error 'repetition-operator operand invalid' from regexp
        //
        // Ideally, we would detect that the regex cannot be translated to
        // SQL (perhaps because it's not a valid java regex). Currently the
        // database gives an error, and that's better than nothing.
        Throwable throwable = null;
        try {
            couldTranslate =
                checkRegex(
                    "\"(?i).*\\Qa\"\"\\); window.alert(\"\"woot');\\E.*\"");
        } catch (SQLException e) {
            throwable = e;
        }
        switch (getDialect().getDatabaseProduct()) {
        case MYSQL:
            assertNotNull(throwable);
            assertTrue(couldTranslate);
            assertTrue(
                throwable.getMessage().contains(
                    "Got error 'repetition-operator operand invalid' from "
                    + "regexp"));
            break;
        case POSTGRESQL:
            assertNotNull(throwable);
            assertTrue(couldTranslate);
            assertTrue(
                throwable.getMessage(),
                throwable.getMessage().contains(
                    "ERROR: invalid regular expression: quantifier operand "
                    + "invalid"));
            break;
        default:
            // As far as we know, all other databases either handle this regex
            // just fine or our dialect for that database refuses to translate
            // the regex to SQL.
            assertNull(throwable);
        }

        // Every dialect should refuse to translate an invalid regex.
        couldTranslate = checkRegex("ab[cd");
        assertFalse(couldTranslate);
    }

    /**
     * Translates a regular expression into SQL and executes the query.
     * Returns whether the dialect was able to translate the regex.
     *
     * @param regex Java regular expression string
     * @return Whether dialect could translate regex to SQL.
     * @throws SQLException on error
     */
    private boolean checkRegex(String regex) throws SQLException {
        Dialect dialect = getDialect();
        final String sqlRegex =
            dialect.generateRegularExpression(
                dialect.quoteIdentifier("customer", "fname"),
                regex);
        if (sqlRegex != null) {
            String sql =
                "select * from "
                + dialect.quoteIdentifier("customer")
                + " where "
                + sqlRegex;
            final ResultSet resultSet =
                getConnection().createStatement().executeQuery(sql);
            assertFalse(resultSet.next());
            resultSet.close();
            return true;
        } else {
            return false;
        }
    }

    public void testRectifyCase() {
        Dialect dialect = getDialect();
        Dialect dialectSans = getDialect().withQuoting(false);
        switch (dialect.getDatabaseProduct()) {
        case ORACLE:
        case MYSQL:
        case POSTGRESQL:
            assertEquals("EMP", dialectSans.rectifyCase("Emp"));
            assertEquals("EMP", dialectSans.rectifyCase("emp"));
            assertEquals("EMP", dialectSans.rectifyCase("EMP"));
            assertEquals(
                "Emp with Space", dialectSans.rectifyCase("Emp with Space"));
            break;
        }
        Dialect dialectWith = getDialect().withQuoting(true);
        assertEquals("Emp", dialectWith.rectifyCase("Emp"));
        assertEquals("emp", dialectWith.rectifyCase("emp"));
        assertEquals("EMP", dialectWith.rectifyCase("EMP"));
        assertEquals(
            "Emp with Space", dialectWith.rectifyCase("Emp with Space"));
    }

    public void testOracleTypeMapQuirks() throws SQLException {
        MockResultSetMetadata mockResultSetMeta = new MockResultSetMetadata();
        Dialect oracleDialect = new OracleDialect();

        assertTrue(
            "Oracle dialect NUMERIC type with 0 precision, 0 scale should map "
            + "to INT, unless column starts with 'm'",
            oracleDialect.getType(
                mockResultSetMeta.withColumnName("c0")
                    .withColumnType(Types.NUMERIC)
                    .withPrecision(0)
                    .withScale(0)
                    .build(),
                0) == SqlStatement.Type.INT);

        assertTrue(
            "Oracle dialect NUMERIC type with non-zero precision, -127 scale "
            + " should map to DOUBLE.  MONDRIAN-1044",
            oracleDialect.getType(
                mockResultSetMeta.withColumnName("c0")
                    .withColumnType(Types.NUMERIC)
                    .withPrecision(5)
                    .withScale(-127)
                    .build(),
                0) == SqlStatement.Type.DOUBLE);
        assertTrue(
            "Oracle dialect NUMERIC type with precision less than 10, 0 scale "
            + " should map to INT. ",
            oracleDialect.getType(
                mockResultSetMeta.withColumnName("c0")
                    .withColumnType(Types.NUMERIC)
                    .withPrecision(9)
                    .withScale(0)
                    .build(),
                0) == SqlStatement.Type.INT);
        assertTrue(
            "Oracle dialect NUMERIC type with precision = 38, scale = 0"
            + " should map to INT.  38 is a magic number in Oracle "
            + " for integers of unspecified precision.",
            oracleDialect.getType(
                mockResultSetMeta.withColumnName("c0")
                    .withColumnType(Types.NUMERIC)
                    .withPrecision(38)
                    .withScale(0)
                    .build(),
                0) == SqlStatement.Type.INT);
        assertTrue(
            "Oracle dialect DECIMAL type with precision > 9, scale = 0"
            + " should map to DOUBLE (unless magic #38)",
            oracleDialect.getType(
                mockResultSetMeta.withColumnName("c0")
                    .withColumnType(Types.NUMERIC)
                    .withPrecision(20)
                    .withScale(0)
                    .build(),
                0) == SqlStatement.Type.DOUBLE);

        assertTrue(
            "Oracle dialect NUMBER type with precision =0 , scale = -127"
            + " should map to INT.  GROUPING SETS queries can shift"
            + " scale for columns to -127, whether INT or other NUMERIC."
            + " Assume INT unless the column name indicates it is a measure.",
            oracleDialect.getType(
                mockResultSetMeta.withColumnName("c0")
                    .withColumnType(Types.NUMERIC)
                    .withPrecision(0)
                    .withScale(-127)
                    .build(),
                0) == SqlStatement.Type.INT);
        assertTrue(
            "Oracle dialect NUMBER type with precision =0 , scale = -127"
            + " should map to OBJECT if measure name starts with 'm'",
            oracleDialect.getType(
                mockResultSetMeta.withColumnName("m0")
                    .withColumnType(Types.NUMERIC)
                    .withPrecision(0)
                    .withScale(-127)
                    .build(),
                0) == SqlStatement.Type.OBJECT);
    }

    public void testPostgresGreenplumTypeMapQuirks() throws SQLException {
        MockResultSetMetadata mockResultSetMeta = new MockResultSetMetadata();
        Dialect greenplumDialect =
            MockDialect.of(Dialect.DatabaseProduct.GREENPLUM);
        assertTrue(
            "Postgres/Greenplum dialect NUMBER with precision =0, scale = 0"
            + ", measure name starts with 'm' maps to OBJECT",
            greenplumDialect.getType(
                mockResultSetMeta.withColumnName("m0")
                    .withColumnType(Types.NUMERIC)
                    .withPrecision(0)
                    .withScale(0)
                    .build(),
                0) == SqlStatement.Type.OBJECT);
    }

    public void testNetezzaTypeMapQuirks() throws SQLException {
        MockResultSetMetadata mockResultSetMeta = new MockResultSetMetadata();
        Dialect netezzaDialect =
            MockDialect.of(Dialect.DatabaseProduct.NETEZZA);
        assertTrue(
            "Netezza dialect NUMERIC/DECIMAL with precision =38, scale = 0"
            + " means long.  Should be mapped to DOUBLE",
            netezzaDialect.getType(
                mockResultSetMeta
                    .withColumnType(Types.NUMERIC)
                    .withPrecision(38)
                    .withScale(0)
                    .build(),
                0) == SqlStatement.Type.DOUBLE);
        assertTrue(
            "Netezza dialect NUMERIC/DECIMAL with precision =38, scale = 0"
            + " means long.  Should be mapped to DOUBLE",
            netezzaDialect.getType(
                mockResultSetMeta
                    .withColumnType(Types.DECIMAL)
                    .withPrecision(38)
                    .withScale(0)
                    .build(),
                0) == SqlStatement.Type.DOUBLE);
    }

    public void testMonetDBTypeMapQuirks() throws SQLException {
        MockResultSetMetadata mockResultSetMeta = new MockResultSetMetadata();
        Dialect monetDbDialect =
            MockDialect.of(Dialect.DatabaseProduct.MONETDB);
        assertTrue(
            "MonetDB dialect NUMERIC with precision =0, scale = 0"
            + " may be an aggregated decimal, should assume DOUBLE",
            monetDbDialect.getType(
                mockResultSetMeta
                    .withColumnType(Types.NUMERIC)
                    .withPrecision(0)
                    .withScale(0)
                    .build(),
                0) == SqlStatement.Type.DOUBLE);
    }

    public void testJdbcDialectTypeMap() throws SQLException {
        MockResultSetMetadata mockResultSetMeta = new MockResultSetMetadata();
        Dialect postgresDialect = new JdbcDialectImpl();
        assertTrue(
            "JdbcDialectImpl NUMERIC/DECIMAL types w/ precision 0-9"
            + " and scale=0 should return INT",
            postgresDialect.getType(
                mockResultSetMeta
                    .withColumnType(Types.NUMERIC)
                    .withPrecision(5)
                    .withScale(0)
                    .build(),
                0) == SqlStatement.Type.INT);
        assertTrue(
            "JdbcDialectImpl NUMERIC/DECIMAL types w/ precision 0-9"
            + " and scale=0 should return INT",
            postgresDialect.getType(
                mockResultSetMeta
                    .withColumnType(Types.DECIMAL)
                    .withPrecision(5)
                    .withScale(0)
                    .build(),
                0) == SqlStatement.Type.INT);
    }

    public void testMonetBooleanColumn() throws SQLException {
        ResultSetMetaData resultSet = new MockResultSetMetadata()
            .withColumnType(Types.BOOLEAN).build();
        MonetDbDialect monetDbDialect = new MonetDbDialect();
        SqlStatement.Type type = monetDbDialect.getType(resultSet, 0);
        assertEquals(SqlStatement.Type.OBJECT, type);
    }

    public static class MockResultSetMetadata
        extends DelegatingInvocationHandler
    {
        private int precision;
        private int scale;
        private int columnType;
        private String columnName;

        public MockResultSetMetadata withPrecision(int setPrecision) {
            precision = setPrecision;
            return this;
        }

        public MockResultSetMetadata withScale(int setScale) {
            scale = setScale;
            return this;
        }

        public MockResultSetMetadata withColumnType(int setColumnType) {
            columnType = setColumnType;
            return this;
        }

        public MockResultSetMetadata withColumnName(String setColumnName) {
            columnName = setColumnName;
            return this;
        }

        public ResultSetMetaData build() {
            return (ResultSetMetaData) Proxy.newProxyInstance(
                null,
                new Class[] {ResultSetMetaData.class},
                this);
        }

        /** Proxy for {@link ResultSetMetaData#getPrecision(int)}. */
        public int getPrecision(int column) throws SQLException {
            return precision;
        }

        /** Proxy for {@link ResultSetMetaData#getPrecision(int)}. */
        public String getColumnName(int column) throws SQLException {
            return columnName;
        }

        /** Proxy for {@link ResultSetMetaData#getPrecision(int)}. */
        public int getColumnType(int column) throws SQLException {
            return columnType;
        }

        /** Proxy for {@link ResultSetMetaData#getPrecision(int)}. */
        public int getScale(int column) throws SQLException {
            return scale;
        }
    }
}

// End DialectTest.java
