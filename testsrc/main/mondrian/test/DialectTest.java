/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2007-2008 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.test;

import junit.framework.TestCase;
import junit.framework.AssertionFailedError;

import java.sql.Connection;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.util.*;

import mondrian.olap.Util;
import mondrian.spi.Dialect;
import mondrian.spi.impl.JdbcDialectImpl;

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
 * @version $Id$
 * @since May 18, 2007
 */
public class DialectTest extends TestCase {
    private Connection connection;
    private Dialect dialect;

    public DialectTest(String name) {
        super(name);
    }

    protected DataSource getDataSource() {
        TestContext.instance().clearConnection();
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
            dialect = JdbcDialectImpl.create(getDataSource());
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

    public void testAllowsCompoundCountDistinct() {
        String sql =
            dialectize(
                "select count(distinct [customer_id], [product_id])\n" +
                    "from [sales_fact_1997]");
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
                // postgres
                "ERROR: function count\\(integer, integer\\) does not exist",
                // LucidDb
                ".*Invalid number of arguments to function 'COUNT'. Was expecting 1 arguments",
                // teradata
                ".*Syntax error: expected something between the word 'customer_id' and ','\\..*",
            };
            assertQueryFails(sql, errs);
        }
    }

    public void testAllowsCountDistinct() {
        String sql1 = dialectize(
            "select count(distinct [customer_id]) from [sales_fact_1997]");
        // one distinct-count and one nondistinct-agg
        String sql2 = dialectize(
            "select count(distinct [customer_id]),\n" +
                " sum([time_id])\n" +
                "from [sales_fact_1997]");
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
        String sql1 = dialectize(
            "select count(distinct [customer_id]),\n" +
                " count(distinct [time_id])\n" +
                "from [sales_fact_1997]");
        // multiple distinct-counts with group by and other aggs
        String sql3 = dialectize(
            "select [unit_sales],\n" +
                " count(distinct [customer_id]),\n" +
                " count(distinct [product_id])\n" +
                "from [sales_fact_1997]\n" +
                "where [time_id] in (371, 372)\n" +
                "group by [unit_sales]");
        if (getDialect().allowsMultipleCountDistinct()) {
            assertQuerySucceeds(sql1);
            assertQuerySucceeds(sql3);
            assertTrue(getDialect().allowsCountDistinct());
        } else {
            String[] errs = {
                // derby
                "Multiple DISTINCT aggregates are not supported at this time.",
                // access
                "\\[Microsoft\\]\\[ODBC Microsoft Access Driver\\] Syntax error \\(missing operator\\) in query expression '.*'."
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
            stmt = getConnection().createStatement();
            String sql = dialectize("create table [foo] ([i] integer)");
            phase = 1;
            assertFalse(stmt.execute(sql));
            phase = 2;
            sql = dialectize("drop table [foo]");
            assertFalse(stmt.execute(sql));
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
            assertNull(e);
            assertEquals(3, phase);
        } else {
            assertEquals(1, phase);
            assertNotNull(e);
        }
    }

    public void testAllowsFromQuery() {
        String sql =
            dialectize("select * from (select * from [sales_fact_1997]) as [x]");
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
                // postgres
                "ERROR: subquery in FROM must have an alias",
                // teradata
                ".*Syntax error, expected something like a name or a Unicode delimited identifier or an 'UDFCALLNAME' keyword between '\\)' and ';'\\.",
            };
            assertQueryFails(sql, errs);
        } else {
            assertQuerySucceeds(sql);
        }
    }

    public void testRequiresOrderByAlias() {
        String sql =
            dialectize("SELECT [unit_sales]\n" +
                "FROM [sales_fact_1997]\n" +
                "ORDER BY [unit_sales] + [store_id]");
        if (getDialect().requiresOrderByAlias()) {
            assertQueryFails(sql, new String[] {});
        } else {
            assertQuerySucceeds(sql);
        }
    }

    public void testAllowsOrderByAlias() {
        String sql =
            dialectize("SELECT [unit_sales] as [x],\n" +
                " [unit_sales] + [store_id] as [y]\n" +
                "FROM [sales_fact_1997]\n" +
                "ORDER BY [y]");
        if (getDialect().allowsOrderByAlias()) {
            assertQuerySucceeds(sql);
        } else {
            String[] errs = {
                // oracle
                "(?s)ORA-03001: unimplemented feature.*",
                // access
                "\\[Microsoft\\]\\[ODBC Microsoft Access Driver\\] Too few parameters. Expected 1."
            };
            assertQueryFails(sql, errs);
        }
    }

    public void testSupportsGroupByExpressions() {
        String sql =
            dialectize("SELECT sum([unit_sales] + 3) + 8\n" +
                "FROM [sales_fact_1997]\n" +
                "GROUP BY [unit_sales] + [store_id]");
        if (getDialect().supportsGroupByExpressions()) {
            assertQuerySucceeds(sql);
        } else {
            assertQueryFails(
                sql,
                new String[] {
                    "'sum\\(`unit_sales` \\+ 3\\) \\+ 8' isn't in GROUP BY"
                });
        }
    }

    /**
     * Tests that the
     * {@link mondrian.spi.Dialect#supportsGroupingSets()}
     * dialect property is accurate.
     */
    public void testAllowsGroupingSets() {
        String sql =
            dialectize("SELECT [customer_id],\n"
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
                // mysql
                "(?s)You have an error in your SQL syntax; check .*",
                // access
                "(?s)\\[Microsoft\\]\\[ODBC Microsoft Access Driver\\] Syntax error \\(missing operator\\) in query expression 'GROUPING SETS.*",
                // postgres
                "ERROR: syntax error at or near \"SETS\"",
            };
            assertQueryFails(sql, errs);
        }
    }

    public void testSupportsMultiValueInExpr() {
        String sql =
            dialectize("SELECT [unit_sales]\n" +
                "FROM [sales_fact_1997]\n" +
                "WHERE ([unit_sales], [time_id]) IN ((1, 371), (2, 394))");

        boolean supports = getDialect().supportsMultiValueInExpr();

        // Infobright supports this syntax but is too slow for general use so
        // the dialect pretends that it does not.
        switch (getDialect().getDatabaseProduct()) {
        case INFOBRIGHT:
            assertFalse(supports);
            supports = true;
            break;
        }

        if (supports) {
            assertQuerySucceeds(sql);
        } else {
            String[] errs = {
                // derby
                "Syntax error: Encountered \",\" at line 3, column 20.",
                // access
                "\\[Microsoft\\]\\[ODBC Microsoft Access Driver\\] Syntax error \\(comma\\) in query expression '.*'.",
                // teradata
                ".*Syntax error, expected something like a 'SELECT' keyword or '\\(' between '\\(' and the integer '1'\\.",
            };
            assertQueryFails(sql, errs);
        }
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
                            "expected to fail for type=" + type +
                            ", concur=" + concur);
                    }
                } catch (SQLException e) {
                    if (b) {
                        fail("expected to succeed for type=" + type +
                            ", concur=" + concur);
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

        // date value
        final List<String> typeList2 = Arrays.asList("String", "Date");
        assertInline(
            nameList, typeList2,
            new String[]{"a", "2008-04-29"}, new String[]{"b", "2007-01-02"});
    }

    private void assertInline(
        List<String> nameList,
        List<String> typeList,
        String[]... valueList) throws SQLException {
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
        final Dialect.DatabaseProduct databaseProduct =
            getDialect().getDatabaseProduct();
        switch (databaseProduct) {
        case ACCESS:
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
}

// End DialectTest.java
