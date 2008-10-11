/*
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2008-2008 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.spi;

import java.util.List;

/**
 * Description of a SQL dialect.
 *
 * @author jhyde
 * @version $Id$
 * @since Oct 10, 2008
 */
public interface Dialect {
    /**
     * Converts an expression to upper case.
     *
     * <p>For example, for MySQL, {@code toUpper("foo.bar")} returns
     * {@code "UPPER(foo.bar)"}.</p>
     *
     * @param expr SQL expression
     *
     * @return SQL syntax that converts <code>expr</code>
     * into upper case.
     */
    String toUpper(String expr);

    /**
     * Generates a conditional statement in this dialect's syntax.
     *
     * <p>For example, {@code caseWhenElse("b", "1", "0")} returns
     * {@code "case when b then 1 else 0 end"} on Oracle,
     * {@code "Iif(b, 1, 0)"} on Access.
     *
     * @param cond Predicate expression
     * @param thenExpr Expression if condition is true
     * @param elseExpr Expression if condition is false
     * @return Conditional expression
     */
    String caseWhenElse(
        String cond,
        String thenExpr,
        String elseExpr);

    /**
     * Encloses an identifier in quotation marks appropriate for this
     * Dialect.
     *
     * <p>For example,
     * <code>quoteIdentifier("emp")</code> yields a string containing
     * <code>"emp"</code> in Oracle, and a string containing
     * <code>[emp]</code> in Access.
     *
     * @param val Identifier
     *
     * @return Quoted identifier
     */
    String quoteIdentifier(String val);

    /**
     * Appends to a buffer an identifier, quoted appropriately for this
     * Dialect.
     *
     * @param val identifier to quote (must not be null).
     * @param buf Buffer
     */
    void quoteIdentifier(String val, StringBuilder buf);

    /**
     * Encloses an identifier in quotation marks appropriate for the
     * current SQL dialect. For example, in Oracle, where the identifiers
     * are quoted using double-quotes,
     * <code>quoteIdentifier("schema","table")</code> yields a string
     * containing <code>"schema"."table"</code>.
     *
     * @param qual Qualifier. If it is not null,
     *             <code>"<em>qual</em>".</code> is prepended.
     * @param name Name to be quoted.
     * @return Quoted identifier
     */
    String quoteIdentifier(
        String qual,
        String name);

    /**
     * Appends to a buffer a list of identifiers, quoted
     * appropriately for this Dialect.
     *
     * <p>Names in the list may be null, but there must be at least one
     * non-null name in the list.</p>
     *
     * @param buf Buffer
     * @param names List of names to be quoted
     */
    void quoteIdentifier(
        StringBuilder buf,
        String... names);

    /**
     * Returns the character which is used to quote identifiers, or null
     * if quoting is not supported.
     *
     * @return identifier quote
     */
    String getQuoteIdentifierString();

    /**
     * Appends to a buffer a single-quoted SQL string.
     *
     * <p>For example, in the default dialect,
     * <code>quoteStringLiteral(buf, "Can't")</code> appends
     * "<code>'Can''t'</code>" to <code>buf</code>.
     *
     * @param buf Buffer to append to
     * @param s Literal
     */
    void quoteStringLiteral(
        StringBuilder buf,
        String s);

    /**
     * Appends to a buffer a numeric literal.
     *
     * <p>In the default dialect, numeric literals are printed as is.
     *
     * @param buf Buffer to append to
     * @param value Literal
     */
    void quoteNumericLiteral(
        StringBuilder buf,
        String value);

    /**
     * Appends to a buffer a boolean literal.
     *
     * <p>In the default dialect, boolean literals are printed as is.
     *
     * @param buf Buffer to append to
     * @param value Literal
     */
    void quoteBooleanLiteral(
        StringBuilder buf,
        String value);

    /**
     * Appends to a buffer a date literal.
     *
     * <p>For example, in the default dialect,
     * <code>quoteStringLiteral(buf, "1969-03-17")</code>
     * appends <code>DATE '1969-03-17'</code>.
     *
     * @param buf Buffer to append to
     * @param value Literal
     */
    void quoteDateLiteral(
        StringBuilder buf,
        String value);

    /**
     * Appends to a buffer a time literal.
     *
     * <p>For example, in the default dialect,
     * <code>quoteStringLiteral(buf, "12:34:56")</code>
     * appends <code>TIME '12:34:56'</code>.
     *
     * @param buf Buffer to append to
     * @param value Literal
     */
    void quoteTimeLiteral(
        StringBuilder buf,
        String value);

    /**
     * Appends to a buffer a timestamp literal.
     *
     * <p>For example, in the default dialect,
     * <code>quoteStringLiteral(buf, "1969-03-17 12:34:56")</code>
     * appends <code>TIMESTAMP '1969-03-17 12:34:56'</code>.
     *
     * @param buf Buffer to append to
     * @param value Literal
     */
    void quoteTimestampLiteral(
        StringBuilder buf,
        String value);

    /**
     * Returns whether this Dialect requires subqueries in the FROM clause
     * to have an alias.
     *
     * @see #allowsFromQuery()
     *
     * @return whether dialewct requires subqueries to have an alias
     */
    boolean requiresAliasForFromQuery();

    /**
     * Returns whether the SQL dialect allows "AS" in the FROM clause.
     * If so, "SELECT * FROM t AS alias" is a valid query.
     *
     * @return whether dialect allows AS in FROM clause
     */
    boolean allowsAs();

    /**
     * Returns whether this Dialect allows a subquery in the from clause,
     * for example
     *
     * <blockquote><code>SELECT * FROM (SELECT * FROM t) AS x</code></blockquote>
     *
     * @see #requiresAliasForFromQuery()
     *
     * @return whether Dialect allows subquery in FROM clause
     */
    boolean allowsFromQuery();

    /**
     * Returns whether this Dialect allows multiple arguments to the
     * <code>COUNT(DISTINCT ...) aggregate function, for example
     *
     * <blockquote><code>SELECT COUNT(DISTINCT x, y) FROM t</code></blockquote>
     *
     * @see #allowsCountDistinct()
     * @see #allowsMultipleCountDistinct()
     *
     * @return whether Dialect allows multiple arguments to COUNT DISTINCT
     */
    boolean allowsCompoundCountDistinct();

    /**
     * Returns whether this Dialect supports distinct aggregations.
     *
     * <p>For example, Access does not allow
     * <blockquote>
     * <code>select count(distinct x) from t</code>
     * </blockquote>
     *
     * @return whether Dialect allows COUNT DISTINCT
     */
    boolean allowsCountDistinct();

    /**
     * Returns whether this Dialect supports more than one distinct
     * aggregation in the same query.
     *
     * <p>In Derby 10.1,
     * <blockquote>
     *   <code>select couunt(distinct x) from t</code>
     * </blockquote>
     * is OK, but
     * <blockquote>
     *   <code>select couunt(distinct x), count(distinct y) from t</code>
     * </blockquote>
     * gives "Multiple DISTINCT aggregates are not supported at this time."
     *
     * @return whether this Dialect supports more than one distinct
     * aggregation in the same query
     */
    boolean allowsMultipleCountDistinct();

    /**
     * Returns whether this Dialect has performant support of distinct SQL
     * measures in the same query.
     *
     * @return whether this dialect supports multiple count(distinct subquery)
     * measures in one query.
     */
    boolean allowsMultipleDistinctSqlMeasures();

    /**
     * Generates a SQL statement to represent an inline dataset.
     *
     * <p>For example, for Oracle, generates
     *
     * <pre>
     * SELECT 1 AS FOO, 'a' AS BAR FROM dual
     * UNION ALL
     * SELECT 2 AS FOO, 'b' AS BAR FROM dual
     * </pre>
     *
     * <p>For ANSI SQL, generates:
     *
     * <pre>
     * VALUES (1, 'a'), (2, 'b')
     * </pre>
     *
     * @param columnNames List of column names
     * @param columnTypes List of column types ("String" or "Numeric")
     * @param valueList List of rows values
     * @return SQL string
     */
    String generateInline(
        List<String> columnNames,
        List<String> columnTypes,
        List<String[]> valueList);

    /**
     * If Double values need to include additional exponent in its string
     * represenation. This is to make sure that Double literals will be
     * interpreted as doubles by LucidDB.
     *
     * @param value Double value to generate string for
     * @param valueString java string representation for this value.
     * @return whether an additional exponent "E0" needs to be appended
     *
     */
    boolean needsExponent(Object value, String valueString);

    /**
     * Appends to a buffer a value quoted for its type.
     *
     * @param buf Buffer to append to
     * @param value Value
     * @param datatype Datatype of value
     */
    void quote(
        StringBuilder buf,
        Object value,
        Datatype datatype);

    /**
     * Returns whether this dialect supports common SQL Data Definition
     * Language (DDL) statements such as <code>CREATE TABLE</code> and
     * <code>DROP INDEX</code>.
     *
     * <p>Access seems to allow DDL iff the .mdb file is writeable.
     *
     * @see java.sql.DatabaseMetaData#isReadOnly()
     *
     * @return whether this Dialect supports DDL
     */
    boolean allowsDdl();

    /**
     * Returns whether NULL values appear last when sorted using ORDER BY.
     * According to the SQL standard, this is implementation-specific.
     *
     * @return Whether NULL values collate last
     */
    boolean isNullsCollateLast();

    /**
     * Modifies an expression in the ORDER BY clause to ensure that NULL
     * values collate after all non-NULL values.
     * If {@link #isNullsCollateLast()} is true, there's nothing to do.
     *
     * @param expr Expression
     * @return Expression modified so that NULL values collate last
     */
    String forceNullsCollateLast(String expr);

    /**
     * Returns whether this Dialect supports expressions in the GROUP BY
     * clause. Derby/Cloudscape do not.
     *
     * @return Whether this Dialect allows expressions in the GROUP BY
     *   clause
     */
    boolean supportsGroupByExpressions();

    /**
     * Returns whether this Dialect allows the GROUPING SETS construct in
     * the GROUP BY clause. Currently Oracle, DB2 and Teradata.
     *
     * @return Whether this Dialect allows GROUPING SETS clause
     */
    boolean supportsGroupingSets();

    /**
     * Returns whether this Dialect places no limit on the number
     * of rows which can appear as elements of an IN or VALUES
     * expression.
     *
     * @return whether value list length is unlimited
     */
    boolean supportsUnlimitedValueList();

    /**
     * Returns true if this Dialect can include expressions in the ORDER BY
     * clause only by adding an expression to the SELECT clause and using
     * its alias.
     *
     * <p>For example, in such a dialect,
     * <blockquote>
     * <code>SELECT x FROM t ORDER BY x + y</code>
     * </blockquote>
     * would be illegal, but
     * <blockquote>
     * <code>SELECT x, x + y AS z FROM t ORDER BY z</code>
     * </blockquote>
     *
     * would be legal.</p>
     *
     * <p>MySQL, DB2 and Ingres are examples of such dialects.</p>
     *
     * @return Whether this Dialect can include expressions in the ORDER BY
     *   clause only by adding an expression to the SELECT clause and using
     *   its alias
     */
    boolean requiresOrderByAlias();

    /**
     * Returns true if aliases defined in the SELECT clause can be used as
     * expressions in the ORDER BY clause.
     *
     * <p>For example, in such a dialect,
     * <blockquote>
     * <code>SELECT x, x + y AS z FROM t ORDER BY z</code>
     * </blockquote>
     *
     * would be legal.</p>
     *
     * <p>MySQL, DB2 and Ingres are examples of dialects where this is true;
     * Access is a dialect where this is false.</p>
     *
     * @return Whether aliases defined in the SELECT clause can be used as
     * expressions in the ORDER BY clause.
     */
    boolean allowsOrderByAlias();

    /**
     * Returns true if this dialect supports multi-value IN expressions.
     * E.g.,
     *
     * <code>WHERE (col1, col2) IN ((val1a, val2a), (val1b, val2b))</code>
     *
     * @return true if the dialect supports multi-value IN expressions
     */
    boolean supportsMultiValueInExpr();

    /**
     * Returns whether this Dialect supports the given concurrency type
     * in combination with the given result set type.
     *
     * <p>The result is similar to
     * {@link java.sql.DatabaseMetaData#supportsResultSetConcurrency(int, int)},
     * except that the JdbcOdbc bridge in JDK 1.6 overstates its abilities.
     * See bug 1690406.
     *
     * @param type defined in {@link java.sql.ResultSet}
     * @param concurrency type defined in {@link java.sql.ResultSet}
     * @return <code>true</code> if so; <code>false</code> otherwise
     */
    boolean supportsResultSetConcurrency(
        int type,
        int concurrency);

    /**
     * Returns the maximum length of the name of a database column or query
     * alias allowed by this dialect.
     *
     * @see java.sql.DatabaseMetaData#getMaxColumnNameLength()
     *
     * @return maximum number of characters in a column name
     */
    int getMaxColumnNameLength();

    /**
     * Returns the database for this Dialect, or
     * {@link mondrian.spi.Dialect.DatabaseProduct#UNKNOWN} if the database is
     * not a common database.
     *
     * @return Database
     */
    DatabaseProduct getDatabaseProduct();

    /**
     * Enumeration of common database types.
     *
     * <p>Branching on this enumeration allows you to write code which behaves
     * differently for different databases. However, since the capabilities of
     * a database can change between versions, it is recommended that
     * conditional code is in terms of capabilities methods in
     * {@link mondrian.spi.Dialect}.
     *
     * <p>Because there are so many differences between various versions and
     * ports of DB2, we represent them as 3 separate products. If you want to
     * treat them all as one product, note that the {@link #getFamily()} method
     * for {@link #DB2_AS400} and {@link #DB2_OLD_AS400} returns {@link #DB2}.
     */
    enum DatabaseProduct {
        ACCESS,
        UNKNOWN,
        DERBY,
        DB2_OLD_AS400,
        DB2_AS400,
        DB2,
        FIREBIRD,
        INFORMIX,
        INGRES,
        INTERBASE,
        LUCIDDB,
        MSSQL,
        ORACLE,
        POSTGRES,
        MYSQL,
        SYBASE,
        TERADATA;

        /**
         * Return the root of the family of products this database product
         * belongs to.
         *
         * <p>For {@link #DB2_AS400} and {@link #DB2_OLD_AS400} returns
         * {@link #DB2}; for all other database products, returns the same
         * product.
         *
         * @return root of family of database products
         */
        public DatabaseProduct getFamily() {
            switch (this) {
            case DB2_OLD_AS400:
            case DB2_AS400:
                return DB2;
            default:
                return this;
            }
        }
    }

    /**
     * Datatype of a column.
     */
    enum Datatype {
        String {
            public void quoteValue(
                StringBuilder buf, Dialect dialect, String value)
            {
                dialect.quoteStringLiteral(buf, value);
            }
        },

        Numeric {
            public void quoteValue(
                StringBuilder buf, Dialect dialect, String value)
            {
                dialect.quoteNumericLiteral(buf, value);
            }

            public boolean isNumeric() {
                return true;
            }
        },

        Integer {
            public void quoteValue(
                StringBuilder buf, Dialect dialect, String value)
            {
                dialect.quoteNumericLiteral(buf, value);
            }

            public boolean isNumeric() {
                return true;
            }
        },

        Boolean {
            public void quoteValue(
                StringBuilder buf, Dialect dialect, String value)
            {
                dialect.quoteBooleanLiteral(buf, value);
            }
        },

        Date {
            public void quoteValue(
                StringBuilder buf, Dialect dialect, String value)
            {
                dialect.quoteDateLiteral(buf, value);
            }
        },

        Time {
            public void quoteValue(
                StringBuilder buf, Dialect dialect, String value)
            {
                dialect.quoteTimeLiteral(buf, value);
            }
        },

        Timestamp {
            public void quoteValue(
                StringBuilder buf, Dialect dialect, String value)
            {
                dialect.quoteTimestampLiteral(buf, value);
            }
        };

        /**
         * Appends to a buffer a value of this type, in the appropriate format
         * for this dialect.
         *
         * @param buf Buffer
         * @param dialect Dialect
         * @param value Value
         */
        public abstract void quoteValue(
            StringBuilder buf,
            Dialect dialect,
            String value);

        /**
         * Returns whether this is a numeric datatype.
         *
         * @return whether this is a numeric datatype.
         */
        public boolean isNumeric() {
            return false;
        }
    }
}

// End Dialect.java
