/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2008-2009 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.spi.impl;

import mondrian.olap.Util;
import mondrian.olap.MondrianDef;
import mondrian.spi.*;

import java.util.*;
import java.sql.*;
import java.sql.Date;

/**
 * Implementation of {@link Dialect} based on a JDBC connection and metadata.
 *
 * <p>If you are writing a class for a specific database dialect, we recommend
 * that you use this as a base class, so your dialect class will be
 * forwards-compatible. If methods are added to {@link Dialect} in future
 * revisions, default implementations of those methods will be added to this
 * class.</p>
 *
 * <p>Mondrian uses JdbcDialectImpl as a fallback if it cannot find a more
 * specific dialect. JdbcDialectImpl reads properties from the JDBC driver's
 * metadata, so can deduce some of the dialect's behavior.</p>
 *
 * @author jhyde
 * @version $Id$
 * @since Oct 10, 2008
 */
public class JdbcDialectImpl implements Dialect {
    /**
     * String used to quote identifiers.
     */
    private final String quoteIdentifierString;

    /**
     * Product name per JDBC driver.
     */
    private final String productName;

    /**
     * Product version per JDBC driver.
     */
    protected final String productVersion;

    /**
     * Supported result set types.
     */
    private final Set<List<Integer>> supportedResultSetTypes;

    /**
     * Whether database is read-only
     */
    private final boolean readOnly;

    /**
     * Maximum column name length
     */
    private final int maxColumnNameLength;

    /**
     * Indicates whether the database allows selection of columns
     * not listed in the group by clause.
     */
    protected boolean permitsSelectNotInGroupBy;

    /**
     * Major database product (or null if product is not a common one)
     */
    protected final DatabaseProduct databaseProduct;

    private static final int[] RESULT_SET_TYPE_VALUES = {
        ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.TYPE_SCROLL_INSENSITIVE,
        ResultSet.TYPE_SCROLL_SENSITIVE};

    private static final int[] CONCURRENCY_VALUES = {
        ResultSet.CONCUR_READ_ONLY,
        ResultSet.CONCUR_UPDATABLE};

    /**
     * The size required to add quotes around a string - this ought to be
     * large enough to prevent a reallocation.
     */
    private static final int SINGLE_QUOTE_SIZE = 10;
    /**
     * Two strings are quoted and the character '.' is placed between them.
     */
    private static final int DOUBLE_QUOTE_SIZE = 2 * SINGLE_QUOTE_SIZE + 1;

    /**
     * Creates a JdbcDialectImpl.
     *
     * <p>To prevent connection leaks, this constructor does not hold a
     * reference to the connection after the call returns. It makes a copy of
     * everything useful during the call.  Derived classes must do the
     * same.</p>
     *
     * @param connection Connection
     */
    public JdbcDialectImpl(
        Connection connection)
        throws SQLException
    {
        final DatabaseMetaData metaData = connection.getMetaData();
        this.quoteIdentifierString = deduceIdentifierQuoteString(metaData);
        this.productName = deduceProductName(metaData);
        this.productVersion = deduceProductVersion(metaData);
        this.supportedResultSetTypes = deduceSupportedResultSetStyles(metaData);
        this.readOnly = deduceReadOnly(metaData);
        this.maxColumnNameLength = deduceMaxColumnNameLength(metaData);
        this.databaseProduct =
            getProduct(this.productName, this.productVersion);
        this.permitsSelectNotInGroupBy =
            deduceSupportsSelectNotInGroupBy(connection);
    }

    public DatabaseProduct getDatabaseProduct() {
        return databaseProduct;
    }

    public void appendHintsAfterFromClause(
        StringBuilder buf,
        Map<String, String> hints)
    {
        // Hints are always dialect-specific, so the default is a no-op
    }

    public boolean allowsDialectSharing() {
        return true;
    }

    protected int deduceMaxColumnNameLength(DatabaseMetaData databaseMetaData) {
        try {
            return databaseMetaData.getMaxColumnNameLength();
        } catch (SQLException e) {
            throw Util.newInternal(
                e,
                "while detecting maxColumnNameLength");
        }
    }

    protected boolean deduceReadOnly(DatabaseMetaData databaseMetaData) {
        try {
            return databaseMetaData.isReadOnly();
        } catch (SQLException e) {
            throw Util.newInternal(
                e,
                "while detecting isReadOnly");
        }
    }

    protected String deduceProductName(DatabaseMetaData databaseMetaData) {
        try {
            return databaseMetaData.getDatabaseProductName();
        } catch (SQLException e) {
            throw Util.newInternal(e, "while detecting database product");
        }
    }

    protected String deduceIdentifierQuoteString(
        DatabaseMetaData databaseMetaData)
    {
        try {
            final String quoteIdentifierString =
                databaseMetaData.getIdentifierQuoteString();
            return "".equals(quoteIdentifierString)
                // quoting not supported
                ? null
                : quoteIdentifierString;
        } catch (SQLException e) {
            throw Util.newInternal(e, "while quoting identifier");
        }
    }

    protected String deduceProductVersion(DatabaseMetaData databaseMetaData) {
        String productVersion;
        try {
            productVersion = databaseMetaData.getDatabaseProductVersion();
        } catch (SQLException e11) {
            throw Util.newInternal(
                e11,
                "while detecting database product version");
        }
        return productVersion;
    }

    protected Set<List<Integer>> deduceSupportedResultSetStyles(
        DatabaseMetaData databaseMetaData)
    {
        Set<List<Integer>> supports = new HashSet<List<Integer>>();
        try {
            for (int type : RESULT_SET_TYPE_VALUES) {
                for (int concurrency : CONCURRENCY_VALUES) {
                    if (databaseMetaData.supportsResultSetConcurrency(
                            type, concurrency))
                    {
                        String driverName =
                            databaseMetaData.getDriverName();
                        if (type != ResultSet.TYPE_FORWARD_ONLY
                            && driverName.equals(
                                "JDBC-ODBC Bridge (odbcjt32.dll)"))
                        {
                            // In JDK 1.6, the Jdbc-Odbc bridge announces
                            // that it can handle TYPE_SCROLL_INSENSITIVE
                            // but it does so by generating a 'COUNT(*)'
                            // query, and this query is invalid if the query
                            // contains a single-quote. So, override the
                            // driver.
                            continue;
                        }
                        supports.add(
                            new ArrayList<Integer>(
                                Arrays.asList(type, concurrency)));
                    }
                }
            }
        } catch (SQLException e11) {
            throw Util.newInternal(
                e11,
                "while detecting result set concurrency");
        }
        return supports;
    }

     /**
      * <p>Detects whether the database is configured to permit queries
      * that include columns in the SELECT that are not also in the GROUP BY.
      * MySQL is an example of one that does, though this is configurable.</p>
      *
      * <p>The expectation is that this will not change while Mondrian is
      * running, though some databases (MySQL) allow changing it on the fly.</p>
      *
      * @param conn The database connection
      * @return Whether the feature is enabled.
      * @throws SQLException
      */
    protected boolean deduceSupportsSelectNotInGroupBy(Connection conn)
        throws SQLException
    {
        // Most simply don't support it
        return false;
    }

    public String toUpper(String expr) {
        return "UPPER(" + expr + ")";
    }

    public String caseWhenElse(String cond, String thenExpr, String elseExpr) {
        return "CASE WHEN " + cond + " THEN " + thenExpr + " ELSE " + elseExpr
            + " END";
    }

    public String quoteIdentifier(final String val) {
        int size = val.length() + SINGLE_QUOTE_SIZE;
        StringBuilder buf = new StringBuilder(size);

        quoteIdentifier(val, buf);

        return buf.toString();
    }

    public void quoteIdentifier(final String val, final StringBuilder buf) {
        String q = getQuoteIdentifierString();
        if (q == null) {
            // quoting is not supported
            buf.append(val);
            return;
        }
        // if the value is already quoted, do nothing
        //  if not, then check for a dot qualified expression
        //  like "owner.table".
        //  In that case, prefix the single parts separately.
        if (val.startsWith(q) && val.endsWith(q)) {
            // already quoted - nothing to do
            buf.append(val);
            return;
        }

        int k = val.indexOf('.');
        if (k > 0) {
            // qualified
            String val1 = Util.replace(val.substring(0, k), q, q + q);
            String val2 = Util.replace(val.substring(k + 1), q, q + q);
            buf.append(q);
            buf.append(val1);
            buf.append(q);
            buf.append(".");
            buf.append(q);
            buf.append(val2);
            buf.append(q);

        } else {
            // not Qualified
            String val2 = Util.replace(val, q, q + q);
            buf.append(q);
            buf.append(val2);
            buf.append(q);
        }
    }

    public String quoteIdentifier(final String qual, final String name) {
        // We know if the qalifier is null, then only the name is going
        // to be quoted.
        int size = name.length()
            + ((qual == null)
                ? SINGLE_QUOTE_SIZE
                : (qual.length() + DOUBLE_QUOTE_SIZE));
        StringBuilder buf = new StringBuilder(size);

        quoteIdentifier(buf, qual, name);

        return buf.toString();
    }

    public void quoteIdentifier(
        final StringBuilder buf,
        final String... names)
    {
        int nonNullNameCount = 0;
        for (String name : names) {
            if (name == null) {
                continue;
            }
            if (nonNullNameCount > 0) {
                buf.append('.');
            }
            assert name.length() > 0
                : "name should probably be null, not empty";
            quoteIdentifier(name, buf);
            ++nonNullNameCount;
        }
    }

    public String getQuoteIdentifierString() {
        return quoteIdentifierString;
    }

    public void quoteStringLiteral(
        StringBuilder buf,
        String s)
    {
        Util.singleQuoteString(s, buf);
    }

    public void quoteNumericLiteral(
        StringBuilder buf,
        String value)
    {
        buf.append(value);
    }

    public void quoteBooleanLiteral(StringBuilder buf, String value) {
        // NOTE jvs 1-Jan-2007:  See quoteDateLiteral for explanation.
        // In addition, note that we leave out UNKNOWN (even though
        // it is a valid SQL:2003 literal) because it's really
        // NULL in disguise, and NULL is always treated specially.
        if (!value.equalsIgnoreCase("TRUE")
            && !(value.equalsIgnoreCase("FALSE")))
        {
            throw new NumberFormatException(
                "Illegal BOOLEAN literal:  " + value);
        }
        buf.append(value);
    }

    public void quoteDateLiteral(StringBuilder buf, String value) {
        // NOTE jvs 1-Jan-2007: Check that the supplied literal is in valid
        // SQL:2003 date format.  A hack in
        // RolapSchemaReader.lookupMemberChildByName looks for
        // NumberFormatException to suppress it, so that is why
        // we convert the exception here.
        final Date date;
        try {
            date = Date.valueOf(value);
        } catch (IllegalArgumentException ex) {
            throw new NumberFormatException(
                "Illegal DATE literal:  " + value);
        }
        quoteDateLiteral(buf, value, date);
    }

    /**
     * Helper method for {@link #quoteDateLiteral(StringBuilder, String)}.
     *
     * @param buf Buffer to append to
     * @param value Value as string
     * @param date Value as date
     */
    protected void quoteDateLiteral(
        StringBuilder buf,
        String value,
        Date date)
    {
        // SQL:2003 date format: DATE '2008-01-23'.
        buf.append("DATE ");
        Util.singleQuoteString(value, buf);
    }

    public void quoteTimeLiteral(StringBuilder buf, String value) {
        // NOTE jvs 1-Jan-2007:  See quoteDateLiteral for explanation.
        try {
            Time.valueOf(value);
        } catch (IllegalArgumentException ex) {
            throw new NumberFormatException(
                "Illegal TIME literal:  " + value);
        }
        buf.append("TIME ");
        Util.singleQuoteString(value, buf);
    }

    public void quoteTimestampLiteral(
        StringBuilder buf,
        String value)
    {
        // NOTE jvs 1-Jan-2007:  See quoteTimestampLiteral for explanation.
        try {
            Timestamp.valueOf(value);
        } catch (IllegalArgumentException ex) {
            throw new NumberFormatException(
                "Illegal TIMESTAMP literal:  " + value);
        }
        buf.append("TIMESTAMP ");
        Util.singleQuoteString(value, buf);
    }

    public boolean requiresAliasForFromQuery() {
        return false;
    }

    public boolean allowsAs() {
        return true;
    }

    public boolean allowsFromQuery() {
        return true;
    }

    public boolean allowsCompoundCountDistinct() {
        return false;
    }

    public boolean allowsCountDistinct() {
        return true;
    }

    public boolean allowsMultipleCountDistinct() {
        return allowsCountDistinct();
    }

    public boolean allowsMultipleDistinctSqlMeasures() {
        return allowsMultipleCountDistinct();
    }

    public String generateInline(
        List<String> columnNames,
        List<String> columnTypes,
        List<String[]> valueList)
    {
        return generateInlineForAnsi(
            "t", columnNames, columnTypes, valueList, false);
    }

    /**
     * Generic algorithm to generate inline values list,
     * using an optional FROM clause, specified by the caller of this
     * method, appropriate to the dialect of SQL.
     *
     * @param columnNames Column names
     * @param columnTypes Column types
     * @param valueList List rows
     * @param fromClause FROM clause, or null
     * @param cast Whether to cast the values in the first row
     * @return Expression that returns the given values
     */
    protected String generateInlineGeneric(
        List<String> columnNames,
        List<String> columnTypes,
        List<String[]> valueList,
        String fromClause,
        boolean cast)
    {
        final StringBuilder buf = new StringBuilder();
        int columnCount = columnNames.size();
        assert columnTypes.size() == columnCount;

        // Some databases, e.g. Teradata, derives datatype from value of column
        // in first row, and truncates subsequent rows. Therefore, we need to
        // cast every value to the correct length. Figure out the maximum length
        // now.
        Integer[] maxLengths = new Integer[columnCount];
        if (cast) {
            for (int i = 0; i < columnTypes.size(); i++) {
                String columnType = columnTypes.get(i);
                Datatype datatype = Datatype.valueOf(columnType);
                if (datatype == Datatype.String) {
                    int maxLen = -1;
                    for (String[] strings : valueList) {
                        if (strings[i] != null
                            && strings[i].length() > maxLen)
                        {
                            maxLen = strings[i].length();
                        }
                    }
                    maxLengths[i] = maxLen;
                }
            }
        }

        for (int i = 0; i < valueList.size(); i++) {
            if (i > 0) {
                buf.append(" union all ");
            }
            String[] values = valueList.get(i);
            buf.append("select ");
            for (int j = 0; j < values.length; j++) {
                String value = values[j];
                if (j > 0) {
                    buf.append(", ");
                }
                final String columnType = columnTypes.get(j);
                final String columnName = columnNames.get(j);
                Datatype datatype = Datatype.valueOf(columnType);
                final Integer maxLength = maxLengths[j];
                if (maxLength != null) {
                    // Generate CAST for Teradata.
                    buf.append("CAST(");
                    quote(buf, value, datatype);
                    buf.append(" AS VARCHAR(").append(maxLength).append("))");
                } else {
                    quote(buf, value, datatype);
                }
                if (allowsAs()) {
                    buf.append(" as ");
                } else {
                    buf.append(' ');
                }
                quoteIdentifier(columnName, buf);
            }
            if (fromClause != null) {
                buf.append(fromClause);
            }
        }
        return buf.toString();
    }

    /**
     * Generates inline values list using ANSI 'VALUES' syntax.
     * For example,
     *
     * <blockquote><code>SELECT * FROM
     *   (VALUES (1, 'a'), (2, 'b')) AS t(x, y)</code></blockquote>
     *
     * <p>If NULL values are present, we use a CAST to ensure that they
     * have the same type as other columns:
     *
     * <blockquote><code>SELECT * FROM
     * (VALUES (1, 'a'), (2, CASE(NULL AS VARCHAR(1)))) AS t(x, y)
     * </code></blockquote>
     *
     * <p>This syntax is known to work on Derby, but not Oracle 10 or
     * Access.
     *
     * @param alias Table alias
     * @param columnNames Column names
     * @param columnTypes Column types
     * @param valueList List rows
     * @param cast Whether to generate casts
     * @return Expression that returns the given values
     */
    public String generateInlineForAnsi(
        String alias,
        List<String> columnNames,
        List<String> columnTypes,
        List<String[]> valueList,
        boolean cast)
    {
        final StringBuilder buf = new StringBuilder();
        buf.append("SELECT * FROM (VALUES ");
        // Derby pads out strings to a common length, so we cast the
        // string values to avoid this.  Determine the cast type for each
        // column.
        String[] castTypes = null;
        if (cast) {
            castTypes = new String[columnNames.size()];
            for (int i = 0; i < columnNames.size(); i++) {
                String columnType = columnTypes.get(i);
                if (columnType.equals("String")) {
                    castTypes[i] =
                        guessSqlType(columnType, valueList, i);
                }
            }
        }
        for (int i = 0; i < valueList.size(); i++) {
            if (i > 0) {
                buf.append(", ");
            }
            String[] values = valueList.get(i);
            buf.append("(");
            for (int j = 0; j < values.length; j++) {
                String value = values[j];
                if (j > 0) {
                    buf.append(", ");
                }
                final String columnType = columnTypes.get(j);
                Datatype datatype = Datatype.valueOf(columnType);
                if (value == null) {
                    String sqlType =
                        guessSqlType(columnType, valueList, j);
                    buf.append("CAST(NULL AS ")
                        .append(sqlType)
                        .append(")");
                } else if (cast && castTypes[j] != null) {
                    buf.append("CAST(");
                    quote(buf, value, datatype);
                    buf.append(" AS ")
                        .append(castTypes[j])
                        .append(")");
                } else {
                    quote(buf, value, datatype);
                }
            }
            buf.append(")");
        }
        buf.append(") AS ");
        quoteIdentifier(alias, buf);
        buf.append(" (");
        for (int j = 0; j < columnNames.size(); j++) {
            final String columnName = columnNames.get(j);
            if (j > 0) {
                buf.append(", ");
            }
            quoteIdentifier(columnName, buf);
        }
        buf.append(")");
        return buf.toString();
    }

    public boolean needsExponent(Object value, String valueString) {
        return false;
    }

    public void quote(
        StringBuilder buf,
        Object value,
        Datatype datatype)
    {
        if (value == null) {
            buf.append("null");
        } else {
            String valueString = value.toString();
            if (needsExponent(value, valueString)) {
                valueString += "E0";
            }
            datatype.quoteValue(buf, this, valueString);
        }
    }

    /**
     * Guesses the type of a column based upon (a) its basic type,
     * (b) a list of values.
     *
     * @param basicType Basic type
     * @param valueList Value list
     * @param column Column ordinal
     * @return SQL type
     */
    private static String guessSqlType(
        String basicType,
        List<String[]> valueList,
        int column)
    {
        if (basicType.equals("String")) {
            int maxLen = 1;
            for (String[] values : valueList) {
                final String value = values[column];
                if (value == null) {
                    continue;
                }
                maxLen = Math.max(maxLen, value.length());
            }
            return "VARCHAR(" + maxLen + ")";
        } else {
            return "INTEGER";
        }
    }

    public boolean allowsDdl() {
        return !readOnly;
    }

    public NullCollation getNullCollation() {
        return NullCollation.POSINF;
    }

    public String generateOrderItem(
        String expr,
        boolean nullable,
        boolean ascending)
    {
        if (nullable) {
            NullCollation collateLast = getNullCollation();
            switch (collateLast) {
            case NEGINF:
                // For DESC, NULLs already appear last.
                // For ASC, we need to reverse the order.
                // Use the SQL standard syntax 'ORDER BY x ASC NULLS LAST'.
                if (ascending) {
                    return expr + " ASC NULLS LAST";
                } else {
                    return expr + " DESC";
                }
            case POSINF:
                if (ascending) {
                    return expr + " ASC";
                } else {
                    return expr + " DESC NULLS LAST";
                }
            default:
                throw Util.unexpected(collateLast);
            }
        } else {
            if (ascending) {
                return expr + " ASC";
            } else {
                return expr + " DESC";
            }
        }
    }

    public boolean supportsGroupByExpressions() {
        return true;
    }

    public boolean allowsSelectNotInGroupBy() {
        return permitsSelectNotInGroupBy;
    }

    public boolean supportsGroupingSets() {
        return false;
    }

    public boolean supportsUnlimitedValueList() {
        return false;
    }

    public boolean requiresGroupByAlias() {
        return false;
    }

    public boolean requiresOrderByAlias() {
        return false;
    }

    public boolean allowsOrderByAlias() {
        return requiresOrderByAlias();
    }

    public boolean requiresUnionOrderByOrdinal() {
        return false;
    }

    public boolean requiresUnionOrderByExprToBeInSelectClause() {
        return false;
    }

    public boolean supportsMultiValueInExpr() {
        return false;
    }

    public boolean supportsResultSetConcurrency(
        int type,
        int concurrency)
    {
        return supportedResultSetTypes.contains(
            Arrays.asList(type, concurrency));
    }

    public String toString() {
        return productName;
    }

    public int getMaxColumnNameLength() {
        return maxColumnNameLength;
    }

    /**
     * Converts a product name and version (per the JDBC driver) into a product
     * enumeration.
     *
     * @param productName Product name
     * @param productVersion Product version
     * @return database product
     */
    public static DatabaseProduct getProduct(
        String productName,
        String productVersion)
    {
        if (productName.equals("ACCESS")) {
            return DatabaseProduct.ACCESS;
        } else if (productName.trim().toUpperCase().equals("APACHE DERBY")) {
            return DatabaseProduct.DERBY;
        } else if (productName.trim().toUpperCase().equals("DBMS:CLOUDSCAPE")) {
            return DatabaseProduct.DERBY;
        } else if (productName.startsWith("DB2")) {
            if (productName.startsWith("DB2 UDB for AS/400")) {
                // TB "04.03.0000 V4R3m0"
                // this version cannot handle subqueries and is considered "old"
                // DEUKA "05.01.0000 V5R1m0" is ok
                String[] version_release = productVersion.split("\\.", 3);
                /*
                if (version_release.length > 2 &&
                    "04".compareTo(version_release[0]) > 0 ||
                    ("04".compareTo(version_release[0]) == 0
                    && "03".compareTo(version_release[1]) >= 0))
                    return true;
                */
                // assume, that version <= 04 is "old"
                if ("04".compareTo(version_release[0]) >= 0) {
                    return DatabaseProduct.DB2_OLD_AS400;
                } else {
                    return DatabaseProduct.DB2_AS400;
                }
            } else {
                // DB2 on NT returns "DB2/NT"
                return DatabaseProduct.DB2;
            }
        } else if (productName.toUpperCase().indexOf("FIREBIRD") >= 0) {
            return DatabaseProduct.FIREBIRD;
        } else if (productName.startsWith("Informix")) {
            return DatabaseProduct.INFORMIX;
        } else if (productName.toUpperCase().equals("INGRES")) {
            return DatabaseProduct.INGRES;
        } else if (productName.equals("Interbase")) {
            return DatabaseProduct.INTERBASE;
        } else if (productName.toUpperCase().equals("LUCIDDB")) {
            return DatabaseProduct.LUCIDDB;
        } else if (productName.toUpperCase().indexOf("SQL SERVER") >= 0) {
            return DatabaseProduct.MSSQL;
        } else if (productName.equals("Oracle")) {
            return DatabaseProduct.ORACLE;
        } else if (productName.toUpperCase().indexOf("POSTGRE") >= 0) {
            return DatabaseProduct.POSTGRESQL;
        } else if (productName.toUpperCase().indexOf("NETEZZA") >= 0) {
            return DatabaseProduct.NETEZZA;
        } else if (productName.toUpperCase().equals("MYSQL (INFOBRIGHT)")) {
            return DatabaseProduct.INFOBRIGHT;
        } else if (productName.toUpperCase().equals("MYSQL")) {
            return DatabaseProduct.MYSQL;
        } else if (productName.startsWith("HP Neoview")) {
            return DatabaseProduct.NEOVIEW;
        } else if (productName.toUpperCase().indexOf("SYBASE") >= 0) {
            return DatabaseProduct.SYBASE;
        } else if (productName.toUpperCase().indexOf("TERADATA") >= 0) {
            return DatabaseProduct.TERADATA;
        } else {
            return DatabaseProduct.UNKNOWN;
        }
    }
}

// End JdbcDialectImpl.java
