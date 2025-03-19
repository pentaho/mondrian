/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/

package mondrian.spi.impl;

import mondrian.olap.Util;
import mondrian.rolap.SqlStatement;
import mondrian.spi.DialectUtil;

import java.sql.*;
import java.util.List;
import java.util.regex.*;

/**
 * Implementation of {@link mondrian.spi.Dialect} for the Oracle database.
 *
 * @author jhyde
 * @since Nov 23, 2008
 */
public class OracleDialect extends JdbcDialectImpl {

    private final String escapeRegexp = "(\\\\Q([^\\\\Q]+)\\\\E)";
    private final Pattern escapePattern = Pattern.compile(escapeRegexp);

    public static final JdbcDialectFactory FACTORY =
        new JdbcDialectFactory(
            OracleDialect.class,
            DatabaseProduct.ORACLE);

    /**
     * Creates an OracleDialect.
     *
     * @param connection Connection
     */
    public OracleDialect(Connection connection) throws SQLException {
        super(connection);
    }

    public OracleDialect() {
    }

    public boolean allowsAs() {
        return false;
    }

    public String generateInline(
        List<String> columnNames,
        List<String> columnTypes,
        List<String[]> valueList)
    {
        return generateInlineGeneric(
            columnNames, columnTypes, valueList,
            " from dual", false);
    }

    public boolean supportsGroupingSets() {
        return true;
    }

    @Override
    public String generateOrderByNulls(
        String expr,
        boolean ascending,
        boolean collateNullsLast)
    {
        return generateOrderByNullsAnsi(expr, ascending, collateNullsLast);
    }

    @Override
    public boolean allowsJoinOn() {
        return false;
    }

    @Override
    public boolean allowsRegularExpressionInWhereClause() {
        return true;
    }

    @Override
    public String generateRegularExpression(
        String source,
        String javaRegex)
    {
        try {
            Pattern.compile(javaRegex);
        } catch (PatternSyntaxException e) {
            // Not a valid Java regex. Too risky to continue.
            return null;
        }
        javaRegex = DialectUtil.cleanUnicodeAwareCaseFlag(javaRegex);
        StringBuilder mappedFlags = new StringBuilder();
        String[][] mapping = new String[][]{{"c","c"},{"i","i"},{"m","m"}};
        javaRegex = extractEmbeddedFlags( javaRegex, mapping, mappedFlags );

        final Matcher escapeMatcher = escapePattern.matcher(javaRegex);
        while (escapeMatcher.find()) {
            javaRegex =
                javaRegex.replace(
                    escapeMatcher.group(1),
                    escapeMatcher.group(2));
        }
        final StringBuilder sb = new StringBuilder();
        sb.append(source);
        sb.append(" IS NOT NULL AND ");
        sb.append("REGEXP_LIKE(");
        sb.append(source);
        sb.append(", ");
        quoteStringLiteral(sb, javaRegex);
        sb.append(", ");
        quoteStringLiteral(sb, mappedFlags.toString());
        sb.append(")");
        return sb.toString();
    }

    /**
     * Chooses the most appropriate type for accessing the values of a
     * column in a result set.
     *
     * The OracleDialect implementation handles some of the specific
     * quirks of Oracle:  e.g. scale = -127 has special meaning with
     * NUMERIC types and may indicate a FLOAT value if precision is non-zero.
     *
     * @param metaData  Resultset metadata
     * @param columnIndex  index of the column in the result set
     * @return  For Types.NUMERIC and Types.DECIMAL, getType()
     * will return a Type.INT, Type.DOUBLE, or Type.OBJECT based on
     * scale, precision, and column name.
     *
     * @throws SQLException
     */
    @Override
    public SqlStatement.Type getType(
        ResultSetMetaData metaData, int columnIndex)
        throws SQLException
    {
        final int columnType = metaData.getColumnType(columnIndex + 1);
        final int precision = metaData.getPrecision(columnIndex + 1);
        final int scale = metaData.getScale(columnIndex + 1);
        final String columnName = metaData.getColumnName(columnIndex + 1);
        SqlStatement.Type type;

        if (columnType == Types.NUMERIC || columnType == Types.DECIMAL) {
            if (scale == -127 && precision != 0) {
                // non zero precision w/ -127 scale means float in Oracle.
                type = SqlStatement.Type.DOUBLE;
            } else if (columnType == Types.NUMERIC
                && (scale == 0 || scale == -127)
                && precision == 0 && columnName.startsWith("m"))
            {
                // In GROUPING SETS queries, Oracle
                // loosens the type of columns compared to mere GROUP BY
                // queries. We need integer GROUP BY columns to remain integers,
                // otherwise the segments won't be found; but if we convert
                // measure (whose column names are like "m0", "m1") to integers,
                // data loss will occur.
                type = SqlStatement.Type.OBJECT;
            } else if (scale == -127 && precision ==0) {
                type = SqlStatement.Type.INT;
            } else if (scale == 0 && (precision == 38 || precision == 0)) {
                // NUMBER(38, 0) is conventionally used in
                // Oracle for integers of unspecified precision, so let's be
                // bold and assume that they can fit into an int.
                type = SqlStatement.Type.INT;
            } else if (scale == 0 && precision <= 9) {
                // An int (up to 2^31 = 2.1B) can hold any NUMBER(10, 0) value
                // (up to 10^9 = 1B).
                type = SqlStatement.Type.INT;
            } else {
                type = SqlStatement.Type.DOUBLE;
            }

        } else {
            type = super.getType(metaData, columnIndex);
        }
        logTypeInfo(metaData, columnIndex, type);
        return type;
    }

    @Override
    public void quoteStringLiteral(StringBuilder buf, String s) {
        // Ensure Unicode characters are handled correctly
        buf.append('N');
        Util.singleQuoteString(s, buf);
    }
}

// End OracleDialect.java
