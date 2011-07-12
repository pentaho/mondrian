/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2008-2011 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.spi.impl;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Implementation of {@link mondrian.spi.Dialect} for the Oracle database.
 *
 * @author jhyde
 * @version $Id$
 * @since Nov 23, 2008
 */
public class OracleDialect extends JdbcDialectImpl {

    private final String flagsRegexp = "^(\\(\\?([a-zA-Z])\\)).*$";
    private final Pattern flagsPattern = Pattern.compile(flagsRegexp);
    private final String escapeRegexp = "^.*(\\\\Q(.*)\\\\E).*$";
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
    public String generateOrderByNullsLast(String expr, boolean ascending) {
        return generateOrderByNullsLastAnsi(expr, ascending);
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
        String javaRegExp)
    {
        final Matcher flagsMatcher = flagsPattern.matcher(javaRegExp);
        final StringBuilder suffixSb = new StringBuilder();
        if (flagsMatcher.matches()) {
            // We need to convert leading flags into oracle
            // specific flags
            final String flags = flagsMatcher.group(2);
            if (flags.contains("i")) {
                suffixSb.append("i");
            }
            if (flags.contains("c")) {
                suffixSb.append("c");
            }
            if (flags.contains("m")) {
                suffixSb.append("m");
            }
            javaRegExp =
                javaRegExp.replace(
                    flagsMatcher.group(1),
                    "");
        }
        final Matcher escapeMatcher = escapePattern.matcher(javaRegExp);
        if (escapeMatcher.matches()) {
            // We need to convert escape characters \E and \Q into
            // oracle compatible escapes.
            String sequence = escapeMatcher.group(2);
            sequence = sequence.replaceAll("\\\\", "\\\\");
            javaRegExp =
                javaRegExp.replace(
                    escapeMatcher.group(1),
                    sequence);
        }
        final StringBuilder sb = new StringBuilder();
        sb.append("REGEXP_LIKE(");
        sb.append(source);
        sb.append(", ");
        quoteStringLiteral(sb, javaRegExp);
        sb.append(", ");
        quoteStringLiteral(sb, suffixSb.toString());
        sb.append(")");
        return sb.toString();
    }
}

// End OracleDialect.java
