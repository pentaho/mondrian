/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2012-2013 Pentaho and others
// All Rights Reserved.
*/
package mondrian.spi.impl;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Dialect for Cloudera's Impala DB.
 *
 * @author cboyden
 * @since 2/11/13
 */
public class ImpalaDialect extends HiveDialect {
    private final String flagsRegexp = "^(\\(\\?([a-zA-Z])\\)).*$";
    private final Pattern flagsPattern = Pattern.compile(flagsRegexp);
    private final String escapeRegexp = "(\\\\Q([^\\\\Q]+)\\\\E)";
    private final Pattern escapePattern = Pattern.compile(escapeRegexp);

    /**
     * Creates an ImpalaDialect.
     *
     * @param connection Connection
     * @throws java.sql.SQLException on error
     */
    public ImpalaDialect(Connection connection) throws SQLException {
        super(connection);
    }

    public static final JdbcDialectFactory FACTORY =
        new JdbcDialectFactory(
            ImpalaDialect.class,
            DatabaseProduct.HIVE)
        {
            protected boolean acceptsConnection(Connection connection) {
                return super.acceptsConnection(connection)
                    && isDatabase(DatabaseProduct.IMPALA, connection);
            }
        };

    @Override
    public DatabaseProduct getDatabaseProduct() {
        return DatabaseProduct.IMPALA;
    }

    @Override
    protected String generateOrderByNulls(
        String expr,
        boolean ascending,
        boolean collateNullsLast)
    {
        if (ascending) {
            return expr + " ASC";
        } else {
            return expr + " DESC";
        }
    }


    @Override
    public String generateOrderItem(
        String expr,
        boolean nullable,
        boolean ascending,
        boolean collateNullsLast)
    {
        String ret = null;

        if (nullable && collateNullsLast) {
            ret = "CASE WHEN " + expr + " IS NULL THEN 1 ELSE 0 END, ";
        } else {
            ret = "CASE WHEN " + expr + " IS NULL THEN 0 ELSE 1 END, ";
        }

        if (ascending) {
            ret += expr + " ASC";
        } else {
            ret += expr + " DESC";
        }

        return ret;
    }

    @Override
    public boolean allowsMultipleCountDistinct() {
        return false;
    }

    @Override
    public boolean allowsCompoundCountDistinct() {
        return true;
    }

    @Override
    public boolean requiresOrderByAlias() {
        return false;
    }

    @Override
    public boolean requiresAliasForFromQuery() {
        return true;
    }

    @Override
    public boolean supportsGroupByExpressions() {
        return false;
    }

    @Override
    public boolean allowsSelectNotInGroupBy() {
        return false;
    }

    @Override
    public String generateInline(
        List<String> columnNames,
        List<String> columnTypes,
        List<String[]> valueList)
    {
        return generateInlineGeneric(
            columnNames, columnTypes, valueList, null, false);
    }

    public boolean allowsJoinOn() {
        return false;
    }

    @Override
    public void quoteStringLiteral(
        StringBuilder buf,
        String value)
    {
        String quote = "\'";
        String s0 = value;

        if (s0.contains("\\")) {
            s0.replaceAll("\\\\", "\\\\");
        }
        if (s0.contains(quote)) {
            s0 = s0.replaceAll(quote, "\\\\" + quote);
        }

        buf.append(quote);

        buf.append(s0);

        buf.append(quote);
    }

    public boolean allowsRegularExpressionInWhereClause() {
        return true;
    }

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

        // We might have to use case-insensitive matching
        final Matcher flagsMatcher = flagsPattern.matcher(javaRegex);
        boolean caseSensitive = true;
        if (flagsMatcher.matches()) {
            final String flags = flagsMatcher.group(2);
            if (flags.contains("i")) {
                caseSensitive = false;
            }
        }
        if (flagsMatcher.matches()) {
            javaRegex =
                javaRegex.substring(0, flagsMatcher.start(1))
                + javaRegex.substring(flagsMatcher.end(1));
        }
        final Matcher escapeMatcher = escapePattern.matcher(javaRegex);
        while (escapeMatcher.find()) {
            javaRegex =
                javaRegex.replace(
                    escapeMatcher.group(1),
                    escapeMatcher.group(2));
        }
        final StringBuilder sb = new StringBuilder();

        // Now build the string.
        if (caseSensitive) {
            sb.append(source);
        } else {
            sb.append("UPPER(");
            sb.append(source);
            sb.append(")");
        }
        sb.append(" REGEXP ");
        if (caseSensitive) {
            quoteStringLiteral(sb, javaRegex);
        } else {
            quoteStringLiteral(sb, javaRegex.toUpperCase());
        }
        return sb.toString();
    }

    public boolean allowsDdl() {
        return true;
    }
}
// End ImpalaDialect.java
