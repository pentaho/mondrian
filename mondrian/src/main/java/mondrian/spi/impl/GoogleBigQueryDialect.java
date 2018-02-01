/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (c) 2018-2018 Hitachi Vantara and others. All rights reserved.
*/
package mondrian.spi.impl;

import mondrian.spi.DialectUtil;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class GoogleBigQueryDialect extends JdbcDialectImpl {

    public static final JdbcDialectFactory FACTORY =
            new JdbcDialectFactory(
                GoogleBigQueryDialect.class,
                DatabaseProduct.GOOGLEBIGQUERY);

    public GoogleBigQueryDialect(Connection connection) throws SQLException {
        super(connection);
    }

    @Override
    public String getQuoteIdentifierString() {
        return "";
    }

    @Override
    public boolean allowsOrderByAlias() {
        return true;
    }

    @Override
    public boolean allowsDdl() {
        return false;
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) {
        return false;
    }

    @Override
    public boolean allowsRegularExpressionInWhereClause() {
        return true;
    }

    public String generateRegularExpression(String source, String javaRegex) {
        try {
            Pattern.compile(javaRegex);
        } catch (PatternSyntaxException e) {
            // Not a valid Java regex. Too risky to continue.
            return null;
        }
        javaRegex = DialectUtil.cleanUnicodeAwareCaseFlag(javaRegex);
        javaRegex = javaRegex.replace("\\Q", "");
        javaRegex = javaRegex.replace("\\E", "");
        final StringBuilder sb = new StringBuilder();
        sb.append("cast(");
        sb.append(source);
        sb.append(" as string) is not null and ");
        sb.append("REGEXP_CONTAINS( cast(");
        sb.append(source);
        sb.append(" as string), r");
        quoteStringLiteral(sb, javaRegex);
        sb.append(") = true");
        return sb.toString();
    }
}

// End GoogleBigQueryDialect.java