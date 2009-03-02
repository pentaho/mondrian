/*
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2008-2009 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.spi.impl;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Implementation of {@link mondrian.spi.Dialect} for the Infobright database.
 *
 * @author jhyde
 * @version $Id$
 * @since Nov 23, 2008
 */
public class InfobrightDialect extends MySqlDialect {

    public static final JdbcDialectFactory FACTORY =
        new JdbcDialectFactory(
            InfobrightDialect.class,
            // While we're choosing dialects, this still looks like a MySQL
            // connection.
            DatabaseProduct.MYSQL);

    /**
     * Creates an InfobrightDialect.
     *
     * @param connection Connection
     */
    public InfobrightDialect(Connection connection) throws SQLException {
        super(connection);
    }

    public DatabaseProduct getDatabaseProduct() {
        return DatabaseProduct.INFOBRIGHT;
    }

    public boolean allowsCompoundCountDistinct() {
        return false;
    }

    public boolean isNullsCollateLast() {
        // Infobright is similar to MySQL, but apparently NULLs collate
        // last. This is good news, because the workaround that we use on MySQL
        // to force NULLs to collate last would kill Infobright's performance.an
        return true;
    }

    public boolean supportsGroupByExpressions() {
        return false;
    }

    public boolean requiresGroupByAlias() {
        return true;
    }

    public boolean allowsOrderByAlias() {
        return false;
    }

    public boolean requiresOrderByAlias() {
        // Actually, Infobright doesn't ALLOW aliases to be used in the ORDER BY
        // clause, let alone REQUIRE them. Infobright doesn't allow expressions
        // in the ORDER BY clause, so returning true gives the right effect.
        return true;
    }

    public boolean supportsMultiValueInExpr() {
        // Infobright supports multi-value IN by falling through to MySQL,
        // which is very slow (see for example
        // PredicateFilterTest.testFilterAtSameLevel) so we pretend that it
        // does not.
        return false;
    }
}

// End InfobrightDialect.java
