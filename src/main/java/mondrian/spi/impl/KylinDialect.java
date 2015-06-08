/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2015-2015 Pentaho
// All Rights Reserved.
*/
package mondrian.spi.impl;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Implementation of {@link mondrian.spi.Dialect} for Kylin.
 *
 * @author Sebastien Jelsch
 * @since Jun 08, 2015
 */
public class KylinDialect extends JdbcDialectImpl {

    public static final JdbcDialectFactory FACTORY =
            new JdbcDialectFactory(KylinDialect.class, DatabaseProduct.KYLIN) {
                protected boolean acceptsConnection(Connection connection) {
                    return super.acceptsConnection(connection);
                }
            };

    /**
     * Creates a KylinDialect.
     *
     * @param connection Connection
     * @throws SQLException on error
     */
    public KylinDialect(Connection connection) throws SQLException {
        super(connection);
    }

    @Override
    public boolean allowsCountDistinct() {
        return false;
    }

    @Override
    public boolean allowsJoinOn() {
        return true;
    }
}
