/*
 * This software is subject to the terms of the Eclipse Public License v1.0
 * Agreement, available at the following URL:
 * http://www.eclipse.org/legal/epl-v10.html.
 * You must accept the terms of that agreement to use this software.
 *
 * Copyright (c) 2019 Hitachi Vantara..  All rights reserved.
 */
package mondrian.spi.impl;

import java.sql.Connection;
import java.sql.SQLException;

public class SnowflakeDialect extends JdbcDialectImpl {
  public static final JdbcDialectFactory FACTORY =
    new JdbcDialectFactory( SnowflakeDialect.class, DatabaseProduct.SNOWFLAKE );

  public SnowflakeDialect( Connection connection ) throws SQLException {
    super( connection );
  }

  @Override public String getQuoteIdentifierString() {
    return "\"";
  }
}
