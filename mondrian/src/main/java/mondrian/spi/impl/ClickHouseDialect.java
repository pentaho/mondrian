/*
 * This software is subject to the terms of the Eclipse Public License v1.0
 * Agreement, available at the following URL:
 * http://www.eclipse.org/legal/epl-v10.html.
 * You must accept the terms of that agreement to use this software.
 */

package mondrian.spi.impl;

import mondrian.olap.Util;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Implementation of {@link mondrian.spi.Dialect} for ClickHouse
 */
public class ClickHouseDialect extends JdbcDialectImpl {

  public static final JdbcDialectFactory FACTORY =
    new JdbcDialectFactory(ClickHouseDialect.class, DatabaseProduct.CLICKHOUSE);

    public ClickHouseDialect(Connection connection) throws SQLException {
      super(connection);
    }

    @Override
    public void quoteStringLiteral(StringBuilder buf, String s) {
      // Go beyond Util.singleQuoteString; also quote backslash, like MySQL.
      buf.append('\'');
      String s0 = Util.replace(s, "'", "''");
      String s1 = Util.replace(s0, "\\", "\\\\");
      buf.append(s1);
      buf.append('\'');
    }

    @Override
    public boolean supportsMultiValueInExpr() {
      return true;
    }
}
