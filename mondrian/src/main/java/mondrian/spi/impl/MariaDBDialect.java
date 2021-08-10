/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2016 - 2021 Hitachi Vantara
// All Rights Reserved.
*/

package mondrian.spi.impl;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

public class MariaDBDialect extends MySqlDialect {

  public static final JdbcDialectFactory FACTORY =
      new JdbcDialectFactory(
          MariaDBDialect.class,
          DatabaseProduct.MARIADB)
      {
          @Override
          protected boolean acceptsConnection(Connection connection) {
              return super.acceptsConnection(connection);
          }
      };


  public MariaDBDialect( Connection connection ) throws SQLException {
    super( connection );
  }

  @Override
  protected String deduceProductName(DatabaseMetaData databaseMetaData) {
      // It is possible for someone to use the MariaDB JDBC driver with Infobright . . .
      final String productName = super.deduceProductName(databaseMetaData);
      if (isInfobright(databaseMetaData)) {
          return "MySQL (Infobright)";
      }
      return productName;
  }

    /**
     * For MariaDB always allows subquery in the from query clause. Problem is with any store engines, ex. Columnstore.
     *
     * @return Always <tt>true</tt>.
     */
    @Override
    public boolean allowsFromQuery() {
        return true;
    }
}
