/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2017 Pentaho Corporation..  All rights reserved.
*/
package mondrian.spi.impl;

import mondrian.rolap.SqlStatement;

import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

public class PdiDataServiceDialect extends JdbcDialectImpl {

  public static final JdbcDialectFactory FACTORY =
    new JdbcDialectFactory(PdiDataServiceDialect.class, DatabaseProduct.PDI);

  public PdiDataServiceDialect(Connection connection) throws SQLException {
    super(connection);
  }

  public PdiDataServiceDialect() {
  }

  @Override
  public SqlStatement.Type getType(ResultSetMetaData metaData, int columnIndex)
    throws SQLException
  {
    int type = metaData.getColumnType(columnIndex + 1);
    if (type == Types.DECIMAL) {
      return SqlStatement.Type.OBJECT;
    } else {
      return super.getType(metaData, columnIndex);
    }
  }
}
// End PdiDataServiceDialect.java
