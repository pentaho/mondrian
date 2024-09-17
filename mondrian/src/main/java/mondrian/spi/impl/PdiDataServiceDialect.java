/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/
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
