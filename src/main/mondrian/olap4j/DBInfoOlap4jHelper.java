/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2001-2005 Julian Hyde
// Copyright (C) 2005-2016 Pentaho and others
// All Rights Reserved.
*/
package mondrian.olap4j;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import mondrian.olap.Connection;
import mondrian.olap.Dimension;
import mondrian.olap.Hierarchy;
import mondrian.olap.Level;
import mondrian.olap.MondrianDef;
import mondrian.rolap.RolapConnection;
import mondrian.rolap.RolapCubeDimension;
import mondrian.rolap.RolapCubeLevel;
import mondrian.rolap.RolapHierarchy;
import mondrian.rolap.RolapLevel;
import mondrian.spi.Dialect.Datatype;

import org.apache.log4j.Logger;

public class DBInfoOlap4jHelper {
  private static final Logger LOGGER =
    Logger.getLogger(DBInfoOlap4jHelper.class);

  private static MondrianDef.Table getTableDef(Level level) {
    MondrianDef.Table ret = null;
    if (level instanceof RolapLevel) {
      RolapLevel rolapLevel = (RolapLevel) level;
      MondrianDef.RelationOrJoin roj = (MondrianDef.RelationOrJoin) rolapLevel
          .getHierarchy().getRelation();
      if (roj instanceof MondrianDef.Table) {
        ret = ((MondrianDef.Table) roj);
      }
    }
    return ret;
  }

  private static MondrianDef.Hierarchy getDimensionHierarchyDef(Dimension dim) {
    MondrianDef.Hierarchy ret = null;
    Hierarchy hier = dim.getHierarchy();
    if (hier instanceof RolapHierarchy) {
      RolapHierarchy rolapHier = (RolapHierarchy) hier;
      ret = (MondrianDef.Hierarchy) rolapHier.getXmlHierarchy();
    }
    return ret;
  }

  public static String getDBSchemaName(Level level) {
    if (level instanceof RolapCubeLevel) {
      return ((RolapCubeLevel) level).getCube().getSchema().getName();
    }
    MondrianDef.Table tbl = getTableDef(level);
    return tbl == null ? null : tbl.schema;
  }

  public static String getDBTableName(Level level) {
    MondrianDef.Table tbl = getTableDef(level);
    return tbl == null ? null : tbl.name;
  }

  public static String getDBColumnName(Level level) {
    String ret = null;
    if (level instanceof RolapLevel) {
      Level lev = ((RolapLevel) level).getChildLevel();
      if (lev instanceof RolapCubeLevel) {
        MondrianDef.Expression expr = ((RolapCubeLevel) lev)
            .getStarKeyColumn().getExpression();
        ret = ((MondrianDef.Column) expr).name;
      }
    }
    return ret;
  }

  public static Datatype getDBColumnType(Level level) {
    Datatype ret = null;
    if (level instanceof RolapLevel) {
      Level lev = ((RolapLevel) level).getChildLevel();
      if (lev instanceof RolapLevel) {
        ret = ((RolapLevel) lev).getDatatype();
      }
    }
    return ret;
  }

  public static String getFactTableDBForeignKey(Level level) {
    String ret = null;
    Dimension dim = level.getDimension();
    if (dim instanceof RolapCubeDimension) {
      ret = ((RolapCubeDimension) dim).getDimensionDef().foreignKey;
    }
    return ret;
  }

  public static String getDimensionDBPrimaryKey(Level level) {
    return getDimensionHierarchyDef(level.getDimension()).primaryKey;
  }

  public static String getDimensionDBTable(Level level) {
    return ((MondrianDef.Table)
        getDimensionHierarchyDef(level.getDimension())
        .relation).name;
  }

  public static int getDBColumnSQLType(MondrianOlap4jConnection conn,
      Level level) {
    try {
      RolapConnection rolapConnection = conn.unwrap(RolapConnection.class);
      return getDBColumnSQLType(rolapConnection, level);
    } catch (SQLException ex) {
      LOGGER.error("Unable to extract DB connection");
    }
    return Types.NULL;
  }

  public static int getDBColumnSQLType(Connection conn,
      Level level) {
    int ret = Types.NULL;

    if (conn==null) {
      return ret;
    }
    try {
      DatabaseMetaData databaseMetaData = conn.getDataSource()
        .getConnection().getMetaData();
      String schema = getDBSchemaName( level );
      String table = getDBTableName( level );
      String column = getDBColumnName( level );
      ResultSet rs = databaseMetaData.getColumns(null, schema, table, column);
      if (rs!=null && rs.next()){
        ret = rs.getInt("DATA_TYPE");
      }
    } catch (SQLException ex) {
      LOGGER.error("Unable to extract DB connection");
    }
    return ret;
  }
}
// End DBInfoOlap4jHelper.java'
