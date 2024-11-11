/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package mondrian.rolap.aggmatcher;

import junit.framework.Assert;
import mondrian.olap.Util;
import mondrian.rolap.RolapConnection;
import mondrian.rolap.RolapConnectionProperties;
import mondrian.test.FoodMartTestCase;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
  * Test if AggSchemaScan and AggCatalogScan properties are used in JdbcSchema loadTablesOfType
  *
  */
public class AggSchemaScanTest extends FoodMartTestCase {

  public AggSchemaScanTest( String name ) {
    super(name);
  }


  public void testAggScanPropertiesEmptySchema() throws Exception {

    final RolapConnection rolapConn = (RolapConnection) getConnection();
    final DataSource dataSource = rolapConn.getDataSource();
    Connection sqlConnection = null;
    try {
      sqlConnection = dataSource.getConnection();
      Util.PropertyList propertyList = new Util.PropertyList();
      propertyList.put( RolapConnectionProperties.AggregateScanCatalog.name(), "bogus" );
      propertyList.put( RolapConnectionProperties.AggregateScanSchema.name(), "bogus" );
      JdbcSchema jdbcSchema = JdbcSchema.makeDB(dataSource);
      jdbcSchema.resetAllTablesLoaded();
      jdbcSchema.getTablesMap().clear();

      jdbcSchema.loadTables( propertyList );
      Assert.assertEquals( 0, jdbcSchema.getTablesMap().size() );
    } finally {
      if (sqlConnection != null) {
        try {
          sqlConnection.close();
        } catch ( SQLException e) {
          // ignore
        }
      }
    }
  }


  public void testAggScanPropertiesPopulatedSchema() throws Exception {

    final RolapConnection rolapConn = (RolapConnection) getConnection();
    final DataSource dataSource = rolapConn.getDataSource();
    Connection sqlConnection = null;
    try {
      sqlConnection = dataSource.getConnection();
      DatabaseMetaData dbmeta = sqlConnection.getMetaData();
      if ( !dbmeta.supportsSchemasInTableDefinitions() && !dbmeta.supportsCatalogsInTableDefinitions() ) {
        System.out.println( "Database does not support schema or catalog in table definitions.  Cannot run test." );
        return;
      }
      Util.PropertyList propertyList = new Util.PropertyList();
      boolean foundSchema = false;
      // Different databases treat catalogs and schemas differently.  Figure out whether foodmart is a schema or catalog in this database
      try {
        String schema = sqlConnection.getSchema();
        String catalog = sqlConnection.getCatalog();
        if ( schema != null || catalog != null ) {
          foundSchema = true;
          propertyList.put( RolapConnectionProperties.AggregateScanCatalog.name(), catalog );
          propertyList.put( RolapConnectionProperties.AggregateScanSchema.name(), schema );
        }
      } catch ( AbstractMethodError | Exception ex ) {
        // Catch if the JDBC client throws an exception.  Do nothing.
      }

      // Some databases like Oracle do not implement getSchema and getCatalog with the connection, so try the dbmeta instead
      if ( !foundSchema && dbmeta.supportsSchemasInTableDefinitions() ) {
        try ( ResultSet resultSet = dbmeta.getSchemas() ) {
           if ( resultSet.getMetaData().getColumnCount() == 2 ) {
             while ( resultSet.next() ) {
               if ( resultSet.getString( 1 ).equalsIgnoreCase( "foodmart" ) ) {
                 propertyList.put( RolapConnectionProperties.AggregateScanSchema.name(), resultSet.getString( 1 ) );
                 propertyList.put( RolapConnectionProperties.AggregateScanCatalog.name(), resultSet.getString( 2 ) );
                 foundSchema = true;
                 break;
               }
             }
           }

        }
      }

      if (dbmeta.supportsCatalogsInTableDefinitions() && !foundSchema) {
        try ( ResultSet resultSet = dbmeta.getCatalogs() ) {
          if ( resultSet.getMetaData().getColumnCount() == 1 ) {
            while ( resultSet.next() ) {
              if ( resultSet.getString( 1 ).equalsIgnoreCase( "foodmart" ) ) {
                propertyList.put( RolapConnectionProperties.AggregateScanCatalog.name(), resultSet.getString( 1 ) );
                foundSchema = true;
                break;
              }
            }
          }
        }
      }

      if ( !foundSchema ) {
        System.out.println( "Cannot find foodmart schema or catalog in database.  Cannot run test." );
        return;
      }
      JdbcSchema jdbcSchema = JdbcSchema.makeDB(dataSource);
      // Have to clear the table list because creating the connection loads this
      jdbcSchema.resetAllTablesLoaded();
      jdbcSchema.getTablesMap().clear();

      jdbcSchema.loadTables( propertyList );
      //The foodmart schema has 37 tables.
      Assert.assertEquals( 37, jdbcSchema.getTablesMap().size() );
    } finally {
      if (sqlConnection != null) {
        try {
          sqlConnection.close();
        } catch ( SQLException e) {
          // ignore
        }
      }
    }
  }
}
