/*
 * This software is subject to the terms of the Eclipse Public License v1.0
 * Agreement, available at the following URL:
 * http://www.eclipse.org/legal/epl-v10.html.
 * You must accept the terms of that agreement to use this software.
 *
 * Copyright (C) 2005-2024 Hitachi Vantara and others
 * All Rights Reserved.
 */

package mondrian.server;

import mondrian.olap.DriverManager;
import mondrian.olap.MondrianProperties;
import mondrian.olap.MondrianServer;
import mondrian.olap.Util;
import mondrian.olap.Util.PropertyList;
import mondrian.olap4j.MondrianOlap4jDriver;
import mondrian.rolap.RolapConnection;
import mondrian.rolap.RolapConnectionProperties;
import mondrian.rolap.RolapSchema;
import mondrian.spi.CatalogLocator;
import mondrian.tui.XmlaSupport;
import mondrian.util.ClassResolver;
import mondrian.util.LockBox;
import mondrian.util.Pair;
import mondrian.xmla.DataSourcesConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.olap4j.OlapConnection;
import org.olap4j.OlapException;
import org.olap4j.OlapWrapper;
import org.olap4j.impl.Olap4jUtil;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Implementation of {@link mondrian.server.Repository} that reads from a {@code datasources.xml} file.
 *
 * <p>Note that for legacy reasons, the datasources.xml file's root element is called DataSource whereas the olap4j
 * standard calls them Databases. This is why those two concepts are linked, as in
 * {@link FileRepository#getDatabaseNames(RolapConnection)} for example.
 *
 * @author Julian Hyde, Luc Boudreau
 */
public class FileRepository implements Repository {
  private static final Logger LOGGER = LogManager.getLogger( FileRepository.class );
  private static final Object SERVER_INFO_LOCK = new Object();
  private final RepositoryContentFinder repositoryContentFinder;

  private static final Set<FileRepository> instances = Collections.newSetFromMap( new WeakHashMap<>() );
  private static final ScheduledExecutorService executorService =
    Util.getScheduledExecutorService( 1, "mondrian.server.DynamicContentFinder$executorService" );
  private ServerInfo serverInfo;
  private final CatalogLocator locator;
  private final AtomicBoolean shutdown = new AtomicBoolean( false );

  static {
    final Pair<Long, TimeUnit> interval =
      Util.parseInterval( String.valueOf( MondrianProperties.instance().XmlaSchemaRefreshInterval.get() ),
        TimeUnit.MILLISECONDS );

    executorService.scheduleWithFixedDelay( () -> {
        for ( FileRepository next : instances ) {
          next.clearServerInfo();
        }
      },
      0,
      interval.left,
      interval.right );
  }

  public FileRepository( RepositoryContentFinder repositoryContentFinder, CatalogLocator locator ) {
    this.repositoryContentFinder = repositoryContentFinder;
    this.locator = locator;

    if ( repositoryContentFinder == null ) {
      throw new AssertionError();
    }

    instances.add( this );
  }

  protected void clearServerInfo() {
    synchronized ( SERVER_INFO_LOCK ) {
      serverInfo = null;
    }
  }

  public List<Map<String, Object>> getDatabases( RolapConnection connection ) {
    final List<Map<String, Object>> propsList = new ArrayList<>();

    for ( DatabaseInfo dsInfo : getServerInfo().datasourceMap.values() ) {
      propsList.add( dsInfo.properties );
    }

    return propsList;
  }

  @SuppressWarnings( "java:S3776" )
  public OlapConnection getConnection( MondrianServer server, String databaseName, String catalogName, String roleName,
                                       Properties props ) throws SQLException {
    final ServerInfo localServerInfo = getServerInfo();
    DatabaseInfo datasourceInfo;

    if ( databaseName == null ) {
      if ( localServerInfo.datasourceMap.isEmpty() ) {
        throw new OlapException( "No databases configured on this server" );
      }

      datasourceInfo = localServerInfo.datasourceMap.values().iterator().next();
    } else {
      datasourceInfo = localServerInfo.datasourceMap.get( databaseName );

      // In some cases (e.g. PowerBI connecting) the database name is actually the catalog name, search for a match
      if ( datasourceInfo == null && databaseName.equals( catalogName ) ) {
        for ( Map.Entry<String, DatabaseInfo> dbInfo : localServerInfo.datasourceMap.entrySet() ) {
          if ( dbInfo.getValue().catalogMap.containsKey( catalogName ) ) {
            datasourceInfo = dbInfo.getValue();
            break;
          }
        }
      }

      // For legacy, we have to check if the DataSourceInfo matches.
      // We used to mix up DS Info and DS names. The behavior above is the right one. The one below is not.
      // Note also that the DSInfos we sent to the client had the JDBC properties removed for security.
      // We have to account for that here as well.
      if ( datasourceInfo == null ) {
        for ( DatabaseInfo infos : localServerInfo.datasourceMap.values() ) {
          PropertyList pl = Util.parseConnectString( (String) infos.properties.get( "DataSourceInfo" ) );

          pl.remove( RolapConnectionProperties.Jdbc.name() );
          pl.remove( RolapConnectionProperties.JdbcUser.name() );
          pl.remove( RolapConnectionProperties.JdbcPassword.name() );

          if ( pl.toString().equals( databaseName ) ) {
            datasourceInfo = infos;
          }
        }
      }
    }

    if ( datasourceInfo == null ) {
      throw Util.newError( "Unknown database '" + databaseName + "'" );
    }

    if ( catalogName == null ) {
      if ( datasourceInfo.catalogMap.isEmpty() ) {
        throw new OlapException( "No catalogs in the database named " + datasourceInfo.name );
      }

      for ( CatalogInfo catalogInfo : datasourceInfo.catalogMap.values() ) {
        try {
          return getConnection( catalogInfo, server, roleName, props );
        } catch ( Exception e ) {
          LOGGER.warn( "Failed getting connection. Skipping", e );
        }
      }
    } else {
      CatalogInfo namedCatalogInfo = datasourceInfo.catalogMap.get( catalogName );

      if ( namedCatalogInfo == null ) {
        throw Util.newError( "Unknown catalog '" + catalogName + "'" );
      }

      return getConnection( namedCatalogInfo, server, roleName, props );
    }

    throw Util.newError( "No suitable connection found" );
  }

  OlapConnection getConnection( CatalogInfo catalogInfo, MondrianServer server, String roleName, Properties props )
    throws SQLException {
    String connectString = catalogInfo.olap4jConnectString;

    // Save the server for the duration of the call to 'getConnection'.
    final LockBox.Entry entry = MondrianServerRegistry.INSTANCE.lockBox.register( server );

    final Properties properties = new Properties();
    properties.setProperty( RolapConnectionProperties.Instance.name(), entry.getMoniker() );

    if ( roleName != null ) {
      properties.setProperty( RolapConnectionProperties.Role.name(), roleName );
    }

    properties.putAll( props );
    // Make sure we load the Mondrian driver into the ClassLoader.

    try {
      ClassResolver.INSTANCE.forName( MondrianOlap4jDriver.class.getName(), true );
    } catch ( ClassNotFoundException e ) {
      throw new OlapException( "Cannot find mondrian olap4j driver." );
    }

    try ( java.sql.Connection connection = java.sql.DriverManager.getConnection( connectString, properties ) ) {
      return ( (OlapWrapper) connection ).unwrap( OlapConnection.class );
    }
  }

  public void shutdown() {
    if ( !shutdown.getAndSet( true ) ) {
      instances.remove( this );
      repositoryContentFinder.shutdown();
    }
  }

  ServerInfo getServerInfo() {
    synchronized ( SERVER_INFO_LOCK ) {
      if ( this.serverInfo != null ) {
        return this.serverInfo;
      }

      final String content = getRepositoryContentFinder().getContent();
      DataSourcesConfig.DataSources xmlDataSources = XmlaSupport.parseDataSources( content, LOGGER );
      assert xmlDataSources != null;
      ServerInfo localServerInfo = new ServerInfo();

      for ( DataSourcesConfig.DataSource xmlDataSource : xmlDataSources.dataSources ) {
        final Map<String, Object> dsPropsMap = Olap4jUtil.mapOf(
          "DataSourceName",
          xmlDataSource.getDataSourceName(),
          "DataSourceDescription",
          xmlDataSource.getDataSourceDescription(),
          "URL",
          xmlDataSource.getURL(),
          "DataSourceInfo",
          xmlDataSource.getDataSourceInfo(),
          "ProviderName",
          xmlDataSource.getProviderName(),
          "ProviderType",
          xmlDataSource.providerType,
          "AuthenticationMode",
          xmlDataSource.authenticationMode );
        final DatabaseInfo databaseInfo = new DatabaseInfo( xmlDataSource.name, dsPropsMap );
        localServerInfo.datasourceMap.put( xmlDataSource.name, databaseInfo );

        for ( DataSourcesConfig.Catalog xmlCatalog : xmlDataSource.catalogs.catalogs ) {
          if ( databaseInfo.catalogMap.containsKey( xmlCatalog.name ) ) {
            throw Util.newError( "more than one DataSource object has name '" + xmlCatalog.name + "'" );
          }

          String connectString =
            xmlCatalog.dataSourceInfo != null ? xmlCatalog.dataSourceInfo : xmlDataSource.dataSourceInfo;
          // Check if the catalog is part of the connect string. If not, add it.
          final Util.PropertyList connectProperties = Util.parseConnectString( connectString );

          if ( connectProperties.get( RolapConnectionProperties.Catalog.name() ) == null ) {
            connectString += ";" + RolapConnectionProperties.Catalog.name() + "=" + xmlCatalog.definition;
          }

          final CatalogInfo catalogInfo = new CatalogInfo( xmlCatalog.name, connectString, locator );
          databaseInfo.catalogMap.put( xmlCatalog.name, catalogInfo );
        }
      }

      this.serverInfo = localServerInfo;
      return localServerInfo;
    }
  }

  public List<String> getCatalogNames( RolapConnection connection, String databaseName ) {
    return new ArrayList<>( getServerInfo().datasourceMap.get( databaseName ).catalogMap.keySet() );
  }

  public List<String> getDatabaseNames( RolapConnection connection ) {
    return new ArrayList<>( getServerInfo().datasourceMap.keySet() );
  }

  public Map<String, RolapSchema> getRolapSchemas( RolapConnection connection, String databaseName,
                                                   String catalogName ) {
    final RolapSchema schema =
      getServerInfo().datasourceMap.get( databaseName ).catalogMap.get( catalogName ).getRolapSchema();
    return Collections.singletonMap( schema.getName(), schema );
  }

  @Override
  @SuppressWarnings( { "java:S1113", "deprecation" } )
  protected void finalize() {
    shutdown();
  }

  // Class is defined as package-protected in order to be accessible by unit tests
  static class ServerInfo {
    private final Map<String, DatabaseInfo> datasourceMap = new HashMap<>();

    // Method is created to variable has been accessible by unit tests
    Map<String, DatabaseInfo> getDatasourceMap() {
      return datasourceMap;
    }
  }

  // Class is defined as package-protected in order to be accessible by unit tests
  static class DatabaseInfo {
    private final String name;
    private final Map<String, Object> properties;
    Map<String, CatalogInfo> catalogMap = new HashMap<>();

    DatabaseInfo( String name, Map<String, Object> properties ) {
      this.name = name;
      this.properties = properties;
    }

    // Method is created to variable has been accessible by unit tests
    Map<String, Object> getProperties() {
      return properties;
    }
  }

  @SuppressWarnings( { "unused", "java:S1172" } )
  static class CatalogInfo {
    private final String connectString;
    private final String olap4jConnectString;
    private final CatalogLocator locator;

    CatalogInfo( String name, String connectString, CatalogLocator locator ) {
      this.connectString = connectString;
      this.locator = locator;
      this.olap4jConnectString = connectString.startsWith( "jdbc:" ) ? connectString : "jdbc:mondrian:" + connectString;
    }

    private RolapSchema getRolapSchema() {
      RolapConnection rolapConnection = null;

      try {
        rolapConnection = (RolapConnection) DriverManager.getConnection( connectString, this.locator );
        return rolapConnection.getSchema();
      } finally {
        if ( rolapConnection != null ) {
          rolapConnection.close();
        }
      }
    }
  }

  // Method is defined as package-protected in order to be accessible by unit tests
  RepositoryContentFinder getRepositoryContentFinder() {
    return repositoryContentFinder;
  }
}
