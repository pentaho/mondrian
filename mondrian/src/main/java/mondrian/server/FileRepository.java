/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2005-2017 Hitachi Vantara and others
// All Rights Reserved.
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

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import org.olap4j.OlapConnection;
import org.olap4j.OlapException;
import org.olap4j.OlapWrapper;
import org.olap4j.impl.Olap4jUtil;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Implementation of {@link mondrian.server.Repository} that reads
 * from a {@code datasources.xml} file.
 *
 * <p>Note that for legacy reasons, the datasources.xml file's root
 * element is called DataSource whereas the olap4j standard calls them
 * Databases. This is why those two concepts are linked, as in
 * {@link FileRepository#getDatabaseNames(RolapConnection)} for example.
 *
 * @author Julian Hyde, Luc Boudreau
 */
public class FileRepository implements Repository {
    private static final Object SERVER_INFO_LOCK = new Object();
    private final RepositoryContentFinder repositoryContentFinder;

    private static final Logger LOGGER = LogManager.getLogger(MondrianServer.class);

    private static final ScheduledExecutorService executorService =
        Util.getScheduledExecutorService(
            1,
            "mondrian.server.DynamicContentFinder$executorService");

    private ServerInfo serverInfo;

    static{
        final Pair<Long, TimeUnit> interval =
            Util.parseInterval(
                String.valueOf(
                    MondrianProperties.instance()
                        .XmlaSchemaRefreshInterval.get()),
                TimeUnit.MILLISECONDS);
        executorService.scheduleWithFixedDelay(
            new Runnable() {
                public void run() {
                    Iterator<FileRepository> instanceIt = instances.iterator();
                    while ( instanceIt.hasNext() ) {
                        FileRepository next = instanceIt.next();
                        next.clearServerInfo();
                    }

                }
            },
            0,
            interval.left,
            interval.right);
    }
    private final CatalogLocator locator;
    private static final Set<FileRepository> instances = Collections.newSetFromMap(
        new WeakHashMap<FileRepository, Boolean>());

    private AtomicBoolean shutdown = new AtomicBoolean( false );

    public FileRepository(
        RepositoryContentFinder repositoryContentFinder,
        CatalogLocator locator)
    {
        this.repositoryContentFinder = repositoryContentFinder;
        this.locator = locator;
        assert repositoryContentFinder != null;
        instances.add(this);

    }

    protected void clearServerInfo(){
        synchronized (SERVER_INFO_LOCK) {
            serverInfo = null;
        }
    }

    public List<Map<String, Object>> getDatabases(
        RolapConnection connection)
    {
        final List<Map<String, Object>> propsList =
            new ArrayList<Map<String, Object>>();
        for (DatabaseInfo dsInfo : getServerInfo().datasourceMap.values()) {
            propsList.add(dsInfo.properties);
        }
        return propsList;
    }

    public OlapConnection getConnection(
        MondrianServer server,
        String databaseName,
        String catalogName,
        String roleName,
        Properties props)
        throws SQLException
    {
        final ServerInfo serverInfo = getServerInfo();
        DatabaseInfo datasourceInfo;
        if (databaseName == null) {
            if (serverInfo.datasourceMap.size() == 0) {
                throw new OlapException(
                    "No databases configured on this server");
            }
            datasourceInfo =
                serverInfo
                    .datasourceMap
                    .values()
                    .iterator()
                    .next();
        } else {
            datasourceInfo =
                serverInfo.datasourceMap.get(databaseName);

            // For legacy, we have to check if the DataSourceInfo matches.
            // We used to mix up DS Info and DS names. The behavior above is
            // the right one. The one below is not.
            // Note also that the DSInfos we sent to the client had the
            // JDBC properties removed for security. We have to account for
            // that here as well.
            if (datasourceInfo == null) {
                for (DatabaseInfo infos
                    : serverInfo.datasourceMap.values())
                {
                    PropertyList pl =
                        Util.parseConnectString(
                            (String) infos.properties.get("DataSourceInfo"));

                    pl.remove(RolapConnectionProperties.Jdbc.name());
                    pl.remove(RolapConnectionProperties.JdbcUser.name());
                    pl.remove(RolapConnectionProperties.JdbcPassword.name());

                    if (pl.toString().equals(databaseName)) {
                        datasourceInfo = infos;
                    }
                }
            }
        }

        if (datasourceInfo == null) {
            throw Util.newError("Unknown database '" + databaseName + "'");
        }

        if (catalogName == null) {
            if (datasourceInfo.catalogMap.size() == 0) {
                throw new OlapException(
                    "No catalogs in the database named "
                    + datasourceInfo.name);
            }
            for (CatalogInfo catalogInfo : datasourceInfo.catalogMap.values()) {
              try {
                return getConnection(catalogInfo, server, roleName, props);
              } catch (Exception e) {
                LOGGER.warn("Failed getting connection. Skipping", e);
              }
            }
        } else {
          CatalogInfo namedCatalogInfo =
                datasourceInfo.catalogMap.get(catalogName);
          if (namedCatalogInfo == null) {
            throw Util.newError("Unknown catalog '" + catalogName + "'");
          }
          return getConnection(namedCatalogInfo, server, roleName, props);
        }

        throw Util.newError("No suitable connection found");
    }

    OlapConnection getConnection(
        CatalogInfo catalogInfo,
        MondrianServer server,
        String roleName,
        Properties props)
        throws SQLException
    {
      String connectString = catalogInfo.olap4jConnectString;

      // Save the server for the duration of the call to 'getConnection'.
      final LockBox.Entry entry =
          MondrianServerRegistry.INSTANCE.lockBox.register(server);

      final Properties properties = new Properties();
      properties.setProperty(
          RolapConnectionProperties.Instance.name(),
          entry.getMoniker());
      if (roleName != null) {
          properties.setProperty(
              RolapConnectionProperties.Role.name(),
              roleName);
      }
      properties.putAll(props);
      // Make sure we load the Mondrian driver into
      // the ClassLoader.
      try {
        ClassResolver.INSTANCE.forName(
            MondrianOlap4jDriver.class.getName(), true);
      } catch (ClassNotFoundException e) {
          throw new OlapException("Cannot find mondrian olap4j driver.");
      }

      final java.sql.Connection connection =
          java.sql.DriverManager.getConnection(connectString, properties);
      return ((OlapWrapper) connection).unwrap(OlapConnection.class);
    }

    public void shutdown() {
        if(!shutdown.getAndSet(true)) {
            instances.remove(this);
            repositoryContentFinder.shutdown();
        }
    }

    ServerInfo getServerInfo() {
        synchronized (SERVER_INFO_LOCK) {
            if (this.serverInfo != null) {
                return this.serverInfo;
            }

            final String content = getRepositoryContentFinder().getContent();
            DataSourcesConfig.DataSources xmlDataSources =
                XmlaSupport.parseDataSources(content, LOGGER);
            ServerInfo serverInfo = new ServerInfo();

            for (DataSourcesConfig.DataSource xmlDataSource
                : xmlDataSources.dataSources)
            {
                final Map<String, Object> dsPropsMap =
                    Olap4jUtil.<String, Object>mapOf(
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
                        xmlDataSource.authenticationMode);
                final DatabaseInfo databaseInfo =
                    new DatabaseInfo(
                        xmlDataSource.name,
                        dsPropsMap);
                serverInfo.datasourceMap.put(
                    xmlDataSource.name,
                    databaseInfo);
                for (DataSourcesConfig.Catalog xmlCatalog
                    : xmlDataSource.catalogs.catalogs)
                {
                    if (databaseInfo.catalogMap.containsKey(xmlCatalog.name)) {
                        throw Util.newError(
                            "more than one DataSource object has name '"
                            + xmlCatalog.name + "'");
                    }
                    String connectString =
                        xmlCatalog.dataSourceInfo != null
                            ? xmlCatalog.dataSourceInfo
                            : xmlDataSource.dataSourceInfo;
                    // Check if the catalog is part of the connect
                    // string. If not, add it.
                    final Util.PropertyList connectProperties =
                        Util.parseConnectString(connectString);
                    if (connectProperties
                        .get(RolapConnectionProperties.Catalog.name()) == null)
                    {
                        connectString +=
                            ";"
                            + RolapConnectionProperties.Catalog.name()
                            + "="
                            + xmlCatalog.definition;
                    }
                    final CatalogInfo catalogInfo =
                        new CatalogInfo(
                            xmlCatalog.name,
                            connectString,
                            locator);
                    databaseInfo.catalogMap.put(
                        xmlCatalog.name,
                        catalogInfo);
                }
            }
            this.serverInfo = serverInfo;
            return serverInfo;
        }
    }

    public List<String> getCatalogNames(
        RolapConnection connection,
        String databaseName)
    {
        return new ArrayList<String>(
            getServerInfo().datasourceMap.get(databaseName)
                .catalogMap.keySet());
    }

    public List<String> getDatabaseNames(
        RolapConnection connection)
    {
        return new ArrayList<String>(
            getServerInfo().datasourceMap.keySet());
    }

    public Map<String, RolapSchema> getRolapSchemas(
        RolapConnection connection,
        String databaseName,
        String catalogName)
    {
        final RolapSchema schema =
            getServerInfo()
                .datasourceMap.get(databaseName)
                    .catalogMap.get(catalogName)
                        .getRolapSchema();
        return Collections.singletonMap(
            schema.getName(),
            schema);
    }

    @Override protected void finalize() throws Throwable {
        shutdown();
    }

    // Class is defined as package-protected in order to be accessible by unit
    // tests
    static class ServerInfo {
        private Map<String, DatabaseInfo> datasourceMap =
            new HashMap<String, DatabaseInfo>();

        // Method is created to variable has been been accessible by unit tests
        Map<String, DatabaseInfo> getDatasourceMap() {
           return datasourceMap;
        }
    }

    // Class is defined as package-protected in order to be accessible by unit
    // tests
    static class DatabaseInfo {
        private final String name;
        private final Map<String, Object> properties;
        Map<String, CatalogInfo> catalogMap =
            new HashMap<String, CatalogInfo>();

        DatabaseInfo(String name, Map<String, Object> properties) {
            this.name = name;
            this.properties = properties;
        }

        // Method is created to variable has been been accessible by unit tests
        Map<String, Object> getProperties() {
            return properties;
        }
    }

    static class CatalogInfo {
        private final String connectString;
        private final String olap4jConnectString;
        private final CatalogLocator locator;

        CatalogInfo(
            String name,
            String connectString,
            CatalogLocator locator)
        {
            this.connectString = connectString;
            this.locator = locator;
            this.olap4jConnectString =
                connectString.startsWith("jdbc:")
                    ? connectString
                    : "jdbc:mondrian:" + connectString;
        }

        private RolapSchema getRolapSchema() {
            RolapConnection rolapConnection = null;
            try {
                rolapConnection =
                    (RolapConnection)
                        DriverManager.getConnection(
                            connectString, this.locator);
                return rolapConnection.getSchema();
            } finally {
                if (rolapConnection != null) {
                    rolapConnection.close();
                }
            }
        }
    }

    // Method is defined as package-protected in order to be accessible by unit
    // tests
    RepositoryContentFinder getRepositoryContentFinder() {
        return repositoryContentFinder;
    }
}

// End FileRepository.java
