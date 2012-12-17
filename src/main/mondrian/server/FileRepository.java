/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2010-2012 Pentaho
// All Rights Reserved.
*/
package mondrian.server;

import mondrian.olap.*;
import mondrian.olap4j.MondrianOlap4jDriver;
import mondrian.rolap.*;
import mondrian.spi.CatalogLocator;
import mondrian.tui.XmlaSupport;
import mondrian.util.*;
import mondrian.xmla.DataSourcesConfig;

import org.apache.log4j.Logger;

import org.olap4j.*;
import org.olap4j.impl.Olap4jUtil;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;

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

    private static final Logger LOGGER = Logger.getLogger(MondrianServer.class);

    private static final ScheduledExecutorService executorService =
        Util.getScheduledExecutorService(
            1,
            "mondrian.server.DynamicContentFinder$executorService");

    private ServerInfo serverInfo;
    private final ScheduledFuture<?> scheduledFuture;
    private final CatalogLocator locator;

    public FileRepository(
        RepositoryContentFinder repositoryContentFinder,
        CatalogLocator locator)
    {
        this.repositoryContentFinder = repositoryContentFinder;
        this.locator = locator;
        assert repositoryContentFinder != null;
        final Pair<Long, TimeUnit> interval =
            Util.parseInterval(
                String.valueOf(
                    MondrianProperties.instance()
                        .XmlaSchemaRefreshInterval.get()),
                TimeUnit.MILLISECONDS);
        scheduledFuture = executorService.scheduleWithFixedDelay(
            new Runnable() {
                public void run() {
                    synchronized (SERVER_INFO_LOCK) {
                        serverInfo = null;
                    }
                }
            },
            0,
            interval.left,
            interval.right);
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
        final DatabaseInfo datasourceInfo;
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
        }
        if (datasourceInfo == null) {
            throw Util.newError("Unknown database '" + databaseName + "'");
        }

        final CatalogInfo catalogInfo;
        if (catalogName == null) {
            if (datasourceInfo.catalogMap.size() == 0) {
                throw new OlapException(
                    "No catalogs in the database named "
                    + datasourceInfo.name);
            }
            catalogInfo =
                datasourceInfo
                    .catalogMap
                    .values()
                    .iterator()
                    .next();
        } else {
            catalogInfo =
                datasourceInfo.catalogMap.get(catalogName);
        }
        if (catalogInfo == null) {
            throw Util.newError("Unknown catalog '" + catalogName + "'");
        }
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
        // Now create the connection
        final java.sql.Connection connection =
            java.sql.DriverManager.getConnection(connectString, properties);
        return ((OlapWrapper) connection).unwrap(OlapConnection.class);
    }

    public void shutdown() {
        scheduledFuture.cancel(false);
        repositoryContentFinder.shutdown();
    }

    private ServerInfo getServerInfo() {
        synchronized (SERVER_INFO_LOCK) {
            if (this.serverInfo != null) {
                return this.serverInfo;
            }

            final String content = repositoryContentFinder.getContent();
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
                        xmlDataSource.getDataSourceName(),
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

    private static class ServerInfo {
        private Map<String, DatabaseInfo> datasourceMap =
            new HashMap<String, DatabaseInfo>();
    }

    private static class DatabaseInfo {
        private final String name;
        private final Map<String, Object> properties;
        private Map<String, CatalogInfo> catalogMap =
            new HashMap<String, CatalogInfo>();

        DatabaseInfo(String name, Map<String, Object> properties) {
            this.name = name;
            this.properties = properties;
        }
    }

    private static class CatalogInfo {
        private final String connectString;
        private RolapSchema rolapSchema; // populated on demand
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
            if (rolapSchema == null) {
                RolapConnection rolapConnection = null;
                try {
                    rolapConnection =
                        (RolapConnection)
                            DriverManager.getConnection(
                                connectString, this.locator);
                    rolapSchema = rolapConnection.getSchema();
                } finally {
                    if (rolapConnection != null) {
                        rolapConnection.close();
                    }
                }
            }
            return rolapSchema;
        }
    }
}

// End FileRepository.java
