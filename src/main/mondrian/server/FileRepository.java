/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2010-2011 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.server;

import mondrian.olap.*;
import mondrian.olap.DriverManager;
import mondrian.olap4j.MondrianOlap4jDriver;
import mondrian.rolap.*;
import mondrian.tui.XmlaSupport;
import mondrian.xmla.*;
import org.apache.log4j.Logger;
import org.olap4j.*;
import org.olap4j.impl.Olap4jUtil;

import java.sql.*;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Implementation of {@link mondrian.server.Repository} that reads
 * from a {@code datasources.xml} file.
 *
 * <p>Note that for legacy reasons, the datasources.xml file's root
 * element is called DataSource whereas the olap4j standard calls them
 * Databases. This is why those two concepts are linked, as in
 * {@link FileRepository#getDatabaseNames(RolapConnection)} for example.
 *
 * @version $Id$
 * @author Julian Hyde, Luc Boudreau
 */
public class FileRepository implements Repository {
    private ServerInfo serverInfo;
    private final RepositoryContentFinder repositoryContentFinder;

    private static final Logger LOGGER = Logger.getLogger(MondrianServer.class);

    private static AtomicInteger threadNumber = new AtomicInteger(0);

    private final static ScheduledExecutorService executorService =
        Executors.newScheduledThreadPool(
            0,
            new ThreadFactory() {
                public Thread newThread(Runnable r) {
                    Thread t = Executors.defaultThreadFactory().newThread(r);
                    t.setDaemon(true);
                    t.setName(
                        "mondrian.FileRepositoryUpdaterThread"
                        + threadNumber.addAndGet(1));
                    return t;
               }
            });
    private final ScheduledFuture<?> scheduledFuture;

    public FileRepository(RepositoryContentFinder repositoryContentFinder) {
        this.repositoryContentFinder = repositoryContentFinder;
        assert repositoryContentFinder != null;
        scheduledFuture = executorService.scheduleWithFixedDelay(
            new Runnable() {
                public void run() {
                    serverInfo = null;
                }
            },
            0,
            MondrianProperties.instance().XmlaSchemaRefreshInterval.get(),
            TimeUnit.MILLISECONDS);
    }

    public List<Map<String, Object>> getDatabases(
        RolapConnection connection)
    {
        final List<Map<String, Object>> propsList =
            new ArrayList<Map<String, Object>>();
        for (DatasourceInfo dsInfo : getServerInfo().datasourceMap.values()) {
            propsList.add(dsInfo.properties);
        }
        return propsList;
    }

    public OlapConnection getConnection(
        MondrianServer server,
        String datasourceName,
        String catalogName,
        String roleName,
        Properties props)
        throws SQLException
    {
        final ServerInfo serverInfo = getServerInfo();
        final DatasourceInfo datasourceInfo;
        if (datasourceName == null) {
            if (serverInfo.datasourceMap.size() == 0) {
                throw new OlapException(
                    "No datasources configured on this server");
            }
            datasourceInfo =
                serverInfo
                    .datasourceMap
                    .values()
                    .iterator()
                    .next();
        } else {
            datasourceInfo =
                serverInfo.datasourceMap.get(datasourceName);
        }
        if (datasourceInfo == null) {
            throw Util.newError("unknown catalog '" + datasourceName + "'");
        }

        final CatalogInfo catalogInfo;
        if (catalogName == null) {
            if (datasourceInfo.catalogMap.size() == 0) {
                throw new OlapException(
                    "No catalogs in the datasource named "
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
            throw Util.newError("unknown schema '" + catalogName + "'");
        }
        String connectString = catalogInfo.olap4jConnectString;
        final Properties properties = new Properties();
        properties.setProperty(
            RolapConnectionProperties.Instance.name(),
            server.getId());
        if (roleName != null) {
            properties.setProperty(
                RolapConnectionProperties.Role.name(),
                roleName);
        }
        properties.putAll(props);
        // Make sure we load the Mondrian driver into
        // the ClassLoader.
        try {
            Class.forName(MondrianOlap4jDriver.class.getName());
        } catch (ClassNotFoundException e) {
            throw new OlapException("Cannot find mondrian olap4j driver.");
        }
        // Now create the connection
        final java.sql.Connection connection =
            java.sql.DriverManager.getConnection(connectString, properties);
        return ((OlapWrapper) connection).unwrap(OlapConnection.class);
    }

    public void shutdown() {
        scheduledFuture.cancel(true);
        repositoryContentFinder.shutdown();
    }

    private synchronized ServerInfo getServerInfo() {
        if (serverInfo != null) {
            return serverInfo;
        }
        final String content = repositoryContentFinder.getContent();
        DataSourcesConfig.DataSources xmlDataSources =
            XmlaSupport.parseDataSources(content, LOGGER);
        serverInfo = new ServerInfo();

        for (DataSourcesConfig.DataSource xmlDataSource
            : xmlDataSources.dataSources)
        {
            final Map<String, Object> dsPropsMap =
                Olap4jUtil.<String, Object>mapOf(
                    "DataSourceName", xmlDataSource.getDataSourceName(),
                    "DataSourceDescription", xmlDataSource
                        .getDataSourceDescription(),
                    "URL", xmlDataSource.getURL(),
                    "DataSourceInfo", xmlDataSource.getDataSourceName(),
                    "ProviderName", xmlDataSource.getProviderName(),
                    "ProviderType", xmlDataSource.providerType,
                    "AuthenticationMode", xmlDataSource.authenticationMode);
            final DatasourceInfo catalogInfo =
                new DatasourceInfo(
                    xmlDataSource.name,
                    dsPropsMap);
            serverInfo.datasourceMap.put(
                xmlDataSource.name,
                catalogInfo);
            for (DataSourcesConfig.Catalog xmlCatalog
                    : xmlDataSource.catalogs.catalogs)
            {
                if (catalogInfo.catalogMap.containsKey(xmlCatalog.name)) {
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
                if (connectProperties.get(
                    RolapConnectionProperties.Catalog.name()) == null)
                {
                    connectString +=
                        ";"
                        + RolapConnectionProperties.Catalog.name()
                        + "="
                        + xmlCatalog.definition;
                }
                final CatalogInfo schemaInfo =
                    new CatalogInfo(
                        xmlCatalog.name,
                        connectString);
                catalogInfo.catalogMap.put(
                    xmlCatalog.name,
                    schemaInfo);
            }
        }
        return serverInfo;
    }

    public List<String> getCatalogNames(
        RolapConnection connection,
        String databaseName)
    {
        return
            new ArrayList<String>(
                    getServerInfo()
                        .datasourceMap.get(databaseName)
                            .catalogMap.keySet());
    }

    public List<String> getDatabaseNames(
        RolapConnection connection)
    {
        return
            new ArrayList<String>(
                    getServerInfo()
                        .datasourceMap.keySet());
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
        private Map<String, DatasourceInfo> datasourceMap =
            new HashMap<String, DatasourceInfo>();
    }

    private static class DatasourceInfo {
        private final String name;
        private final Map<String, Object> properties;
        private Map<String, CatalogInfo> catalogMap =
            new HashMap<String, CatalogInfo>();
        DatasourceInfo(String name, Map<String, Object> properties) {
            this.name = name;
            this.properties = properties;
        }
    }

    private static class CatalogInfo {
        private final String name;
        private final String connectString;
        private RolapSchema rolapSchema; // populated on demand
        private final String olap4jConnectString;

        CatalogInfo(String name, String connectString) {
            this.name = name;
            this.connectString = connectString;
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
                                connectString, null);
                    rolapSchema =
                        (RolapSchema) rolapConnection.getSchema();
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
