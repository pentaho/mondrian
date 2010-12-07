/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2010-2010 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.server;

import mondrian.olap.*;
import mondrian.olap.DriverManager;
import mondrian.rolap.*;
import mondrian.xmla.DataSourcesConfig;
import mondrian.xmla.XmlaUtil;
import org.apache.log4j.Logger;
import org.olap4j.OlapConnection;
import org.olap4j.OlapWrapper;
import org.olap4j.impl.Olap4jUtil;

import java.sql.*;
import java.util.*;

/**
 * Implementation of {@link mondrian.server.Repository} that reads
 * from a {@code datasources.xml} file.
 *
 * @version $Id$
 * @author Julian Hyde
 */
public class FileRepository implements Repository {
    private ServerInfo serverInfo;
    private final RepositoryContentFinder repositoryContentFinder;
    private List<Map<String, Object>> dataSourceList =
        new ArrayList<Map<String, Object>>();

    private static final Logger LOGGER = Logger.getLogger(MondrianServer.class);

    public FileRepository(RepositoryContentFinder repositoryContentFinder) {
        this.repositoryContentFinder = repositoryContentFinder;
        assert repositoryContentFinder != null;
    }

    public List<Map<String, Object>> getDataSources(
        RolapConnection connection)
    {
        return dataSourceList;
    }

    public OlapConnection getConnection(
        MondrianServer server,
        String catalogName,
        String schemaName,
        String roleName,
        Properties props)
        throws SQLException
    {
        final CatalogInfo catalogInfo;
        final ServerInfo serverInfo = getServerInfo();
        if (catalogName == null) {
            // No catalog name specified. Take the first.
            final Iterator<CatalogInfo> iter =
                serverInfo.catalogMap.values().iterator();
            catalogInfo = iter.hasNext() ? iter.next() : null;
        } else {
            catalogInfo = serverInfo.catalogMap.get(catalogName);
        }
        if (catalogInfo == null) {
            throw /* new XmlaException(
                CLIENT_FAULT_FC,
                HSB_CONNECTION_DATA_SOURCE_CODE,
                HSB_CONNECTION_DATA_SOURCE_FAULT_FS, */
                Util.newError(
                    "no data source is configured with name '"
                    + catalogName + "'") /* ) */;
        }
        final SchemaInfo schemaInfo;
        if (schemaName == null) {
            // No schema name specified. Take the first.
            final Iterator<SchemaInfo> iter =
                catalogInfo.schemaMap.values().iterator();
            schemaInfo = iter.hasNext() ? iter.next() : null;
        } else {
            schemaInfo = catalogInfo.schemaMap.get(schemaName);
        }
        if (schemaInfo == null) {
            throw Util.newError("unknown schema '" + schemaName + "'");
        }
        String connectString = schemaInfo.olap4jConnectString;
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
        final java.sql.Connection connection =
            java.sql.DriverManager.getConnection(connectString, properties);
        return ((OlapWrapper) connection).unwrap(OlapConnection.class);
    }

    private synchronized ServerInfo getServerInfo() {
        if (serverInfo != null) {
            return serverInfo;
        }
        final String content = repositoryContentFinder.getContent();
        DataSourcesConfig.DataSources xmlDataSources =
            XmlaUtil.parseDataSources(content, LOGGER);
        serverInfo = new ServerInfo();

        // Iterate over catalogs (XMLA calls these DataSources).
        for (DataSourcesConfig.DataSource xmlDataSource
            : xmlDataSources.dataSources)
        {
            dataSourceList.add(
                Olap4jUtil.<String, Object>mapOf(
                    "DataSourceName", xmlDataSource.getDataSourceName(),
                    "DataSourceDescription", xmlDataSource
                        .getDataSourceDescription(),
                    "URL", xmlDataSource.getURL(),
                    "DataSourceInfo", xmlDataSource.getDataSourceName(),
                    "ProviderName", xmlDataSource.getProviderName(),
                    "ProviderType", xmlDataSource.providerType,
                    "AuthenticationMode", xmlDataSource.authenticationMode));
            final CatalogInfo catalogInfo =
                new CatalogInfo(
                    xmlDataSource.name,
                    xmlDataSource.dataSourceInfo);
            if (serverInfo.catalogMap.containsKey(catalogInfo.name)) {
                throw Util.newError(
                    "more than one DataSource object has name '"
                    + catalogInfo.name + "'");
            }
            serverInfo.catalogMap.put(
                catalogInfo.name,
                catalogInfo);

            // Iterate over schemas (XMLA calls these Catalogs).
            for (DataSourcesConfig.Catalog xmlCatalog
                : xmlDataSource.catalogs.catalogs)
            {
                final SchemaInfo schemaInfo =
                    new SchemaInfo(
                        xmlCatalog.name,
                        xmlCatalog.dataSourceInfo != null
                            ? xmlCatalog.dataSourceInfo
                            : xmlDataSource.dataSourceInfo);
                catalogInfo.schemaMap.put(
                    schemaInfo.name,
                    schemaInfo);
            }
        }
        return serverInfo;
    }

    public List<String> getCatalogNames(
        RolapConnection connection)
    {
        final ServerInfo serverInfo = getServerInfo();
        return new ArrayList<String>(serverInfo.catalogMap.keySet());
    }

    public Map<String, RolapSchema> getCatalogSchemas(
        RolapConnection connection, String catalogName)
    {
        final ServerInfo serverInfo = getServerInfo();
        final CatalogInfo catalogInfo =
            serverInfo.catalogMap.get(catalogName);
        if (catalogInfo == null) {
            return null;
        }
        return catalogInfo.getSchemaList();
    }

    private static class ServerInfo {
        private final Map<String, CatalogInfo> catalogMap =
            new HashMap<String, CatalogInfo>();
    }

    private static class CatalogInfo {
        private final String name;
        private final Map<String, SchemaInfo> schemaMap =
            new HashMap<String, SchemaInfo>();

        CatalogInfo(String name, String connectString) {
            this.name = name;
            Util.discard(connectString);
        }

        public Map<String, RolapSchema> getSchemaList() {
            final Map<String, RolapSchema> map =
                new LinkedHashMap<String, RolapSchema>();
            for (SchemaInfo schemaInfo : schemaMap.values()) {
                map.put(schemaInfo.name, schemaInfo.getRolapSchema());
            }
            return map;
        }
    }

    private static class SchemaInfo {
        private final String name;
        private final String connectString;
        private RolapSchema rolapSchema; // populated on demand
        private final String olap4jConnectString;

        SchemaInfo(String name, String connectString) {
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
