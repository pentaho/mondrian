/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2006-2010 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.server;

import mondrian.olap.*;
import mondrian.rolap.CacheControlImpl;
import mondrian.rolap.RolapSchema;
import mondrian.util.Pair;
import mondrian.xmla.*;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * Implementation of
 * {@link RepositoryContentFinder} that
 * periodically reloads the content of the repository.
 *
 * @version $Id$
 * @author Thiyagu Ajit
 * @author Luc Boudreau
 * @author Julian Hyde
 */
public class DynamicContentFinder
    extends UrlRepositoryContentFinder
{
    protected String lastDataSourcesConfigString;

    /**
     * Contains the timestamp in millis when the
     * schema next needs to be checked for updates.
     */
    private long nextUpdate;

    private final int refreshIntervalMillis;
    protected DataSourcesConfig.DataSources dataSources;

    private static final Logger LOGGER = Logger.getLogger(MondrianServer.class);

    /**
     * Creates a DynamicContentFinder.
     *
     * @param dataSourcesConfigUrl URL of repository
     * @param refreshIntervalMillis Interval to check for changes
     */
    public DynamicContentFinder(
        String dataSourcesConfigUrl,
        int refreshIntervalMillis)
    {
        super(dataSourcesConfigUrl);
        this.refreshIntervalMillis = refreshIntervalMillis;
        this.nextUpdate = Long.MIN_VALUE;
        reloadDataSources();
    }

    /**
     * Checks for updates to datasources content, flushes obsolete catalogs.
     */
    public void reloadDataSources() {
        try {
            String dataSourcesConfigString = getContent();
            if (!hasDataSourcesContentChanged(dataSourcesConfigString)) {
                return;
            }
            DataSourcesConfig.DataSources newDataSources =
                XmlaUtil.parseDataSources(
                    dataSourcesConfigString, LOGGER);
            if (newDataSources == null) {
                return;
            }
            flushObsoleteCatalogs(newDataSources);
            this.dataSources = newDataSources;
            this.lastDataSourcesConfigString = dataSourcesConfigString;
        } catch (Exception e) {
            throw Util.newError(
                e,
                "Failed to parse data sources config '" + url + "'");
        }
    }

    protected boolean hasDataSourcesContentChanged(
        String dataSourcesConfigString)
    {
        return dataSourcesConfigString != null
            && !dataSourcesConfigString.equals(
                this.lastDataSourcesConfigString);
    }

    private Map<String, Pair<DataSourcesConfig.DataSource,
            DataSourcesConfig.Catalog>> createCatalogMap(
        DataSourcesConfig.DataSources newDataSources)
    {
        Map<String,
            Pair<DataSourcesConfig.DataSource,
                DataSourcesConfig.Catalog>> newDatasourceCatalogNames =
                new HashMap<String,
                    Pair<DataSourcesConfig.DataSource,
                        DataSourcesConfig.Catalog>>();
        for (DataSourcesConfig.DataSource dataSource
            : newDataSources.dataSources)
        {
            for (DataSourcesConfig.Catalog catalog
                : dataSource.catalogs.catalogs)
            {
                if (catalog.dataSourceInfo == null) {
                    catalog.dataSourceInfo = dataSource.dataSourceInfo;
                }
                newDatasourceCatalogNames.put(
                    catalog.name, Pair.of(dataSource, catalog));
            }
        }
        return newDatasourceCatalogNames;
    }

    public void flushObsoleteCatalogs(
        DataSourcesConfig.DataSources newDataSources)
    {
        if (dataSources == null) {
            return;
        }

        Map<String,
            Pair<DataSourcesConfig.DataSource,
                DataSourcesConfig.Catalog>> newDatasourceCatalogs =
            createCatalogMap(newDataSources);

        for (DataSourcesConfig.DataSource oldDataSource
            : dataSources.dataSources)
        {
            for (DataSourcesConfig.Catalog oldCatalog
                : oldDataSource.catalogs.catalogs)
            {
                Pair<DataSourcesConfig.DataSource, DataSourcesConfig.Catalog>
                    pair =
                        newDatasourceCatalogs.get(oldCatalog.name);
                if (pair == null
                    || !areCatalogsEqual(
                    oldDataSource, oldCatalog, pair.left, pair.right))
                {
                    flushCatalog(oldCatalog.name);
                }
            }
        }
    }

    public void check() {
        // Check if an update is necessary
        final long now = System.currentTimeMillis();
        if (now > nextUpdate) {
            reloadDataSources();
            nextUpdate = now + refreshIntervalMillis;
        }
    }

    protected void flushCatalog(String catalogName) {
        for (RolapSchema schema : RolapSchema.getRolapSchemas()) {
            if (schema.getName().equals(catalogName)) {
                new CacheControlImpl().flushSchema(schema);
            }
        }
    }

    public static boolean areCatalogsEqual(
        DataSourcesConfig.DataSource dataSource1,
        DataSourcesConfig.Catalog catalog1,
        DataSourcesConfig.DataSource dataSource2,
        DataSourcesConfig.Catalog catalog2)
    {
        return
            Util.equals(dsi(dataSource1, catalog1), dsi(dataSource2, catalog2))
            && catalog1.name.equals(catalog2.name)
            && catalog1.definition.equals(catalog2.definition);
    }

    public static String dsi(
        DataSourcesConfig.DataSource dataSource,
        DataSourcesConfig.Catalog catalog)
    {
        return catalog.dataSourceInfo == null && dataSource != null
            ? dataSource.dataSourceInfo
            : catalog.dataSourceInfo;
    }
}

// End DynamicContentFinder.java
