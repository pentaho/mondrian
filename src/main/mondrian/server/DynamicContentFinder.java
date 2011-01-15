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
import mondrian.rolap.*;
import mondrian.util.*;
import mondrian.xmla.*;

import org.apache.log4j.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Implementation of
 * {@link RepositoryContentFinder} that
 * periodically reloads the content of the repository.
 *
 * <p>The updates are performed by a background thread.
 * It is important to call {@link DynamicContentFinder#shutdown()}
 * once this object can be disposed of.
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

    protected DataSourcesConfig.DataSources dataSources;

    private static final Logger LOGGER =
        Logger.getLogger(MondrianServer.class);

    private static AtomicInteger threadNumber = new AtomicInteger(0);

    private final static ScheduledExecutorService executorService =
        Executors.newScheduledThreadPool(
            0,
            new ThreadFactory() {
                public Thread newThread(Runnable r) {
                    Thread t = Executors.defaultThreadFactory().newThread(r);
                    t.setDaemon(true);
                    t.setName(
                        "mondrian.DynamicContentFinderUpdaterThread"
                        + threadNumber.addAndGet(1));
                    return t;
               }
            });

    private final ScheduledFuture<?> scheduledTask;

    /**
     * Creates a DynamicContentFinder.
     * @param dataSourcesConfigUrl URL of repository
     */
    public DynamicContentFinder(
        String dataSourcesConfigUrl)
    {
        super(dataSourcesConfigUrl);
        reloadDataSources();
        scheduledTask = executorService.scheduleWithFixedDelay(
            new Runnable() {
                public void run() {
                    reloadDataSources();
                }
            },
            0,
            MondrianProperties.instance().XmlaSchemaRefreshInterval.get(),
            TimeUnit.MILLISECONDS);
    }

    /**
     * Cleans up all background updating jobs.
     */
    public void shutdown() {
        scheduledTask.cancel(true);
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
