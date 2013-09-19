/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.server;

import mondrian.olap.*;
import mondrian.rolap.RolapSchema;
import mondrian.tui.XmlaSupport;
import mondrian.util.Pair;
import mondrian.xmla.DataSourcesConfig;

import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of
 * {@link RepositoryContentFinder} that
 * periodically reloads the content of the repository.
 *
 * <p>The updates are performed by a background thread.
 * It is important to call {@link DynamicContentFinder#shutdown()}
 * once this object can be disposed of.
 *
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

    private final Timer timer;

    /**
     * Creates a DynamicContentFinder.
     * @param dataSourcesConfigUrl URL of repository
     */
    public DynamicContentFinder(
        String dataSourcesConfigUrl)
    {
        super(dataSourcesConfigUrl);
        reloadDataSources();
        timer = Util.newTimer(
            "mondrian.server.DynamicContentFinder$timer",
            true);
        final Pair<Long, TimeUnit> interval =
            Util.parseInterval(
                String.valueOf(
                    MondrianProperties.instance()
                        .XmlaSchemaRefreshInterval.get()),
                TimeUnit.MILLISECONDS);
        final long period = interval.right.toMillis(interval.left);
        timer.schedule(
            new TimerTask() {
                public void run() {
                    reloadDataSources();
                }
            },
            period,
            period);
    }

    /**
     * Cleans up all background updating jobs.
     */
    public void shutdown() {
        super.shutdown();
        timer.cancel();
    }

    /**
     * Checks for updates to datasources content, flushes obsolete catalogs.
     */
    public synchronized void reloadDataSources() {
        try {
            String dataSourcesConfigString = getContent();
            if (!hasDataSourcesContentChanged(dataSourcesConfigString)) {
                return;
            }
            DataSourcesConfig.DataSources newDataSources =
                XmlaSupport.parseDataSources(
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

    private Map<
        String,
        Pair<DataSourcesConfig.DataSource, DataSourcesConfig.Catalog>>
    createCatalogMap(
        DataSourcesConfig.DataSources newDataSources)
    {
        Map<
            String,
            Pair<DataSourcesConfig.DataSource, DataSourcesConfig.Catalog>>
            newDatasourceCatalogNames =
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

    public synchronized void flushObsoleteCatalogs(
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
                schema.getInternalConnection().getCacheControl(null)
                    .flushSchema(schema);
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
