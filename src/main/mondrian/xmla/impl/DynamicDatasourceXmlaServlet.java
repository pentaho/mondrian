/*
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2007 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/

package mondrian.xmla.impl;

import mondrian.olap.Util;
import mondrian.rolap.CacheControlImpl;
import mondrian.rolap.RolapSchema;
import mondrian.xmla.DataSourcesConfig;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Extends DefaultXmlaServlet to add dynamic datasource loading capability.
 * Limitations : Catalog name should be unique across the datasources
 *
 * @author Thiyagu, Ajit
 */
public class DynamicDatasourceXmlaServlet extends DefaultXmlaServlet {
    protected URL dataSourcesConfigUrl;
    protected String lastDataSourcesConfigString;

    protected void doPost(
            HttpServletRequest request,
            HttpServletResponse response)
            throws ServletException, IOException {
        reloadDataSources();
        super.doPost(request, response);
    }

    /**
     * Checks for updates to datasources content, flushes obsolete catalogs
     */
    void reloadDataSources() {
        try {
            String dataSourcesConfigString = readDataSourcesContent(dataSourcesConfigUrl);
            if (hasDataSourcesContentChanged(dataSourcesConfigString)) {
                DataSourcesConfig.DataSources newDataSources =
                        parseDataSources(dataSourcesConfigString);
                if (newDataSources != null) {
                    flushObsoleteCatalogs(newDataSources);
                    this.dataSources = newDataSources;
                    xmlaHandler = null;
                    lastDataSourcesConfigString = dataSourcesConfigString;
                }
            }
        } catch (Exception e) {
            throw Util.newError(e, "Failed to parse data sources config '" +
                    dataSourcesConfigUrl.toExternalForm() + "'");
        }
    }

    protected boolean hasDataSourcesContentChanged
            (String dataSourcesConfigString) {
        return dataSourcesConfigString != null
                && !dataSourcesConfigString.equals(this.lastDataSourcesConfigString);
    }

    /**
     * Overides XmlaServlet.parseDataSourcesUrl to store dataSorucesConfigUrl,
     * Datasoruces Configuration content
     * @param dataSourcesConfigUrl
     */
    protected DataSourcesConfig.DataSources parseDataSourcesUrl
            (URL dataSourcesConfigUrl) {
        this.dataSourcesConfigUrl = dataSourcesConfigUrl;
        try {
            String dataSourcesConfigString = readDataSourcesContent(dataSourcesConfigUrl);
            if (lastDataSourcesConfigString == null) {
                // This is the first time we are reading any datasource config
                this.lastDataSourcesConfigString = dataSourcesConfigString;
            }
            return parseDataSources(dataSourcesConfigString);
        } catch (Exception e) {
            throw Util.newError(e, "Failed to parse data sources config '" +
                    dataSourcesConfigUrl.toExternalForm() + "'");
        }
    }

    void flushObsoleteCatalogs(DataSourcesConfig.DataSources newDataSources) {
        Map<String, DataSourcesConfig.Catalog> newDatasourceCatalogs =
                createCatalogMap(newDataSources);

        for (DataSourcesConfig.DataSource oldDataSource : dataSources.dataSources) {
            for (DataSourcesConfig.Catalog oldCatalog : oldDataSource.catalogs.catalogs) {
                DataSourcesConfig.Catalog newCatalog =
                        newDatasourceCatalogs.get(oldCatalog.name);
                if (!(newCatalog != null &&
                        areCatalogsEqual(oldCatalog, newCatalog))) {
                    flushCatalog(oldCatalog.name);
                }
            }
        }
    }

    private Map<String, DataSourcesConfig.Catalog> createCatalogMap
            (DataSourcesConfig.DataSources newDataSources) {
        Map<String, DataSourcesConfig.Catalog> newDatasourceCatalogNames =
                new HashMap<String, DataSourcesConfig.Catalog>();
        for (DataSourcesConfig.DataSource dataSource : newDataSources.dataSources) {
            for (DataSourcesConfig.Catalog catalog : dataSource.catalogs.catalogs) {
                newDatasourceCatalogNames.put(catalog.name, catalog);
            }
        }
        return newDatasourceCatalogNames;
    }

    void flushCatalog(String catalogName) {
        Iterator schemas = RolapSchema.getRolapSchemas();
        while (schemas.hasNext()) {
            RolapSchema curSchema = (RolapSchema) schemas.next();
            if (curSchema.getName().equals(catalogName)) {
                new CacheControlImpl().flushSchema(curSchema);
            }
        }
    }

    boolean areCatalogsEqual
            (DataSourcesConfig.Catalog catalog1, DataSourcesConfig.Catalog catalog2) {

        if ((catalog1.getDataSourceInfo() != null &&
                catalog2.getDataSourceInfo() == null) ||
                (catalog2.getDataSourceInfo() != null &&
                        catalog1.getDataSourceInfo() == null)) {
            return false;
        }

        if ((catalog1.getDataSourceInfo() == null &&
                catalog2.getDataSourceInfo() == null) ||
                (catalog1.getDataSourceInfo().equals(catalog2.getDataSourceInfo()))) {

            return (catalog1.name.equals(catalog2.name) &&
                    catalog1.definition.equals(catalog2.definition));
        }
        return false;
    }
}
