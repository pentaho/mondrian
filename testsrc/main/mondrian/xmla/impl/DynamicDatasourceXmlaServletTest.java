/*
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2007 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.xmla.impl;

import junit.framework.TestCase;
import mondrian.xmla.DataSourcesConfig;
import org.eigenbase.xom.DOMWrapper;
import org.eigenbase.xom.Parser;
import org.eigenbase.xom.XOMException;
import org.eigenbase.xom.XOMUtil;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

/**
 * Unit test for DynamicDatasourceXmlaServlet
 *
 * @author Thiyagu, Ajit
 * @version $Id$
 * @since Mar 30, 2007
 */
public class DynamicDatasourceXmlaServletTest extends TestCase {

    private static final String CATALOG_0_NAME = "FoodMart0";
    private static final String CATALOG_1_NAME = "FoodMart1";
    private static final String CATALOG_2_NAME = "FoodMart2";
    private static final String CATALOG_0_DEFINITION = "<Catalog name='" + CATALOG_0_NAME + "'><Definition>Provider=mondrian;Jdbc=jdbc:odbc:MondrianFoodMart0;JdbcDrivers=sun.jdbc.odbc.JdbcOdbcDriver</Definition></Catalog>";
    private static final String CATALOG_0_UPDATED_DEFINITION = "<Catalog name='" + CATALOG_0_NAME + "'><Definition>Provider=mondrian;Jdbc=jdbc:odbc:MondrianFoodMart0.0;JdbcDrivers=sun.jdbc.odbc.JdbcOdbcDriver</Definition></Catalog>";
    private static final String CATALOG_1_DEFINITION = "<Catalog name='" + CATALOG_1_NAME + "'><Definition>Provider=mondrian;Jdbc=jdbc:odbc:MondrianFoodMart1;JdbcDrivers=sun.jdbc.odbc.JdbcOdbcDriver</Definition></Catalog>";
    private static final String CATALOG_2_DEFINITION = "<Catalog name='" + CATALOG_2_NAME + "'><Definition>Provider=mondrian;Jdbc=jdbc:odbc:MondrianFoodMart2;JdbcDrivers=sun.jdbc.odbc.JdbcOdbcDriver</Definition></Catalog>";
    private static final String DATASOURCE_1_NAME = "DATASOURCENAME1";
    private static final String DATASOURCE_2_NAME = "DATASOURCENAME2";

    public void testFlushObsoleteCatalogsForNewCatalog() throws Exception {
        MockDynamicDatasourceXmlaServlet xmlaServlet = new MockDynamicDatasourceXmlaServlet();
        DataSourcesConfig.DataSources newDataSources = getDataSources(CATALOG_0_DEFINITION, CATALOG_1_DEFINITION);
        xmlaServlet.flushObsoleteCatalogs(newDataSources);
        assertTrue(xmlaServlet.flushCatalogList().isEmpty());
    }

    public void testFlushObsoleteCatalogsForUpdateCatalog() throws Exception {
        MockDynamicDatasourceXmlaServlet xmlaServlet = new MockDynamicDatasourceXmlaServlet();
        DataSourcesConfig.DataSources newDataSources = getDataSources(CATALOG_0_UPDATED_DEFINITION);
        xmlaServlet.flushObsoleteCatalogs(newDataSources);
        assertTrue(xmlaServlet.flushCatalogList().contains(CATALOG_0_NAME));
    }

    public void testFlushObsoleteCatalogsForUnchangedCatalog() throws Exception {
        MockDynamicDatasourceXmlaServlet xmlaServlet = new MockDynamicDatasourceXmlaServlet();
        DataSourcesConfig.DataSources newDataSources = getDataSources(CATALOG_0_DEFINITION, CATALOG_1_DEFINITION);
        xmlaServlet.flushObsoleteCatalogs(newDataSources);
        assertFalse(xmlaServlet.flushCatalogList().contains(CATALOG_0_NAME));
    }

    public void testFlushObsoleteCatalogsForDeletedCatalog() throws Exception {
        MockDynamicDatasourceXmlaServlet xmlaServlet = new MockDynamicDatasourceXmlaServlet();
        DataSourcesConfig.DataSources newDataSources = getDataSources(CATALOG_1_DEFINITION);
        xmlaServlet.flushObsoleteCatalogs(newDataSources);
        assertTrue(xmlaServlet.flushCatalogList().contains(CATALOG_0_NAME));
    }

    public void testMergeDataSourcesForAlteringCatalogAcrossDataSources() throws Exception {
        Map<String, String[]> dsCatalog = new HashMap<String, String[]>();
        dsCatalog.put(DATASOURCE_1_NAME, new String[]{CATALOG_0_UPDATED_DEFINITION, CATALOG_1_DEFINITION});
        dsCatalog.put(DATASOURCE_2_NAME, new String[]{CATALOG_2_DEFINITION});
        MockDynamicDatasourceXmlaServlet xmlaServlet = new MockDynamicDatasourceXmlaServlet();
        DataSourcesConfig.DataSources newDataSources = getDataSources(dsCatalog);
        xmlaServlet.flushObsoleteCatalogs(newDataSources);
        assertTrue(xmlaServlet.flushCatalogList().contains(CATALOG_0_NAME));
    }

    public void testAreCatalogsEqual() throws Exception {
        DataSourcesConfig.DataSources newDataSources = getDataSources(CATALOG_0_DEFINITION, CATALOG_0_UPDATED_DEFINITION, CATALOG_1_DEFINITION, CATALOG_2_DEFINITION);
        DataSourcesConfig.DataSource datasource = newDataSources.dataSources[0];
        DataSourcesConfig.Catalog catalog0 = datasource.catalogs.catalogs[0];
        DataSourcesConfig.Catalog catalog0Updated = datasource.catalogs.catalogs[1];
        DataSourcesConfig.Catalog catalog1 = datasource.catalogs.catalogs[2];
        DataSourcesConfig.Catalog catalog2 = datasource.catalogs.catalogs[3];
        DynamicDatasourceXmlaServlet xmlaServlet = new DynamicDatasourceXmlaServlet();
        assertFalse(xmlaServlet.areCatalogsEqual(catalog0, catalog0Updated));
        assertTrue(xmlaServlet.areCatalogsEqual(catalog0, catalog0));
        assertFalse(xmlaServlet.areCatalogsEqual(catalog1, catalog2));
    }


    private DataSourcesConfig.DataSources getDataSources(String... catalogs) throws XOMException {
        HashMap<String, String[]> hashMap = new HashMap<String, String[]>();
        hashMap.put(DATASOURCE_1_NAME, catalogs);
        return getDataSources(hashMap);
    }

    private DataSourcesConfig.DataSources getDataSources(Map<String, String[]> dsCatalog) throws XOMException {
        StringBuilder ds = new StringBuilder();
        ds.append("<?xml version=\"1.0\"?>");
        ds.append("<DataSources>");
        for (Map.Entry<String, String[]> entry : dsCatalog.entrySet()) {
            final String dsName = entry.getKey();
            ds.append("<DataSource> ");
            ds.append("  <DataSourceName>").append(dsName).append("</DataSourceName>");
            ds.append("       <DataSourceDescription>DATASOURCE_DESCRIPTION</DataSourceDescription>");
            ds.append("       <URL>http://localhost:8080/mondrian/xmla</URL>");
            ds.append("       <DataSourceInfo>Provider=mondrian;Jdbc=jdbc:oracle:thin:foodmart/foodmart@//marmalade.hydromatic.net:1521/XE;JdbcUser=foodmart;JdbcPassword=foodmart;JdbcDrivers=oracle.jdbc.OracleDriver;Catalog=/WEB-INF/queries/FoodMart.xml</DataSourceInfo>");
            ds.append("       <ProviderName>Mondrian</ProviderName>");
            ds.append("       <ProviderType>MDP</ProviderType>");
            ds.append("       <AuthenticationMode>Unauthenticated</AuthenticationMode>");
            ds.append("       <Catalogs>");
            final String[] catalogs = entry.getValue();
            for (String catalog : catalogs) {
                ds.append(catalog);
            }
            ds.append("       </Catalogs>");
            ds.append("</DataSource>");
        }
        ds.append("</DataSources>");
        final Parser xmlParser = XOMUtil.createDefaultParser();
        final DOMWrapper def = xmlParser.parse(ds.toString());
        return new DataSourcesConfig.DataSources(def);
    }

    public void testReloadDataSources() throws Exception {
        MockDynamicDatasourceXmlaServlet xmlaServlet = new MockDynamicDatasourceXmlaServlet();
        DataSourcesConfig.DataSources ds1 = getDataSources(CATALOG_0_DEFINITION, CATALOG_1_DEFINITION);
        DataSourcesConfig.DataSources ds2 = getDataSources(CATALOG_1_DEFINITION, CATALOG_2_DEFINITION);

        File dsFile = null;
        try {
            dsFile = File.createTempFile(Long.toString(System.currentTimeMillis()), null);

            OutputStream out = new FileOutputStream(dsFile);
            out.write(ds1.toXML().getBytes());
            out.flush();

            xmlaServlet.parseDataSourcesUrl(dsFile.toURL()); //Simulate servlet init

            out = new FileOutputStream(dsFile);
            out.write(ds2.toXML().getBytes());
            out.flush();

            xmlaServlet.reloadDataSources();

            assertTrue(xmlaServlet.containsCatalog(DATASOURCE_1_NAME, CATALOG_1_NAME));
            assertTrue(xmlaServlet.containsCatalog(DATASOURCE_1_NAME, CATALOG_2_NAME));
            assertFalse(xmlaServlet.containsCatalog(DATASOURCE_1_NAME, CATALOG_0_NAME));

            out = new FileOutputStream(dsFile);
            out.write(ds1.toXML().getBytes());
            out.flush();

            xmlaServlet.reloadDataSources();
            assertTrue(xmlaServlet.containsCatalog(DATASOURCE_1_NAME, CATALOG_0_NAME));
            assertTrue(xmlaServlet.containsCatalog(DATASOURCE_1_NAME, CATALOG_1_NAME));
            assertFalse(xmlaServlet.containsCatalog(DATASOURCE_1_NAME, CATALOG_2_NAME));
        } finally {
            if (dsFile != null) {
                dsFile.delete();
            }
        }
    }


    class MockDynamicDatasourceXmlaServlet extends DynamicDatasourceXmlaServlet {
        private List<String> flushCatalogList = new Vector<String>();

        public MockDynamicDatasourceXmlaServlet() throws XOMException {
            dataSources = getDataSources(CATALOG_0_DEFINITION);
        }

        public boolean containsCatalog(String datasourceName, String catalogName) {
            return locateCatalog(datasourceName, catalogName) != null;
        }

        public DataSourcesConfig.Catalog locateCatalog(
            String datasourceName,
            String catalogName)
        {
            for (DataSourcesConfig.DataSource ds : dataSources.dataSources) {
                if (ds.name.equals(datasourceName)) {
                    for (DataSourcesConfig.Catalog catalog : ds.catalogs.catalogs) {
                        if (catalog.name.equals(catalogName)) {
                            return catalog;
                        }
                    }
                }
            }
            return null;
        }

        void flushCatalog(String catalogName) {
            flushCatalogList.add(catalogName);
        }

        public List flushCatalogList() {
            return flushCatalogList;
        }
    }
}

// End DynamicDatasourceXmlaServletTest.java