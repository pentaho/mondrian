/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2006-2012 Pentaho
// All Rights Reserved.
*/
package mondrian.xmla.impl;

import mondrian.olap.*;
import mondrian.server.DynamicContentFinder;
import mondrian.util.Pair;
import mondrian.xmla.DataSourcesConfig;

import junit.framework.TestCase;

import org.eigenbase.xom.*;
import org.eigenbase.xom.Parser;

import org.olap4j.impl.Olap4jUtil;

import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Unit test for DynamicDatasourceXmlaServlet
 *
 * @author Thiyagu, Ajit
 * @since Mar 30, 2007
 */
public class DynamicDatasourceXmlaServletTest extends TestCase {

    private static final String CATALOG_0_NAME = "FoodMart0";
    private static final String CATALOG_1_NAME = "FoodMart1";
    private static final String CATALOG_2_NAME = "FoodMart2";
    private static final String CATALOG_0_DEFINITION =
        "<Catalog name='" + CATALOG_0_NAME + "'>"
        + "<Definition>Provider=mondrian;Jdbc=jdbc:odbc:MondrianFoodMart0;"
        + "JdbcDrivers=sun.jdbc.odbc.JdbcOdbcDriver"
        + "</Definition>"
        + "</Catalog>";
    private static final String CATALOG_0_UPDATED_DEFINITION =
        "<Catalog name='" + CATALOG_0_NAME + "'>"
        + "<Definition>Provider=mondrian;Jdbc=jdbc:odbc:MondrianFoodMart0.0;"
        + "JdbcDrivers=sun.jdbc.odbc.JdbcOdbcDriver"
        + "</Definition>"
        + "</Catalog>";
    private static final String CATALOG_1_DEFINITION =
        "<Catalog name='" + CATALOG_1_NAME + "'>"
        + "<Definition>Provider=mondrian;Jdbc=jdbc:odbc:MondrianFoodMart1;"
        + "JdbcDrivers=sun.jdbc.odbc.JdbcOdbcDriver"
        + "</Definition>"
        + "</Catalog>";
    private static final String CATALOG_2_DEFINITION =
        "<Catalog name='" + CATALOG_2_NAME + "'>"
        + "<Definition>Provider=mondrian;Jdbc=jdbc:odbc:MondrianFoodMart2;"
        + "JdbcDrivers=sun.jdbc.odbc.JdbcOdbcDriver"
        + "</Definition>"
        + "</Catalog>";
    private static final String DATASOURCE_1_NAME = "DATASOURCENAME1";
    private static final String DATASOURCE_2_NAME = "DATASOURCENAME2";

    public void testFlushObsoleteCatalogsForNewCatalog() throws Exception {
        DataSourcesConfig.DataSources newDataSources =
            getDataSources(CATALOG_0_DEFINITION, CATALOG_1_DEFINITION);
        final MockDynamicContentFinder finder =
            new MockDynamicContentFinder(
                "inline:" + getDataSourceContent(CATALOG_0_DEFINITION));
        finder.flushObsoleteCatalogs(newDataSources);
        assertTrue(finder.flushCatalogList().isEmpty());
        finder.shutdown();
    }

    public void testFlushObsoleteCatalogsForUpdateCatalog() throws Exception {
        DataSourcesConfig.DataSources newDataSources =
            getDataSources(CATALOG_0_UPDATED_DEFINITION);
        final MockDynamicContentFinder finder =
            new MockDynamicContentFinder(
                "inline:" + getDataSourceContent(CATALOG_0_DEFINITION));
        finder.flushObsoleteCatalogs(newDataSources);
        assertTrue(finder.flushCatalogList().contains(CATALOG_0_NAME));
        finder.shutdown();
    }

    public void testFlushObsoleteCatalogsForUnchangedCatalog() throws Exception
    {
        DataSourcesConfig.DataSources newDataSources =
            getDataSources(CATALOG_0_DEFINITION, CATALOG_1_DEFINITION);
        final MockDynamicContentFinder finder =
            new MockDynamicContentFinder(
                "inline:" + getDataSourceContent(CATALOG_0_DEFINITION));
        finder.flushObsoleteCatalogs(newDataSources);
        assertFalse(finder.flushCatalogList().contains(CATALOG_0_NAME));
        finder.shutdown();
    }

    public void testFlushObsoleteCatalogsForDeletedCatalog() throws Exception {
        DataSourcesConfig.DataSources newDataSources =
            getDataSources(CATALOG_1_DEFINITION);
        final MockDynamicContentFinder finder =
            new MockDynamicContentFinder(
                "inline:" + getDataSourceContent(CATALOG_0_DEFINITION));
        finder.flushObsoleteCatalogs(newDataSources);
        assertTrue(finder.flushCatalogList().contains(CATALOG_0_NAME));
        finder.shutdown();
    }

    public void testMergeDataSourcesForAlteringCatalogAcrossDataSources()
        throws Exception
    {
        DataSourcesConfig.DataSources newDataSources =
            getDataSources(
                Olap4jUtil.mapOf(
                    DATASOURCE_1_NAME,
                    new String[] {
                        CATALOG_0_UPDATED_DEFINITION,
                        CATALOG_1_DEFINITION},
                    DATASOURCE_2_NAME,
                    new String[] {
                        CATALOG_2_DEFINITION}));
        final MockDynamicContentFinder finder =
            new MockDynamicContentFinder(
                "inline:" + getDataSourceContent(CATALOG_0_DEFINITION));
        finder.flushObsoleteCatalogs(newDataSources);
        assertTrue(finder.flushCatalogList().contains(CATALOG_0_NAME));
        finder.shutdown();
    }

    public void testAreCatalogsEqual() throws Exception {
        DataSourcesConfig.DataSources newDataSources =
            getDataSources(
                CATALOG_0_DEFINITION,
                CATALOG_0_UPDATED_DEFINITION,
                CATALOG_1_DEFINITION,
                CATALOG_2_DEFINITION);
        DataSourcesConfig.DataSource datasource = newDataSources.dataSources[0];
        DataSourcesConfig.Catalog catalog0 = datasource.catalogs.catalogs[0];
        DataSourcesConfig.Catalog catalog0Updated =
            datasource.catalogs.catalogs[1];
        DataSourcesConfig.Catalog catalog1 = datasource.catalogs.catalogs[2];
        DataSourcesConfig.Catalog catalog2 = datasource.catalogs.catalogs[3];
        assertFalse(
            DynamicContentFinder.areCatalogsEqual(
                datasource, catalog0, datasource, catalog0Updated));
        assertTrue(
            DynamicContentFinder.areCatalogsEqual(
                datasource, catalog0, datasource, catalog0));
        assertFalse(
            DynamicContentFinder.areCatalogsEqual(
                datasource, catalog1, datasource, catalog2));
    }

    private static DataSourcesConfig.DataSources getDataSources(
        String... catalogs)
        throws XOMException
    {
        return getDataSources(Olap4jUtil.mapOf(DATASOURCE_1_NAME, catalogs));
    }

    private static String getDataSourceContent(String... catalogs) {
        return getDataSourceString(
            Olap4jUtil.mapOf(DATASOURCE_1_NAME, catalogs));
    }

    private static DataSourcesConfig.DataSources getDataSources(
        Map<String, String[]> dsCatalog)
        throws XOMException
    {
        final String str = getDataSourceString(dsCatalog);
        final Parser xmlParser = XOMUtil.createDefaultParser();
        final DOMWrapper def = xmlParser.parse(str);
        return new DataSourcesConfig.DataSources(def);
    }

    private static String getDataSourceString(Map<String, String[]> dsCatalog) {
        StringBuilder ds = new StringBuilder();
        ds.append("<?xml version=\"1.0\"?>");
        ds.append("<DataSources>");
        for (Map.Entry<String, String[]> entry : dsCatalog.entrySet()) {
            final String dsName = entry.getKey();
            ds.append("<DataSource> ");
            ds.append("  <DataSourceName>")
                .append(dsName)
                .append("</DataSourceName>");
            ds.append(
                "       <DataSourceDescription>"
                + "DATASOURCE_DESCRIPTION"
                + "</DataSourceDescription>");
            ds.append("       <URL>http://localhost:8080/mondrian/xmla</URL>");
            ds.append("       <DataSourceInfo>Provider=mondrian;")
                .append("Jdbc=jdbc:oracle:thin:foodmart/foodmart@")
                .append("//marmalade.hydromatic.net:1521/XE;")
                .append("JdbcUser=foodmart;JdbcPassword=foodmart;")
                .append("JdbcDrivers=oracle.jdbc.OracleDriver;")
                .append("Catalog=/WEB-INF/queries/FoodMart.mondrian.xml")
                .append("</DataSourceInfo>");
            ds.append("       <ProviderName>Mondrian</ProviderName>");
            ds.append("       <ProviderType>MDP</ProviderType>");
            ds.append("       <AuthenticationMode>")
                .append("Unauthenticated")
                .append("</AuthenticationMode>");
            ds.append("       <Catalogs>");
            final String[] catalogs = entry.getValue();
            for (String catalog : catalogs) {
                ds.append(catalog);
            }
            ds.append("       </Catalogs>");
            ds.append("</DataSource>");
        }
        ds.append("</DataSources>");
        return ds.toString();
    }

    public void testReloadDataSources() throws Exception {
        DataSourcesConfig.DataSources ds1 =
            getDataSources(CATALOG_0_DEFINITION, CATALOG_1_DEFINITION);
        DataSourcesConfig.DataSources ds2 =
            getDataSources(CATALOG_1_DEFINITION, CATALOG_2_DEFINITION);

        File dsFile = File.createTempFile(
            getClass().getName()  + "-datasources", ".xml");
        dsFile.deleteOnExit();

        OutputStream out = new FileOutputStream(dsFile);
        out.write(ds1.toXML().getBytes());
        out.close();

        final MockDynamicContentFinder finder =
            new MockDynamicContentFinder(
                dsFile.toURL().toString());

        out = new FileOutputStream(dsFile);
        out.write(ds2.toXML().getBytes());
        out.close();

        finder.reloadDataSources();
        assertTrue(
            finder.containsCatalog(DATASOURCE_1_NAME, CATALOG_1_NAME));
        assertTrue(
            finder.containsCatalog(DATASOURCE_1_NAME, CATALOG_2_NAME));
        assertFalse(
            finder.containsCatalog(DATASOURCE_1_NAME, CATALOG_0_NAME));

        out = new FileOutputStream(dsFile);
        out.write(ds1.toXML().getBytes());
        out.flush();

        finder.reloadDataSources();
        assertTrue(
            finder.containsCatalog(DATASOURCE_1_NAME, CATALOG_0_NAME));
        assertTrue(
            finder.containsCatalog(DATASOURCE_1_NAME, CATALOG_1_NAME));
        assertFalse(
            finder.containsCatalog(DATASOURCE_1_NAME, CATALOG_2_NAME));
        finder.shutdown();
    }

    public void testAutoReloadDataSources() throws Exception {
        DataSourcesConfig.DataSources ds1 =
            getDataSources(CATALOG_0_DEFINITION, CATALOG_1_DEFINITION);
        DataSourcesConfig.DataSources ds2 =
            getDataSources(CATALOG_1_DEFINITION, CATALOG_2_DEFINITION);

        File dsFile = File.createTempFile(
            getClass().getName()  + "-datasources", ".xml");
        dsFile.deleteOnExit();

        OutputStream out = new FileOutputStream(dsFile);
        out.write(ds1.toXML().getBytes());
        out.flush();

        final MockDynamicContentFinder finder =
            new MockDynamicContentFinder(
                dsFile.toURL().toString());

        out = new FileOutputStream(dsFile);
        out.write(ds2.toXML().getBytes());
        out.close();

        finder.reloadDataSources();
        assertTrue(
            finder.containsCatalog(DATASOURCE_1_NAME, CATALOG_1_NAME));
        assertTrue(
            finder.containsCatalog(DATASOURCE_1_NAME, CATALOG_2_NAME));
        assertFalse(
            finder.containsCatalog(DATASOURCE_1_NAME, CATALOG_0_NAME));

        out = new FileOutputStream(dsFile);
        out.write(ds1.toXML().getBytes());
        out.close();

        // Wait for it to auto-reload.
        final Pair<Long, TimeUnit> interval =
            Util.parseInterval(
                MondrianProperties.instance().XmlaSchemaRefreshInterval.get(),
                TimeUnit.MILLISECONDS);
        Thread.sleep(
            interval.right.toMillis(interval.left)
            + 1000);

        assertTrue(
            finder.containsCatalog(DATASOURCE_1_NAME, CATALOG_0_NAME));
        assertTrue(
            finder.containsCatalog(DATASOURCE_1_NAME, CATALOG_1_NAME));
        assertFalse(
            finder.containsCatalog(DATASOURCE_1_NAME, CATALOG_2_NAME));
        finder.shutdown();
    }

    private static class MockDynamicContentFinder extends DynamicContentFinder
    {
        private List<String> flushCatalogList = new ArrayList<String>();

        public MockDynamicContentFinder(String dataSources)
        {
            super(dataSources);
        }

        protected void flushCatalog(String catalogName) {
            flushCatalogList.add(catalogName);
        }

        public List<String> flushCatalogList() {
            return flushCatalogList;
        }

        public boolean containsCatalog(
            String datasourceName,
            String catalogName)
        {
            return locateCatalog(datasourceName, catalogName) != null;
        }

        public synchronized DataSourcesConfig.Catalog locateCatalog(
            String datasourceName,
            String catalogName)
        {
            for (DataSourcesConfig.DataSource ds : dataSources.dataSources) {
                if (ds.name.equals(datasourceName)) {
                    for (DataSourcesConfig.Catalog catalog
                             : ds.catalogs.catalogs)
                    {
                        if (catalog.name.equals(catalogName)) {
                            return catalog;
                        }
                    }
                }
            }
            return null;
        }
    }
}

// End DynamicDatasourceXmlaServletTest.java
