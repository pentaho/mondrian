/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.test;

import mondrian.olap.MondrianServer;
import mondrian.olap.Util;
import mondrian.rolap.RolapConnectionProperties;
import mondrian.server.StringRepositoryContentFinder;
import mondrian.server.UrlRepositoryContentFinder;
import mondrian.xmla.test.XmlaTestContext;

import junit.framework.TestCase;

import org.olap4j.OlapConnection;
import org.olap4j.metadata.Catalog;
import org.olap4j.metadata.NamedList;

import java.net.MalformedURLException;
import java.sql.SQLException;

/**
 * Test suite for server functionality in {@link MondrianServer}.
 *
 * @author jhyde
 * @since 2010/11/22
 */
public class MondrianServerTest extends TestCase {
    /**
     * Tests an embedded server.
     */
    public void testEmbedded() {
        TestContext testContext = TestContext.instance();
        final MondrianServer server =
            MondrianServer.forConnection(testContext.getConnection());
        final int id = server.getId();
        assertNotNull(id);
        server.shutdown();
    }

    /**
     * Tests a server with its own repository.
     */
    public void testStringRepository() throws MalformedURLException {
        final MondrianServer server =
            MondrianServer.createWithRepository(
                new StringRepositoryContentFinder("foo bar"),
                null);
        final int id = server.getId();
        assertNotNull(id);
        server.shutdown();
    }

    /**
     * Tests a server that reads its repository from a file URL.
     */
    public void testRepository() throws MalformedURLException, SQLException {
        final XmlaTestContext xmlaTestContext = new XmlaTestContext();
        final MondrianServer server =
            MondrianServer.createWithRepository(
                new UrlRepositoryContentFinder(
                    "inline:" + xmlaTestContext.getDataSourcesString()),
                null);
        final int id = server.getId();
        assertNotNull(id);
        OlapConnection connection =
            server.getConnection("FoodMart", "FoodMart", null);
        final NamedList<Catalog> catalogs =
            connection.getOlapCatalogs();
        assertEquals(1, catalogs.size());
        assertEquals("FoodMart", catalogs.get(0).getName());
        server.shutdown();
    }

    /**
     * Tests a server that reads its repository from a file URL.
     */
    public void testRepositoryWithBadCatalog() throws Exception {
        final XmlaTestContext xmlaTestContext = new XmlaTestContext() {
            Util.PropertyList connectProperties =
                Util.parseConnectString(getConnectString());
            String catalogUrl = connectProperties.get(
                RolapConnectionProperties.Catalog.name());
            public String getDataSourcesString() {
                return super.getDataSourcesString()
                    .replace(
                        "</Catalog>",
                        "</Catalog>\n"
                        + "<Catalog name='__1'>\n"
                        + "<DataSourceInfo>Provider=mondrian;Jdbc='jdbc:derby:non-existing-db'</DataSourceInfo>\n"
                        + "<Definition>" + catalogUrl + "</Definition>\n"
                        + "</Catalog>\n");
            }
        };
        final MondrianServer server =
            MondrianServer.createWithRepository(
                new UrlRepositoryContentFinder(
                    "inline:" + xmlaTestContext.getDataSourcesString()),
                null);
        final int id = server.getId();
        assertNotNull(id);
        OlapConnection connection =
            server.getConnection("FoodMart", "FoodMart", null);
        final NamedList<Catalog> catalogs =
            connection.getOlapCatalogs();
        assertEquals(1, catalogs.size());
        assertEquals("FoodMart", catalogs.get(0).getName());
        server.shutdown();
    }
}

// End MondrianServerTest.java
