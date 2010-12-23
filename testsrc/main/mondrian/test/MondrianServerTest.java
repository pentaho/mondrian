/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2010-2010 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.test;

import junit.framework.TestCase;
import mondrian.olap.MondrianServer;
import mondrian.server.StringRepositoryContentFinder;
import mondrian.server.UrlRepositoryContentFinder;
import mondrian.xmla.test.XmlaTestContext;
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
 * @version $Id$
 */
public class MondrianServerTest extends TestCase {
    /**
     * Tests an embedded server.
     */
    public void testEmbedded() {
        TestContext testContext = TestContext.instance();
        final MondrianServer server =
            MondrianServer.forConnection(testContext.getConnection());
        final String id = server.getId();
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
        final String id = server.getId();
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
        final String id = server.getId();
        assertNotNull(id);
        OlapConnection connection =
            server.getConnection("FoodMart", "FoodMart", null);
        final NamedList<Catalog> catalogs =
            connection.getMetaData().getOlapCatalogs();
        assertEquals(1, catalogs.size());
        assertEquals("FoodMart", catalogs.get(0).getName());
        server.shutdown();
    }
}

// End MondrianServerTest.java
