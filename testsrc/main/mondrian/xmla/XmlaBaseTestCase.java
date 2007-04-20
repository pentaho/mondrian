/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2007 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.xmla;

import mondrian.test.FoodMartTestCase;
import mondrian.test.DiffRepository;
import mondrian.test.TestContext;
import mondrian.tui.XmlaSupport;
import mondrian.olap.Util;
import mondrian.rolap.RolapConnectionProperties;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

import javax.servlet.ServletException;
import javax.servlet.Servlet;
import java.io.IOException;
import java.util.*;

/**
 * Extends FoodMartTestCase, adding support for testing XMLA specific
 * functionality, for example LAST_SCHEMA_UPDATE
 *
 * @author mkambol
 * @version $Id$
 */

public abstract class XmlaBaseTestCase extends FoodMartTestCase {
    protected static final String LAST_SCHEMA_UPDATE_DATE_PROP = "last.schema.update.date";
    protected static final String LAST_SCHEMA_UPDATE_DATE = "somedate";
    private static final String LAST_SCHEMA_UPDATE_NODE_NAME = "LAST_SCHEMA_UPDATE";
    protected SortedMap<String, String> catalogNameUrls = null;
    protected Servlet servlet;

    private static int sessionIdCounter = 1000;
    private static Map<String,String> sessionIdMap =
        new HashMap<String, String>();
    // session id property
    public static final String SESSION_ID_PROP     = "session.id";

    public XmlaBaseTestCase() {
    }

    public XmlaBaseTestCase(String name) {
        super(name);
    }

    protected abstract DiffRepository getDiffRepos();

    protected String fileToString(String filename) throws Exception {
        String var = "${" + filename + "}";
        String s = getDiffRepos().expand(null, var);
        if (s.startsWith("$")) {
            getDiffRepos().amend(var, "\n\n");
        }
        return s;
    }

    protected Document replaceLastSchemaUpdateDate(Document doc) {
        NodeList elements = doc.getElementsByTagName(LAST_SCHEMA_UPDATE_NODE_NAME);
        if(elements.getLength() ==0){
            return doc;
        }

        Node lastSchemaUpdateNode = elements.item(0);
        lastSchemaUpdateNode.getFirstChild().setNodeValue(LAST_SCHEMA_UPDATE_DATE);
        return doc;
    }

    protected Map<String, String> getCatalogNameUrls(TestContext testContext) {
        if (catalogNameUrls == null) {
            catalogNameUrls = new TreeMap<String, String>();
            String connectString = testContext.getConnectString();
            Util.PropertyList connectProperties =
                        Util.parseConnectString(connectString);
            String catalog = connectProperties.get(
                RolapConnectionProperties.Catalog.name());
            catalogNameUrls.put("FoodMart", catalog);
        }
        return catalogNameUrls;
    }

    protected void makeServlet(TestContext testContext)
            throws IOException, ServletException, SAXException {

        getSessionId(Action.CLEAR);

        String connectString = testContext.getConnectString();
        Map<String, String> catalogNameUrls =
            getCatalogNameUrls(testContext);
        servlet =
            XmlaSupport.makeServlet(
                connectString, catalogNameUrls,
                getServletCallbackClass().getName());
    }

    protected abstract Class<? extends XmlaRequestCallback> getServletCallbackClass();

    protected void setUp() throws Exception {
        makeServlet(getTestContext());
    }

    protected void tearDown() throws Exception {
    }

    enum Action {
        CREATE,
        QUERY,
        CLEAR
    }

    /**
     * Creates, retrieves or clears the session id for this test.
     *
     * @param action Action to perform
     * @return Session id for create, query; null for clear
     */
    protected abstract String getSessionId(Action action);

    protected static String getSessionId(String name, Action action) {
        switch (action) {
        case CLEAR:
            sessionIdMap.put(name, null);
            return null;

        case QUERY:
            return sessionIdMap.get(name);

        case CREATE:
            String sessionId = sessionIdMap.get(name);
            if (sessionId == null) {
                int id = sessionIdCounter++;
                StringBuilder buf = new StringBuilder();
                buf.append(name);
                buf.append("-");
                buf.append(id);
                buf.append("-foo");
                sessionId = buf.toString();
                sessionIdMap.put(name, sessionId);
            }
            return sessionId;

        default:
            throw new UnsupportedOperationException();
        }
    }
}

// End XmlaBaseTestCase.java
