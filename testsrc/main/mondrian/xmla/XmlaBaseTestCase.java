/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2008 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.xmla;

import mondrian.test.FoodMartTestCase;
import mondrian.test.DiffRepository;
import mondrian.test.TestContext;
import mondrian.tui.XmlaSupport;
import mondrian.tui.XmlUtil;
import mondrian.olap.Util;
import mondrian.olap.Role;
import mondrian.rolap.RolapConnectionProperties;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;
import org.custommonkey.xmlunit.XMLAssert;
import junit.framework.AssertionFailedError;
import javax.servlet.Servlet;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.w3c.dom.Element;
import java.io.IOException;
import java.io.ByteArrayInputStream;
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
    private Servlet servlet;

    private static int sessionIdCounter = 1000;
    private static Map<String,String> sessionIdMap =
        new HashMap<String, String>();
    // session id property
    public static final String SESSION_ID_PROP     = "session.id";
    // request.type
    public static final String REQUEST_TYPE_PROP = "request.type";// data.source.info
    public static final String DATA_SOURCE_INFO_PROP = "data.source.info";
    public static final String DATA_SOURCE_INFO = "MondrianFoodMart";// catalog
    public static final String CATALOG_PROP     = "catalog";
    public static final String CATALOG_NAME_PROP = "catalog.name";
    public static final String CATALOG          = "FoodMart";// cube
    public static final String CUBE_NAME_PROP   = "cube.name";
    public static final String SALES_CUBE       = "Sales";// format
    public static final String HR_CUBE          = "HR";
    public static final String FORMAT_PROP     = "format";
    public static final String FORMAT_MULTI_DIMENSIONAL = "Multidimensional";
    private static final boolean DEBUG = false;

    // Associate a Role with a query
    private static final ThreadLocal<Role> roles = new ThreadLocal<Role>();

    static class CallBack implements XmlaRequestCallback {
        public CallBack() {
        }

        public void init(ServletConfig servletConfig) throws ServletException {
        }

        public boolean processHttpHeader(
                HttpServletRequest request,
                HttpServletResponse response,
                Map<String, Object> context) throws Exception {
if (DEBUG) {
System.out.println("XmlaBaseTestCase.CallBack.processHttpHeader");
}
            Role role = roles.get();
            if (role != null) {
if (DEBUG) {
System.out.println("XmlaBaseTestCase.CallBack.processHttpHeader: has Role");
}
                context.put(XmlaConstants.CONTEXT_ROLE, role);
            }
            return true;
        }

        public void preAction(
            HttpServletRequest request,
            Element[] requestSoapParts,
            Map<String, Object> context) throws Exception {
        }

        public String generateSessionId(Map<String, Object> context) {
            return null;
        }

        public void postAction(
            HttpServletRequest request,
            HttpServletResponse response,
            byte[][] responseSoapParts,
            Map<String, Object> context) throws Exception {
        }
    }

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
        if (elements.getLength() ==0) {
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

    protected Servlet getServlet(TestContext testContext)
        throws IOException, ServletException, SAXException
    {
        if (servlet == null) {
            getSessionId(Action.CLEAR);

            String connectString = testContext.getConnectString();
            Map<String, String> catalogNameUrls =
                getCatalogNameUrls(testContext);
            servlet =
                XmlaSupport.makeServlet(
                    connectString, catalogNameUrls,
                    getServletCallbackClass().getName());
        }
        return servlet;
    }

    protected void clearServlet() {
        servlet = null;
    }

    protected abstract Class<? extends XmlaRequestCallback>
    getServletCallbackClass();

    protected Properties getDefaultRequestProperties(String requestType) {
        Properties props = new Properties();
        props.setProperty(REQUEST_TYPE_PROP, requestType);
        props.setProperty(CATALOG_PROP, CATALOG);
        props.setProperty(CATALOG_NAME_PROP, CATALOG);
        props.setProperty(CUBE_NAME_PROP, SALES_CUBE);
        props.setProperty(FORMAT_PROP, FORMAT_MULTI_DIMENSIONAL);
        props.setProperty(DATA_SOURCE_INFO_PROP, DATA_SOURCE_INFO);
        return props;
    }

    protected Document fileToDocument(String filename)
                throws IOException , SAXException {
        String s = getDiffRepos().expand(null, filename);
        if (s.equals(filename)) {
            s = "<?xml version='1.0'?><Empty/>";
        }
        // Give derived class a chance to change the content.
        s = filter(getDiffRepos().getCurrentTestCaseName(true), filename, s);

        return XmlUtil.parse(new ByteArrayInputStream(s.getBytes()));
    }

    /**
     * Filters the content of a test resource. The default implementation
     * returns the content unchanged, but a derived class might override this
     * method to change the content.
     *
     * @param testCaseName Name of current test case, e.g. "testFoo"
     * @param filename Name of requested content, e.g.  "${request}"
     * @param content Content
     * @return Modified content
     */
    protected String filter(
        String testCaseName,
        String filename,
        String content)
    {
        Util.discard(testCaseName); // might be used by derived class
        Util.discard(filename); // might be used by derived class
        return content;
    }

    /**
     * Executes an XMLA request, reading the text of the request and the
     * response from attributes in {@link #getDiffRepos()}.
     *
     * @param requestType Request type: "DISCOVER_DATASOURCES", "EXECUTE", etc.
     * @param props Properties for request
     * @param testContext Test context
     */
    public void doTest(
        String requestType,
        Properties props,
        TestContext testContext) throws Exception
    {
        doTest(requestType, props, testContext, null);
    }

    public void doTest(
        String requestType,
        Properties props,
        TestContext testContext,
        Role role) throws Exception
    {
        String requestText = fileToString("request");
        doTestInline(
            requestType, requestText, "${response}",
            props, testContext, role);
    }

    public void doTestInline(
        String requestType,
        String requestText,
        String respFileName,
        Properties props,
        TestContext testContext)
        throws Exception
    {
        doTestInline(
            requestType, requestText, respFileName,
            props, testContext, null);
    }

    public void doTestInline(
        String requestType,
        String requestText,
        String respFileName,
        Properties props,
        TestContext testContext,
        Role role)
        throws Exception
    {
        Document responseDoc = (respFileName != null)
            ? fileToDocument(respFileName)
            : null;

        String connectString = testContext.getConnectString();
        Map<String,String> catalogNameUrls = getCatalogNameUrls(testContext);

        Document expectedDoc;

        // Test 'schemadata' first, so that if it fails, we will be able to
        // amend the ref file with the fullest XML response.
        final String ns = "cxmla";
        expectedDoc = (responseDoc != null)
            ? XmlaSupport.transformSoapXmla(
                responseDoc, new String[][] {{"content", "schemadata"}}, ns)
            : null;
        doTests(
            requestText, props, null, connectString, catalogNameUrls,
            expectedDoc, XmlaBasicTest.CONTENT_SCHEMADATA, role);

        if (requestType.equals("EXECUTE")) {
            return;
        }

        expectedDoc = (responseDoc != null)
            ? XmlaSupport.transformSoapXmla(
                responseDoc, new String[][] {{"content", "none"}}, ns)
            : null;

        doTests(requestText, props, null, connectString, catalogNameUrls,
                expectedDoc, XmlaBasicTest.CONTENT_NONE, role);

        expectedDoc = (responseDoc != null)
            ? XmlaSupport.transformSoapXmla(
                responseDoc, new String[][] {{"content", "data"}}, ns)
            : null;
        doTests(requestText, props, null, connectString, catalogNameUrls,
                expectedDoc, XmlaBasicTest.CONTENT_DATA, role);

        expectedDoc = (responseDoc != null)
            ? XmlaSupport.transformSoapXmla(
                responseDoc, new String[][] {{"content", "schema"}}, ns)
            : null;
        doTests(requestText, props, null, connectString, catalogNameUrls,
                expectedDoc, XmlaBasicTest.CONTENT_SCHEMA, role);
    }

    protected void doTests(
            String soapRequestText,
            Properties props,
            String soapResponseText,
            String connectString,
            Map<String, String> catalogNameUrls,
            Document expectedDoc,
            String content) throws Exception
    {
        doTests(
            soapRequestText, props, soapResponseText,
            connectString, catalogNameUrls, expectedDoc,
                content, null);
    }

    protected void doTests(
            String soapRequestText,
            Properties props,
            String soapResponseText,
            String connectString,
            Map<String, String> catalogNameUrls,
            Document expectedDoc,
            String content,
            Role role) throws Exception
    {
        if (content != null) {
            props.setProperty(XmlaBasicTest.CONTENT_PROP, content);
        }
        soapRequestText = Util.replaceProperties(
            soapRequestText, Util.toMap(props));

if (DEBUG) {
System.out.println("XmlaBaseTestCase.doTests: soapRequestText=" + soapRequestText);
}
        Document soapReqDoc = XmlUtil.parseString(soapRequestText);

        Document xmlaReqDoc = XmlaSupport.extractBodyFromSoap(soapReqDoc);

        // do XMLA
        byte[] bytes =
            XmlaSupport.processXmla(xmlaReqDoc, connectString, catalogNameUrls, role);
        String response = new String(bytes);
if (DEBUG) {
System.out.println("XmlaBaseTestCase.doTests: xmla response=" + response);
}
        if (XmlUtil.supportsValidation()) {
            if (XmlaSupport.validateXmlaUsingXpath(bytes)) {
if (DEBUG) {
                System.out.println("XmlaBaseTestCase.doTests: XML Data is Valid");
}
            }
        }

        // do SOAP-XMLA
        try {
            String callBackClassName = CallBack.class.getName();
            if (role != null) {
                roles.set(role);
            }
            bytes = XmlaSupport.processSoapXmla(
                soapReqDoc,
                connectString,
                catalogNameUrls,
                callBackClassName);
        } finally {
            if (role != null) {
                // Java4 does not support the ThreadLocal remove() method
                // so we use the set(null).
                roles.set(null);
            }
        }
        response = new String(bytes);
if (DEBUG) {
System.out.println("XmlaBaseTestCase.doTests: soap response=" + response);
}
        if (XmlUtil.supportsValidation()) {
            if (XmlaSupport.validateSoapXmlaUsingXpath(bytes)) {
if (DEBUG) {
                System.out.println("XmlaBaseTestCase.doTests: XML Data is Valid");
}
            }
        }

        Document gotDoc = ignoreLastUpdateDate(XmlUtil.parse(bytes));
        String gotStr = XmlUtil.toString(gotDoc, true);
        gotStr = Util.maskVersion(gotStr);
        if (expectedDoc != null) {
            String expectedStr = XmlUtil.toString(expectedDoc, true);
if (DEBUG) {
System.out.println("XmlaBaseTestCase.doTests: GOT:\n" + gotStr);
System.out.println("XmlaBaseTestCase.doTests: EXPECTED:\n" + expectedStr);
System.out.println("XmlaBaseTestCase.doTests: BEFORE ASSERT");
}
            try {
                XMLAssert.assertXMLEqual(expectedStr, gotStr);
            } catch (AssertionFailedError e) {
                if (content.equals(XmlaBasicTest.CONTENT_SCHEMADATA)) {
                    // Let DiffRepository do the comparison. It will output
                    // a textual difference, and will update the logfile,
                    // XmlaBasicTest.log.xml. If you agree with the change,
                    // copy this file to XmlaBasicTest.ref.xml.
                    getDiffRepos().assertEquals("response", "${response}", gotStr);
                } else {
                    throw e;
                }
            }
        } else {
            if (content.equals(XmlaBasicTest.CONTENT_SCHEMADATA)) {
                getDiffRepos().amend("${response}", gotStr);
            }
        }
    }

    private Document ignoreLastUpdateDate(Document document) {
        NodeList elements = document.getElementsByTagName("LAST_SCHEMA_UPDATE");
        for (int i = elements.getLength(); i > 0; i--) {
            removeNode(elements.item(i - 1));
        }
        return document;
    }

    private void removeNode(Node node) {
        Node parentNode = node.getParentNode();
        parentNode.removeChild(node);
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
