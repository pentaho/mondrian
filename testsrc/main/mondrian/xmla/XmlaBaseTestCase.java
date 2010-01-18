/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2006-2010 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.xmla;

import mondrian.test.FoodMartTestCase;
import mondrian.test.DiffRepository;
import mondrian.test.TestContext;
import mondrian.tui.*;
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
    protected static final String LAST_SCHEMA_UPDATE_DATE =
        "xxxx-xx-xxTxx:xx:xx";
    private static final String LAST_SCHEMA_UPDATE_NODE_NAME =
        "LAST_SCHEMA_UPDATE";
    protected SortedMap<String, String> catalogNameUrls = null;
    private Servlet servlet;

    private static int sessionIdCounter = 1000;
    private static Map<String, String> sessionIdMap =
        new HashMap<String, String>();
    // session id property
    public static final String SESSION_ID_PROP     = "session.id";
    // request.type
    public static final String REQUEST_TYPE_PROP =
        "request.type";// data.source.info
    public static final String DATA_SOURCE_INFO_PROP = "data.source.info";
    public static final String DATA_SOURCE_INFO = "MondrianFoodMart";// catalog
    public static final String CATALOG_PROP     = "catalog";
    public static final String CATALOG_NAME_PROP = "catalog.name";
    public static final String CATALOG          = "FoodMart";// cube
    public static final String CUBE_NAME_PROP   = "cube.name";
    public static final String SALES_CUBE       = "Sales";// format
    public static final String FORMAT_PROP     = "format";
    public static final String FORMAT_MULTI_DIMENSIONAL = "Multidimensional";
    protected static final boolean DEBUG = false;

    // Associate a Role with a query
    private static final ThreadLocal<Role> roles = new ThreadLocal<Role>();

    protected String generateExpectedString(Properties props)
        throws Exception
    {
        String expectedStr = fileToString("response");
        if (props != null) {
            // YES, duplicate the above
            String sessionId = getSessionId(Action.QUERY);
            if (sessionId != null) {
                props.put(SESSION_ID_PROP, sessionId);
            }
            expectedStr = Util.replaceProperties(
                expectedStr, Util.toMap(props));
        }
        return expectedStr;
    }

    protected String generateRequestString(Properties props)
        throws Exception
    {
        String requestText = fileToString("request");

        if (props != null) {
            String sessionId = getSessionId(Action.QUERY);
            if (sessionId != null) {
                props.put(SESSION_ID_PROP, sessionId);
            }
            requestText = Util.replaceProperties(
                requestText, Util.toMap(props));
        }
if (DEBUG) {
System.out.println("requestText=" + requestText);
}
        return requestText;
    }

    protected void validate(
        byte[] bytes,
        Document expectedDoc,
        TestContext testContext,
        boolean replace)
        throws Exception
    {
        if (XmlUtil.supportsValidation()) {
            if (XmlaSupport.validateSoapXmlaUsingXpath(bytes)) {
                if (DEBUG) {
                    System.out.println("XML Data is Valid");
                }
            }
        }

        Document gotDoc = XmlUtil.parse(bytes);
        gotDoc = replaceLastSchemaUpdateDate(gotDoc);
        String gotStr = XmlUtil.toString(gotDoc, true);
        gotStr = Util.maskVersion(gotStr);
        gotStr = testContext.upgradeActual(gotStr);
        if (expectedDoc == null) {
            if (replace) {
                getDiffRepos().amend("${response}", gotStr);
            }
            return;
        }
        expectedDoc = replaceLastSchemaUpdateDate(expectedDoc);
        String expectedStr = XmlUtil.toString(expectedDoc, true);
        try {
            XMLAssert.assertXMLEqual(expectedStr, gotStr);
        } catch (AssertionFailedError e) {
            // Let DiffRepository do the comparison. It will output
            // a textual difference, and will update the logfile,
            // XmlaBasicTest.log.xml. If you agree with the change,
            // copy this file to XmlaBasicTest.ref.xml.
            if (replace) {
                gotStr =
                    gotStr.replaceAll(
                        " SessionId=\"[^\"]*\" ",
                        " SessionId=\"\\${session.id}\" ");
                getDiffRepos().assertEquals(
                    "response",
                    "${response}",
                    gotStr);
            } else {
                throw e;
            }
        }
    }

    public void doTest(Properties props) throws Exception {
        String requestText = generateRequestString(props);
        Document reqDoc = XmlUtil.parseString(requestText);

        Servlet servlet = getServlet(getTestContext());
        byte[] bytes = XmlaSupport.processSoapXmla(reqDoc, servlet);

        String expectedStr = generateExpectedString(props);
        Document expectedDoc = XmlUtil.parseString(expectedStr);
        validate(bytes, expectedDoc, TestContext.instance(), true);
    }

    protected void doTest(
        MockHttpServletRequest req,
        Properties props) throws Exception
    {
        String requestText = generateRequestString(props);

        MockHttpServletResponse res = new MockHttpServletResponse();
        res.setCharacterEncoding("UTF-8");

        Servlet servlet = getServlet(getTestContext());
        servlet.service(req, res);

        int statusCode = res.getStatusCode();
        if (statusCode == HttpServletResponse.SC_OK) {
            byte[] bytes = res.toByteArray();
            String expectedStr = generateExpectedString(props);
            Document expectedDoc = XmlUtil.parseString(expectedStr);
            validate(bytes, expectedDoc, TestContext.instance(), true);

        } else if (statusCode == HttpServletResponse.SC_CONTINUE) {
            // remove the Expect header from request and try again
if (DEBUG) {
System.out.println("Got CONTINUE");
}

            req.clearHeader(XmlaRequestCallback.EXPECT);
            req.setBodyContent(requestText);

            servlet.service(req, res);

            statusCode = res.getStatusCode();
            if (statusCode == HttpServletResponse.SC_OK) {
                byte[] bytes = res.toByteArray();
                String expectedStr = generateExpectedString(props);
                Document expectedDoc = XmlUtil.parseString(expectedStr);
                validate(bytes, expectedDoc, TestContext.instance(), true);

            } else {
                fail("Bad status code: "  + statusCode);
            }
        } else {
            fail("Bad status code: "  + statusCode);
        }
    }

    protected void helperTestExpect(boolean doSessionId)
        throws Exception
    {
        if (doSessionId) {
            Util.discard(getSessionId(Action.CREATE));
        }
        MockHttpServletRequest req = new MockHttpServletRequest();
        req.setMethod("POST");
        req.setContentType("text/xml");
        req.setHeader(
            XmlaRequestCallback.EXPECT,
            XmlaRequestCallback.EXPECT_100_CONTINUE);

        Properties props = new Properties();
        doTest(req, props);
    }

    protected void helperTest(boolean doSessionId)
        throws Exception
    {
        if (doSessionId) {
            getSessionId(Action.CREATE);
        }
        Properties props = new Properties();
        doTest(props);
    }

    static class CallBack implements XmlaRequestCallback {
        public CallBack() {
        }

        public void init(ServletConfig servletConfig) throws ServletException {
        }

        public boolean processHttpHeader(
            HttpServletRequest request,
            HttpServletResponse response,
            Map<String, Object> context) throws Exception
        {
            Role role = roles.get();
            if (role != null) {
                context.put(XmlaConstants.CONTEXT_ROLE, role);
            }
            return true;
        }

        public void preAction(
            HttpServletRequest request,
            Element[] requestSoapParts,
            Map<String, Object> context) throws Exception
        {
        }

        public String generateSessionId(Map<String, Object> context) {
            return null;
        }

        public void postAction(
            HttpServletRequest request,
            HttpServletResponse response,
            byte[][] responseSoapParts,
            Map<String, Object> context) throws Exception
        {
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
        NodeList elements =
            doc.getElementsByTagName(LAST_SCHEMA_UPDATE_NODE_NAME);
        for (int i = 0; i < elements.getLength(); i++) {
            Node node = elements.item(i);
            node.getFirstChild().setNodeValue(
                LAST_SCHEMA_UPDATE_DATE);
        }
        return doc;
    }

    private String ignoreLastUpdateDate(String document) {
        return document.replaceAll(
            "\"LAST_SCHEMA_UPDATE\": \"....-..-..T..:..:..\"",
            "\"LAST_SCHEMA_UPDATE\": \"" + LAST_SCHEMA_UPDATE_DATE + "\"");
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
        throws IOException, SAXException
    {
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
     * @throws Exception on error
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
        requestText = testContext.upgradeQuery(requestText);
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
        String connectString = testContext.getConnectString();
        Map<String, String> catalogNameUrls = getCatalogNameUrls(testContext);

        boolean xml = !requestText.contains("application/json");
        if (!xml) {
            String responseStr = (respFileName != null)
                ? fileToString(respFileName)
                : null;
            doTestsJson(
                requestText, props,
                testContext, connectString, catalogNameUrls,
                responseStr, Enumeration.Content.Data, role);
            return;
        }

        final Document responseDoc = (respFileName != null)
            ? fileToDocument(respFileName)
            : null;
        Document expectedDoc;

        // Test 'schemadata' first, so that if it fails, we will be able to
        // amend the ref file with the fullest XML response.
        final String ns = "cxmla";
        expectedDoc = (responseDoc != null)
            ? XmlaSupport.transformSoapXmla(
                responseDoc, new String[][] {{"content", "schemadata"}}, ns)
            : null;
        doTests(
            requestText, props,
            testContext, connectString, catalogNameUrls,
            expectedDoc, Enumeration.Content.SchemaData, role, true);

        if (requestType.equals("EXECUTE")) {
            return;
        }

        expectedDoc = (responseDoc != null)
            ? XmlaSupport.transformSoapXmla(
                responseDoc, new String[][] {{"content", "none"}}, ns)
            : null;
        doTests(
            requestText, props,
            testContext, connectString, catalogNameUrls,
            expectedDoc, Enumeration.Content.None, role, false);

        expectedDoc = (responseDoc != null)
            ? XmlaSupport.transformSoapXmla(
                responseDoc, new String[][] {{"content", "data"}}, ns)
            : null;
        doTests(
            requestText, props,
            testContext, connectString, catalogNameUrls,
            expectedDoc, Enumeration.Content.Data, role, false);

        expectedDoc = (responseDoc != null)
            ? XmlaSupport.transformSoapXmla(
                responseDoc, new String[][] {{"content", "schema"}}, ns)
            : null;
        doTests(
            requestText, props,
            testContext, connectString, catalogNameUrls,
            expectedDoc, Enumeration.Content.Schema, role, false);
    }

    /**
     * Executes a SOAP request.
     *
     * @param soapRequestText SOAP request
     * @param props Name/value pairs to substitute in the request
     * @param testContext Test context
     * @param connectString Connect string
     * @param catalogNameUrls Map from catalog names to URL
     * @param expectedDoc Expected SOAP output
     * @param content Content type
     * @param role Role in which to execute query, or null
     * @param replace Whether to generate a replacement reference log into
     *    TestName.log.xml if there is an exception. If you are running the same
     *    request with different content types and the same reference log, you
     *    should pass {@code true} for the content type that has the most
     *    information (generally
     *    {@link mondrian.xmla.Enumeration.Content#SchemaData})
     * @throws Exception on error
     */
    protected void doTests(
        String soapRequestText,
        Properties props,
        TestContext testContext,
        String connectString,
        Map<String, String> catalogNameUrls,
        Document expectedDoc,
        Enumeration.Content content,
        Role role,
        boolean replace) throws Exception
    {
        if (content != null) {
            props.setProperty(XmlaBasicTest.CONTENT_PROP, content.name());
        }
        soapRequestText = Util.replaceProperties(
            soapRequestText, Util.toMap(props));

        Document soapReqDoc = XmlUtil.parseString(soapRequestText);
        Document xmlaReqDoc = XmlaSupport.extractBodyFromSoap(soapReqDoc);

        // do XMLA
        byte[] bytes =
            XmlaSupport.processXmla(
                xmlaReqDoc, connectString, catalogNameUrls, role);
        if (XmlUtil.supportsValidation()) {
            if (XmlaSupport.validateXmlaUsingXpath(bytes)) {
                if (DEBUG) {
                    System.out.println(
                        "XmlaBaseTestCase.doTests: XML Data is Valid");
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

        if (DEBUG) {
            System.out.println(
                "XmlaBaseTestCase.doTests: soap response=" + new String(bytes));
        }

        validate(bytes, expectedDoc, testContext, replace);
    }

    protected void doTestsJson(
        String soapRequestText,
        Properties props,
        TestContext testContext,
        String connectString,
        Map<String, String> catalogNameUrls,
        String expectedStr,
        Enumeration.Content content,
        Role role) throws Exception
    {
        if (content != null) {
            props.setProperty(XmlaBasicTest.CONTENT_PROP, content.name());
        }
        soapRequestText = Util.replaceProperties(
            soapRequestText, Util.toMap(props));

        Document soapReqDoc = XmlUtil.parseString(soapRequestText);
        Document xmlaReqDoc = XmlaSupport.extractBodyFromSoap(soapReqDoc);

        // do XMLA
        byte[] bytes =
            XmlaSupport.processXmla(
                xmlaReqDoc, connectString, catalogNameUrls, role);
        if (XmlUtil.supportsValidation()) {
            if (XmlaSupport.validateXmlaUsingXpath(bytes)) {
                if (DEBUG) {
                    System.out.println(
                        "XmlaBaseTestCase.doTests: XML Data is Valid");
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
        if (DEBUG) {
            System.out.println(
                "XmlaBaseTestCase.doTests: soap response=" + new String(bytes));
        }
        if (XmlUtil.supportsValidation()) {
            if (XmlaSupport.validateSoapXmlaUsingXpath(bytes)) {
                if (DEBUG) {
                    System.out.println(
                        "XmlaBaseTestCase.doTests: XML Data is Valid");
                }
            }
        }

        String gotStr = new String(bytes);
        gotStr = ignoreLastUpdateDate(gotStr);
        gotStr = Util.maskVersion(gotStr);
        gotStr = testContext.upgradeActual(gotStr);
        if (expectedStr != null) {
            // Let DiffRepository do the comparison. It will output
            // a textual difference, and will update the logfile,
            // XmlaBasicTest.log.xml. If you agree with the change,
            // copy this file to XmlaBasicTest.ref.xml.
            getDiffRepos().assertEquals(
                "response",
                "${response}",
                gotStr);
        }
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

    protected static abstract class XmlaRequestCallbackImpl
        implements XmlaRequestCallback
    {
        private static final String MY_SESSION_ID = "my_session_id";
        private final String name;

        protected XmlaRequestCallbackImpl(String name) {
            this.name = name;
        }

        public void init(ServletConfig servletConfig) throws ServletException {
        }

        public boolean processHttpHeader(
            HttpServletRequest request,
            HttpServletResponse response,
            Map<String, Object> context)
            throws Exception
        {
            String expect = request.getHeader(XmlaRequestCallback.EXPECT);
            if ((expect != null)
                && expect.equalsIgnoreCase(
                    XmlaRequestCallback.EXPECT_100_CONTINUE))
            {
                Helper.generatedExpectResponse(
                    request, response, context);
                return false;
            } else {
                return true;
            }
        }

        public void preAction(
            HttpServletRequest request,
            Element[] requestSoapParts,
            Map<String, Object> context)
            throws Exception
        {
        }

        private void setSessionId(Map<String, Object> context) {
            context.put(
                MY_SESSION_ID,
                getSessionId(name, Action.CREATE));
        }

        public String generateSessionId(Map<String, Object> context) {
            setSessionId(context);
            return (String) context.get(MY_SESSION_ID);
        }

        public void postAction(
            HttpServletRequest request,
            HttpServletResponse response,
            byte[][] responseSoapParts,
            Map<String, Object> context)
            throws Exception
        {
        }
    }
}

// End XmlaBaseTestCase.java
