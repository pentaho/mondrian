/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2006-2013 Pentaho and others
// All Rights Reserved.
*/
package mondrian.xmla;

import mondrian.olap.*;
import mondrian.rolap.RolapConnectionProperties;
import mondrian.test.*;
import mondrian.tui.*;
import mondrian.util.LockBox;

import junit.framework.AssertionFailedError;

import org.olap4j.metadata.XmlaConstants;

import org.custommonkey.xmlunit.XMLAssert;
import org.w3c.dom.*;
import org.xml.sax.SAXException;

import java.io.*;
import java.lang.ref.WeakReference;
import java.util.*;
import java.util.regex.Pattern;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Extends FoodMartTestCase, adding support for testing XMLA specific
 * functionality, for example LAST_SCHEMA_UPDATE
 *
 * @author mkambol
 */
public abstract class XmlaBaseTestCase extends FoodMartTestCase {
    protected static final String LAST_SCHEMA_UPDATE_DATE =
        "xxxx-xx-xxTxx:xx:xx";
    private static final String LAST_SCHEMA_UPDATE_NODE_NAME =
        "LAST_SCHEMA_UPDATE";
    protected SortedMap<String, String> catalogNameUrls = null;

    private static int sessionIdCounter = 1000;
    private static Map<String, String> sessionIdMap =
        new HashMap<String, String>();
    // session id property
    public static final String SESSION_ID_PROP     = "session.id";
    // request.type
    public static final String REQUEST_TYPE_PROP =
        "request.type";// data.source.info
    public static final String DATA_SOURCE_INFO_PROP = "data.source.info";
    public static final String DATA_SOURCE_INFO = "FoodMart";// catalog
    public static final String CATALOG_PROP     = "catalog";
    public static final String CATALOG_NAME_PROP = "catalog.name";
    public static final String CATALOG          = "FoodMart";// cube
    public static final String CUBE_NAME_PROP   = "cube.name";
    public static final String SALES_CUBE       = "Sales";// format
    public static final String FORMAT_PROP     = "format";
    public static final String FORMAT_MULTI_DIMENSIONAL = "Multidimensional";
    public static final String ROLE_PROP = "Role";
    public static final String LOCALE_PROP = "locale";
    protected static final boolean DEBUG = false;

    private Resource resource;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        resource = Resource.acquire();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        resource = null;
    }

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
        boolean replace,
        boolean validate)
        throws Exception
    {
        if (validate && XmlUtil.supportsValidation()) {
            if (XmlaSupport.validateSoapXmlaUsingXpath(bytes)) {
                if (DEBUG) {
                    System.out.println("XML Data is Valid");
                }
            }
        }

        Document gotDoc = XmlUtil.parse(bytes);
        gotDoc = replaceLastSchemaUpdateDate(gotDoc);
        String gotStr = XmlUtil.toString(gotDoc, true);
        gotStr = maskVersion(gotStr);
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
        validate(bytes, expectedDoc, TestContext.instance(), true, true);
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
            validate(bytes, expectedDoc, TestContext.instance(), true, true);

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
                validate(
                    bytes, expectedDoc, TestContext.instance(), true, true);

            } else {
                fail("Bad status code: "  + statusCode);
            }
        } else {
            fail("Bad status code: "  + statusCode);
        }
    }

    protected void helperTestExpect(boolean doSessionId)
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
        try {
            doTest(req, props);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected void helperTest(boolean doSessionId)
    {
        if (doSessionId) {
            getSessionId(Action.CREATE);
        }
        Properties props = new Properties();
        try {
            doTest(props);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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
        // Give derived class a chance to change the content.
        s = filter(getDiffRepos().getCurrentTestCaseName(true), filename, s);
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
        getSessionId(Action.CLEAR);

        String connectString = testContext.getConnectString();
        Map<String, String> catalogNameUrls =
            getCatalogNameUrls(testContext);
        connectString = filterConnectString(connectString);
        return
            XmlaSupport.makeServlet(
                connectString,
                catalogNameUrls,
                getServletCallbackClass().getName(),
                resource.servletCache);
    }

    protected String filterConnectString(String original) {
        return original;
    }

    /**
     * Masks Mondrian's version number from a string.
     * Note that this method does a mostly blind replacement
     * of the version string and may replace strings that
     * just happen to have the same sequence.
     *
     * @param str String
     * @return String with each occurrence of mondrian's version number
     *    (e.g. "2.3.0.0") replaced with "${mondrianVersion}"
     */
    protected static String maskVersion(String str) {
        MondrianServer.MondrianVersion mondrianVersion =
            MondrianServer.forId(null).getVersion();
        String versionString = mondrianVersion.getVersionString();
        // regex characters that wouldn't be expected before or after the
        // version string.  This avoids a false match when the version
        // string digits appear in other contexts (e.g. $3.56)
        String charsOutOfContext = "([^,\\$\\d])";
        String matchString = charsOutOfContext + Pattern.quote(versionString)
            + charsOutOfContext;
        return str.replaceAll(matchString, "$1\\${mondrianVersion}$2");
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

    protected Document fileToDocument(String filename, Properties props)
        throws IOException, SAXException
    {
        final String var = "${" + filename + "}";
        String s = getDiffRepos().expand(null, var);
        s = Util.replaceProperties(
            s, Util.toMap(props));
        if (s.equals(filename)) {
            s = "<?xml version='1.0'?><Empty/>";
            getDiffRepos().amend(var, s);
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
            requestType, requestText, "response",
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
                responseStr, XmlaConstants.Content.Data, role);
            return;
        }

        final Document responseDoc = (respFileName != null)
            ? fileToDocument(respFileName, props)
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
            expectedDoc, XmlaConstants.Content.SchemaData, role, true);

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
            expectedDoc, XmlaConstants.Content.None, role, false);

        expectedDoc = (responseDoc != null)
            ? XmlaSupport.transformSoapXmla(
                responseDoc, new String[][] {{"content", "data"}}, ns)
            : null;
        doTests(
            requestText, props,
            testContext, connectString, catalogNameUrls,
            expectedDoc, XmlaConstants.Content.Data, role, false);

        expectedDoc = (responseDoc != null)
            ? XmlaSupport.transformSoapXmla(
                responseDoc, new String[][] {{"content", "schema"}}, ns)
            : null;
        doTests(
            requestText, props,
            testContext, connectString, catalogNameUrls,
            expectedDoc, XmlaConstants.Content.Schema, role, false);
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
     *    {@link org.olap4j.metadata.XmlaConstants.Content#SchemaData})
     * @throws Exception on error
     */
    protected void doTests(
        String soapRequestText,
        Properties props,
        TestContext testContext,
        String connectString,
        Map<String, String> catalogNameUrls,
        Document expectedDoc,
        XmlaConstants.Content content,
        Role role,
        boolean replace) throws Exception
    {
        if (content != null) {
            props.setProperty(XmlaBasicTest.CONTENT_PROP, content.name());
        }

        // Even though it is not used, it is important that entry is in scope
        // until after request has returned. Prevents role's lock box entry from
        // being garbage collected.
        LockBox.Entry entry = null;
        if (role != null) {
            final MondrianServer mondrianServer =
                MondrianServer.forConnection(testContext.getConnection());
            entry = mondrianServer.getLockBox().register(role);
            connectString += "; Role='" + entry.getMoniker() + "'";
            props.setProperty(XmlaBaseTestCase.ROLE_PROP, entry.getMoniker());
        }
        soapRequestText = Util.replaceProperties(
            soapRequestText, Util.toMap(props));

        Document soapReqDoc = XmlUtil.parseString(soapRequestText);
        Document xmlaReqDoc = XmlaSupport.extractBodyFromSoap(soapReqDoc);

        // do XMLA
        byte[] bytes =
            XmlaSupport.processXmla(
                xmlaReqDoc, connectString, catalogNameUrls, role,
                resource.serverCache);

        if (XmlUtil.supportsValidation()
            // We can't validate against the schema when the content type
            // is Data because it doesn't respect the XDS.
            && !content.equals(XmlaConstants.Content.Data))
        {
            // Validating requires a <?xml header.
            String response = new String(bytes);
            if (!response.startsWith("<?xml version=\"1.0\"?>")) {
                response =
                    "<?xml version=\"1.0\"?>"
                    + Util.nl
                    + response;
            }
            if (XmlaSupport.validateXmlaUsingXpath(response.getBytes())) {
                if (DEBUG) {
                    System.out.println(
                        "XmlaBaseTestCase.doTests: XML Data is Valid");
                }
            }
        }

        // do SOAP-XMLA
        String callBackClassName = CallBack.class.getName();
        bytes = XmlaSupport.processSoapXmla(
            soapReqDoc,
            filterConnectString(connectString),
            catalogNameUrls,
            callBackClassName,
            role,
            resource.servletCache);

        if (DEBUG) {
            System.out.println(
                "XmlaBaseTestCase.doTests: soap response="
                + new String(bytes));
        }

        validate(
            bytes, expectedDoc, testContext, replace,
            content.equals(XmlaConstants.Content.Data) ? false : true);
        Util.discard(entry);
    }

    protected void doTestsJson(
        String soapRequestText,
        Properties props,
        TestContext testContext,
        String connectString,
        Map<String, String> catalogNameUrls,
        String expectedStr,
        XmlaConstants.Content content,
        Role role) throws Exception
    {
        if (content != null) {
            props.setProperty(XmlaBasicTest.CONTENT_PROP, content.name());
        }
        if (role != null) {
            final MondrianServer mondrianServer =
                MondrianServer.forConnection(testContext.getConnection());
            final LockBox.Entry entry =
                mondrianServer.getLockBox().register(role);
            props.setProperty(XmlaBaseTestCase.ROLE_PROP, entry.getMoniker());
        }
        soapRequestText = Util.replaceProperties(
            soapRequestText, Util.toMap(props));

        Document soapReqDoc = XmlUtil.parseString(soapRequestText);
        Document xmlaReqDoc = XmlaSupport.extractBodyFromSoap(soapReqDoc);

        // do XMLA
        byte[] bytes =
            XmlaSupport.processXmla(
                xmlaReqDoc, connectString, catalogNameUrls, role,
                resource.serverCache);

        // do SOAP-XMLA
        String callBackClassName = CallBack.class.getName();
        bytes = XmlaSupport.processSoapXmla(
            soapReqDoc,
            connectString,
            catalogNameUrls,
            callBackClassName,
            role,
            resource.servletCache);
        if (DEBUG) {
            System.out.println(
                "XmlaBaseTestCase.doTests: soap response=" + new String(bytes));
        }

        String gotStr = new String(bytes);
        gotStr = ignoreLastUpdateDate(gotStr);
        gotStr = maskVersion(gotStr);
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

    /**
     * Holder for resources (e.g. caches) that are shared between tests.
     *
     * <p>At any time there are either 0 or 1 instances. (It's a
     * reference-counted optional singleton.)</p>
     *
     * <p>Resources are acquired on
     * {@link mondrian.xmla.XmlaBaseTestCase#setUp()}</p> and released on
     * {@link XmlaBaseTestCase#tearDown()}. This releases them faster than
     * if they were fields inside each {@link mondrian.xmla.XmlaBaseTestCase}
     * instance.
     */
    private static class Resource {
        private static WeakReference<Resource> REF;

        static synchronized Resource acquire() {
            Resource resource;
            if (REF == null || (resource = REF.get()) == null) {
                resource = new Resource();
                REF = new WeakReference<Resource>(resource);
            }
            return resource;
        }

        protected void finalize() {
            for (MondrianServer server : serverCache.values()) {
                server.shutdown();
            }
            serverCache.clear();
            for (Servlet servlet : servletCache.values()) {
                servlet.destroy();
            }
            servletCache.clear();
        }

        /**
         * Cache servlet instances between test invocations. Prevents creation
         * of many spurious MondrianServer instances.
         */
        private final Map<List<String>, Servlet> servletCache =
            new HashMap<List<String>, Servlet>();

        /**
         * Cache servlet instances between test invocations. Prevents creation
         * of many spurious MondrianServer instances.
         */
        private final Map<List<String>, MondrianServer> serverCache =
            new TestContext.WeakMap<List<String>, MondrianServer>();
    }
}

// End XmlaBaseTestCase.java
