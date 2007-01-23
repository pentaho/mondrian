/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2002-2006 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 29 March, 2002
*/
package mondrian.xmla;

import mondrian.olap.Util;
import mondrian.test.TestContext;
import mondrian.tui.*;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import org.xml.sax.SAXException;
import org.custommonkey.xmlunit.XMLAssert;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.Servlet;


/**
 * These test the compatibility of Mondrian XMLA with Excel 2000.
 * Simba (the maker of the O2X bridge) supplied captured request/response
 * soap messages between Excel 2000 and SQL Server. These form the
 * basis of the output files in the  excel_2000 directory.
 *
 * @author Richard M. Emberson
 * @version $Id$
 */
public class XmlaExcel2000Test extends XmlaBaseTestCase {

    private static String EXPECT = XmlaRequestCallback.EXPECT;
    private static String EXPECT_100_CONTINUE = XmlaRequestCallback.EXPECT_100_CONTINUE;

    private static final String XMLA_DIRECTORY = "testsrc/main/mondrian/xmla/";

    private static final boolean DEBUG = false;
    protected File testDir;
    // session id properpty
    public static final String SESSION_ID_PROP     = "session.id";

    public XmlaExcel2000Test() {
        super();
    }
    public XmlaExcel2000Test(String name) {
        super(name);
    }

    protected String getOutFileName(String nos) {
        return "excel_2000_"+ nos + "_out.xml";
    }

    protected String getSessionId() {
        return sessionId;
    }

    protected String fileToString(String filename) throws IOException {
        File file = new File(testDir, filename);
        String requestText = XmlaSupport.readFile(file);
        return requestText;
    }

    protected String generateExpectedString(String nos, Properties props)
            throws Exception {
        String expectedFileName = getOutFileName(nos);

        String expectedStr = fileToString(expectedFileName);
        if (props != null) {
            // YES, duplicate the above
            if (getSessionId() != null) {
                props.put(SESSION_ID_PROP, getSessionId());
            }
            expectedStr = Util.replaceProperties(
                expectedStr, Util.toMap(props));
        }
        return expectedStr;
    }

    static class CallBack implements XmlaRequestCallback {
        static String MY_SESSION_ID = "my_session_id";
        CallBack() {
        }

        public void init(ServletConfig servletConfig) throws ServletException {
        }

        public boolean processHttpHeader(
            HttpServletRequest request,
            HttpServletResponse response,
            Map<String, String> context) throws Exception
        {
            String expect = request.getHeader(XmlaRequestCallback.EXPECT);
            if ((expect != null) &&
                expect.equalsIgnoreCase(XmlaRequestCallback.EXPECT_100_CONTINUE)) {

                XmlaRequestCallback.Helper.generatedExpectResponse(
                    request, response, context);
                return false;
            } else {
                return true;
            }
        }

        public void preAction(
                HttpServletRequest request,
                Element[] requestSoapParts,
                Map<String, String> context) throws Exception {
        }

        private void setSessionId(Map context) {
            if (XmlaExcel2000Test.sessionId == null) {
                makeSessionId();
            }

            context.put(MY_SESSION_ID, XmlaExcel2000Test.sessionId);
        }

        public String generateSessionId(Map<String, String> context) {
            setSessionId(context);
            return (String) context.get(MY_SESSION_ID);
        }
        public void postAction(
                    HttpServletRequest request,
                    HttpServletResponse response,
                    byte[][] responseSoapParts,
                    Map<String, String> context) throws Exception {
        }
    }

    static int sessionIdCounter = 1000;
    static String sessionId = null;

    protected static void makeSessionId() {
        int id = XmlaExcel2000Test.sessionIdCounter++;
        StringBuilder buf = new StringBuilder();
        buf.append("XmlaExcel2000Test-");
        buf.append(id);
        buf.append("-foo");
        String sessionId = buf.toString();

        // set class sessionid
        XmlaExcel2000Test.sessionId = sessionId;
    }


    protected Servlet servlet;
    protected String[][] catalogNameUrls = null;

    protected void setUp() throws Exception {
        testDir = new File(XMLA_DIRECTORY + "/excel_2000");
        makeServlet();
    }
    protected void tearDown() throws Exception {
    }
    protected void makeServlet()
            throws IOException, ServletException, SAXException {

        XmlaExcel2000Test.sessionId = null;

        String connectString = getConnectionString();
        String[][] catalogNameUrls = getCatalogNameUrls();
        servlet = XmlaSupport.makeServlet(connectString, catalogNameUrls, CallBack.class.getName());
    }


    protected Document fileToDocument(String filename)
                throws IOException , SAXException {
        File file = new File(testDir, filename);
        Document doc = XmlUtil.parse(file);
        return doc;
    }

    public TestContext getTestContext() {
        return TestContext.instance();
    }

    protected String getConnectionString() {
        return getTestContext().getConnectString();
    }
    protected String[][] getCatalogNameUrls() {
        if (catalogNameUrls == null) {
            String connectString = getConnectionString();
            Util.PropertyList connectProperties =
                        Util.parseConnectString(connectString);
            String catalog = connectProperties.get("catalog");
            catalogNameUrls = new String[][] {
                { "FoodMart", catalog }
            };
        }
        return catalogNameUrls;
    }

    // good 3/26
    public void test01() throws Exception {
        helperTest("01", false);
    }

    // BeginSession
    // good 3/26
    public void test02() throws Exception {
        helperTest("02", false);
    }
    // good 3/27
    public void test03() throws Exception {
        helperTest("03", true);
    }
    // good 3/27
    public void test04() throws Exception {
        helperTest("04", true);
    }
    // good 3/27
    public void test05() throws Exception {
        helperTest("05", true);
    }
    // good 3/27
    public void test06() throws Exception {
        helperTest("06", true);
    }

    // good 3/27
    // BeginSession
    public void test07() throws Exception {
        helperTest("07", false);
    }
    // good 3/27
    public void test08() throws Exception {
        helperTest("08", true);
    }
    // good 3/27
    public void test09() throws Exception {
        helperTest("09", true);
    }
    // good 3/27
    public void test10() throws Exception {
        helperTest("10", true);
    }
    // good 3/27
    public void test11() throws Exception {
        helperTest("11", true);
    }
    // good 3/27
    public void test12() throws Exception {
        helperTest("12", true);
    }
    // good 3/27
    public void test13() throws Exception {
        helperTest("13", true);
    }
    // good 3/27
    public void test14() throws Exception {
        helperTest("14", true);
    }
    // good 3/27
    public void test15() throws Exception {
        helperTest("15", true);
    }
    // good 3/27
    public void test16() throws Exception {
        helperTest("16", true);
    }
    public void test17() throws Exception {
        helperTest("17", true);
    }
    // good 3/27
    public void test18() throws Exception {
        helperTest("18", true);
    }
    protected void helperTest(String nos, boolean doSessionId)
            throws Exception {
        if (doSessionId) {
            if (XmlaExcel2000Test.sessionId == null) {
                makeSessionId();
            }
        }
        Properties props = new Properties();
        doTest(nos, props);
    }
    /////////////////////////////////////////////////////////////////////////
    // expect
    /////////////////////////////////////////////////////////////////////////
    public void testExpect01() throws Exception {
        helperTestExpect("01", false);
    }
    public void testExpect02() throws Exception {
        helperTestExpect("02", false);
    }
    public void testExpect03() throws Exception {
        helperTestExpect("03", true);
    }
    public void testExpect04() throws Exception {
        helperTestExpect("04", true);
    }
    public void testExpect05() throws Exception {
        helperTestExpect("05", true);
    }
    public void testExpect06() throws Exception {
        helperTestExpect("06", true);
    }

    protected void helperTestExpect(String nos, boolean doSessionId)
            throws Exception {
        if (doSessionId) {
            if (XmlaExcel2000Test.sessionId == null) {
                makeSessionId();
            }
        }
        MockHttpServletRequest req = new MockHttpServletRequest();
        req.setMethod("POST");
        req.setContentType("text/xml");
        req.setHeader(EXPECT, EXPECT_100_CONTINUE);

        Properties props = new Properties();
        doTest(req, nos, props);
    }
    /////////////////////////////////////////////////////////////////////////
    // helpers
    /////////////////////////////////////////////////////////////////////////
    public void doTest(
            MockHttpServletRequest req,
            String nos,
            Properties props
            ) throws Exception {
        String requestText = generateRequestString(nos, props);

        MockHttpServletResponse res = new MockHttpServletResponse();
        res.setCharacterEncoding("UTF-8");

        if (servlet == null) {
            makeServlet();
        }

        servlet.service(req, res);

        int statusCode = res.getStatusCode();
        if (statusCode == HttpServletResponse.SC_OK) {

            byte[] bytes = res.toByteArray();
            String expectedStr = generateExpectedString(nos, props);
            Document expectedDoc = XmlUtil.parseString(expectedStr);
            validate(bytes, expectedDoc);

        } else if (statusCode == HttpServletResponse.SC_CONTINUE) {
            // remove the Expect header from request and try again
if (DEBUG) {
System.out.println("Got CONTINUE");
}

            req.clearHeader(EXPECT);
            req.setBodyContent(requestText);

            servlet.service(req, res);

            statusCode = res.getStatusCode();
            if (statusCode == HttpServletResponse.SC_OK) {
                byte[] bytes = res.toByteArray();
                String expectedStr = generateExpectedString(nos, props);
                Document expectedDoc = XmlUtil.parseString(expectedStr);
                validate(bytes, expectedDoc);

            } else {
                fail("Bad status code: " +statusCode);
            }
        } else {
            fail("Bad status code: " +statusCode);

        }
    }

    public void doTest(String nos, Properties props) throws Exception {
        String requestText = generateRequestString(nos, props);
        Document reqDoc = XmlUtil.parseString(requestText);

        if (servlet == null) {
            makeServlet();
        }
        byte[] bytes = XmlaSupport.processSoapXmla(reqDoc, servlet);

        String expectedStr = generateExpectedString(nos, props);
        Document expectedDoc = XmlUtil.parseString(expectedStr);
        validate(bytes, expectedDoc);

    }
    protected void validate(byte[] bytes, Document expectedDoc)
            throws Exception {
if (DEBUG) {
        String response = new String(bytes);
System.out.println("response="+response);
}
        if (XmlUtil.supportsValidation()) {
            if (XmlaSupport.validateSoapXmlaUsingXpath(bytes)) {
if (DEBUG) {
                System.out.println("XML Data is Valid");
}
            }
        }

        Document gotDoc = XmlUtil.parse(bytes);
	    String gotStr = XmlUtil.toString(replaceLastSchemaUpdateDate(gotDoc), true);
        String expectedStr = XmlUtil.toString(replaceLastSchemaUpdateDate(expectedDoc), true);
if (DEBUG) {
System.out.println("GOT:\n"+gotStr);
System.out.println("EXPECTED:\n"+expectedStr);
System.out.println("XXXXXXX");
}
        XMLAssert.assertXMLEqual(expectedStr, gotStr);
    }

    protected String generateRequestString(String nos, Properties props)
            throws Exception {
        String reqFileName = "excel_2000_" + nos + "_in.xml";
if (DEBUG) {
System.out.println("reqFileName="+reqFileName);
}
        String requestText = fileToString(reqFileName);

        if (props != null) {
            if (XmlaExcel2000Test.sessionId != null) {
                props.put(SESSION_ID_PROP, XmlaExcel2000Test.sessionId);
            }
            requestText = Util.replaceProperties(
                requestText, Util.toMap(props));
        }
if (DEBUG) {
System.out.println("requestText="+requestText);
}
        return requestText;
    }

}

// XmlaExcel2000Test.java
