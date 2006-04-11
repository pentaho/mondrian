/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2002-2002 Kana Software, Inc.
// Copyright (C) 2002-2005 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 29 March, 2002
*/
package mondrian.xmla;

import mondrian.test.FoodMartTestCase;
import mondrian.test.TestContext;
import mondrian.olap.*;
import mondrian.tui.*;
import mondrian.xmla.*;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.CharacterData;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Element;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Enumeration;
import java.util.Properties;
import java.util.StringTokenizer;
import org.xml.sax.SAXException;
import org.custommonkey.xmlunit.XMLAssert;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.Servlet;

/** 
 * This is a test of the XMLA Fault generation - errors occur/are-detected in   
 * in Mondrian XMLA and a SOAP Fault is returned. 
 * <p>
 * There is a set of tests dealing with Authorization and HTTP Header
 * Expect and Continue dialog. These are normally done at the webserver
 * level and can be removed here if desired. (I wrote them before I 
 * realized that Mondrian XMLA would not handle any Authorization issues
 * if it were in a webserver.)
 * 
 * @author <a>Richard M. Emberson</a>
 * @version 
 */
public class XmlaErrorTest extends FoodMartTestCase 
        implements XmlaConstants {

    private static final String XMLA_DIRECTORY = "testsrc/main/mondrian/xmla/";

    private static final boolean DEBUG = false;

    private static String AUTHORIZATION = XmlaRequestCallback.AUTHORIZATION;
    private static String EXPECT = XmlaRequestCallback.EXPECT;
    private static String EXPECT_100_CONTINUE = XmlaRequestCallback.EXPECT_100_CONTINUE;

    static boolean doAuthorization = false;
    static String user = null;
    static String password = null;

    static boolean isEquals(String s1, String s2) {
        return (s1 == s2) ||
            ((s1 != null) && (s2 != null) && (s1.equals(s2)));
    }
    static class CallBack implements XmlaRequestCallback {
        static String MY_SESSION_ID = "my_session_id";

        CallBack() {
        }

        public void init(ServletConfig servletConfig) throws ServletException {
        }
        public boolean processHttpHeader(HttpServletRequest request, 
                HttpServletResponse response,
                Map context) throws Exception {

            // look for authorization
            // Authorization: Basic ZWRnZTphYmNkMTIzNC4=
            // tjones:abcd1234$$.

if (DEBUG) {
System.out.println("doAuthorization=" +doAuthorization);
System.out.println("AUTH_TYPE=" +request.getAuthType());
}
            if (doAuthorization) {
                Enumeration values = request.getHeaders(AUTHORIZATION);
                if ((values == null) || (! values.hasMoreElements())) {
                    throw XmlaRequestCallback.Helper.authorizationException(
                        new Exception("Authorization: no header value"));
                }
                String authScheme = (String) values.nextElement();
if (DEBUG) {
System.out.println("authScheme=" +authScheme);
}
                if (! values.hasMoreElements()) {
                    throw XmlaRequestCallback.Helper.authorizationException(
                        new Exception("Authorization: too few header value"));
                }

                String encoded = (String) values.nextElement();
if (DEBUG) {
System.out.println("encoded=" +encoded);
}
                byte[] bytes = XmlaUtil.decodeBase64(encoded);
                String userPass = new String(bytes);
if (DEBUG) {
System.out.println("userPass=" +userPass);
}
                if (! authScheme.equals(HttpServletRequest.BASIC_AUTH)) {
                    throw XmlaRequestCallback.Helper.authorizationException(
                        new Exception("Authorization: bad schema: " + authScheme));
                }
                int index = userPass.indexOf(':');
                if (index == -1) {
                    throw XmlaRequestCallback.Helper.authorizationException(
                        new Exception( "Authorization: badly formed userPass in encoding: " + encoded));
                }
                String userid = userPass.substring(0, index);
                String password = userPass.substring(index+1, userPass.length());
if (DEBUG) {
System.out.println("userid=" +userid);
System.out.println("password=" +password);
}
                if (! isEquals(userid, XmlaErrorTest.user)) {
                    throw XmlaRequestCallback.Helper.authorizationException(
                        new Exception(
                          "Authorization: bad userid: " + 
                            userid +
                            " should be: " +
                            XmlaErrorTest.user
                        ));
                }
                if (! isEquals(password, XmlaErrorTest.password)) {
                    throw XmlaRequestCallback.Helper.authorizationException(
                        new Exception(
                          "Authorization: bad password: " + 
                            password +
                            " should be: " +
                            XmlaErrorTest.password
                        ));
                }
            }
            String expect = request.getHeader(EXPECT);
            if ((expect != null) && 
                expect.equalsIgnoreCase(EXPECT_100_CONTINUE)) {

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
                Map context) throws Exception {

            if (XmlaExcelXPTest.sessionId == null) {
                makeSessionId();
            }
            context.put(MY_SESSION_ID, XmlaExcelXPTest.sessionId);

        }

        public String generateSessionId(Map context) {
            return (String) context.get(MY_SESSION_ID);
        }
        public void postAction(
                    HttpServletRequest request,
                    HttpServletResponse response,
                    byte[][] responseSoapParts,
                    Map context) throws Exception {
        }
    }

    static Element[] getChildElements(Node node) {
        List list = new ArrayList();

        NodeList nlist = node.getChildNodes();
        int len = nlist.getLength();
        for (int i = 0; i < len; i++) {
            Node child = nlist.item(i);
            if (child instanceof Element) {
                list.add(child);
            }
        }

        return (Element[]) list.toArray(new Element[0]);
    }
    static CharacterData getCharacterData(Node node) {
        NodeList nlist = node.getChildNodes();
        int len = nlist.getLength();
        for (int i = 0; i < len; i++) {
            Node child = nlist.item(i);
            if (child instanceof CharacterData) {
                return (CharacterData) child;
            }
        }
        return null;
        
    }
    static String getNodeContent(Node n) {
        CharacterData cd = getCharacterData(n);
        return (cd instanceof CharacterData)
            ? ((CharacterData)cd).getData()
            : null;
    }

    static class Fault {
        String faultCode;
        String faultString;
        String faultActor;
        String errorNS;
        String errorCode;
        String errorDesc;

        Fault(String faultCode,
              String faultString,
              String faultActor,
              String errorNS,
              String errorCode,
              String errorDesc) {
            this.faultCode = faultCode;
            this.faultString = faultString;
            this.faultActor = faultActor;
            this.errorNS = errorNS;
            this.errorCode = errorCode;
            this.errorDesc = errorDesc;
        }

        Fault(Node[] faultNodes) throws Exception {
            if (faultNodes.length < 3 || faultNodes.length > 4) {
                throw new Exception("SOAP Fault node has " +
                    faultNodes.length+" children");
            }
            // fault code element
            Node node = faultNodes[0];
            faultCode = getNodeContent(node);

            // fault string element
            node = faultNodes[1];
            faultString = getNodeContent(node);

            // actor element
            node = faultNodes[2];
            faultActor = getNodeContent(node);

            if (faultNodes.length > 3) {
                // detail element
                node = faultNodes[3];
                faultNodes = getChildElements(node);
                if (faultNodes.length != 1) {
                    throw new Exception("SOAP Fault detail node has " +
                        faultNodes.length+" children");
                }
                // error element
                node = faultNodes[0];
                errorNS = node.getNamespaceURI();
                faultNodes = getChildElements(node);
                if (faultNodes.length != 2) {
                    throw new Exception("SOAP Fault detail error node has " +
                        faultNodes.length+" children");
                }
                // error code element
                node = faultNodes[0];
                errorCode = getNodeContent(node);

                // error desc element
                node = faultNodes[1];
                errorDesc = getNodeContent(node);

            }
        }
        Fault(Node faultNode) throws Exception {
            this(getChildElements(faultNode));
        }

        String getFaultCode() {
            return faultCode;
        }
        String getFaultString() {
            return faultString;
        }
        String getFaultActor() {
            return faultActor;
        }
        boolean hasDetailError() {
            return (errorCode != null);
        }
        String getDetailErrorCode() {
            return errorCode;
        }
        String getDetailErrorDesc() {
            return errorDesc;
        }
        public String toString() {
            StringBuffer buf = new StringBuffer(100);
            buf.append("faultCode=");
            buf.append(faultCode);
            buf.append(", faultString=");
            buf.append(faultString);
            buf.append(", faultActor=");
            buf.append(faultActor);
            buf.append(", errorNS=");
            buf.append(errorNS);
            buf.append(", errorCode=");
            buf.append(errorCode);
            buf.append(", errorDesc=");
            buf.append(errorDesc);
            return buf.toString();
        }
        void checkSame(Fault expectedFault) throws Exception {
            if (! isEquals(this.faultCode, expectedFault.faultCode)) {
                notSame("faultcode", this.faultCode, expectedFault.faultCode);
            }
            if (! isEquals(this.faultString, expectedFault.faultString)) {
                notSame("faultstring", this.faultString, expectedFault.faultString);
            }
            if (! isEquals(this.faultActor, expectedFault.faultActor)) {
                notSame("faultactor", this.faultActor, expectedFault.faultActor);
            }
            if (! isEquals(this.errorNS, expectedFault.errorNS)) {
                throw new Exception("For error element namespace " +
                    " Expected " + 
                    this.errorNS +
                    " but Got " + 
                    expectedFault.errorNS
                    );
            }
            if (! isEquals(this.errorCode, expectedFault.errorCode)) {
                notSame("error.code", this.errorCode, expectedFault.errorCode);
            }
        }
        private void notSame(String elementName, String expected, String got) 
                throws Exception {
            throw new Exception("For element " +
                elementName +
                " Expected " + 
                expected +
                " but Got " + 
                got
                );
        }

    }

    static int sessionIdCounter = 1000;
    static String sessionId = null;

    protected static void makeSessionId() {
        int id = XmlaExcelXPTest.sessionIdCounter++;
        StringBuffer buf = new StringBuffer();
        buf.append("XmlaExcelXPTest-");
        buf.append(id);
        buf.append("-foo");
        String sessionId = buf.toString();

        // set class sessionid
        XmlaExcelXPTest.sessionId = sessionId;
    }

    protected File testDir;
    protected Servlet servlet;

    public XmlaErrorTest() {
    }
    public XmlaErrorTest(String name) {
        super(name);
    }


    protected void setUp() throws Exception {
        testDir = new File(XMLA_DIRECTORY + "/error");
        makeServlet();
    }
    protected void tearDown() throws Exception {
    }
    protected void makeServlet() 
            throws IOException, ServletException, SAXException {

        XmlaExcelXPTest.sessionId = null;

        String connectString = getConnectionString();
        servlet = XmlaSupport.makeServlet(connectString, CallBack.class.getName());
    }
    public TestContext getTestContext() {
        return TestContext.instance();
    }
    protected String getConnectionString() {
        return getTestContext().getConnectString();
    }

    protected String fileToString(String filename) throws IOException {
        File file = new File(testDir, filename);
        String requestText = XmlaSupport.readFile(file);
        return requestText;
    }


    /////////////////////////////////////////////////////////////////////////
    // tests
    /////////////////////////////////////////////////////////////////////////

    /////////////////////////////////////////////////////////////////////////
    // bad XML
    /////////////////////////////////////////////////////////////////////////
    // junk rather than xml
    public void testJunk() throws Exception {
        String reqFileName = "Junk_in.error";
        Fault expectedFault = new Fault(XmlaException.formatFaultCode(
                    CLIENT_FAULT_FC, 
                    USM_DOM_PARSE_CODE),
                    USM_DOM_PARSE_FAULT_FS,
                    FAULT_ACTOR,
                    MONDRIAN_NAMESPACE,
                    USM_DOM_PARSE_CODE, null);

        doTest(reqFileName, expectedFault);
    }
    // bad soap envolope element tag
    public void testBadXml01() throws Exception {
        String reqFileName = "BadXml01_in.error";
        Fault expectedFault = new Fault(XmlaException.formatFaultCode(
                    CLIENT_FAULT_FC, 
                    USM_DOM_PARSE_CODE),
                    USM_DOM_PARSE_FAULT_FS,
                    FAULT_ACTOR,
                    MONDRIAN_NAMESPACE,
                    USM_DOM_PARSE_CODE, null);

        doTest(reqFileName, expectedFault);
    }
    // bad soap namespace
    public void testBadXml02() throws Exception {
        String reqFileName = "BadXml02_in.error";
        Fault expectedFault = new Fault(XmlaException.formatFaultCode(
                    CLIENT_FAULT_FC, 
                    USM_DOM_PARSE_CODE),
                    USM_DOM_PARSE_FAULT_FS,
                    FAULT_ACTOR,
                    MONDRIAN_NAMESPACE,
                    USM_DOM_PARSE_CODE, null);

        doTest(reqFileName, expectedFault);
    }

    /////////////////////////////////////////////////////////////////////////
    // bad action
    /////////////////////////////////////////////////////////////////////////
    public void testBadAction01() throws Exception {
        String reqFileName = "BadAction01_in.error";
        Fault expectedFault = new Fault(XmlaException.formatFaultCode(
                    CLIENT_FAULT_FC, 
                    HSB_BAD_SOAP_BODY_CODE),
                    HSB_BAD_SOAP_BODY_FAULT_FS,
                    FAULT_ACTOR,
                    MONDRIAN_NAMESPACE,
                    HSB_BAD_SOAP_BODY_CODE, null);

        doTest(reqFileName, expectedFault);
    }
    public void testBadAction02() throws Exception {
        String reqFileName = "BadAction02_in.error";
        Fault expectedFault = new Fault(XmlaException.formatFaultCode(
                    CLIENT_FAULT_FC, 
                    HSB_BAD_SOAP_BODY_CODE),
                    HSB_BAD_SOAP_BODY_FAULT_FS,
                    FAULT_ACTOR,
                    MONDRIAN_NAMESPACE,
                    HSB_BAD_SOAP_BODY_CODE, null);

        doTest(reqFileName, expectedFault);
    }
    public void testBadAction03() throws Exception {
        String reqFileName = "BadAction03_in.error";
        Fault expectedFault = new Fault(XmlaException.formatFaultCode(
                    CLIENT_FAULT_FC, 
                    HSB_BAD_SOAP_BODY_CODE),
                    HSB_BAD_SOAP_BODY_FAULT_FS,
                    FAULT_ACTOR,
                    MONDRIAN_NAMESPACE,
                    HSB_BAD_SOAP_BODY_CODE, null);

        doTest(reqFileName, expectedFault);
    }
    /////////////////////////////////////////////////////////////////////////
    // bad soap structure
    /////////////////////////////////////////////////////////////////////////
    public void testBadSoap01() throws Exception {
        String reqFileName = "DoubleHeader.xml";
        Fault expectedFault = new Fault(XmlaException.formatFaultCode(
                    CLIENT_FAULT_FC, 
                    USM_DOM_PARSE_CODE),
                    USM_DOM_PARSE_FAULT_FS,
                    FAULT_ACTOR,
                    MONDRIAN_NAMESPACE,
                    USM_DOM_PARSE_CODE, null);

        doTest(reqFileName, expectedFault);
    }
    public void testBadSoap02() throws Exception {
        String reqFileName = "DoubleBody.xml";
        Fault expectedFault = new Fault(XmlaException.formatFaultCode(
                    CLIENT_FAULT_FC, 
                    USM_DOM_PARSE_CODE),
                    USM_DOM_PARSE_FAULT_FS,
                    FAULT_ACTOR,
                    MONDRIAN_NAMESPACE,
                    USM_DOM_PARSE_CODE, null);

        doTest(reqFileName, expectedFault);
    }

    /////////////////////////////////////////////////////////////////////////
    // authorization
    /////////////////////////////////////////////////////////////////////////
    // no authorization field in header
    public void testAuth01() throws Exception {
        String reqFileName = "EmptyExecute.xml";
        Fault expectedFault = new Fault(XmlaException.formatFaultCode(
                    CLIENT_FAULT_FC, 
                    CHH_AUTHORIZATION_CODE),
                    CHH_AUTHORIZATION_FAULT_FS,
                    FAULT_ACTOR,
                    MONDRIAN_NAMESPACE,
                    CHH_AUTHORIZATION_CODE, null);
        doAuthorization = true;
        try {
            doTest(reqFileName, expectedFault);
        } finally {
            servlet = null;
            doAuthorization = false;
        }
    }
    // the user/password is not base64 encode and no ':' character
    public void testAuth02() throws Exception {
        String reqFileName = "EmptyExecute.xml";
        Fault expectedFault = new Fault(XmlaException.formatFaultCode(
                    CLIENT_FAULT_FC, 
                    CHH_AUTHORIZATION_CODE),
                    CHH_AUTHORIZATION_FAULT_FS,
                    FAULT_ACTOR,
                    MONDRIAN_NAMESPACE,
                    CHH_AUTHORIZATION_CODE, null);

        doAuthorization = true;

        String requestText = fileToString(reqFileName);
        byte[] reqBytes = requestText.getBytes();

        MockHttpServletRequest req = new MockHttpServletRequest(reqBytes);
        req.setMethod("POST");
        req.setContentType("text/xml");

        req.setAuthType(HttpServletRequest.BASIC_AUTH);
        req.setHeader(AUTHORIZATION, HttpServletRequest.BASIC_AUTH);
        req.setHeader(AUTHORIZATION, "FOOBAR");

        try {
            doTest(req, expectedFault);
        } finally {
            servlet = null;
            doAuthorization = false;
        }
    }
    // this should work
    public void testAuth03() throws Exception {
        String reqFileName = "EmptyExecute.xml";
        Fault expectedFault = null;

        doAuthorization = true;

        String requestText = fileToString(reqFileName);
        byte[] reqBytes = requestText.getBytes();

        MockHttpServletRequest req = new MockHttpServletRequest(reqBytes);
        req.setMethod("POST");
        req.setContentType("text/xml");

        req.setAuthType(HttpServletRequest.BASIC_AUTH);
        req.setHeader(AUTHORIZATION, HttpServletRequest.BASIC_AUTH);

        String user = "MY_USER";
        String password = "MY_PASSWORD";
        XmlaErrorTest.user = user;
        XmlaErrorTest.password = password;
        String credential = user + ':' + password;
        String encoded = XmlaUtil.encodeBase64(credential.getBytes());

        req.setHeader(AUTHORIZATION, encoded);

        try {
            doTest(req, expectedFault);
            req.setHeader(EXPECT, EXPECT_100_CONTINUE);
if (DEBUG) {
System.out.println("DO IT AGAIN");
}
            doTest(req, expectedFault);
        } finally {
            servlet = null;
            XmlaErrorTest.doAuthorization = false;
            XmlaErrorTest.user = null;
            XmlaErrorTest.password = null;
        }
    }
    // fail: bad user name
    public void testAuth04() throws Exception {
        String reqFileName = "EmptyExecute.xml";
        Fault expectedFault = new Fault(XmlaException.formatFaultCode(
                    CLIENT_FAULT_FC, 
                    CHH_AUTHORIZATION_CODE),
                    CHH_AUTHORIZATION_FAULT_FS,
                    FAULT_ACTOR,
                    MONDRIAN_NAMESPACE,
                    CHH_AUTHORIZATION_CODE, null);

        doAuthorization = true;

        String requestText = fileToString(reqFileName);
        byte[] reqBytes = requestText.getBytes();

        MockHttpServletRequest req = new MockHttpServletRequest(reqBytes);
        req.setMethod("POST");
        req.setContentType("text/xml");

        req.setAuthType(HttpServletRequest.BASIC_AUTH);
        req.setHeader(AUTHORIZATION, HttpServletRequest.BASIC_AUTH);

        String user = "MY_USER";
        String password = "MY_PASSWORD";
        XmlaErrorTest.user = user + "FOO";
        XmlaErrorTest.password = password;
        String credential = user + ':' + password;
        String encoded = XmlaUtil.encodeBase64(credential.getBytes());

        req.setHeader(AUTHORIZATION, encoded);

        try {
            doTest(req, expectedFault);
        } finally {
            servlet = null;
            XmlaErrorTest.doAuthorization = false;
            XmlaErrorTest.user = null;
            XmlaErrorTest.password = null;
        }
    }
    // fail: bad password
    public void testAuth05() throws Exception {
        String reqFileName = "EmptyExecute.xml";
        Fault expectedFault = new Fault(XmlaException.formatFaultCode(
                    CLIENT_FAULT_FC, 
                    CHH_AUTHORIZATION_CODE),
                    CHH_AUTHORIZATION_FAULT_FS,
                    FAULT_ACTOR,
                    MONDRIAN_NAMESPACE,
                    CHH_AUTHORIZATION_CODE, null);

        doAuthorization = true;

        String requestText = fileToString(reqFileName);
        byte[] reqBytes = requestText.getBytes();

        MockHttpServletRequest req = new MockHttpServletRequest(reqBytes);
        req.setMethod("POST");
        req.setContentType("text/xml");

        req.setAuthType(HttpServletRequest.BASIC_AUTH);
        req.setHeader(AUTHORIZATION, HttpServletRequest.BASIC_AUTH);

        String user = "MY_USER";
        String password = "MY_PASSWORD";
        XmlaErrorTest.user = user;
        XmlaErrorTest.password = password + "FOO";
        String credential = user + ':' + password;
        String encoded = XmlaUtil.encodeBase64(credential.getBytes());

        req.setHeader(AUTHORIZATION, encoded);

        try {
            doTest(req, expectedFault);
        } finally {
            servlet = null;
            XmlaErrorTest.doAuthorization = false;
            XmlaErrorTest.user = null;
            XmlaErrorTest.password = null;
        }
    }
    /////////////////////////////////////////////////////////////////////////
    // bad header
    /////////////////////////////////////////////////////////////////////////
    public void testBadHeader01() throws Exception {
        String reqFileName = "MustUnderstand.xml";
        // remember, errors in headers do not have detail sections
        Fault expectedFault = new Fault(XmlaException.formatFaultCode(
                    MUST_UNDERSTAND_FAULT_FC, 
                    HSH_MUST_UNDERSTAND_CODE),
                    HSH_MUST_UNDERSTAND_FAULT_FS,
                    FAULT_ACTOR,
                    null, null, null);

        doTest(reqFileName, expectedFault);
    }
    /////////////////////////////////////////////////////////////////////////
    // bad body
    /////////////////////////////////////////////////////////////////////////
    public void testBadBody01() throws Exception {
        String reqFileName = "DiscoveryExecute.xml";
        Fault expectedFault = new Fault(XmlaException.formatFaultCode(
                    CLIENT_FAULT_FC, 
                    HSB_BAD_SOAP_BODY_CODE),
                    HSB_BAD_SOAP_BODY_FAULT_FS,
                    FAULT_ACTOR,
                    MONDRIAN_NAMESPACE,
                    HSB_BAD_SOAP_BODY_CODE, null);

        doTest(reqFileName, expectedFault);
    }

    public void testBadBody02() throws Exception {
        String reqFileName = "BadMethod.xml";
        Fault expectedFault = new Fault(XmlaException.formatFaultCode(
                    CLIENT_FAULT_FC, 
                    HSB_BAD_SOAP_BODY_CODE),
                    HSB_BAD_SOAP_BODY_FAULT_FS,
                    FAULT_ACTOR,
                    MONDRIAN_NAMESPACE,
                    HSB_BAD_SOAP_BODY_CODE, null);

        doTest(reqFileName, expectedFault);
    }
    public void testBadBody03() throws Exception {
        String reqFileName = "BadMethodNS.xml";
        Fault expectedFault = new Fault(XmlaException.formatFaultCode(
                    CLIENT_FAULT_FC, 
                    HSB_BAD_SOAP_BODY_CODE),
                    HSB_BAD_SOAP_BODY_FAULT_FS,
                    FAULT_ACTOR,
                    MONDRIAN_NAMESPACE,
                    HSB_BAD_SOAP_BODY_CODE, null);

        doTest(reqFileName, expectedFault);
    }
    public void testBadBody04() throws Exception {
        String reqFileName = "Discovery01.xml";
        Fault expectedFault = new Fault(XmlaException.formatFaultCode(
                    CLIENT_FAULT_FC, 
                    HSB_BAD_REQUEST_TYPE_CODE),
                    HSB_BAD_REQUEST_TYPE_FAULT_FS,
                    FAULT_ACTOR,
                    MONDRIAN_NAMESPACE,
                    HSB_BAD_REQUEST_TYPE_CODE, null);

        doTest(reqFileName, expectedFault);
    }
    public void testBadBody05() throws Exception {
        String reqFileName = "Discovery02.xml";
        Fault expectedFault = new Fault(XmlaException.formatFaultCode(
                    CLIENT_FAULT_FC, 
                    HSB_BAD_RESTRICTIONS_CODE),
                    HSB_BAD_RESTRICTIONS_FAULT_FS,
                    FAULT_ACTOR,
                    MONDRIAN_NAMESPACE,
                    HSB_BAD_RESTRICTIONS_CODE, null);

        doTest(reqFileName, expectedFault);
    }
    public void testBadBody06() throws Exception {
        String reqFileName = "Discovery03.xml";
        Fault expectedFault = new Fault(XmlaException.formatFaultCode(
                    CLIENT_FAULT_FC, 
                    HSB_BAD_PROPERTIES_CODE),
                    HSB_BAD_PROPERTIES_FAULT_FS,
                    FAULT_ACTOR,
                    MONDRIAN_NAMESPACE,
                    HSB_BAD_PROPERTIES_CODE, null);

        doTest(reqFileName, expectedFault);
    }
    public void testBadBody07() throws Exception {
        String reqFileName = "Execute01.xml";
        Fault expectedFault = new Fault(XmlaException.formatFaultCode(
                    CLIENT_FAULT_FC, 
                    HSB_BAD_COMMAND_CODE),
                    HSB_BAD_COMMAND_FAULT_FS,
                    FAULT_ACTOR,
                    MONDRIAN_NAMESPACE,
                    HSB_BAD_COMMAND_CODE, null);

        doTest(reqFileName, expectedFault);
    }
    public void testBadBody08() throws Exception {
        String reqFileName = "Execute02.xml";
        Fault expectedFault = new Fault(XmlaException.formatFaultCode(
                    CLIENT_FAULT_FC, 
                    HSB_BAD_PROPERTIES_CODE),
                    HSB_BAD_PROPERTIES_FAULT_FS,
                    FAULT_ACTOR,
                    MONDRIAN_NAMESPACE,
                    HSB_BAD_PROPERTIES_CODE, null);

        doTest(reqFileName, expectedFault);
    }
    public void testBadBody09() throws Exception {
        String reqFileName = "Discovery04.xml";
        Fault expectedFault = new Fault(XmlaException.formatFaultCode(
                    CLIENT_FAULT_FC, 
                    HSB_BAD_RESTRICTION_LIST_CODE),
                    HSB_BAD_RESTRICTION_LIST_FAULT_FS,
                    FAULT_ACTOR,
                    MONDRIAN_NAMESPACE,
                    HSB_BAD_RESTRICTION_LIST_CODE, null);

        doTest(reqFileName, expectedFault);
    }
    public void testBadBody10() throws Exception {
        String reqFileName = "Discovery05.xml";
        Fault expectedFault = new Fault(XmlaException.formatFaultCode(
                    CLIENT_FAULT_FC, 
                    HSB_BAD_PROPERTIES_LIST_CODE),
                    HSB_BAD_PROPERTIES_LIST_FAULT_FS,
                    FAULT_ACTOR,
                    MONDRIAN_NAMESPACE,
                    HSB_BAD_PROPERTIES_LIST_CODE, null);

        doTest(reqFileName, expectedFault);
    }
    public void testBadBody11() throws Exception {
        String reqFileName = "Execute03.xml";
        Fault expectedFault = new Fault(XmlaException.formatFaultCode(
                    CLIENT_FAULT_FC, 
                    HSB_BAD_PROPERTIES_LIST_CODE),
                    HSB_BAD_PROPERTIES_LIST_FAULT_FS,
                    FAULT_ACTOR,
                    MONDRIAN_NAMESPACE,
                    HSB_BAD_PROPERTIES_LIST_CODE, null);

        doTest(reqFileName, expectedFault);
    }
    public void testBadBody12() throws Exception {
        String reqFileName = "Execute04.xml";
        Fault expectedFault = new Fault(XmlaException.formatFaultCode(
                    CLIENT_FAULT_FC, 
                    HSB_BAD_STATEMENT_CODE),
                    HSB_BAD_STATEMENT_FAULT_FS,
                    FAULT_ACTOR,
                    MONDRIAN_NAMESPACE,
                    HSB_BAD_STATEMENT_CODE, null);

        doTest(reqFileName, expectedFault);
    }
    public void testBadBody13() throws Exception {
        String reqFileName = "Execute05.xml";
        Fault expectedFault = new Fault(XmlaException.formatFaultCode(
                    CLIENT_FAULT_FC, 
                    HSB_DRILLDOWN_BAD_MAXROWS_CODE),
                    HSB_DRILLDOWN_BAD_MAXROWS_FAULT_FS,
                    FAULT_ACTOR,
                    MONDRIAN_NAMESPACE,
                    HSB_DRILLDOWN_BAD_MAXROWS_CODE, null);

        doTest(reqFileName, expectedFault);
    }
    public void testBadBody14() throws Exception {
        String reqFileName = "Execute06.xml";
        Fault expectedFault = new Fault(XmlaException.formatFaultCode(
                    CLIENT_FAULT_FC, 
                    HSB_DRILLDOWN_BAD_FIRST_ROWSET_CODE),
                    HSB_DRILLDOWN_BAD_FIRST_ROWSET_FAULT_FS,
                    FAULT_ACTOR,
                    MONDRIAN_NAMESPACE,
                    HSB_DRILLDOWN_BAD_FIRST_ROWSET_CODE, null);

        doTest(reqFileName, expectedFault);
    }
    public void testBadBody15() throws Exception {
        String reqFileName = "Execute07.xml";
        Fault expectedFault = new Fault(XmlaException.formatFaultCode(
                    CLIENT_FAULT_FC, 
                    HSB_DRILLDOWN_ERROR_CODE),
                    HSB_DRILLDOWN_ERROR_FAULT_FS,
                    FAULT_ACTOR,
                    MONDRIAN_NAMESPACE,
                    HSB_DRILLDOWN_ERROR_CODE, null);

        doTest(reqFileName, expectedFault);
    }





    /////////////////////////////////////////////////////////////////////////
    // helper
    /////////////////////////////////////////////////////////////////////////
    public void doTest(
            MockHttpServletRequest req,
            Fault expectedFault
            ) throws Exception {

        MockHttpServletResponse res = new MockHttpServletResponse();
        res.setCharacterEncoding("UTF-8");

        if (servlet == null) {
            makeServlet();
        }

        servlet.service(req, res);

        int statusCode = res.getStatusCode();
        if (statusCode == HttpServletResponse.SC_OK) {

            byte[] bytes = res.toByteArray();
            processResults(bytes, expectedFault);

        } else if (statusCode == HttpServletResponse.SC_CONTINUE) {
            // remove the Expect header from request and try again
if (DEBUG) {
System.out.println("Got CONTINUE");
}

            req.clearHeader(EXPECT);
            req.clearHeader(AUTHORIZATION);
            doAuthorization = false;

            servlet.service(req, res);

            statusCode = res.getStatusCode();
            if (statusCode == HttpServletResponse.SC_OK) {
                byte[] bytes = res.toByteArray();
                processResults(bytes, expectedFault);
            } else {
                fail("Bad status code: " +statusCode);
            }
        } else {
            fail("Bad status code: " +statusCode);

        }
    }
    public void doTest(
            String reqFileName,
            Fault expectedFault
            ) throws Exception {

        String requestText = fileToString(reqFileName);
if (DEBUG) {
System.out.println("reqFileName="+reqFileName);
}
        if (servlet == null) {
            makeServlet();
        }
        // do SOAP-XMLA
        byte[] bytes = XmlaSupport.processSoapXmla(requestText, servlet);
        processResults(bytes, expectedFault);
    }

    protected void processResults(byte[] results, Fault expectedFault) 
            throws Exception {

if (DEBUG) {
String response = new String(results);
System.out.println("response="+response);
}
        Node[] fnodes = XmlaSupport.extractFaultNodesFromSoap(results);
        if ((fnodes == null) || (fnodes.length == 0)) {
            if (expectedFault != null) {
                 // error
                fail("Failed to get SOAP Fault element in SOAP Body node");
            } 
        }
        if (expectedFault != null) {
            Fault fault = new Fault(fnodes);

if (DEBUG) {
System.out.println("fault="+fault);
System.out.println("expectedFault="+expectedFault);
}
            fault.checkSame(expectedFault);
        }
    }
}
