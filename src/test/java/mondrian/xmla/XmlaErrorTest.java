/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2006-2010 Pentaho and others
// All Rights Reserved.
*/
package mondrian.xmla;

import mondrian.olap.Util;
import mondrian.rolap.RolapConnectionProperties;
import mondrian.test.DiffRepository;
import mondrian.test.TestContext;
import mondrian.tui.*;
import mondrian.util.Base64;

import org.w3c.dom.*;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.*;
import java.util.Enumeration;
import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Test of the XMLA Fault generation - errors occur/are-detected in
 * in Mondrian XMLA and a SOAP Fault is returned.
 *
 * <p>There is a set of tests dealing with Authorization and HTTP Header
 * Expect and Continue dialog. These are normally done at the webserver
 * level and can be removed here if desired. (I wrote them before I
 * realized that Mondrian XMLA would not handle any Authorization issues
 * if it were in a webserver.)
 *
 * @author Richard M. Emberson
 */
@SuppressWarnings({"ThrowableInstanceNeverThrown"})
public class XmlaErrorTest extends XmlaBaseTestCase
    implements XmlaConstants
{
    static boolean doAuthorization = false;
    static String user = null;
    static String password = null;

    static class Callback implements XmlaRequestCallback {
        static String MY_SESSION_ID = "my_session_id";

        Callback() {
        }

        public void init(ServletConfig servletConfig) throws ServletException {
        }

        public boolean processHttpHeader(
            HttpServletRequest request,
            HttpServletResponse response,
            Map<String, Object> context) throws Exception
        {
            // look for authorization
            // Authorization: Basic ZWRnZTphYmNkMTIzNC4=
            // tjones:abcd1234$$.

if (DEBUG) {
System.out.println("doAuthorization=" + doAuthorization);
System.out.println("AUTH_TYPE=" + request.getAuthType());
}
            if (doAuthorization) {
                Enumeration values = request.getHeaders(AUTHORIZATION);
                if ((values == null) || (! values.hasMoreElements())) {
                    throw XmlaRequestCallback.Helper.authorizationException(
                        new Exception("Authorization: no header value"));
                }
                String authScheme = (String) values.nextElement();
if (DEBUG) {
System.out.println("authScheme=" + authScheme);
}
                if (! values.hasMoreElements()) {
                    throw XmlaRequestCallback.Helper.authorizationException(
                        new Exception("Authorization: too few header value"));
                }

                String encoded = (String) values.nextElement();
if (DEBUG) {
System.out.println("encoded=" + encoded);
}
                byte[] bytes = Base64.decode(encoded);
                String userPass = new String(bytes);
if (DEBUG) {
System.out.println("userPass=" + userPass);
}
                if (! authScheme.equals(HttpServletRequest.BASIC_AUTH)) {
                    throw XmlaRequestCallback.Helper.authorizationException(
                        new Exception(
                            "Authorization: bad schema: " + authScheme));
                }
                int index = userPass.indexOf(':');
                if (index == -1) {
                    throw XmlaRequestCallback.Helper.authorizationException(
                        new Exception(
                            "Authorization: badly formed userPass in encoding: "
                            + encoded));
                }
                String userid = userPass.substring(0, index);
                String password =
                    userPass.substring(index + 1, userPass.length());
if (DEBUG) {
System.out.println("userid=" + userid);
System.out.println("password=" + password);
}
                if (!Util.equals(userid, XmlaErrorTest.user)) {
                    throw XmlaRequestCallback.Helper.authorizationException(
                        new Exception(
                            "Authorization: bad userid: "
                            + userid
                            + " should be: "
                            + XmlaErrorTest.user));
                }
                if (!Util.equals(password, XmlaErrorTest.password)) {
                    throw XmlaRequestCallback.Helper.authorizationException(
                        new Exception(
                            "Authorization: bad password: "
                            + password
                            + " should be: "
                            + XmlaErrorTest.password));
                }
            }

            String expect = request.getHeader(EXPECT);
            if ((expect != null)
                && expect.equalsIgnoreCase(EXPECT_100_CONTINUE))
            {
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
            Map<String, Object> context) throws Exception
        {
            context.put(
                MY_SESSION_ID,
                getSessionId("XmlaExcelXPTest", Action.CREATE));
        }

        public String generateSessionId(Map<String, Object> context) {
            return (String) context.get(MY_SESSION_ID);
        }

        public void postAction(
            HttpServletRequest request,
            HttpServletResponse response,
            byte[][] responseSoapParts,
            Map<String, Object> context) throws Exception
        {
        }
    }

    static Element[] getChildElements(Node node) {
        List<Element> list = new ArrayList<Element>();

        NodeList nlist = node.getChildNodes();
        int len = nlist.getLength();
        for (int i = 0; i < len; i++) {
            Node child = nlist.item(i);
            if (child instanceof Element) {
                list.add((Element) child);
            }
        }

        return list.toArray(new Element[list.size()]);
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
        return (cd != null)
            ? cd.getData()
            : null;
    }

    private static class Fault {
        final String faultCode;
        final String faultString;
        final String faultActor;
        final String errorNS;
        final String errorCode;
        final String errorDesc;

        Fault(
            String faultCode,
            String faultString,
            String faultActor,
            String errorNS,
            String errorCode,
            String errorDesc)
        {
            this.faultCode = faultCode;
            this.faultString = faultString;
            this.faultActor = faultActor;
            this.errorNS = errorNS;
            this.errorCode = errorCode;
            this.errorDesc = errorDesc;
        }

        Fault(Node[] faultNodes) throws Exception {
            if (faultNodes.length < 3 || faultNodes.length > 4) {
                throw new Exception(
                    "SOAP Fault node has " + faultNodes.length + " children");
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
                    throw new Exception(
                        "SOAP Fault detail node has "
                        + faultNodes.length
                        + " children");
                }
                // error element
                node = faultNodes[0];
                errorNS = node.getNamespaceURI();
                faultNodes = getChildElements(node);
                if (faultNodes.length != 2) {
                    throw new Exception(
                        "SOAP Fault detail error node has "
                        + faultNodes.length
                        + " children");
                }
                // error code element
                node = faultNodes[0];
                errorCode = getNodeContent(node);

                // error desc element
                node = faultNodes[1];
                errorDesc = getNodeContent(node);
            } else {
                errorNS = errorCode = errorDesc = null;
            }
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
            return
            "faultCode=" + faultCode + ", faultString=" + faultString
            + ", faultActor=" + faultActor + ", errorNS=" + errorNS
            + ", errorCode=" + errorCode + ", errorDesc=" + errorDesc;
        }

        void checkSame(Fault expectedFault) throws Exception {
            if (!Util.equals(this.faultCode, expectedFault.faultCode)) {
                notSame("faultcode", expectedFault.faultCode, this.faultCode);
            }
            if (!Util.equals(this.faultString, expectedFault.faultString)) {
                notSame(
                    "faultstring", expectedFault.faultString, this.faultString);
            }
            if (!Util.equals(this.faultActor, expectedFault.faultActor)) {
                notSame(
                    "faultactor",
                    expectedFault.faultActor,
                    this.faultActor);
            }
            if (!Util.equals(this.errorNS, expectedFault.errorNS)) {
                throw new Exception(
                    "For error element namespace "
                    + " Expected "
                    + expectedFault.errorNS
                    + " but Got "
                    + this.errorNS);
            }
            if (!Util.equals(this.errorCode, expectedFault.errorCode)) {
                notSame("error.code", expectedFault.errorCode, this.errorCode);
            }
        }

        private void notSame(String elementName, String expected, String got)
            throws Exception
        {
            throw new Exception(
                "For element " + elementName
                + " expected [" + expected
                + "] but got [" + got + "]");
        }
    }

    private static PrintStream systemErr;

    public XmlaErrorTest() {
    }

    public XmlaErrorTest(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        super.setUp();

        // NOTE jvs 27-Feb-2007:  Since this test produces errors
        // intentionally, squelch the ones that SAX produces on stderr
        systemErr = System.err;
        System.setErr(new PrintStream(new ByteArrayOutputStream()));
    }

    protected void tearDown() throws Exception {
        // Restore stderr
        System.setErr(systemErr);
        super.tearDown();
    }

    protected DiffRepository getDiffRepos() {
        return DiffRepository.lookup(XmlaErrorTest.class);
    }

    protected Class<? extends XmlaRequestCallback> getServletCallbackClass() {
        return Callback.class;
    }

    protected Map<String, String> getCatalogNameUrls(TestContext testContext) {
        if (catalogNameUrls == null) {
            String connectString = testContext.getConnectString();
            Util.PropertyList connectProperties =
                        Util.parseConnectString(connectString);
            String catalog = connectProperties.get(
                RolapConnectionProperties.Catalog.name());
            catalogNameUrls = new TreeMap<String, String>();
            catalogNameUrls.put("FoodMart", catalog);
        }
        return catalogNameUrls;
    }

    /////////////////////////////////////////////////////////////////////////
    // tests
    /////////////////////////////////////////////////////////////////////////

    /////////////////////////////////////////////////////////////////////////
    // bad XML
    /////////////////////////////////////////////////////////////////////////

    // junk rather than xml
    public void testJunk() throws Exception {
        Fault expectedFault =
            new Fault(
                XmlaException.formatFaultCode(
                    CLIENT_FAULT_FC,
                    USM_DOM_PARSE_CODE),
                    USM_DOM_PARSE_FAULT_FS,
                    FAULT_ACTOR,
                    MONDRIAN_NAMESPACE,
                    USM_DOM_PARSE_CODE, null);

        doTest(expectedFault);
    }

    // bad soap envolope element tag
    public void testBadXml01() throws Exception {
        Fault expectedFault =
            new Fault(
                XmlaException.formatFaultCode(
                    CLIENT_FAULT_FC,
                    USM_DOM_PARSE_CODE),
                    USM_DOM_PARSE_FAULT_FS,
                    FAULT_ACTOR,
                    MONDRIAN_NAMESPACE,
                    USM_DOM_PARSE_CODE, null);

        doTest(expectedFault);
    }

    // bad soap namespace
    public void testBadXml02() throws Exception {
        Fault expectedFault =
            new Fault(
                XmlaException.formatFaultCode(
                    CLIENT_FAULT_FC,
                    USM_DOM_PARSE_CODE),
                    USM_DOM_PARSE_FAULT_FS,
                    FAULT_ACTOR,
                    MONDRIAN_NAMESPACE,
                    USM_DOM_PARSE_CODE, null);

        doTest(expectedFault);
    }

    /////////////////////////////////////////////////////////////////////////
    // bad action
    /////////////////////////////////////////////////////////////////////////

    public void testBadAction01() throws Exception {
        Fault expectedFault =
            new Fault(
                XmlaException.formatFaultCode(
                    CLIENT_FAULT_FC,
                    HSB_BAD_SOAP_BODY_CODE),
                    HSB_BAD_SOAP_BODY_FAULT_FS,
                    FAULT_ACTOR,
                    MONDRIAN_NAMESPACE,
                    HSB_BAD_SOAP_BODY_CODE, null);

        doTest(expectedFault);
    }

    public void testBadAction02() throws Exception {
        Fault expectedFault =
            new Fault(
                XmlaException.formatFaultCode(
                    CLIENT_FAULT_FC,
                    HSB_BAD_SOAP_BODY_CODE),
                    HSB_BAD_SOAP_BODY_FAULT_FS,
                    FAULT_ACTOR,
                    MONDRIAN_NAMESPACE,
                    HSB_BAD_SOAP_BODY_CODE, null);

        doTest(expectedFault);
    }

    public void testBadAction03() throws Exception {
        Fault expectedFault =
            new Fault(
                XmlaException.formatFaultCode(
                    CLIENT_FAULT_FC,
                    HSB_BAD_SOAP_BODY_CODE),
                    HSB_BAD_SOAP_BODY_FAULT_FS,
                    FAULT_ACTOR,
                    MONDRIAN_NAMESPACE,
                    HSB_BAD_SOAP_BODY_CODE, null);

        doTest(expectedFault);
    }

    /////////////////////////////////////////////////////////////////////////
    // bad soap structure
    /////////////////////////////////////////////////////////////////////////
    public void testBadSoap01() throws Exception {
        Fault expectedFault =
            new Fault(
                XmlaException.formatFaultCode(
                    CLIENT_FAULT_FC,
                    USM_DOM_PARSE_CODE),
                    USM_DOM_PARSE_FAULT_FS,
                    FAULT_ACTOR,
                    MONDRIAN_NAMESPACE,
                    USM_DOM_PARSE_CODE, null);

        doTest(expectedFault);
    }

    public void testBadSoap02() throws Exception {
        Fault expectedFault =
            new Fault(
                XmlaException.formatFaultCode(
                    CLIENT_FAULT_FC,
                    USM_DOM_PARSE_CODE),
                    USM_DOM_PARSE_FAULT_FS,
                    FAULT_ACTOR,
                    MONDRIAN_NAMESPACE,
                    USM_DOM_PARSE_CODE, null);

        doTest(expectedFault);
    }

    /////////////////////////////////////////////////////////////////////////
    // authorization
    /////////////////////////////////////////////////////////////////////////

    // no authorization field in header
    public void testAuth01() throws Exception {
        Fault expectedFault =
            new Fault(
                XmlaException.formatFaultCode(
                    CLIENT_FAULT_FC,
                    CHH_AUTHORIZATION_CODE),
                    CHH_AUTHORIZATION_FAULT_FS,
                    FAULT_ACTOR,
                    MONDRIAN_NAMESPACE,
                    CHH_AUTHORIZATION_CODE, null);
        doAuthorization = true;
        try {
            doTest(expectedFault);
        } finally {
            doAuthorization = false;
        }
    }

    // the user/password is not base64 encode and no ':' character
    public void testAuth02() throws Exception {
        Fault expectedFault =
            new Fault(
                XmlaException.formatFaultCode(
                    CLIENT_FAULT_FC,
                    CHH_AUTHORIZATION_CODE),
                    CHH_AUTHORIZATION_FAULT_FS,
                    FAULT_ACTOR,
                    MONDRIAN_NAMESPACE,
                    CHH_AUTHORIZATION_CODE, null);

        doAuthorization = true;

        String requestText = fileToString("request");
        byte[] reqBytes = requestText.getBytes();

        MockHttpServletRequest req = new MockHttpServletRequest(reqBytes);
        req.setMethod("POST");
        req.setContentType("text/xml");

        req.setAuthType(HttpServletRequest.BASIC_AUTH);
        req.setHeader(
            XmlaRequestCallback.AUTHORIZATION, HttpServletRequest.BASIC_AUTH);
        req.setHeader(XmlaRequestCallback.AUTHORIZATION, "FOOBAR");

        try {
            doTest(req, expectedFault);
        } finally {
            doAuthorization = false;
        }
    }

    // this should work
    public void testAuth03() throws Exception {
        Fault expectedFault = null;

        doAuthorization = true;

        String requestText = fileToString("request");
        byte[] reqBytes = requestText.getBytes();

        MockHttpServletRequest req = new MockHttpServletRequest(reqBytes);
        req.setMethod("POST");
        req.setContentType("text/xml");

        req.setAuthType(HttpServletRequest.BASIC_AUTH);
        req.setHeader(
            XmlaRequestCallback.AUTHORIZATION, HttpServletRequest.BASIC_AUTH);

        String user = "MY_USER";
        String password = "MY_PASSWORD";
        XmlaErrorTest.user = user;
        XmlaErrorTest.password = password;
        String credential = user + ':' + password;
        String encoded = Base64.encodeBytes(credential.getBytes());

        req.setHeader(XmlaRequestCallback.AUTHORIZATION, encoded);

        try {
            doTest(req, expectedFault);
            req.setHeader(
                XmlaRequestCallback.EXPECT,
                XmlaRequestCallback.EXPECT_100_CONTINUE);
if (DEBUG) {
System.out.println("DO IT AGAIN");
}
            doTest(req, expectedFault);
        } finally {
            XmlaErrorTest.doAuthorization = false;
            XmlaErrorTest.user = null;
            XmlaErrorTest.password = null;
        }
    }

    // fail: bad user name
    public void testAuth04() throws Exception {
        Fault expectedFault =
            new Fault(
                XmlaException.formatFaultCode(
                    CLIENT_FAULT_FC,
                    CHH_AUTHORIZATION_CODE),
                    CHH_AUTHORIZATION_FAULT_FS,
                    FAULT_ACTOR,
                    MONDRIAN_NAMESPACE,
                    CHH_AUTHORIZATION_CODE, null);

        doAuthorization = true;

        String requestText = fileToString("request");
        byte[] reqBytes = requestText.getBytes();

        MockHttpServletRequest req = new MockHttpServletRequest(reqBytes);
        req.setMethod("POST");
        req.setContentType("text/xml");

        req.setAuthType(HttpServletRequest.BASIC_AUTH);
        req.setHeader(
            XmlaRequestCallback.AUTHORIZATION, HttpServletRequest.BASIC_AUTH);

        String user = "MY_USER";
        String password = "MY_PASSWORD";
        XmlaErrorTest.user = user + "FOO";
        XmlaErrorTest.password = password;
        String credential = user + ':' + password;
        String encoded = Base64.encodeBytes(credential.getBytes());

        req.setHeader(XmlaRequestCallback.AUTHORIZATION, encoded);

        try {
            doTest(req, expectedFault);
        } finally {
            XmlaErrorTest.doAuthorization = false;
            XmlaErrorTest.user = null;
            XmlaErrorTest.password = null;
        }
    }

    // fail: bad password
    public void testAuth05() throws Exception {
        Fault expectedFault =
            new Fault(
                XmlaException.formatFaultCode(
                    CLIENT_FAULT_FC,
                    CHH_AUTHORIZATION_CODE),
                    CHH_AUTHORIZATION_FAULT_FS,
                    FAULT_ACTOR,
                    MONDRIAN_NAMESPACE,
                    CHH_AUTHORIZATION_CODE, null);

        doAuthorization = true;

        String requestText = fileToString("request");
        byte[] reqBytes = requestText.getBytes();

        MockHttpServletRequest req = new MockHttpServletRequest(reqBytes);
        req.setMethod("POST");
        req.setContentType("text/xml");

        req.setAuthType(HttpServletRequest.BASIC_AUTH);
        req.setHeader(
            XmlaRequestCallback.AUTHORIZATION, HttpServletRequest.BASIC_AUTH);

        String user = "MY_USER";
        String password = "MY_PASSWORD";
        XmlaErrorTest.user = user;
        XmlaErrorTest.password = password + "FOO";
        String credential = user + ':' + password;
        String encoded = Base64.encodeBytes(credential.getBytes());

        req.setHeader(XmlaRequestCallback.AUTHORIZATION, encoded);

        try {
            doTest(req, expectedFault);
        } finally {
            XmlaErrorTest.doAuthorization = false;
            XmlaErrorTest.user = null;
            XmlaErrorTest.password = null;
        }
    }

    // bad header
    public void testBadHeader01() throws Exception {
        // remember, errors in headers do not have detail sections
        Fault expectedFault =
            new Fault(
                XmlaException.formatFaultCode(
                    MUST_UNDERSTAND_FAULT_FC,
                    HSH_MUST_UNDERSTAND_CODE),
                    HSH_MUST_UNDERSTAND_FAULT_FS,
                    FAULT_ACTOR,
                    null, null, null);

        doTest(expectedFault);
    }

    // bad body
    public void testBadBody01() throws Exception {
        Fault expectedFault =
            new Fault(
                XmlaException.formatFaultCode(
                    CLIENT_FAULT_FC,
                    HSB_BAD_SOAP_BODY_CODE),
                    HSB_BAD_SOAP_BODY_FAULT_FS,
                    FAULT_ACTOR,
                    MONDRIAN_NAMESPACE,
                    HSB_BAD_SOAP_BODY_CODE, null);

        doTest(expectedFault);
    }

    public void testBadBody02() throws Exception {
        Fault expectedFault =
            new Fault(
                XmlaException.formatFaultCode(
                    CLIENT_FAULT_FC,
                    HSB_BAD_SOAP_BODY_CODE),
                    HSB_BAD_SOAP_BODY_FAULT_FS,
                    FAULT_ACTOR,
                    MONDRIAN_NAMESPACE,
                    HSB_BAD_SOAP_BODY_CODE, null);

        doTest(expectedFault);
    }

    public void testBadBody03() throws Exception {
        Fault expectedFault =
            new Fault(
                XmlaException.formatFaultCode(
                    CLIENT_FAULT_FC,
                    HSB_BAD_SOAP_BODY_CODE),
                    HSB_BAD_SOAP_BODY_FAULT_FS,
                    FAULT_ACTOR,
                    MONDRIAN_NAMESPACE,
                    HSB_BAD_SOAP_BODY_CODE, null);

        doTest(expectedFault);
    }

    public void testBadBody04() throws Exception {
        Fault expectedFault =
            new Fault(
                XmlaException.formatFaultCode(
                    CLIENT_FAULT_FC,
                    HSB_BAD_REQUEST_TYPE_CODE),
                    HSB_BAD_REQUEST_TYPE_FAULT_FS,
                    FAULT_ACTOR,
                    MONDRIAN_NAMESPACE,
                    HSB_BAD_REQUEST_TYPE_CODE, null);

        doTest(expectedFault);
    }

    public void testBadBody05() throws Exception {
        Fault expectedFault =
            new Fault(
                XmlaException.formatFaultCode(
                    CLIENT_FAULT_FC,
                    HSB_BAD_RESTRICTIONS_CODE),
                    HSB_BAD_RESTRICTIONS_FAULT_FS,
                    FAULT_ACTOR,
                    MONDRIAN_NAMESPACE,
                    HSB_BAD_RESTRICTIONS_CODE, null);

        doTest(expectedFault);
    }

    public void testBadBody06() throws Exception {
        Fault expectedFault =
            new Fault(
                XmlaException.formatFaultCode(
                    CLIENT_FAULT_FC,
                    HSB_BAD_PROPERTIES_CODE),
                    HSB_BAD_PROPERTIES_FAULT_FS,
                    FAULT_ACTOR,
                    MONDRIAN_NAMESPACE,
                    HSB_BAD_PROPERTIES_CODE, null);

        doTest(expectedFault);
    }

    public void testBadBody07() throws Exception {
        Fault expectedFault =
            new Fault(
                XmlaException.formatFaultCode(
                    CLIENT_FAULT_FC,
                    HSB_BAD_COMMAND_CODE),
                    HSB_BAD_COMMAND_FAULT_FS,
                    FAULT_ACTOR,
                    MONDRIAN_NAMESPACE,
                    HSB_BAD_COMMAND_CODE, null);

        doTest(expectedFault);
    }

    public void testBadBody08() throws Exception {
        Fault expectedFault =
            new Fault(
                XmlaException.formatFaultCode(
                    CLIENT_FAULT_FC,
                    HSB_BAD_PROPERTIES_CODE),
                    HSB_BAD_PROPERTIES_FAULT_FS,
                    FAULT_ACTOR,
                    MONDRIAN_NAMESPACE,
                    HSB_BAD_PROPERTIES_CODE, null);

        doTest(expectedFault);
    }

    public void testBadBody09() throws Exception {
        Fault expectedFault =
            new Fault(
                XmlaException.formatFaultCode(
                    CLIENT_FAULT_FC,
                    HSB_BAD_RESTRICTION_LIST_CODE),
                    HSB_BAD_RESTRICTION_LIST_FAULT_FS,
                    FAULT_ACTOR,
                    MONDRIAN_NAMESPACE,
                    HSB_BAD_RESTRICTION_LIST_CODE, null);

        doTest(expectedFault);
    }

    public void testBadBody10() throws Exception {
        Fault expectedFault =
            new Fault(
                XmlaException.formatFaultCode(
                    CLIENT_FAULT_FC,
                    HSB_BAD_PROPERTIES_LIST_CODE),
                    HSB_BAD_PROPERTIES_LIST_FAULT_FS,
                    FAULT_ACTOR,
                    MONDRIAN_NAMESPACE,
                    HSB_BAD_PROPERTIES_LIST_CODE, null);

        doTest(expectedFault);
    }

    public void testBadBody11() throws Exception {
        Fault expectedFault =
            new Fault(
                XmlaException.formatFaultCode(
                    CLIENT_FAULT_FC,
                    HSB_BAD_PROPERTIES_LIST_CODE),
                    HSB_BAD_PROPERTIES_LIST_FAULT_FS,
                    FAULT_ACTOR,
                    MONDRIAN_NAMESPACE,
                    HSB_BAD_PROPERTIES_LIST_CODE, null);

        doTest(expectedFault);
    }

    public void testBadBody12() throws Exception {
        Fault expectedFault =
            new Fault(
                XmlaException.formatFaultCode(
                    CLIENT_FAULT_FC,
                    HSB_BAD_STATEMENT_CODE),
                    HSB_BAD_STATEMENT_FAULT_FS,
                    FAULT_ACTOR,
                    MONDRIAN_NAMESPACE,
                    HSB_BAD_STATEMENT_CODE, null);

        doTest(expectedFault);
    }

    public void testBadBody13() throws Exception {
        Fault expectedFault =
            new Fault(
                XmlaException.formatFaultCode(
                    CLIENT_FAULT_FC,
                    HSB_DRILL_THROUGH_FORMAT_CODE),
                HSB_DRILL_THROUGH_FORMAT_FAULT_FS,
                FAULT_ACTOR,
                MONDRIAN_NAMESPACE,
                HSB_DRILL_THROUGH_FORMAT_CODE,
                null);

        doTest(expectedFault);
    }

    public void testBadBody14() throws Exception {
        Fault expectedFault =
            new Fault(
                XmlaException.formatFaultCode(
                    CLIENT_FAULT_FC,
                    HSB_DRILL_THROUGH_FORMAT_CODE),
                HSB_DRILL_THROUGH_FORMAT_FAULT_FS,
                FAULT_ACTOR,
                MONDRIAN_NAMESPACE,
                HSB_DRILL_THROUGH_FORMAT_CODE,
                null);

        doTest(expectedFault);
    }

    public void testBadBody15() throws Exception {
        Fault expectedFault =
            new Fault(
                XmlaException.formatFaultCode(
                    CLIENT_FAULT_FC,
                    HSB_DRILL_THROUGH_FORMAT_CODE),
                HSB_DRILL_THROUGH_FORMAT_FAULT_FS,
                FAULT_ACTOR,
                MONDRIAN_NAMESPACE,
                HSB_DRILL_THROUGH_FORMAT_CODE,
                null);

        doTest(expectedFault);
    }


    /////////////////////////////////////////////////////////////////////////
    // helper
    /////////////////////////////////////////////////////////////////////////

    protected void doTest(
        MockHttpServletRequest req,
        Fault expectedFault) throws Exception
    {
        MockHttpServletResponse res = new MockHttpServletResponse();
        res.setCharacterEncoding("UTF-8");

        Servlet servlet = getServlet(getTestContext());
        servlet.service(req, res);

        int statusCode = res.getStatusCode();
        if (statusCode == HttpServletResponse.SC_OK) {
            byte[] bytes = res.toByteArray();
            processResults(bytes, expectedFault);

        } else if (statusCode == HttpServletResponse.SC_UNAUTHORIZED) {
            byte[] bytes = res.toByteArray();
            processResults(bytes, expectedFault);

        } else if (statusCode == HttpServletResponse.SC_CONTINUE) {
            // remove the Expect header from request and try again
if (DEBUG) {
System.out.println("Got CONTINUE");
}

            req.clearHeader(XmlaRequestCallback.EXPECT);
            req.clearHeader(XmlaRequestCallback.AUTHORIZATION);
            doAuthorization = false;

            servlet.service(req, res);

            statusCode = res.getStatusCode();
            if (statusCode == HttpServletResponse.SC_OK) {
                byte[] bytes = res.toByteArray();
                processResults(bytes, expectedFault);
            } else {
                fail("Bad status code: " + statusCode);
            }
        } else {
            fail("Bad status code: " + statusCode);
        }
    }

    protected void doTest(
        Fault expectedFault) throws Exception
    {
        String requestText = fileToString("request");
        Servlet servlet = getServlet(getTestContext());
        // do SOAP-XMLA
        byte[] bytes = XmlaSupport.processSoapXmla(requestText, servlet);
        processResults(bytes, expectedFault);
    }

    protected void processResults(byte[] results, Fault expectedFault)
        throws Exception
    {
if (DEBUG) {
String response = new String(results);
System.out.println("response=" + response);
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
System.out.println("fault=" + fault);
System.out.println("expectedFault=" + expectedFault);
}
            fault.checkSame(expectedFault);
        }
    }

    protected String getSessionId(Action action) {
        return getSessionId("XmlaExcelXPTest", action);
    }
}

// End XmlaErrorTest.java
