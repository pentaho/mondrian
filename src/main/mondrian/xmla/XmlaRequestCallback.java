/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2005-2007 Pentaho
// All Rights Reserved.
*/
package mondrian.xmla;

import org.w3c.dom.Element;

import java.util.Map;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


/**
 * Extract data from HTTP request, SOAP header for following XML/A request.<p/>
 *
 * Fill context binding with whatever data you want, then use them in
 * {@link XmlaServlet#handleSoapHeader} and {@link XmlaServlet#handleSoapBody}.
 *
 * @author Gang Chen
 */
public interface XmlaRequestCallback {
    String AUTHORIZATION = "Authorization";
    String EXPECT = "Expect";
    String EXPECT_100_CONTINUE = "100-continue";

    public class Helper {
        public static XmlaException authorizationException(Exception ex) {
            return new XmlaException(
                XmlaConstants.CLIENT_FAULT_FC,
                XmlaConstants.CHH_AUTHORIZATION_CODE,
                XmlaConstants.CHH_AUTHORIZATION_FAULT_FS,
                ex);
        }

        /*
    HTTP/1.1 100 Continue
    Server: Microsoft-IIS/5.0
    Date: Tue, 21 Feb 2006 21:07:57 GMT
    X-Powered-By: ASP.NET
        */
        public static void generatedExpectResponse(
            HttpServletRequest request,
            HttpServletResponse response,
            Map<String, Object> context) throws Exception
        {
            response.reset();
            response.setStatus(HttpServletResponse.SC_CONTINUE);
        }
    }

    void init(ServletConfig servletConfig) throws ServletException;

    /**
     * Process the request header items. Specifically if present the
     * Authorization and Expect headers. If the Authorization header is
     * present, then the callback can validate the user/password. If
     * authentication fails, the callback should throw an XmlaException
     * with the correct XmlaConstants values. The XmlaRequestCallback.Helper
     * class contains the authorizationException method that can be used
     * by a callback to generate the XmlaException with the correct values.
     * If the Expect header is set with "100-continue", then it is
     * upto the callback to create the appropriate response and return false.
     * In this case, the XmlaServlet stops processing and returns the
     * response to the client application. To facilitate the generation of
     * the response, the XmlaRequestCallback.Helper has the method
     * generatedExpectResponse that can be called by the callback.
     * <p>
     * Note that it is upto the XMLA client to determine whether or not
     * there is an Expect header entry (ADOMD.NET seems to like to do this).
     *
     * @return true if XmlaServlet handling is to continue and false if
     *         there was an Expect header "100-continue".
     */
    boolean processHttpHeader(
        HttpServletRequest request,
        HttpServletResponse response,
        Map<String, Object> context) throws Exception;

    /**
     * This is called after the headers have been process but before the
     * body (DISCOVER/EXECUTE) has been processed.
     *
     */
    void preAction(
        HttpServletRequest request,
        Element[] requestSoapParts,
        Map<String, Object> context) throws Exception;

    /**
     * The Callback is requested to generate a sequence id string. This
     * sequence id was requested by the XMLA client and will be used
     * for all subsequent communications in the Soap Header block.
     *
     * Implementation can return <code>null</code> if they do not want
     * to generate a custom session ID, in which case, the default algorithm
     * to generate session IDs will be used.
     * @param context The context of this query.
     * @return An arbitrary session id to use, or <code>null</code>.
     */
    String generateSessionId(Map<String, Object> context);

    /**
     * This is called after all Mondrian processing (DISCOVER/EXECUTE) has
     * occurred.
     *
     */
    void postAction(
        HttpServletRequest request,
        HttpServletResponse response,
        byte[][] responseSoapParts,
        Map<String, Object> context) throws Exception;
}

// End XmlaRequestCallback.java
