/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2011-2011 Pentaho and others
// All Rights Reserved.
*/
package mondrian.xmla.impl;

import mondrian.xmla.*;

import org.w3c.dom.Element;

import java.util.Map;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * This is an abstract implementation of {@link XmlaRequestCallback}
 * specialized in authenticating the requests coming in. Subclasses are
 * only required to implement {@link #authenticate(String, String, String)}.
 *
 * <p>Once implemented, you only need to register your class using the XMLA
 * servlet config, within your web.xml descriptor.
 *
 * @author LBoudreau
 */
public abstract class AuthenticatingXmlaRequestCallback
    implements XmlaRequestCallback
{
    public String generateSessionId(Map<String, Object> context) {
        // We don't want to override the session ID generation algorithm.
        return null;
    }

    public void init(ServletConfig servletConfig) throws ServletException {
        // Nothing to initialize here. Subclasses can override
        // this if they wish.
    }

    public void postAction(
        HttpServletRequest request,
        HttpServletResponse response,
        byte[][] responseSoapParts,
        Map<String, Object> context)
        throws Exception
    {
        return;
    }

    public void preAction(
        HttpServletRequest request,
        Element[] requestSoapParts,
        Map<String, Object> context)
        throws Exception
    {
        /*
         * This is where the magic happens. At this stage, we have
         * the username/password known. We will delegate the authentication
         * process down to the subclass.
         */
        final String roleNames =
            authenticate(
                (String) context.get(XmlaConstants.CONTEXT_XMLA_USERNAME),
                (String) context.get(XmlaConstants.CONTEXT_XMLA_PASSWORD),
                (String) context.get(XmlaConstants.CONTEXT_XMLA_SESSION_ID));
        context.put(
            XmlaConstants.CONTEXT_ROLE_NAME,
            roleNames);
    }

    /**
     * This function is expected to do two things.
     * <ul>
     * <li>Validate the credentials.</li>
     * <li>Return a comma separated list of role names associated to
     * these credentials.</li>
     * </ul>
     * <p>Should there be any problems with the credentials, subclasses
     * can invoke {@link #throwAuthenticationException(String)} to throw
     * an authentication exception back to the client.
     * @param username Username used for authentication, as specified
     * in the SOAP security header. Might be null.
     * @param password Password used for authentication, as specified
     * in the SOAP security header. Might be null.
     * @param sessionID A unique identifier for this client session.
     * Session IDs should remain the same between different queries from
     * a same client, although some clients do not implement the XMLA
     * Session header properly, resulting in a new session ID for each
     * request.
     * @return A comma separated list of roles associated to this user,
     * or <code>null</code> for root access.
     */
    public abstract String authenticate(
        String username,
        String password,
        String sessionID);

    /**
     * Helper method to create and throw an authentication exception.
     * @param reason A textual explanation of why the credentials are
     * rejected.
     */
    protected void throwAuthenticationException(String reason) {
        throw new XmlaException(
            XmlaConstants.CLIENT_FAULT_FC,
            XmlaConstants.CHH_AUTHORIZATION_CODE,
            XmlaConstants.CHH_AUTHORIZATION_FAULT_FS,
            new Exception(
                "There was a problem with the credentials: "
                + reason));
    }

    public boolean processHttpHeader(
        HttpServletRequest request,
        HttpServletResponse response,
        Map<String, Object> context)
        throws Exception
    {
        // We do not perform any special header treatment.
        return true;
    }
}
// End AuthenticatingXmlaRequestCallback.java
