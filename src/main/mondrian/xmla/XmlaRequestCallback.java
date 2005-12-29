/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2005-2005 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.xmla;

import java.util.Map;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;

import org.w3c.dom.Element;


/**
 * Extract data from HTTP request, SOAP header for following XML/A request.<p/>
 *
 * Fill context binding with whatever data you want, then use them in
 * {@link XmlaServlet#handleSoapHeader} and {@link XmlaServlet#handleSoapBody}.
 *
 * @author Gang Chen
 * @version $Id$
 */
public interface XmlaRequestCallback {

    public void init(ServletConfig servletConfig) throws ServletException;

    public void invoke(Map context,
                       HttpServletRequest request,
                       Element soapHeader,
                       Element soapBody) throws Exception;
}

// End XmlaRequestCallback.java
