<%@ page import="mondrian.xmla.test.XmlaTestServletRequestWrapper"
         language="java"
         contentType="text/xml" %><%
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// (C) Copyright 2003-2006 Julian Hyde, Sherman Wood
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// Julian Hyde, 3 June, 2003

    final ServletRequest requestWrapper = new XmlaTestServletRequestWrapper(request);
    final ServletContext servletContext = config.getServletContext();
    final RequestDispatcher dispatcher = servletContext.getNamedDispatcher("MondrianXmlaServlet");
    dispatcher.forward(requestWrapper, response);
%>