<%@ page import="mondrian.xmla.test.XmlaTestServletRequestWrapper"
         language="java"
         contentType="text/xml" %><%
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2003-2005 Julian Hyde, Sherman Wood
// Copyright (C) 2005-2006 Pentaho
// All Rights Reserved.
//
// Julian Hyde, 3 June, 2003

    final ServletRequest requestWrapper = new XmlaTestServletRequestWrapper(request);
    final ServletContext servletContext = config.getServletContext();
    final RequestDispatcher dispatcher = servletContext.getNamedDispatcher("MondrianXmlaServlet");
    dispatcher.forward(requestWrapper, response);
%>