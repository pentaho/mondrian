<%@ page import="mondrian.xmla.XmlaMediator,
                 mondrian.xmla.XmlaServlet,
                 java.io.OutputStream"%>
<%@ page language="java" %>
<%--
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2003-2005 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// Julian Hyde, 3 June, 2003
--%>
<%!
    final XmlaMediator mediator = new XmlaMediator();
%>
<%
    final ServletContext servletContext = config.getServletContext();
    final RequestDispatcher dispatcher = servletContext.getNamedDispatcher("MondrianXmlaServlet");
    dispatcher.forward(request, response);
%>
