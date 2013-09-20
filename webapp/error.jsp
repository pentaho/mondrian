<%--
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002 - 2013 Pentaho Corporation..  All rights reserved.
--%>

<%@ page
  session="true"
  contentType="text/html; charset=ISO-8859-1"
%>
<%@ page import="java.io.PrintWriter"%>

<html>
<head>
  <title>JPivot had an error ...</title>
</head>
<body bgcolor=white>

  <h2>JPivot had an error ...</h2><p/>
<%
        Throwable e = (Throwable) request.getAttribute("javax.servlet.jsp.jspException");
      while (e != null) {
%>      <h2><%= e.toString() %></h2><pre>
<%
        e.printStackTrace(new PrintWriter(out));
%>
        </pre>

<%
        Throwable prev = e;
        e = e.getCause();
        if (e == prev)
          break;
      }
%>

</body>
</html>
