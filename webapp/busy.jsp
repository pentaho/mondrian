/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

<%@ page
  session="true"
  contentType="text/html; charset=ISO-8859-1"
%>
<%@ taglib prefix="c" uri="http://java.sun.com/jstl/core" %>

<html>
<head>
  <title>JPivot is busy ...</title>
  <meta http-equiv="refresh" content="1; URL=<c:out value="${requestSynchronizer.resultURI}"/>">
</head>
<body bgcolor=white>

  <h2>JPivot is busy ...</h2>

  Please wait until your results are computed. Click
  <a href="<c:out value="${requestSynchronizer.resultURI}"/>">here</a>
  if your browser does not support redirects.

</body>
</html>
