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
