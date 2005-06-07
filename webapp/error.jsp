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
