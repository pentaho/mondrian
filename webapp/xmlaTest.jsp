<%@ page import="mondrian.xmla.XmlaMediator,
                 mondrian.olap.Util,
                 mondrian.xom.XMLUtil,
                 java.io.PrintWriter,
                 java.io.StringWriter,
                 mondrian.xmla.XmlaTest,
                 java.util.HashMap,
                 java.util.Arrays"%>
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
    /**
     * XML-encodes a string.
     */
    private static String wrap(String s) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        XMLUtil.stringEncodeXML(s, pw);
        pw.flush();
        return sw.toString();
    }

    private static final HashMap requestMap = new XmlaTest("foo").getRequests();
%>
<html>
<head>
<title>Mondrian XML for Analysis Tester</title>
<style>
.{
font-family:"verdana";
}

.resulttable {
background-color:#AAAAAA;
}

.slicer {
background-color:#DDDDDD;
font-size:10pt;
}

.columnheading {
background-color:#DDDDDD;
font-size:10pt;
}

.rowheading {
background-color:#DDDDDD;
font-size:10pt;
}

.cell {
font-family:"courier";
background-color:#FFFFFF;
font-size:10pt;
text-align:right;
}

</style>
</head>
<body>

<%
    String[] requestKeys = (String[]) requestMap.keySet().toArray(new String[0]);
    Arrays.sort(requestKeys);
    String defaultRequest = (String) requestMap.get("testCubes");
    int whichRequest = 0;
    if (request.getParameter("whichrequest") != null) {
        whichRequest = Integer.valueOf(request.getParameter("whichrequest")).intValue();
    }
    if (whichRequest >= requestKeys.length) {
        whichRequest = requestKeys.length - 1;
    }
    defaultRequest = (String) requestMap.get(requestKeys[whichRequest]);
    String postURL = request.getParameter("postURL");
    if (postURL == null) {
        postURL = "xmla.jsp";
    }
%>

<form id='requestChooser' method='POST' action='xmlaTest.jsp'>
  <input type='hidden' name='postURL' value='<%= postURL %>'/>
  <table>
    <tr>
      <td>
        <select name="whichrequest"><%

    for (int i = 0; i < requestKeys.length; i++) {

      %>
      <option <%= whichRequest == i ? " selected" : "" %>
      value="<%= i%>"><%= i %>. <%= requestKeys[i].substring(4) %>
      </option>
      <%
    }
      %>

        </select>
      </td>
    </tr>

    <tr>
      <td>
        <input type="submit" value="show request">
      </td>
    </tr>
  </table>
</form>


<form id='form' method='POST' action='<%= postURL %>'>
  <input type='hidden' name='postURL' value='<%= postURL %>'/>
  <table>
    <tr>
      <td>Request</td>
      <td>
        <textarea rows='25' cols='120' name='SOAPRequest' id='SOAPRequest' style='font-size: 9pt'>
<%= wrap(defaultRequest) %>
        </textarea>
      </td>
    </tr>
    <tr>
      <td>Method</td>
      <td>
        <input type="radio" name="SOAPAction" value="urn:schemas-microsoft-com:xml-analysis:Discover" checked="true"/>Discover
        <input type="radio" name="SOAPAction" value="urn:schemas-microsoft-com:xml-analysis:Execute"/>Execute
      </td>
    </tr>
    <tr>
      <td>&nbsp;</td>
      <td><input type='submit' value='Run'/></td>
    </tr>
  </table>
</form>

<p>&nbsp;</p>

<form id='postForm' method='POST' action='xmlaTest.jsp'>
  <table border='1'>
    <tr>
      <td>Target URL:</td>
      <td><%= postURL %></td>
    </tr>
    <tr>
      <td>New target URL:</td>
      <td><input size='55' name='postURL' value='<%= postURL %>'/><br/>
          <input type='submit' value='Change'/>
      </td>
    </tr>
  </table>
</form>

</body>
</html>
