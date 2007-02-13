<%@ page import="mondrian.olap.Util,
		 org.w3c.dom.Element,
                 org.eigenbase.xom.XMLUtil,
                 java.io.PrintWriter,
                 java.io.StringWriter,
                 mondrian.xmla.test.XmlaTestContext,
                 mondrian.xmla.XmlaUtil,
                 java.util.Map,
                 java.util.Arrays,
                 org.apache.log4j.Logger"%>
<%@ page language="java" %>
<%--
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2003-2006 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// Julian Hyde, 3 June, 2003
--%>
<%!

    private static final Logger LOGGER = Logger.getLogger("mondrian.jsp.xmlaTest");

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

<a href=".">back to index</a><p/>

<%
    
    XmlaTestContext context = new XmlaTestContext(application);
    
    Object[] requestResponses = context.defaultRequestResponsePairs();

    int whichRequest = 0;
    if (request.getParameter("whichrequest") != null) {
        whichRequest = Integer.valueOf(request.getParameter("whichrequest")).intValue();
    }
    if (whichRequest >= requestResponses.length) {
        whichRequest = requestResponses.length - 1;
    }
    Object[] defaultRequestResponse = (Object[]) requestResponses[whichRequest];
    Element requestElem = (Element) defaultRequestResponse[1];
    String defaultRequest = XmlaUtil.element2Text(requestElem);
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

    for (int i = 0; i < requestResponses.length; i++) {
	  Object[] thisRequestResponse = (Object[]) requestResponses[i];
	  String fileName = (String) thisRequestResponse[0];
      %>
      <option <%= whichRequest == i ? " selected" : "" %>
      value="<%= i%>"><%= i %>. <%= fileName.substring(0,fileName.length() - 4) %>
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

<a href=".">back to index</a><p/>

</body>
</html>
