<%@ page language="java" %>
<%--
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// Julian Hyde, June 20, 2002
--%>
<%@ taglib uri="/WEB-INF/mdxtable.tld" prefix="mdx" %>
<html>
<head>
  <title>Mondrian Pivot Table</title>
</head>
<body>

<mdx:query name="pivotQuery" resultCache="true">
select
  {[Measures].[Unit Sales], [Measures].[Store Sales]} on columns,
  Order([Product].children, [Measures].[Store Sales], DESC) on rows
from Sales
where ([Time].[1997], [Gender].[M])
</mdx:query>

<h1>Mondrian Pivot Table</h1>

<mdx:transform query="pivotQuery" xsltURI="/WEB-INF/mdxpivot.xsl" xsltCache="false"/>

<p>[<a href="morph_pivot.jsp">Morph</a>]</p>

<p>Query:<blockquote><pre>
<mdx:transform query="pivotQuery" xsltURI="/WEB-INF/mdxquery.xsl" xsltCache="false"/>
</pre></blockquote></p>

</body>
</head>
