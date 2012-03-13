<%@ page language="java" %>
<%--
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2002-2005 Julian Hyde and others
// All Rights Reserved.
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
