<%@ page language="java" %>
<%--
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2002-2005 Julian Hyde and others.
// All Rights Reserved.
//
// Andreas Voss, 27 March, 2002
--%>
<%@ taglib uri="/WEB-INF/mdxtable.tld" prefix="mdx" %>
<html>
<head>
  <title>Mondrian MDX Table Taglib Demo</title>
</head>
<body>

<a href=".">back to index</a><p/>


<mdx:query name="query1" resultCache="true">
select
  {[Measures].[Unit Sales], [Measures].[Store Cost], [Measures].[Store Sales]} on columns,
  CrossJoin(
    { [Promotion Media].[All Media].[Radio],
      [Promotion Media].[All Media].[TV],
      [Promotion Media].[All Media].[Sunday Paper],
      [Promotion Media].[All Media].[Street Handout] },
    [Product].[All Products].[Drink].children) on rows
from Sales
where ([Time].[1997])
</mdx:query>

<mdx:query name="query2" resultCache="true">
select
  [Product].[All Products].[Drink].children on rows,
  CrossJoin(
    {[Measures].[Unit Sales], [Measures].[Store Sales]},
    { [Promotion Media].[All Media].[Radio],
      [Promotion Media].[All Media].[TV],
      [Promotion Media].[All Media].[Sunday Paper],
      [Promotion Media].[All Media].[Street Handout] }
    ) on columns
from Sales
where ([Time].[1997])
</mdx:query>

<mdx:query name="query3" resultCache="true">
select
  {[Measures].[Unit Sales], [Measures].[Store Sales]} on columns,
  Order([Product].[Product Department].members, [Measures].[Store Sales], DESC) on rows
from Sales
</mdx:query>

<mdx:query name="query4" resultCache="true">
select
  [Product].[All Products].[Drink].children on columns
from Sales
where ([Measures].[Unit Sales], [Promotion Media].[All Media].[Street Handout], [Time].[1997])
</mdx:query>

<mdx:query name="query5" resultCache="true">
select
  NON EMPTY CrossJoin([Product].[All Products].[Drink].children, [Customers].[All Customers].[USA].[WA].Children) on rows,
  CrossJoin(
    {[Measures].[Unit Sales], [Measures].[Store Sales]},
    { [Promotion Media].[All Media].[Radio],
      [Promotion Media].[All Media].[TV],
      [Promotion Media].[All Media].[Sunday Paper],
      [Promotion Media].[All Media].[Street Handout] }
    ) on columns
from Sales
where ([Time].[1997])
</mdx:query>

<mdx:query name="query6" resultCache="true">
select from Sales
where ([Measures].[Store Sales], [Time].[1997], [Promotion Media].[All Media].[TV])
</mdx:query>



<h1>Mondrian Taglib Examples</h1>

<h3>CrossJoin Example 1</h3>

The current slicer is <strong><mdx:transform query="query1" xsltURI="/WEB-INF/mdxslicer.xsl" xsltCache="true"/></strong>.
<p>
<mdx:transform query="query1" xsltURI="/WEB-INF/mdxtable.xsl" xsltCache="false"/>


<h3>CrossJoin Example 2</h3>

The same thing the other way round. The current slicer is <strong><mdx:transform query="query2" xsltURI="/WEB-INF/mdxslicer.xsl" xsltCache="true"/></strong>.
<p>
<mdx:transform query="query2" xsltURI="/WEB-INF/mdxtable.xsl" xsltCache="false"/>


<h3>Simple Table</h3>
<mdx:transform query="query3" xsltURI="/WEB-INF/mdxtable.xsl" xsltCache="false"/>
<p>

<h3>1-dim Table</h3>
A One dimensional table. Slicer is
<strong>"<mdx:transform query="query4" xsltURI="/WEB-INF/mdxslicer.xsl" xsltCache="false"/>"</strong>

<p>
<mdx:transform query="query4" xsltURI="/WEB-INF/mdxtable.xsl" xsltCache="false"/>


<h3>CrossJoin on both axes</h3>
Here is the slicer: <strong>"<mdx:transform query="query5" xsltURI="/WEB-INF/mdxslicer.xsl" xsltCache="false"/>"</strong>
<p>
<mdx:transform query="query5" xsltURI="/WEB-INF/mdxtable.xsl" xsltCache="false"/>

<h3>0-dim example</h3>
0-dim "tables" may be useful, if you want to display calculated numbers in a form or
text. For example, the Store Sales were <b>
<mdx:transform query="query6" xsltURI="/WEB-INF/mdxvalue.xsl" xsltCache="false"/></b>
in 1997 for TV promoted products.
<p>

<a href=".">back to index</a><p/>

</body>
</head>
