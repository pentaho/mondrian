/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

<%@ page session="true" contentType="text/html; charset=ISO-8859-1" %>
<%@ taglib uri="http://www.tonbeller.com/jpivot" prefix="jp" %>
<%@ taglib prefix="c" uri="http://java.sun.com/jstl/core" %>

<%-- uses a dataSource --%>
<%-- jp:mondrianQuery id="query01" dataSource="jdbc/MondrianFoodmart" catalogUri="/WEB-INF/demo/FoodMart.xml" --%>

<%-- uses mysql --%>
<%-- jp:mondrianQuery id="query01" jdbcDriver="com.mysql.jdbc.Driver" jdbcUrl="jdbc:mysql://localhost/foodmart" catalogUri="/WEB-INF/queries/FoodMart.xml"--%>

<%-- uses a role defined in FoodMart.xml --%>
<%-- jp:mondrianQuery role="California manager" id="query01" jdbcDriver="sun.jdbc.odbc.JdbcOdbcDriver" jdbcUrl="jdbc:odbc:MondrianFoodMart" catalogUri="/WEB-INF/queries/FoodMart.xml" --%>

<jp:mondrianQuery id="query01" jdbcDriver="sun.jdbc.odbc.JdbcOdbcDriver" jdbcUrl="jdbc:odbc:MondrianFoodMart" catalogUri="/WEB-INF/queries/FoodMart.xml" role="California manager">
select
  {[Measures].[Unit Sales], [Measures].[Store Cost], [Measures].[Store Sales]} on columns,
  {([Marital Status].[All Marital Status], [Customers], [Product].[All Products])} on rows
from Sales
where ([Time].[1997])
</jp:mondrianQuery>

<c:set var="title01" scope="session">Test query uses Mondrian OLAP, using role 'California manager'</c:set>
