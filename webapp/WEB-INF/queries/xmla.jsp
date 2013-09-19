/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

<%@ page session="true" pageEncoding="UTF-8" contentType="text/html; charset=ISO-8859-1" %>
<%@ taglib uri="http://www.tonbeller.com/jpivot" prefix="jp" %>
<%@ taglib prefix="c" uri="http://java.sun.com/jstl/core" %>


<!-- 
    * Change uri attribute to your deployment of this webapp.
    * The dataSource attribute is necessary for Mondrian's XMLA.
-->
<jp:xmlaQuery id="query01"
    uri="http://localhost:8080/mondrian/xmla"
    dataSource="Provider=Mondrian;DataSource=MondrianFoodMart;"
  	catalog="FoodMart">
select
  {[Measures].[Unit Sales], [Measures].[Store Cost], [Measures].[Store Sales]} on columns,
  {([Promotion Media].[All Media], [Product].[All Products])} ON rows
from Sales
where ([Time].[1997])
</jp:xmlaQuery>

<c:set var="title01" scope="session">Accessing Mondrian By XMLA</c:set>
