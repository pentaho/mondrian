<%@page contentType="text/html"%>
<%--
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 6 August, 2001
--%>
<%
	String[] queries = new String[] {
		// #0
		"select {[Measures].[Unit Sales]} on columns\r\n" +
		" from Sales",

		// mdx sample #1
		"select\r\n" +
		"	 {[Measures].[Unit Sales]} on columns,\r\n" +
		"	 order(except([Promotion Media].[Media Type].members,{[Promotion Media].[Media Type].[No Media]}),[Measures].[Unit Sales],DESC) on rows\r\n" +
		"from Sales",

		// mdx sample #2
		"select\r\n" +
		"	 { [Measures].[Units Shipped], [Measures].[Units Ordered] } on columns,\r\n" +
		"	 NON EMPTY [Store].[Store Name].members on rows\r\n" +
		"from Warehouse",

		// mdx sample #3
		"with member [Measures].[Store Sales Last Period] as '([Measures].[Store Sales], Time.PrevMember)'\r\n" +
		"select\r\n" +
		"	 {[Measures].[Store Sales Last Period]} on columns,\r\n" +
		"	 {TopCount([Product].[Product Department].members,5, [Measures].[Store Sales Last Period])} on rows\r\n" +
		"from Sales\r\n" +
		"where ([Time].[1998])",

		// mdx sample #4
		"with member [Measures].[Total Store Sales] as 'Sum(YTD(),[Measures].[Store Sales])'\r\n" +
		"select\r\n" +
		"	 {[Measures].[Total Store Sales]} on columns,\r\n" +
		"	 {TopCount([Product].[Product Department].members,5, [Measures].[Total Store Sales])} on rows\r\n" +
		"from Sales\r\n" +
		"where ([Time].[1997].[Q2].[4])",

		// mdx sample #5
		"with member [Measures].[Store Profit Rate] as '([Measures].[Store Sales]-[Measures].[Store Cost])/[Measures].[Store Cost]', format = '#.00%'\r\n" +
		"select\r\n" +
		"	 {[Measures].[Store Cost],[Measures].[Store Sales],[Measures].[Store Profit Rate]} on columns,\r\n" +
		"	 Order([Product].[Product Department].members, [Measures].[Store Profit Rate], BDESC) on rows\r\n" +
		"from Sales\r\n" +
		"where ([Time].[1997])",

		// mdx sample #6
		"with\r\n" +
		"	member [Product].[All Products].[Drink].[Percent of Alcoholic Drinks] as '[Product].[All Products].[Drink].[Alcoholic Beverages]/[Product].[All Products].[Drink]', format = '#.00%'\r\n" +
		"select\r\n" +
		"	{ [Product].[All Products].[Drink].[Percent of Alcoholic Drinks] } on columns,\r\n" +
		"	order([Customers].[All Customers].[USA].[WA].Children, [Product].[All Products].[Drink].[Percent of Alcoholic Drinks],BDESC ) on rows\r\n" +
		"from Sales\r\n" +
		"where ( [Measures].[Unit Sales] )",

		// mdx sample #7
		"with member [Measures].[Accumulated Sales] as 'Sum(YTD(),[Measures].[Store Sales])'\r\n" +
		"select\r\n" +
		"	 {[Measures].[Store Sales],[Measures].[Accumulated Sales]} on columns,\r\n" +
		"	 {Descendants([Time].[1997],[Time].[Month])} on rows\r\n" +
		"from Sales",

		// #8
		"select\r\n" +
		" {[Measures].[Unit Sales], [Measures].[Ever]} on columns,\r\n" +
		" [Gender].members on rows\r\n" +
		"from Sales",

		// #9
		"with\r\n" +
		"  member [Product].[Non dairy] as '[Product].[All Products] - [Product].[Food].[Dairy]'\r\n" +
		"  member [Measures].[Dairy ever] as 'sum([Time].members, ([Measures].[Unit Sales],[Product].[Food].[Dairy]))'\r\n" +
		"  set [Customers who never bought dairy] as 'filter([Customers].members, [Measures].[Dairy ever] = 0)'\r\n" +
		"select\r\n" +
		" {[Measures].[Unit Sales], [Measures].[Dairy ever]}  on columns,\r\n" +
		"  [Customers who never bought dairy] on rows\r\n" +
		"from Sales\r\n",

		// #10
		"select {[Has bought dairy].members} on columns,\r\n" +
		" {[Customers].[USA]} on rows\r\n" +
		"from Sales\r\n" +
		"where ([Measures].[Unit Sales])",

		// #11
		"WITH\r\n" +
		"   MEMBER [Measures].[StoreType] AS \r\n" +
		"   '[Store].CurrentMember.Properties(\"Store Type\")',\r\n" +
		"   SOLVE_ORDER = 2\r\n" +
		"   MEMBER [Measures].[ProfitPct] AS \r\n" +
		"   'Val((Measures.[Store Sales] - Measures.[Store Cost]) / Measures.[Store Sales])',\r\n" +
		"   SOLVE_ORDER = 1, FORMAT_STRING = 'Percent'\r\n" +
		"SELECT\r\n" +
		"   { [Store].[Store Name].Members} ON COLUMNS,\r\n" +
		"   { [Measures].[Store Sales], [Measures].[Store Cost], [Measures].[StoreType],\r\n" +
		"   [Measures].[ProfitPct] } ON ROWS\r\n" +
		"FROM Sales",

		// #12
		"WITH\r\n" +
		"   MEMBER [Product].[Beer and Wine].[BigSeller] AS\r\n" +
		"  'IIf([Product].[Beer and Wine] > 100, \"Yes\",\"No\")'\r\n" +
		"SELECT\r\n" +
		"   {[Product].[BigSeller]} ON COLUMNS,\r\n" +
		"   {Store.[Store Name].Members} ON ROWS\r\n" +
		"FROM Sales",

		// #13
		"WITH\r\n" +
		"   MEMBER [Measures].[ProfitPct] AS \r\n" +
		"   'Val((Measures.[Store Sales] - Measures.[Store Cost]) / Measures.[Store Sales])',\r\n" +
		"   SOLVE_ORDER = 1, FORMAT_STRING = 'Percent'\r\n" +
		"   MEMBER [Measures].[ProfitValue] AS \r\n" +
		"   '[Measures].[Store Sales] * [Measures].[ProfitPct]',\r\n" +
		"   SOLVE_ORDER = 2, FORMAT_STRING = 'Currency'\r\n" +
		"SELECT\r\n" +
		"   { [Store].[Store Name].Members} ON COLUMNS,\r\n" +
		"   { [Measures].[Store Sales], [Measures].[Store Cost], [Measures].[ProfitValue],\r\n" +
		"   [Measures].[ProfitPct] } ON ROWS\r\n" +
		"FROM Sales",

		// #14: cyclical calculated members
		"WITH\r\n" +
		"   MEMBER [Product].[X] AS '[Product].[Y]'\r\n" +
		"   MEMBER [Product].[Y] '[Product].[X]'\r\n" +
		"SELECT\r\n" +
		"   {[Product].[X]} ON COLUMNS,\r\n" +
		"   {Store.[Store Name].Members} ON ROWS\r\n" +
		"FROM Sales",

		// #15
		"WITH MEMBER MEASURES.ProfitPercent AS\r\n" +
		"     '([Measures].[Store Sales]-[Measures].[Store Cost])/([Measures].[Store Cost])',\r\n" +
		" FORMAT_STRING = '#.00%', SOLVE_ORDER = 1\r\n" +
		" MEMBER [Time].[First Half 97] AS  '[Time].[1997].[Q1] + [Time].[1997].[Q2]'\r\n" +
		" MEMBER [Time].[Second Half 97] AS '[Time].[1997].[Q3] + [Time].[1997].[Q4]'\r\n" +
		" SELECT {[Time].[First Half 97],\r\n" +
		"     [Time].[Second Half 97],\r\n" +
		"     [Time].[1997].CHILDREN} ON COLUMNS,\r\n" +
		" {[Store].[Store Country].[USA].CHILDREN} ON ROWS\r\n" +
		" FROM [Sales]\r\n" +
		" WHERE (MEASURES.ProfitPercent)",

		// #16 (= mdx sample #7, but uses virtual cube)
		"with member [Measures].[Accumulated Sales] as 'Sum(YTD(),[Measures].[Store Sales])'\r\n" +
		"select\r\n" +
		"	 {[Measures].[Store Sales],[Measures].[Accumulated Sales]} on columns,\r\n" +
		"	 {Descendants([Time].[1997],[Time].[Month])} on rows\r\n" +
		"from [Warehouse and Sales]",

		// #17 Virtual cube. Note that Unit Sales is independent of Warehouse.
		"select CrossJoin(\r\n"+
		"  {[Warehouse].DefaultMember, [Warehouse].[USA].children},\r\n" +
		"  {[Measures].[Unit Sales], [Measures].[Units Shipped]}) on columns,\r\n" +
		" [Time].children on rows\r\n" +
		"from [Warehouse and Sales]",

		// #18 bug: should allow dimension to be used as shorthand for member
		"select {[Measures].[Unit Sales]} on columns,\r\n" +
		" {[Store], [Store].children} on rows\r\n" +
		"from [Sales]",

		// #19 bug: should allow 'members(n)' (and do it efficiently)
		"select {[Measures].[Unit Sales]} on columns,\r\n" +
		" {[Customers].members(0)} on rows\r\n" +
		"from [Sales]",
	};

%>

<html>
<head>
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

<title>JSP Page</title>
</head>
<body>
    <form action="adhoc.jsp" method="post">
    <table>
        <tr>
            <td>
                <select name="whichquery">   
		<% 

		for (int i=0; i<queries.length; i++) { 

			%>
			<option 
			<%
			
			if (request.getParameter("whichquery") != null) {
				if (Integer.valueOf(request.getParameter("whichquery")).intValue() == i) {
					out.print(" selected");
				}
			}
			%>

			value="<% out.print(i);%>">Sample Query #<%out.print(i);
			 
			%>

			</option>
                
		<% 
		} 		
		%>

                </select>
            </td>
        </tr>
       
        <tr>
            <td>
                <input type="submit" value="show query">
            </td>
        </tr>
	</table>
	</form>

	<form action="mdxquery">
    	<table>
        <tr>
       	<td>
        <tr>
            <td>
                <textarea id='queryArea' name="queryString" rows=10 cols=80><% 
			if (request.getParameter("whichquery") != null) {
				out.println(queries[Integer.valueOf(request.getParameter("whichquery")).intValue()]);
			} 
		%></textarea>
            </td>
        </tr>
        <tr>
            <td>
                <input type="submit" value="process MDX query">
            </td>
        </tr>

        <% if (request.getAttribute("result") != null) { %>
        <tr>
            <td valign=top>Results:
            <% out.println(request.getAttribute("result")); %>
            </td>
        </tr>
        <% } %>	

    </table>
    </form>

</body>
</html>
