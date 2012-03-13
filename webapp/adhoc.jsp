<%@page contentType="text/html"%>
<%--
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2001-2005 Julian Hyde and others
// All Rights Reserved.
//
// jhyde, 6 August, 2001
--%>
<%
    final String nl = System.getProperty("line.separator");
    String[] queries = new String[] {
        // #0
        "select {[Measures].[Unit Sales]} on columns" + nl +
        " from Sales",

        // mdx sample #1
        "select" + nl +
        "    {[Measures].[Unit Sales]} on columns," + nl +
        "    order(except([Promotion Media].[Media Type].members,{[Promotion Media].[Media Type].[No Media]}),[Measures].[Unit Sales],DESC) on rows" + nl +
        "from Sales",

        // mdx sample #2
        "select" + nl +
        "    { [Measures].[Units Shipped], [Measures].[Units Ordered] } on columns," + nl +
        "    NON EMPTY [Store].[Store Name].members on rows" + nl +
        "from Warehouse",

        // mdx sample #3
        "with member [Measures].[Store Sales Last Period] as '([Measures].[Store Sales], Time.PrevMember)'" + nl +
        "select" + nl +
        "    {[Measures].[Store Sales Last Period]} on columns," + nl +
        "    {TopCount([Product].[Product Department].members,5, [Measures].[Store Sales Last Period])} on rows" + nl +
        "from Sales" + nl +
        "where ([Time].[1998])",

        // mdx sample #4
        "with member [Measures].[Total Store Sales] as 'Sum(YTD(),[Measures].[Store Sales])'" + nl +
        "select" + nl +
        "    {[Measures].[Total Store Sales]} on columns," + nl +
        "    {TopCount([Product].[Product Department].members,5, [Measures].[Total Store Sales])} on rows" + nl +
        "from Sales" + nl +
        "where ([Time].[1997].[Q2].[4])",

        // mdx sample #5
        "with member [Measures].[Store Profit Rate] as '([Measures].[Store Sales]-[Measures].[Store Cost])/[Measures].[Store Cost]', format = '#.00%'" + nl +
        "select" + nl +
        "    {[Measures].[Store Cost],[Measures].[Store Sales],[Measures].[Store Profit Rate]} on columns," + nl +
        "    Order([Product].[Product Department].members, [Measures].[Store Profit Rate], BDESC) on rows" + nl +
        "from Sales" + nl +
        "where ([Time].[1997])",

        // mdx sample #6
        "with" + nl +
        "   member [Product].[All Products].[Drink].[Percent of Alcoholic Drinks] as '[Product].[All Products].[Drink].[Alcoholic Beverages]/[Product].[All Products].[Drink]', format = '#.00%'" + nl +
        "select" + nl +
        "   { [Product].[All Products].[Drink].[Percent of Alcoholic Drinks] } on columns," + nl +
        "   order([Customers].[All Customers].[USA].[WA].Children, [Product].[All Products].[Drink].[Percent of Alcoholic Drinks],BDESC ) on rows" + nl +
        "from Sales" + nl +
        "where ( [Measures].[Unit Sales] )",

        // mdx sample #7
        "with member [Measures].[Accumulated Sales] as 'Sum(YTD(),[Measures].[Store Sales])'" + nl +
        "select" + nl +
        "    {[Measures].[Store Sales],[Measures].[Accumulated Sales]} on columns," + nl +
        "    {Descendants([Time].[1997],[Time].[Month])} on rows" + nl +
        "from Sales",

        // #8
        "select" + nl +
        " {[Measures].[Unit Sales]} on columns," + nl +
        " [Gender].members on rows" + nl +
        "from Sales",

/*
        // #9
        "with" + nl +
        "  member [Product].[Non dairy] as '[Product].[All Products] - [Product].[Food].[Dairy]'" + nl +
        "  member [Measures].[Dairy ever] as 'sum([Time].members, ([Measures].[Unit Sales],[Product].[Food].[Dairy]))'" + nl +
        "  set [Customers who never bought dairy] as 'filter([Customers].members, [Measures].[Dairy ever] = 0)'" + nl +
        "select" + nl +
        " {[Measures].[Unit Sales], [Measures].[Dairy ever]}  on columns," + nl +
        "  [Customers who never bought dairy] on rows" + nl +
        "from Sales\r\n",
*/

        // #10
        "WITH" + nl +
        "   MEMBER [Measures].[StoreType] AS " + nl +
        "   '[Store].CurrentMember.Properties(\"Store Type\")'," + nl +
        "   SOLVE_ORDER = 2" + nl +
        "   MEMBER [Measures].[ProfitPct] AS " + nl +
        "   '(Measures.[Store Sales] - Measures.[Store Cost]) / Measures.[Store Sales]'," + nl +
        "   SOLVE_ORDER = 1, FORMAT_STRING = 'Percent'" + nl +
        "SELECT" + nl +
        "   { [Store].[Store Name].Members} ON COLUMNS," + nl +
        "   { [Measures].[Store Sales], [Measures].[Store Cost], [Measures].[StoreType]," + nl +
        "   [Measures].[ProfitPct] } ON ROWS" + nl +
        "FROM Sales",

        // #11
        "WITH" + nl +
        "   MEMBER [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[BigSeller] AS" + nl +
        "  'IIf([Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine] > 100, \"Yes\",\"No\")'" + nl +
        "SELECT" + nl +
        "   {[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[BigSeller]} ON COLUMNS," + nl +
        "   {Store.[Store Name].Members} ON ROWS" + nl +
        "FROM Sales",

        // #12
        "WITH" + nl +
        "   MEMBER [Measures].[ProfitPct] AS " + nl +
        "   '([Measures].[Store Sales] - [Measures].[Store Cost]) / [Measures].[Store Sales]'," + nl +
        "   SOLVE_ORDER = 1, FORMAT_STRING = 'Percent'" + nl +
        "   MEMBER [Measures].[ProfitValue] AS " + nl +
        "   '[Measures].[Store Sales] * [Measures].[ProfitPct]'," + nl +
        "   SOLVE_ORDER = 2, FORMAT_STRING = 'Currency'" + nl +
        "SELECT" + nl +
        "   { [Store].[Store Name].Members} ON COLUMNS," + nl +
        "   { [Measures].[Store Sales], [Measures].[Store Cost], [Measures].[ProfitValue]," + nl +
        "   [Measures].[ProfitPct] } ON ROWS" + nl +
        "FROM Sales",

/*
        // #13: cyclical calculated members
        "WITH" + nl +
        "   MEMBER [Product].[X] AS '[Product].[Y]'" + nl +
        "   MEMBER [Product].[Y] AS '[Product].[X]'" + nl +
        "SELECT" + nl +
        "   {[Product].[X]} ON COLUMNS," + nl +
        "   {Store.[Store Name].Members} ON ROWS" + nl +
        "FROM Sales",
*/

        // #14
        "WITH MEMBER MEASURES.ProfitPercent AS" + nl +
        "     '([Measures].[Store Sales]-[Measures].[Store Cost])/([Measures].[Store Cost])'," + nl +
        " FORMAT_STRING = '#.00%', SOLVE_ORDER = 1" + nl +
        " MEMBER [Time].[First Half 97] AS  '[Time].[1997].[Q1] + [Time].[1997].[Q2]'" + nl +
        " MEMBER [Time].[Second Half 97] AS '[Time].[1997].[Q3] + [Time].[1997].[Q4]'" + nl +
        " SELECT {[Time].[First Half 97]," + nl +
        "     [Time].[Second Half 97]," + nl +
        "     [Time].[1997].CHILDREN} ON COLUMNS," + nl +
        " {[Store].[Store Country].[USA].CHILDREN} ON ROWS" + nl +
        " FROM [Sales]" + nl +
        " WHERE ([Measures].[ProfitPercent])",

        // #15 (= mdx sample #7, but uses virtual cube)
        "with member [Measures].[Accumulated Sales] as 'Sum(YTD(),[Measures].[Store Sales])'" + nl +
        "select" + nl +
        "    {[Measures].[Store Sales],[Measures].[Accumulated Sales]} on columns," + nl +
        "    {Descendants([Time].[1997],[Time].[Month])} on rows" + nl +
        "from [Warehouse and Sales]",

        // #16 Virtual cube. Note that Unit Sales is independent of Warehouse.
        "select CrossJoin(\r\n"+
        "  {[Warehouse].DefaultMember, [Warehouse].[USA].children}," + nl +
        "  {[Measures].[Unit Sales], [Measures].[Units Shipped]}) on columns," + nl +
        " [Time].children on rows" + nl +
        "from [Warehouse and Sales]",

        // #17 crossjoins on rows and columns, and a slicer
        "select" + nl +
        "  CrossJoin(" + nl +
        "    {[Measures].[Unit Sales], [Measures].[Store Sales]}," + nl +
        "    {[Time].[1997].[Q2].children}) on columns, " + nl +
        "  CrossJoin(" + nl +
        "    CrossJoin(" + nl +
        "      [Gender].members," + nl +
        "      [Marital Status].members)," + nl +
        "   {[Store], [Store].children}) on rows" + nl +
        "from [Sales]" + nl +
        "where (" + nl +
        " [Product].[Food]," + nl +
        " [Education Level].[High School Degree]," + nl +
        " [Promotions].DefaultMember)",
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

<a href=".">back to index</a><p/>

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
            if (request.getParameter("queryString") != null) {
                out.println(request.getParameter("queryString"));
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

<a href=".">back to index</a><p/>

</body>
</html>
