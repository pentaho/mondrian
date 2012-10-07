<%@ page session="true" contentType="text/html; charset=ISO-8859-1" %>
<%@ taglib uri="http://www.tonbeller.com/jpivot" prefix="jp" %>
<%@ taglib prefix="c" uri="http://java.sun.com/jstl/core" %>

<%-- uses a dataSource --%>
<%-- jp:mondrianQuery id="query01" dataSource="jdbc/MondrianFoodmart" catalogUri="/WEB-INF/demo/FoodMart.mondrian.xml" --%>

<%-- uses mysql --%>
<%-- jp:mondrianQuery id="query01" jdbcDriver="com.mysql.jdbc.Driver" jdbcUrl="jdbc:mysql://localhost/foodmart" catalogUri="/WEB-INF/queries/FoodMart.mondrian.xml"--%>

<%-- uses a role defined in FoodMart.mondrian.xml --%>
<%-- jp:mondrianQuery role="California manager" id="query01" jdbcDriver="com.mysql.jdbc.Driver" jdbcUrl="jdbc:mysql://localhost/foodmart?user=foodmart&password=foodmart" catalogUri="/WEB-INF/queries/FoodMart.mondrian.xml" --%>

<jp:mondrianQuery id="query01" jdbcDriver="com.mysql.jdbc.Driver" jdbcUrl="jdbc:mysql://localhost/foodmart?user=foodmart&password=foodmart" catalogUri="/WEB-INF/queries/FoodMart.mondrian.xml" role="California manager">
select
  {[Measures].[Unit Sales], [Measures].[Store Cost], [Measures].[Store Sales]} on columns,
  {([Marital Status].[All Marital Status], [Customers], [Product].[All Products])} on rows
from Sales
where ([Time].[1997])
</jp:mondrianQuery>

<c:set var="title01" scope="session">Test query uses Mondrian OLAP, using role 'California manager'</c:set>
