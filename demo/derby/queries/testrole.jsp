<%@ page session="true" contentType="text/html; charset=ISO-8859-1" %>
<%@ taglib uri="http://www.tonbeller.com/jpivot" prefix="jp" %>
<%@ taglib prefix="c" uri="http://java.sun.com/jstl/core" %>

<jp:mondrianQuery id="query01" jdbcDriver="org.apache.derby.jdbc.EmbeddedDriver" jdbcUrl="jdbc:derby:classpath:/foodmart" catalogUri="/WEB-INF/queries/FoodMart.xml"
   role="California manager"
   jdbcUser="sa" jdbcPassword="sa" connectionPooling="false">
select
  {[Measures].[Unit Sales], [Measures].[Store Cost], [Measures].[Store Sales]} on columns,
  {([Marital Status].[All Marital Status], [Customers], [Product].[All Products] ) } on rows
from Sales
where ([Time].[1997])

</jp:mondrianQuery>

<c:set var="title01" scope="session">Test query uses Mondrian OLAP, using role 'California manager'</c:set>
