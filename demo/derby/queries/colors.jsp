<%@ page session="true" contentType="text/html; charset=ISO-8859-1" %>
<%@ taglib uri="http://www.tonbeller.com/jpivot" prefix="jp" %>
<%@ taglib prefix="c" uri="http://java.sun.com/jstl/core" %>


<jp:mondrianQuery id="query01" jdbcDriver="org.apache.derby.jdbc.EmbeddedDriver" jdbcUrl="jdbc:derby:classpath:/foodmart" catalogUri="/WEB-INF/queries/FoodMart.xml"
   jdbcUser="sa" jdbcPassword="sa" connectionPooling="false">

with member [Measures].[ROI] as '(([Measures].[Store Sales] - [Measures].[Store Cost]) / [Measures].[Store Cost])', format_string = IIf((((([Measures].[Store Sales] - [Measures].[Store Cost]) / [Measures].[Store Cost]) * 100.0) > 150.0), "|#.00%|style='green'", IIf((((([Measures].[Store Sales] - [Measures].[Store Cost]) / [Measures].[Store Cost]) * 100.0) < 150.0), "|#.00%|style='red'", "#.00%"))
select {[Measures].[ROI], [Measures].[Store Cost], [Measures].[Store Sales]} ON columns,
  {[Product].[All Products]} ON rows
from [Sales]
where [Time].[1997]
</jp:mondrianQuery>

<c:set var="title01" scope="session">Colors</c:set>
