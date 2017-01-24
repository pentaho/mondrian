/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2004-2005 Julian Hyde
// Copyright (C) 2005-2017 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.test.FoodMartTestCase;
import mondrian.test.TestContext;

import org.apache.log4j.Appender;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;

import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.List;

public class OrderKeyOneToOneCheckTest extends FoodMartTestCase {

  private Appender memberSourceAppender;
  private Appender sqlReaderAppender;

  private final List<Object> memberSourceLogErrors = new ArrayList<>();
  private final List<Object> sqlReaderLogErrors = new ArrayList<>();

  @Override
  protected void setUp() throws Exception {
    Logger memberSourceLogger = Logger.getLogger(SqlMemberSource.class);
    Logger sqlReaderLogger = Logger.getLogger(SqlTupleReader.class);


    memberSourceAppender = Mockito.mock(Appender.class);
    sqlReaderAppender = Mockito.mock(Appender.class);
    memberSourceLogger.addAppender(memberSourceAppender);
    sqlReaderLogger.addAppender(sqlReaderAppender);

    Mockito.doAnswer(
        new Answer() {
          public Object answer(InvocationOnMock invocation) {
            Object loggingEvent = invocation.getArguments()[0];
            memberSourceLogErrors.add(loggingEvent.toString());
            return null;
          }})
        .when(memberSourceAppender).doAppend(Mockito.any(LoggingEvent.class));

    Mockito.doAnswer(
        new Answer() {
          public Object answer(InvocationOnMock invocation) {
            Object loggingEvent = invocation.getArguments()[0];
            sqlReaderLogErrors.add(loggingEvent.toString());
            return null;
          }})
        .when(sqlReaderAppender).doAppend(Mockito.any(LoggingEvent.class));
  }

  @Override
  protected void tearDown() throws Exception {
    Logger memberSourceLogger = Logger.getLogger(SqlMemberSource.class);
    Logger sqlReaderLogger = Logger.getLogger(SqlTupleReader.class);

    memberSourceLogger.removeAppender(memberSourceAppender);
    sqlReaderLogger.removeAppender(sqlReaderAppender);

    memberSourceLogErrors.clear();
    sqlReaderLogErrors.clear();
  }

  @Override
  public TestContext getTestContext() {
    TestContext testContext = super.getTestContext()
            .withFreshConnection();
    testContext.flushSchemaCache();
    return testContext
        .withSchema(
            ""
            + "<?xml version=\"1.0\"?>\n"
            + "<Schema name=\"FoodMart 2358\">\n"
            + "  <Dimension name=\"Time\" type=\"TimeDimension\">\n"
            + "    <Hierarchy hasAll=\"false\" primaryKey=\"time_id\">\n"
            + "      <Table name=\"time_by_day\"/>\n"
            + "      <Level name=\"Year\" column=\"the_year\" type=\"Numeric\" uniqueMembers=\"true\"\n"
            + "          levelType=\"TimeYears\"/> \n"
            + "      <Level name=\"Quarter\" column=\"quarter\" ordinalColumn=\"month_of_year\" uniqueMembers=\"false\" levelType=\"TimeQuarters\"/>  \n"
            + "      <Level name=\"Month\" column=\"month_of_year\" uniqueMembers=\"false\" type=\"Numeric\"\n"
            + "          levelType=\"TimeMonths\"/>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n"
            + "<Cube name=\"Sales\" defaultMeasure=\"Unit Sales\">\n"
            + "  <Table name=\"sales_fact_1997\"/>\n"
            + "  <DimensionUsage name=\"Time\" source=\"Time\" foreignKey=\"time_id\"/>\n"
            + "  <Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\"\n"
            + "      formatString=\"Standard\"/>\n"
            + "</Cube>\n"
            + "</Schema>");
  }

  public void testMemberSource() {
    String mdx =
            "with member [Measures].[Count Month] as 'Count(Descendants(Time.CurrentMember, [Time].[Month]))' \n"
            + "select [Measures].[Count Month] on 0,\n"
            + "[Time].[1997] on 1 \n"
            + "from [Sales]";

    this.getTestContext().executeQuery(mdx);

    assertEquals(
        "Running with modified schema should log 8 error",
        8,
        sqlReaderLogErrors.size());
    assertEquals(
        "Running with modified schema should log 8 error",
        8,
        memberSourceLogErrors.size());
  }

  public void testSqlReader() {
    String mdx = ""
          + "select [Time].[Quarter].Members on 0"
          + "from [Sales]";

    this.getTestContext().executeQuery(mdx);

    assertEquals(
        "Running with modified schema should log 16 error",
        16,
        sqlReaderLogErrors.size());
  }
}
// End OrderKeyOneToOneCheckTest.java
