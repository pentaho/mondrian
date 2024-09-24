/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2003-2005 Julian Hyde
// Copyright (C) 2005-2021 Hitachi Vantara
// All Rights Reserved.
//
// jhyde, Feb 14, 2003
*/
package mondrian.test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;

import org.apache.logging.log4j.Level;
import org.olap4j.CellSet;
import org.olap4j.OlapConnection;
import org.olap4j.OlapStatement;
import org.olap4j.layout.RectangularCellSetFormatter;

import mondrian.olap.CacheControl;
import mondrian.olap.Cube;
import mondrian.olap.MondrianProperties;
import mondrian.olap.QueryTiming;
import mondrian.olap.Util;
import mondrian.rolap.RolapUtil;
import mondrian.spi.ProfileHandler;

/**
 * Tests related to explain plan and QueryTiming
 * 
 * @author Benny
 *
 */
public class ExplainPlanTest extends FoodMartTestCase {

  public ExplainPlanTest() {
    super();
  }

  public ExplainPlanTest( String name ) {
    super( name );
  }

  public void testExplain() throws SQLException {
    Level originalLevel = RolapUtil.PROFILE_LOGGER.getLevel();
    Util.setLevel( RolapUtil.PROFILE_LOGGER, Level.OFF ); // Must turn off in case test environment has enabled profiling
    OlapConnection connection = TestContext.instance().getOlap4jConnection();
    final OlapStatement statement = connection.createStatement();
    final ResultSet resultSet =
        statement.executeQuery( "explain plan for\n" + "select [Measures].[Unit Sales] on 0,\n"
            + "  Filter([Product].Children, [Measures].[Unit Sales] > 100) on 1\n" + "from [Sales]" );
    assertTrue( resultSet.next() );
    assertEquals( 1, resultSet.getMetaData().getColumnCount() );
    assertEquals( "PLAN", resultSet.getMetaData().getColumnName( 1 ) );
    assertEquals( Types.VARCHAR, resultSet.getMetaData().getColumnType( 1 ) );
    String s = resultSet.getString( 1 );
    TestContext.assertStubbedEqualsVerbose( "Axis (COLUMNS):\n"
        + "SetListCalc(name=SetListCalc, class=class mondrian.olap.fun.SetFunDef$SetListCalc, "
        + "type=SetType<MemberType<member=[Measures].[Unit Sales]>>, resultStyle=MUTABLE_LIST)\n"
        + "    2(name=2, class=class mondrian.olap.fun.SetFunDef$SetListCalc$2, type=MemberType<member=[Measures]"
        + ".[Unit Sales]>, resultStyle=VALUE)\n"
        + "        Literal(name=Literal, class=class mondrian.calc.impl.ConstantCalc, "
        + "type=MemberType<member=[Measures].[Unit Sales]>, resultStyle=VALUE_NOT_NULL, value=[Measures].[Unit "
        + "Sales])\n" + "\n" + "Axis (ROWS):\n"
        + "ImmutableIterCalc(name=ImmutableIterCalc, class=class mondrian.olap.fun.FilterFunDef$ImmutableIterCalc, "
        + "type=SetType<MemberType<hierarchy=[Product]>>, resultStyle=ITERABLE)\n"
        + "    Children(name=Children, class=class mondrian.olap.fun.BuiltinFunTable$22$1, "
        + "type=SetType<MemberType<hierarchy=[Product]>>, resultStyle=LIST)\n"
        + "        CurrentMemberFixed(hierarchy=[Product], name=CurrentMemberFixed, class=class mondrian.olap.fun"
        + ".HierarchyCurrentMemberFunDef$FixedCalcImpl, type=MemberType<hierarchy=[Product]>, resultStyle=VALUE)\n"
        + "    >(name=>, class=class mondrian.olap.fun.BuiltinFunTable$63$1, type=BOOLEAN, resultStyle=VALUE)\n"
        + "        MemberValueCalc(name=MemberValueCalc, class=class mondrian.calc.impl.MemberValueCalc, type=SCALAR,"
        + " resultStyle=VALUE)\n" + "            Literal(name=Literal, class=class mondrian.calc.impl.ConstantCalc, "
        + "type=MemberType<member=[Measures].[Unit Sales]>, resultStyle=VALUE_NOT_NULL, value=[Measures].[Unit "
        + "Sales])\n" + "        Literal(name=Literal, class=class mondrian.calc.impl.ConstantCalc, type=NUMERIC, "
        + "resultStyle=VALUE_NOT_NULL, value=100.0)\n" + "\n", s );
    Util.setLevel( RolapUtil.PROFILE_LOGGER, originalLevel );
  }

  public void testExplainComplex() throws SQLException {
    Level originalLevel = RolapUtil.PROFILE_LOGGER.getLevel();
    Util.setLevel( RolapUtil.PROFILE_LOGGER, Level.OFF );; // Must turn off in case test environment has enabled profiling
    OlapConnection connection = TestContext.instance().getOlap4jConnection();
    final OlapStatement statement = connection.createStatement();
    final String mdx =
        "with member [Time].[Time].[1997].[H1] as\n" + "    Aggregate({[Time].[1997].[Q1], [Time].[1997].[Q2]})\n"
            + "  member [Measures].[Store Margin] as\n" + "    [Measures].[Store Sales] - [Measures].[Store Cost],\n"
            + "      format_string =\n" + "        iif(\n" + "          [Measures].[Unit Sales] > 50000,\n"
            + "          \"\\<b\\>#.00\\<\\/b\\>\",\n" + "           \"\\<i\\>#.00\\<\\/i\\>\")\n"
            + "  set [Hi Val Products] as\n" + "    Filter(\n" + "      Descendants([Product].[Drink], , LEAVES),\n"
            + "     [Measures].[Unit Sales] > 100)\n" + "select\n"
            + "  {[Measures].[Unit Sales], [Measures].[Store Margin]} on 0,\n"
            + "  [Hi Val Products] * [Marital Status].Members on 1\n" + "from [Sales]\n" + "where [Gender].[F]";

    // Plan before execution.
    final ResultSet resultSet = statement.executeQuery( "explain plan for\n" + mdx );
    assertTrue( resultSet.next() );
    String s = resultSet.getString( 1 );
    TestContext.assertStubbedEqualsVerbose( "Axis (FILTER):\n"
        + "SetListCalc(name=SetListCalc, class=class mondrian.olap.fun.SetFunDef$SetListCalc, "
        + "type=SetType<MemberType<member=[Gender].[F]>>, resultStyle=MUTABLE_LIST)\n"
        + "    ()(name=(), class=class mondrian.olap.fun.SetFunDef$SetListCalc$2, type=MemberType<member=[Gender]"
        + ".[F]>, resultStyle=VALUE)\n"
        + "        Literal(name=Literal, class=class mondrian.calc.impl.ConstantCalc, type=MemberType<member=[Gender]"
        + ".[F]>, resultStyle=VALUE_NOT_NULL, value=[Gender].[F])\n" + "\n" + "Axis (COLUMNS):\n"
        + "SetListCalc(name=SetListCalc, class=class mondrian.olap.fun.SetFunDef$SetListCalc, "
        + "type=SetType<MemberType<member=[Measures].[Unit Sales]>>, resultStyle=MUTABLE_LIST)\n"
        + "    2(name=2, class=class mondrian.olap.fun.SetFunDef$SetListCalc$2, type=MemberType<member=[Measures]"
        + ".[Unit Sales]>, resultStyle=VALUE)\n"
        + "        Literal(name=Literal, class=class mondrian.calc.impl.ConstantCalc, "
        + "type=MemberType<member=[Measures].[Unit Sales]>, resultStyle=VALUE_NOT_NULL, value=[Measures].[Unit "
        + "Sales])\n"
        + "    2(name=2, class=class mondrian.olap.fun.SetFunDef$SetListCalc$2, type=MemberType<member=[Measures]"
        + ".[Store Margin]>, resultStyle=VALUE)\n"
        + "        Literal(name=Literal, class=class mondrian.calc.impl.ConstantCalc, "
        + "type=MemberType<member=[Measures].[Store Margin]>, resultStyle=VALUE_NOT_NULL, value=[Measures].[Store "
        + "Margin])\n" + "\n" + "Axis (ROWS):\n"
        + "CrossJoinIterCalc(name=CrossJoinIterCalc, class=class mondrian.olap.fun.CrossJoinFunDef$CrossJoinIterCalc,"
        + " type=SetType<TupleType<MemberType<member=[Product].[Drink]>, MemberType<hierarchy=[Marital Status]>>>, "
        + "resultStyle=ITERABLE)\n"
        + "    Hi Val Products(name=Hi Val Products, class=class mondrian.mdx.NamedSetExpr$1, type=SetType<MemberType<member=[Product].[Drink]>>,"
        + " resultStyle=ITERABLE)\n" + "    Members(name=Members, class=class mondrian.olap.fun.BuiltinFunTable$27$1, "
        + "type=SetType<MemberType<hierarchy=[Marital Status]>>, resultStyle=MUTABLE_LIST)\n"
        + "        Literal(name=Literal, class=class mondrian.calc.impl.ConstantCalc, "
        + "type=HierarchyType<hierarchy=[Marital Status]>, resultStyle=VALUE_NOT_NULL, value=[Marital Status])\n"
        + "\n", s );

    // Plan after execution, including profiling.
    final ArrayList<String> strings = new ArrayList<String>();
    ( (mondrian.server.Statement) statement ).enableProfiling( new ProfileHandler() {
      public void explain( String plan, QueryTiming timing ) {
        strings.add( plan );
        strings.add( String.valueOf( timing ) );
      }
    } );

    final CellSet cellSet = statement.executeOlapQuery( mdx );
    new RectangularCellSetFormatter( true ).format( cellSet, new PrintWriter( new StringWriter() ) );
    cellSet.close();
    assertEquals( 8, strings.size() );
    String actual =
        strings.get( 0 ).replaceAll( "callMillis=[0-9]+", "callMillis=nnn" ).replaceAll( "[0-9]+ms", "nnnms" );
    TestContext.assertStubbedEqualsVerbose( "NamedSet (Hi Val Products):\n"
        + "MutableIterCalc(name=MutableIterCalc, class=class mondrian.olap.fun.FilterFunDef$MutableIterCalc, type=SetType<MemberType<member=[Product].[Drink]>>, resultStyle=ITERABLE, callCount=3, callMillis=nnn, elementCount=44, elementSquaredCount=968)\n"
        + "    Descendants(name=Descendants, class=class mondrian.olap.fun.DescendantsFunDef$-anonymous-class-, type=SetType<MemberType<member=[Product].[Drink]>>, resultStyle=MUTABLE_LIST)\n"
        + "        Literal(name=Literal, class=class mondrian.calc.impl.ConstantCalc, type=MemberType<member=[Product].[Drink]>, resultStyle=VALUE_NOT_NULL, value=[Product].[Drink], callCount=3, callMillis=nnn)\n"
        + "    >(name=>, class=class mondrian.olap.fun.BuiltinFunTable$-anonymous-class-$-anonymous-class-, type=BOOLEAN, resultStyle=VALUE, callCount=435, callMillis=nnn)\n"
        + "        MemberValueCalc(name=MemberValueCalc, class=class mondrian.calc.impl.MemberValueCalc, type=SCALAR, resultStyle=VALUE, callCount=435, callMillis=nnn)\n"
        + "            Literal(name=Literal, class=class mondrian.calc.impl.ConstantCalc, type=MemberType<member=[Measures].[Unit Sales]>, resultStyle=VALUE_NOT_NULL, value=[Measures].[Unit Sales])\n"
        + "        Literal(name=Literal, class=class mondrian.calc.impl.ConstantCalc, type=NUMERIC, resultStyle=VALUE_NOT_NULL, value=100.0, callCount=435, callMillis=nnn)\n",
        actual );

    assertTrue( strings.get( 1 ), strings.get( 1 ).contains( "FilterFunDef invoked 6 times for total of" ) );

    actual = strings.get( 2 ).replaceAll( "callMillis=[0-9]+", "callMillis=nnn" ).replaceAll( "[0-9]+ms", "nnnms" );
    TestContext.assertStubbedEqualsVerbose( "Axis (COLUMNS):\n"
        + "SetListCalc(name=SetListCalc, class=class mondrian.olap.fun.SetFunDef$SetListCalc, type=SetType<MemberType<member=[Measures].[Unit Sales]>>, resultStyle=MUTABLE_LIST, callCount=2, callMillis=nnn, elementCount=4, elementSquaredCount=8)\n"
        + "    2(name=2, class=class mondrian.olap.fun.SetFunDef$SetListCalc$-anonymous-class-, type=MemberType<member=[Measures].[Unit Sales]>, resultStyle=VALUE)\n"
        + "        Literal(name=Literal, class=class mondrian.calc.impl.ConstantCalc, type=MemberType<member=[Measures].[Unit Sales]>, resultStyle=VALUE_NOT_NULL, value=[Measures].[Unit Sales], callCount=2, callMillis=nnn)\n"
        + "    2(name=2, class=class mondrian.olap.fun.SetFunDef$SetListCalc$-anonymous-class-, type=MemberType<member=[Measures].[Store Margin]>, resultStyle=VALUE)\n"
        + "        Literal(name=Literal, class=class mondrian.calc.impl.ConstantCalc, type=MemberType<member=[Measures].[Store Margin]>, resultStyle=VALUE_NOT_NULL, value=[Measures].[Store Margin], callCount=2, callMillis=nnn)\n",
        actual );

    actual = strings.get( 4 ).replaceAll( "callMillis=[0-9]+", "callMillis=nnn" ).replaceAll( "[0-9]+ms", "nnnms" );
    TestContext.assertStubbedEqualsVerbose( "Axis (ROWS):\n"
        + "CrossJoinIterCalc(name=CrossJoinIterCalc, class=class mondrian.olap.fun.CrossJoinFunDef$CrossJoinIterCalc, type=SetType<TupleType<MemberType<member=[Product].[Drink]>, MemberType<hierarchy=[Marital Status]>>>, resultStyle=ITERABLE, callCount=2, callMillis=nnn, elementCount=0, elementSquaredCount=0)\n"
        + "    Hi Val Products(name=Hi Val Products, class=class mondrian.mdx.NamedSetExpr$-anonymous-class-, type=SetType<MemberType<member=[Product].[Drink]>>, resultStyle=ITERABLE)\n"
        + "    Members(name=Members, class=class mondrian.olap.fun.BuiltinFunTable$-anonymous-class-$-anonymous-class-, type=SetType<MemberType<hierarchy=[Marital Status]>>, resultStyle=MUTABLE_LIST)\n"
        + "        Literal(name=Literal, class=class mondrian.calc.impl.ConstantCalc, type=HierarchyType<hierarchy=[Marital Status]>, resultStyle=VALUE_NOT_NULL, value=[Marital Status], callCount=2, callMillis=nnn)\n",
        actual );

    actual = strings.get( 6 ).replaceAll( "callMillis=[0-9]+", "callMillis=nnn" ).replaceAll( "[0-9]+ms", "nnnms" );
    TestContext.assertStubbedEqualsVerbose( "QueryBody:\n", actual );

    assertTrue( strings.get( 3 ), strings.get( 3 ).contains(
        "SqlStatement-SqlTupleReader.readTuples [[Product].[Product " + "Category]] invoked 1 times for total of " ) );
    Util.setLevel( RolapUtil.PROFILE_LOGGER, originalLevel );
  }

  public void testExplainInvalid() throws SQLException {
    OlapConnection connection = TestContext.instance().getOlap4jConnection();
    final OlapStatement statement = connection.createStatement();
    try {
      final ResultSet resultSet =
          statement.executeQuery( "select\n" + "  {[Measures].[Unit Sales], [Measures].[Store Margin]} on 0,\n"
              + "  [Hi Val Products] * [Marital Status].Members on 1\n" + "from [Sales]\n" + "where [Gender].[F]" );
      fail( "expected error, got " + resultSet );
    } catch ( SQLException e ) {
      TestContext.checkThrowable( e, "MDX object '[Measures].[Store Margin]' not found in cube 'Sales'" );
    }
  }

  /**
   * Verifies all QueryTiming elements
   * 
   * @throws SQLException
   */
  public void testQueryTimingAnalyzer() throws SQLException {

    final String mdx =
        "WITH\r\n"
            + " SET [*NATIVE_CJ_SET_WITH_SLICER] AS 'FILTER(NONEMPTYCROSSJOIN([*BASE_MEMBERS__Gender_],[*BASE_MEMBERS__Education Level_]), NOT ISEMPTY ([Measures].[Unit Sales]))'\r\n"
            + " SET [*NATIVE_CJ_SET] AS 'GENERATE([*NATIVE_CJ_SET_WITH_SLICER], {([Gender].CURRENTMEMBER)})'\r\n"
            + " SET [*SORTED_ROW_AXIS] AS 'ORDER([*CJ_ROW_AXIS],[Gender].CURRENTMEMBER.ORDERKEY,BASC)'\r\n"
            + " SET [*BASE_MEMBERS__Education Level_] AS '{[Education Level].[All Education Levels].[Bachelors Degree],[Education Level].[All Education Levels].[Graduate Degree]}'\r\n"
            + " SET [*BASE_MEMBERS__Measures_] AS '{[Measures].[*FORMATTED_MEASURE_0],[Measures].[*SUMMARY_MEASURE_1]}'\r\n"
            + " SET [*BASE_MEMBERS__Gender_] AS '[Gender].[Gender].MEMBERS'\r\n"
            + " SET [*CJ_SLICER_AXIS] AS 'GENERATE([*NATIVE_CJ_SET_WITH_SLICER], {([Education Level].CURRENTMEMBER)})'\r\n"
            + " SET [*NATIVE_MEMBERS__Gender_] AS 'GENERATE([*NATIVE_CJ_SET], {[Gender].CURRENTMEMBER})'\r\n"
            + " SET [*CJ_ROW_AXIS] AS 'GENERATE([*NATIVE_CJ_SET], {([Gender].CURRENTMEMBER)})'\r\n"
            + " MEMBER [Gender].[*TOTAL_MEMBER_SEL~AGG] AS 'AGGREGATE({[Gender].[All Gender]})', SOLVE_ORDER=-100\r\n"
            + " MEMBER [Gender].[*TOTAL_MEMBER_SEL~AVG] AS 'AVG([*CJ_ROW_AXIS])', SOLVE_ORDER=300\r\n"
            + " MEMBER [Gender].[*TOTAL_MEMBER_SEL~MAX] AS 'MAX([*CJ_ROW_AXIS])', SOLVE_ORDER=300\r\n"
            + " MEMBER [Gender].[*TOTAL_MEMBER_SEL~SUM] AS 'SUM([*CJ_ROW_AXIS])', SOLVE_ORDER=100\r\n"
            + " MEMBER [Measures].[*FORMATTED_MEASURE_0] AS '[Measures].[Unit Sales]', FORMAT_STRING = 'Standard', SOLVE_ORDER=500\r\n"
            + " MEMBER [Measures].[*SUMMARY_MEASURE_1] AS 'Rank(([Gender].CURRENTMEMBER),[*CJ_ROW_AXIS],[Measures].[Unit Sales])', FORMAT_STRING = '#,##0', SOLVE_ORDER=200\r\n"
            + " SELECT\r\n" + " [*BASE_MEMBERS__Measures_] ON COLUMNS\r\n" + " , NON EMPTY\r\n"
            + " UNION({[Gender].[*TOTAL_MEMBER_SEL~MAX]},UNION({[Gender].[*TOTAL_MEMBER_SEL~AVG]},UNION({[Gender].[*TOTAL_MEMBER_SEL~AGG]},UNION({[Gender].[*TOTAL_MEMBER_SEL~SUM]},[*SORTED_ROW_AXIS])))) ON ROWS\r\n"
            + " FROM [Sales]\r\n" + " WHERE ([*CJ_SLICER_AXIS])";

    ArrayList<String> strings = executeOlapQuery( mdx );
    assertEquals( 20, strings.size() );
    assertTrue( strings.get( 19 ), strings.get( 19 ).contains( "RankFunDef invoked 16 times" ) );
    assertTrue( strings.get( 19 ), strings.get( 19 ).contains( "EvalForSlicer invoked 6 times" ) );
    assertTrue( strings.get( 19 ), strings.get( 19 ).contains( "SumFunDef invoked 4 times" ) );
    assertTrue( strings.get( 19 ), strings.get( 19 ).contains( "Sort invoked 2 times" ) );
    assertTrue( strings.get( 19 ), strings.get( 19 ).contains( "AggregateFunDef invoked 4" ) );
    assertTrue( strings.get( 19 ), strings.get( 19 ).contains( "AvgFunDef invoked 4 times" ) );
    assertTrue( strings.get( 19 ), strings.get( 19 ).contains( "FilterFunDef invoked 2 times" ) );
    assertTrue( strings.get( 19 ), strings.get( 19 ).contains( "EvalForSort invoked 2 times " ) );
    assertTrue( strings.get( 19 ), strings.get( 19 ).contains( "MinMaxFunDef invoked 4 times" ) );
    assertTrue( strings.get( 19 ), strings.get( 19 ).contains( "OrderFunDef invoked 2 times" ) );
    assertTrue( strings.get( 19 ), strings.get( 19 ).contains(
        "SqlStatement-SqlTupleReader.readTuples [[Gender].[Gender], [Education Level].[Education Level]] invoked 1 times" ) );
  }

  public void testMutiKeySort() throws SQLException {
    final String mdx =
        "WITH\r\n"
            + " SET [*NATIVE_CJ_SET] AS 'NONEMPTYCROSSJOIN([*BASE_MEMBERS__Gender_],NONEMPTYCROSSJOIN([*BASE_MEMBERS__Education Level_],[*BASE_MEMBERS__Product_]))'\r\n"
            + " SET [*SORTED_ROW_AXIS] AS 'ORDER([*CJ_ROW_AXIS],[Education Level].CURRENTMEMBER.ORDERKEY,BASC,ANCESTOR([Product].CURRENTMEMBER, [Product].[Product Family]).ORDERKEY,BASC,ANCESTOR([Product].CURRENTMEMBER, [Product].[Product Department]).ORDERKEY,BASC,[Measures].[*SORTED_MEASURE],BDESC)'\r\n"
            + " SET [*SORTED_COL_AXIS] AS 'ORDER([*CJ_COL_AXIS],[Gender].CURRENTMEMBER.ORDERKEY,BASC,[Measures].CURRENTMEMBER.ORDERKEY,BASC)'\r\n"
            + " SET [*BASE_MEMBERS__Education Level_] AS '{[Education Level].[All Education Levels].[Graduate Degree]}'\r\n"
            + " SET [*BASE_MEMBERS__Measures_] AS '{[Measures].[*FORMATTED_MEASURE_0]}'\r\n"
            + " SET [*BASE_MEMBERS__Gender_] AS '[Gender].[Gender].MEMBERS'\r\n"
            + " SET [*CJ_COL_AXIS] AS 'GENERATE([*NATIVE_CJ_SET], {([Gender].CURRENTMEMBER)})'\r\n"
            + " SET [*BASE_MEMBERS__Product_] AS 'FILTER([Product].[Product Category].MEMBERS,ANCESTOR([Product].CURRENTMEMBER, [Product].[Product Family]) IN {[Product].[All Products].[Drink]})'\r\n"
            + " SET [*CJ_ROW_AXIS] AS 'GENERATE([*NATIVE_CJ_SET], {([Education Level].CURRENTMEMBER,[Product].CURRENTMEMBER)})'\r\n"
            + " MEMBER [Measures].[*FORMATTED_MEASURE_0] AS '[Measures].[Unit Sales]', FORMAT_STRING = 'Standard', SOLVE_ORDER=500\r\n"
            + " MEMBER [Measures].[*SORTED_MEASURE] AS '([Measures].[*FORMATTED_MEASURE_0],[Gender].[M])', SOLVE_ORDER=400\r\n"
            + " SELECT\r\n" + " CROSSJOIN([*SORTED_COL_AXIS],[*BASE_MEMBERS__Measures_]) ON COLUMNS\r\n"
            + " , NON EMPTY\r\n" + " [*SORTED_ROW_AXIS] ON ROWS\r\n" + " FROM [Sales]";

    ArrayList<String> strings = executeOlapQuery( mdx );
    assertTrue( strings.get( strings.size() - 1 ), strings.get( strings.size() - 1 ).contains(
        "OrderFunDef invoked 5 times" ) );
  }

  /**
   * Verifies that we don't double count the time spent in instrumented functions such as a SUM within a SUM MDX
   * expression.
   * 
   * @throws SQLException
   */
  public void testNestedSumFunDef() throws SQLException {
    final String mdx =
        "WITH\r\n"
            + " SET [*NATIVE_CJ_SET] AS 'FILTER([Time].[Month].MEMBERS, NOT ISEMPTY ([Measures].[Unit Sales]))'\r\n"
            + " SET [*SORTED_ROW_AXIS] AS 'ORDER([*CJ_ROW_AXIS],[Time].CURRENTMEMBER.ORDERKEY,BASC,ANCESTOR([Time].CURRENTMEMBER,[Time].[Quarter]).ORDERKEY,BASC)'\r\n"
            + " SET [*BASE_MEMBERS__Measures_] AS '{[Measures].[*FORMATTED_MEASURE_0],[Measures].[*CALCULATED_MEASURE_1]}'\r\n"
            + " SET [*BASE_MEMBERS__Time_] AS '[Time].[Month].MEMBERS'\r\n"
            + " SET [*CJ_ROW_AXIS] AS 'GENERATE([*NATIVE_CJ_SET], {([Time].CURRENTMEMBER)})'\r\n"
            + " MEMBER [Measures].[*CALCULATED_MEASURE_1] AS 'SUM(YTD(), [Measures].[Unit Sales])', SOLVE_ORDER=0\r\n"
            + " MEMBER [Measures].[*FORMATTED_MEASURE_0] AS '[Measures].[Unit Sales]', FORMAT_STRING = 'Standard', SOLVE_ORDER=500\r\n"
            + " MEMBER [Time].[*TOTAL_MEMBER_SEL~SUM] AS 'SUM([*CJ_ROW_AXIS])', SOLVE_ORDER=100\r\n" + " SELECT\r\n"
            + " [*BASE_MEMBERS__Measures_] ON COLUMNS\r\n" + " , NON EMPTY\r\n"
            + " UNION({[Time].[*TOTAL_MEMBER_SEL~SUM]},[*SORTED_ROW_AXIS]) ON ROWS\r\n" + " FROM [Sales]";

    ArrayList<String> strings = executeOlapQuery( mdx );
    assertEquals( 14, strings.size() );
    assertTrue( strings.get( 13 ), strings.get( 13 ).contains( "SumFunDef invoked 52 times for total of " ) );
    assertTrue( strings.get( 13 ), strings.get( 13 ).contains( "XtdFunDef invoked 24 times for total of " ) );
    assertTrue( strings.get( 13 ), strings.get( 13 ).contains( "FilterFunDef invoked 2 times for total of " ) );
    assertTrue( strings.get( 13 ), strings.get( 13 ).contains( "OrderFunDef invoked 2 times for total of " ) );
  }

  /**
   * Verifies the QueryTimings for when the Aggregate total CM solve order is ABOVE the compound slicer member solve
   * order
   * 
   * @throws SQLException
   */
  public void testAggAboveSlicerSolveOrder() throws SQLException {

    final String mdx =
        "WITH\r\n"
            + " SET [*NATIVE_CJ_SET_WITH_SLICER] AS 'NONEMPTYCROSSJOIN([*BASE_MEMBERS__Education Level_],NONEMPTYCROSSJOIN([*BASE_MEMBERS__Customers_],[*BASE_MEMBERS__Product_]))'\r\n"
            + " SET [*NATIVE_CJ_SET] AS 'GENERATE([*NATIVE_CJ_SET_WITH_SLICER], {([Education Level].CURRENTMEMBER,[Customers].CURRENTMEMBER)})'\r\n"
            + " SET [*SORTED_ROW_AXIS] AS 'ORDER([*CJ_ROW_AXIS],[Education Level].CURRENTMEMBER.ORDERKEY,BASC,[Customers].CURRENTMEMBER.ORDERKEY,BASC,ANCESTOR([Customers].CURRENTMEMBER,[Customers].[City]).ORDERKEY,BASC)'\r\n"
            + " SET [*BASE_MEMBERS__Education Level_] AS '{[Education Level].[All Education Levels].[Bachelors Degree],[Education Level].[All Education Levels].[Graduate Degree],[Education Level].[All Education Levels].[High School Degree]}'\r\n"
            + " SET [*BASE_MEMBERS__Measures_] AS '{[Measures].[*FORMATTED_MEASURE_0]}'\r\n"
            + " SET [*BASE_MEMBERS__Customers_] AS '{[Customers].[All Customers].[USA].[CA].[Colma].[Catherine Beaudoin],[Customers].[All Customers].[USA].[CA].[San Jose].[Richard Smith]}'\r\n"
            + " SET [*CJ_SLICER_AXIS] AS 'GENERATE([*NATIVE_CJ_SET_WITH_SLICER], {([Product].CURRENTMEMBER)})'\r\n"
            + " SET [*BASE_MEMBERS__Product_] AS '{[Product].[All Products].[Food],[Product].[All Products].[Non-Consumable]}'\r\n"
            + " SET [*CJ_ROW_AXIS] AS 'GENERATE([*NATIVE_CJ_SET], {([Education Level].CURRENTMEMBER,[Customers].CURRENTMEMBER)})'\r\n"
            + " MEMBER [Customers].[*DEFAULT_MEMBER] AS '[Customers].DEFAULTMEMBER', SOLVE_ORDER=-400\r\n"
            + " MEMBER [Education Level].[*TOTAL_MEMBER_SEL~AGG] AS 'AGGREGATE([*CJ_ROW_AXIS])', SOLVE_ORDER=-100\r\n"
            + " MEMBER [Measures].[*FORMATTED_MEASURE_0] AS '[Measures].[Unit Sales]', FORMAT_STRING = 'Standard', SOLVE_ORDER=500\r\n"
            + " SELECT\r\n" + " [*BASE_MEMBERS__Measures_] ON COLUMNS\r\n" + " , NON EMPTY\r\n"
            + " UNION(CROSSJOIN({[Education Level].[*TOTAL_MEMBER_SEL~AGG]},{([Customers].[*DEFAULT_MEMBER])}),[*SORTED_ROW_AXIS]) ON ROWS\r\n"
            + " FROM [Sales]\r\n" + " WHERE ([*CJ_SLICER_AXIS])";

    ArrayList<String> strings = executeOlapQuery( mdx );

    assertTrue( strings.get( 19 ), strings.get( 19 ).contains( "EvalForSlicer invoked 4 times" ) );
    assertTrue( strings.get( 19 ), strings.get( 19 ).contains( "AggregateFunDef invoked 2 times" ) );

  }

  /**
   * Verifies the QueryTimings for when the Aggregate total CM solve order is BELOW the compound slicer member solve
   * order
   * 
   * @throws SQLException
   */
  public void testAggBelowSlicerSolveOrder() throws SQLException {

    int original = MondrianProperties.instance().CompoundSlicerMemberSolveOrder.get();
    MondrianProperties.instance().CompoundSlicerMemberSolveOrder.set( 0 );

    final String mdx =
        "WITH\r\n"
            + " SET [*NATIVE_CJ_SET_WITH_SLICER] AS 'NONEMPTYCROSSJOIN([*BASE_MEMBERS__Education Level_],NONEMPTYCROSSJOIN([*BASE_MEMBERS__Customers_],[*BASE_MEMBERS__Product_]))'\r\n"
            + " SET [*NATIVE_CJ_SET] AS 'GENERATE([*NATIVE_CJ_SET_WITH_SLICER], {([Education Level].CURRENTMEMBER,[Customers].CURRENTMEMBER)})'\r\n"
            + " SET [*SORTED_ROW_AXIS] AS 'ORDER([*CJ_ROW_AXIS],[Education Level].CURRENTMEMBER.ORDERKEY,BASC,[Customers].CURRENTMEMBER.ORDERKEY,BASC,ANCESTOR([Customers].CURRENTMEMBER,[Customers].[City]).ORDERKEY,BASC)'\r\n"
            + " SET [*BASE_MEMBERS__Education Level_] AS '{[Education Level].[All Education Levels].[Bachelors Degree],[Education Level].[All Education Levels].[Graduate Degree],[Education Level].[All Education Levels].[High School Degree]}'\r\n"
            + " SET [*BASE_MEMBERS__Measures_] AS '{[Measures].[*FORMATTED_MEASURE_0]}'\r\n"
            + " SET [*BASE_MEMBERS__Customers_] AS '{[Customers].[All Customers].[USA].[CA].[Colma].[Catherine Beaudoin],[Customers].[All Customers].[USA].[CA].[San Jose].[Richard Smith]}'\r\n"
            + " SET [*CJ_SLICER_AXIS] AS 'GENERATE([*NATIVE_CJ_SET_WITH_SLICER], {([Product].CURRENTMEMBER)})'\r\n"
            + " SET [*BASE_MEMBERS__Product_] AS '{[Product].[All Products].[Food],[Product].[All Products].[Non-Consumable]}'\r\n"
            + " SET [*CJ_ROW_AXIS] AS 'GENERATE([*NATIVE_CJ_SET], {([Education Level].CURRENTMEMBER,[Customers].CURRENTMEMBER)})'\r\n"
            + " MEMBER [Customers].[*DEFAULT_MEMBER] AS '[Customers].DEFAULTMEMBER', SOLVE_ORDER=-400\r\n"
            + " MEMBER [Education Level].[*TOTAL_MEMBER_SEL~AGG] AS 'AGGREGATE([*CJ_ROW_AXIS])', SOLVE_ORDER=-100\r\n"
            + " MEMBER [Measures].[*FORMATTED_MEASURE_0] AS '[Measures].[Unit Sales]', FORMAT_STRING = 'Standard', SOLVE_ORDER=500\r\n"
            + " SELECT\r\n" + " [*BASE_MEMBERS__Measures_] ON COLUMNS\r\n" + " , NON EMPTY\r\n"
            + " UNION(CROSSJOIN({[Education Level].[*TOTAL_MEMBER_SEL~AGG]},{([Customers].[*DEFAULT_MEMBER])}),[*SORTED_ROW_AXIS]) ON ROWS\r\n"
            + " FROM [Sales]\r\n" + " WHERE ([*CJ_SLICER_AXIS])";

    try {
      ArrayList<String> strings = executeOlapQuery( mdx );
      assertTrue( strings.get( 19 ), strings.get( 19 ).contains( "EvalForSlicer invoked 6 times" ) );
      assertTrue( strings.get( 19 ), strings.get( 19 ).contains( "AggregateFunDef invoked 4 times" ) );
    } finally {
      MondrianProperties.instance().CompoundSlicerMemberSolveOrder.set( original );
    }
  }

  private ArrayList<String> executeOlapQuery( String mdx ) throws SQLException {
    OlapConnection connection = TestContext.instance().getOlap4jConnection();
    final CacheControl cacheControl = TestContext.instance().getConnection().getCacheControl( null );

    // Flush the entire cache.
    final Cube salesCube = TestContext.instance().getConnection().getSchema().lookupCube( "Sales", true );
    final CacheControl.CellRegion measuresRegion = cacheControl.createMeasuresRegion( salesCube );
    cacheControl.flush( measuresRegion );

    final OlapStatement statement = connection.createStatement();

    final ArrayList<String> strings = new ArrayList<String>();
    ( (mondrian.server.Statement) statement ).enableProfiling( new ProfileHandler() {
      public void explain( String plan, QueryTiming timing ) {
        strings.add( plan );
        strings.add( String.valueOf( timing ) );
      }
    } );

    CellSet cellSet = statement.executeOlapQuery( mdx );
    cellSet.close();
    return strings;
  }
}

// End ExplainPlanTest.java
