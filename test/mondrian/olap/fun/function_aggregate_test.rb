# This software is subject to the terms of the Eclipse Public License v1.0
# Agreement, available at the following URL:
# http://www.eclipse.org/legal/epl-v10.html.
# You must accept the terms of that agreement to use this software.
#
# Copyright (C) 2003-2005 Julian Hyde
# Copyright (C) 2005-2021 Hitachi Vantara and others
# Copyright (C) 2026 eazyBI
# All Rights Reserved.

# frozen_string_literal: true

require_relative "../../../test_helper"

# Java: mondrian/olap/fun/FunctionTest.java
describe "Aggregate and Statistical Functions" do
  before(:all) do
    create_olap_connection
  end

  describe "Aggregate" do
    # Java: FunctionTest#testAggregateDepends
    it "depends on correct hierarchies" do
      skip "assertExprDependsOn not yet available"
      # TODO: assertExprDependsOn not yet available
    end

    # Java: FunctionTest#testAggregate
    it "aggregates CA and OR store sales" do
      assert_query_returns @olap, <<~MDX, <<~RESULT
        WITH MEMBER [Store].[CA plus OR] AS 'AGGREGATE({[Store].[USA].[CA], [Store].[USA].[OR]})'
        SELECT {[Measures].[Unit Sales], [Measures].[Store Sales]} ON COLUMNS,
              {[Store].[USA].[CA], [Store].[USA].[OR], [Store].[CA plus OR]} ON ROWS
        FROM Sales
        WHERE ([1997].[Q1])
      MDX
        Axis #0:
        {[Time].[1997].[Q1]}
        Axis #1:
        {[Measures].[Unit Sales]}
        {[Measures].[Store Sales]}
        Axis #2:
        {[Store].[USA].[CA]}
        {[Store].[USA].[OR]}
        {[Store].[CA plus OR]}
        Row #0: 16,890
        Row #0: 36,175.20
        Row #1: 19,287
        Row #1: 40,170.29
        Row #2: 36,177
        Row #2: 76,345.49
      RESULT
    end

    # Java: FunctionTest#testAggregate2
    it "aggregates half-year sales across store states" do
      assert_query_returns @olap, <<~MDX, <<~RESULT
        WITH
          Member [Time].[Time].[1st Half Sales] AS 'Aggregate({Time.[1997].[Q1], Time.[1997].[Q2]})'
          Member [Time].[Time].[2nd Half Sales] AS 'Aggregate({Time.[1997].[Q3], Time.[1997].[Q4]})'
          Member [Time].[Time].[Difference] AS 'Time.[2nd Half Sales] - Time.[1st Half Sales]'
        SELECT
           { [Store].[Store State].Members} ON COLUMNS,
           { Time.[1st Half Sales], Time.[2nd Half Sales], Time.[Difference]} ON ROWS
        FROM Sales
        WHERE [Measures].[Store Sales]
      MDX
        Axis #0:
        {[Measures].[Store Sales]}
        Axis #1:
        {[Store].[Canada].[BC]}
        {[Store].[Mexico].[DF]}
        {[Store].[Mexico].[Guerrero]}
        {[Store].[Mexico].[Jalisco]}
        {[Store].[Mexico].[Veracruz]}
        {[Store].[Mexico].[Yucatan]}
        {[Store].[Mexico].[Zacatecas]}
        {[Store].[USA].[CA]}
        {[Store].[USA].[OR]}
        {[Store].[USA].[WA]}
        Axis #2:
        {[Time].[1st Half Sales]}
        {[Time].[2nd Half Sales]}
        {[Time].[Difference]}
        Row #0:
        Row #0:
        Row #0:
        Row #0:
        Row #0:
        Row #0:
        Row #0:
        Row #0: 74,571.95
        Row #0: 71,943.17
        Row #0: 125,779.50
        Row #1:
        Row #1:
        Row #1:
        Row #1:
        Row #1:
        Row #1:
        Row #1:
        Row #1: 84,595.89
        Row #1: 70,333.90
        Row #1: 138,013.72
        Row #2:
        Row #2:
        Row #2:
        Row #2:
        Row #2:
        Row #2:
        Row #2:
        Row #2: 10,023.94
        Row #2: -1,609.27
        Row #2: 12,234.22
      RESULT
    end

    # Java: FunctionTest#testAggregateWithIIF
    it "aggregate with IIF condition" do
      assert_query_returns @olap, <<~MDX, <<~RESULT
        with member store.foo as 'iif(3>1,aggregate({[Store].[All Stores].[USA].[OR]}),aggregate({[Store].[All Stores].[USA].[CA]}))'
        select {store.foo} on 0 from sales
      MDX
        Axis #0:
        {}
        Axis #1:
        {[Store].[foo]}
        Row #0: 67,659
      RESULT
    end

    # Java: FunctionTest#testAggregate2AllMembers
    it "aggregates half-year sales with AllMembers" do
      assert_query_returns @olap, <<~MDX, <<~RESULT
        WITH
          Member [Time].[Time].[1st Half Sales] AS 'Aggregate({Time.[1997].[Q1], Time.[1997].[Q2]})'
          Member [Time].[Time].[2nd Half Sales] AS 'Aggregate({Time.[1997].[Q3], Time.[1997].[Q4]})'
          Member [Time].[Time].[Difference] AS 'Time.[2nd Half Sales] - Time.[1st Half Sales]'
        SELECT
           { [Store].[Store State].AllMembers} ON COLUMNS,
           { Time.[1st Half Sales], Time.[2nd Half Sales], Time.[Difference]} ON ROWS
        FROM Sales
        WHERE [Measures].[Store Sales]
      MDX
        Axis #0:
        {[Measures].[Store Sales]}
        Axis #1:
        {[Store].[Canada].[BC]}
        {[Store].[Mexico].[DF]}
        {[Store].[Mexico].[Guerrero]}
        {[Store].[Mexico].[Jalisco]}
        {[Store].[Mexico].[Veracruz]}
        {[Store].[Mexico].[Yucatan]}
        {[Store].[Mexico].[Zacatecas]}
        {[Store].[USA].[CA]}
        {[Store].[USA].[OR]}
        {[Store].[USA].[WA]}
        Axis #2:
        {[Time].[1st Half Sales]}
        {[Time].[2nd Half Sales]}
        {[Time].[Difference]}
        Row #0:
        Row #0:
        Row #0:
        Row #0:
        Row #0:
        Row #0:
        Row #0:
        Row #0: 74,571.95
        Row #0: 71,943.17
        Row #0: 125,779.50
        Row #1:
        Row #1:
        Row #1:
        Row #1:
        Row #1:
        Row #1:
        Row #1:
        Row #1: 84,595.89
        Row #1: 70,333.90
        Row #1: 138,013.72
        Row #2:
        Row #2:
        Row #2:
        Row #2:
        Row #2:
        Row #2:
        Row #2:
        Row #2: 10,023.94
        Row #2: -1,609.27
        Row #2: 12,234.22
      RESULT
    end

    # Java: FunctionTest#testAggregateToSimulateCompoundSlicer
    it "simulates compound slicer with aggregate" do
      assert_query_returns @olap, <<~MDX, <<~RESULT
        WITH MEMBER [Time].[Time].[1997 H1] as 'Aggregate({[Time].[1997].[Q1], [Time].[1997].[Q2]})'
          MEMBER [Education Level].[College or higher] as 'Aggregate({[Education Level].[Bachelors Degree], [Education Level].[Graduate Degree]})'
        SELECT {[Measures].[Unit Sales], [Measures].[Store Sales]} on columns,
          {[Product].children} on rows
        FROM [Sales]
        WHERE ([Time].[1997 H1], [Education Level].[College or higher], [Gender].[F])
      MDX
        Axis #0:
        {[Time].[1997 H1], [Education Level].[College or higher], [Gender].[F]}
        Axis #1:
        {[Measures].[Unit Sales]}
        {[Measures].[Store Sales]}
        Axis #2:
        {[Product].[Drink]}
        {[Product].[Food]}
        {[Product].[Non-Consumable]}
        Row #0: 1,797
        Row #0: 3,620.49
        Row #1: 15,002
        Row #1: 31,931.88
        Row #2: 3,845
        Row #2: 8,173.22
      RESULT
    end

    # Java: FunctionTest#testMultiselectCalculations
    # Tests behavior where CurrentMember occurs in calculated members and that
    # member is a set. Mondrian's behavior is consistent with MSAS 2K: it
    # returns zeroes. SSAS 2005 returns an error.
    it "multiselect calculations with declining stores count" do
      assert_query_returns @olap, <<~MDX, <<~RESULT
        WITH
        MEMBER [Measures].[Declining Stores Count] AS
         ' Count(Filter(Descendants(Store.CurrentMember, Store.[Store Name]), [Store Sales] < ([Store Sales],Time.Time.PrevMember))) '
         MEMBER
          [Store].[XL_QZX] AS 'Aggregate ({ [Store].[All Stores].[USA].[WA] , [Store].[All Stores].[USA].[CA] })'
        SELECT
          NON EMPTY HIERARCHIZE(AddCalculatedMembers({DrillDownLevel({[Product].[All Products]})}))
            DIMENSION PROPERTIES PARENT_UNIQUE_NAME ON COLUMNS
        FROM [Sales]
        WHERE ([Measures].[Declining Stores Count], [Time].[1998].[Q3], [Store].[XL_QZX])
      MDX
        Axis #0:
        {[Measures].[Declining Stores Count], [Time].[1998].[Q3], [Store].[XL_QZX]}
        Axis #1:
        {[Product].[All Products]}
        {[Product].[Drink]}
        {[Product].[Food]}
        {[Product].[Non-Consumable]}
        Row #0: .00
        Row #0: .00
        Row #0: .00
        Row #0: .00
      RESULT
    end
  end

  describe "Avg" do
    # Java: FunctionTest#testAvg
    it "computes average store sales across USA children" do
      assert_expression_returns @olap,
        "AVG({[Store].[All Stores].[USA].children},[Measures].[Store Sales])",
        "188,412.71"
    end
  end

  describe "Correlation" do
    # Java: FunctionTest#testCorrelation
    it "computes correlation between unit sales and store sales" do
      assert_expression_returns @olap,
        "Correlation({[Store].[All Stores].[USA].children}, [Measures].[Unit Sales], [Measures].[Store Sales]) * 1000000",
        "999,906"
    end
  end

  describe "Count" do
    # Java: FunctionTest#testCount
    it "counts members including empty" do
      # The depends-on assertions use assertExprDependsOn which is not available
      assert_expression_returns @olap,
        "count({[Promotion Media].[Media Type].members})", "14"

      # applied to an empty set
      assert_expression_returns @olap,
        "count({[Gender].Parent}, IncludeEmpty)", "0"
    end

    # Java: FunctionTest#testCountExcludeEmpty
    it "counts excluding empty with crossjoin" do
      assert_query_returns @olap, <<~MDX, <<~RESULT
        with member [Measures].[Promo Count] as
         ' Count(Crossjoin({[Measures].[Unit Sales]},
         {[Promotion Media].[Media Type].members}), EXCLUDEEMPTY)'
        select {[Measures].[Unit Sales], [Measures].[Promo Count]} on columns,
         {[Product].[Drink].[Beverages].[Carbonated Beverages].[Soda].children} on rows
        from Sales
      MDX
        Axis #0:
        {}
        Axis #1:
        {[Measures].[Unit Sales]}
        {[Measures].[Promo Count]}
        Axis #2:
        {[Product].[Drink].[Beverages].[Carbonated Beverages].[Soda].[Excellent]}
        {[Product].[Drink].[Beverages].[Carbonated Beverages].[Soda].[Fabulous]}
        {[Product].[Drink].[Beverages].[Carbonated Beverages].[Soda].[Skinner]}
        {[Product].[Drink].[Beverages].[Carbonated Beverages].[Soda].[Token]}
        {[Product].[Drink].[Beverages].[Carbonated Beverages].[Soda].[Washington]}
        Row #0: 738
        Row #0: 14
        Row #1: 632
        Row #1: 13
        Row #2: 655
        Row #2: 14
        Row #3: 735
        Row #3: 14
        Row #4: 647
        Row #4: 12
      RESULT

      # applied to an empty set
      assert_expression_returns @olap,
        "count({[Gender].Parent}, ExcludeEmpty)", "0"
    end

    # Java: FunctionTest#testCountExcludeEmptyNull
    # Tests that the 'null' value is regarded as empty, even if the
    # underlying cell has fact table rows.
    it "treats null as empty for EXCLUDEEMPTY" do
      assert_query_returns @olap, <<~MDX, <<~RESULT
        WITH MEMBER [Measures].[Foo] AS
            Iif([Time].CurrentMember.Name = 'Q2', 1, NULL)
          MEMBER [Measures].[Bar] AS
            Iif([Time].CurrentMember.Name = 'Q2', 1, 0)
          Member [Time].[Time].[CountExc] AS
            Count([Time].[1997].Children, EXCLUDEEMPTY),
            SOLVE_ORDER = 2
          Member [Time].[Time].[CountInc] AS
            Count([Time].[1997].Children, INCLUDEEMPTY),
            SOLVE_ORDER = 2
        SELECT {[Measures].[Foo],
           [Measures].[Bar],
           [Measures].[Unit Sales]} ON 0,
          {[Time].[1997].Children,
           [Time].[CountExc],
           [Time].[CountInc]} ON 1
        FROM [Sales]
      MDX
        Axis #0:
        {}
        Axis #1:
        {[Measures].[Foo]}
        {[Measures].[Bar]}
        {[Measures].[Unit Sales]}
        Axis #2:
        {[Time].[1997].[Q1]}
        {[Time].[1997].[Q2]}
        {[Time].[1997].[Q3]}
        {[Time].[1997].[Q4]}
        {[Time].[CountExc]}
        {[Time].[CountInc]}
        Row #0:
        Row #0: 0
        Row #0: 66,291
        Row #1: 1
        Row #1: 1
        Row #1: 62,610
        Row #2:
        Row #2: 0
        Row #2: 65,848
        Row #3:
        Row #3: 0
        Row #3: 72,024
        Row #4: 1
        Row #4: 4
        Row #4: 4
        Row #5: 4
        Row #5: 4
        Row #5: 4
      RESULT
    end

    # Java: FunctionTest#testCountExcludeEmptyOnCubeWithNoCountFacts
    # Bug MONDRIAN-710: Count with ExcludeEmpty throws an exception when
    # the cube does not have a factCountMeasure.
    it "counts exclude empty on cube with no count facts" do
      assert_query_returns @olap, <<~MDX, <<~RESULT
        WITH
          MEMBER [Measures].[count] AS '
            COUNT([Store Type].[Store Type].MEMBERS, EXCLUDEEMPTY)'
         SELECT
          {[Measures].[count]} ON AXIS(0)
         FROM [Warehouse]
      MDX
        Axis #0:
        {}
        Axis #1:
        {[Measures].[count]}
        Row #0: 5
      RESULT
    end

    # Java: FunctionTest#testCountExcludeEmptyOnVirtualCubeWithNoCountFacts
    it "counts exclude empty on virtual cube with no count facts" do
      assert_query_returns @olap, <<~MDX, <<~RESULT
        WITH
          MEMBER [Measures].[count] AS '
            COUNT([Store].MEMBERS, EXCLUDEEMPTY)'
         SELECT
          {[Measures].[count]} ON AXIS(0)
         FROM [Warehouse and Sales]
      MDX
        Axis #0:
        {}
        Axis #1:
        {[Measures].[count]}
        Row #0: 31
      RESULT
    end
  end

  describe "Covariance" do
    # Java: FunctionTest#testCovariance
    it "computes covariance of unit sales and store sales" do
      assert_expression_returns @olap,
        "Covariance({[Store].[All Stores].[USA].children}, [Measures].[Unit Sales], [Measures].[Store Sales])",
        "1,355,761,899"
    end

    # Java: FunctionTest#testCovarianceN
    it "computes CovarianceN of unit sales and store sales" do
      assert_expression_returns @olap,
        "CovarianceN({[Store].[All Stores].[USA].children}, [Measures].[Unit Sales], [Measures].[Store Sales])",
        "2,033,642,849"
    end
  end

  describe "IIfNumeric" do
    # Java: FunctionTest#testIIfNumeric
    it "returns correct value based on numeric condition" do
      assert_expression_returns @olap,
        "IIf(([Measures].[Unit Sales],[Product].[Drink].[Alcoholic Beverages].[Beer and Wine]) > 100, 45, 32)",
        "45"

      # Compare two members. The system needs to figure out that they are
      # both numeric, and use the right overloaded version of ">", otherwise
      # we'll get a ClassCastException at runtime.
      assert_expression_returns @olap,
        "IIf([Measures].[Unit Sales] > [Measures].[Store Sales], 45, 32)",
        "32"
    end
  end

  describe "Max" do
    # Java: FunctionTest#testMax
    it "computes max store sales across USA children" do
      assert_expression_returns @olap,
        "MAX({[Store].[All Stores].[USA].children},[Measures].[Store Sales])",
        "263,793.22"
    end

    # Java: FunctionTest#testMaxNegative
    # Bug 1771928, "Max() works incorrectly with negative values"
    it "handles negative values correctly" do
      assert_query_returns @olap, <<~MDX, <<~RESULT
        with
          member [Customers].[Neg] as '-1'
          member [Customers].[Min] as 'Min({[Customers].[Neg]})'
          member [Customers].[Max] as 'Max({[Customers].[Neg]})'
        select {[Customers].[Neg],[Customers].[Min],[Customers].[Max]} on 0
        from Sales
      MDX
        Axis #0:
        {}
        Axis #1:
        {[Customers].[Neg]}
        {[Customers].[Min]}
        {[Customers].[Max]}
        Row #0: -1
        Row #0: -1
        Row #0: -1
      RESULT
    end
  end

  describe "Median" do
    # Java: FunctionTest#testMedian
    it "computes median store sales" do
      assert_expression_returns @olap,
        "MEDIAN({[Store].[All Stores].[USA].children},[Measures].[Store Sales])",
        "159,167.84"

      # single value
      assert_expression_returns @olap,
        "MEDIAN({[Store].[All Stores].[USA]}, [Measures].[Store Sales])",
        "565,238.13"
    end

    # Java: FunctionTest#testMedian2
    it "computes median across time members with half-year aggregates" do
      assert_query_returns @olap, <<~MDX, <<~RESULT
        WITH
           Member [Time].[Time].[1st Half Sales] AS 'Sum({[Time].[1997].[Q1], [Time].[1997].[Q2]})'
           Member [Time].[Time].[2nd Half Sales] AS 'Sum({[Time].[1997].[Q3], [Time].[1997].[Q4]})'
           Member [Time].[Time].[Median] AS 'Median(Time.[Time].Members)'
        SELECT
           NON EMPTY { [Store].[Store Name].Members} ON COLUMNS,
           { [Time].[1st Half Sales], [Time].[2nd Half Sales], [Time].[Median]} ON ROWS
        FROM Sales
        WHERE [Measures].[Store Sales]
      MDX
        Axis #0:
        {[Measures].[Store Sales]}
        Axis #1:
        {[Store].[USA].[CA].[Beverly Hills].[Store 6]}
        {[Store].[USA].[CA].[Los Angeles].[Store 7]}
        {[Store].[USA].[CA].[San Diego].[Store 24]}
        {[Store].[USA].[CA].[San Francisco].[Store 14]}
        {[Store].[USA].[OR].[Portland].[Store 11]}
        {[Store].[USA].[OR].[Salem].[Store 13]}
        {[Store].[USA].[WA].[Bellingham].[Store 2]}
        {[Store].[USA].[WA].[Bremerton].[Store 3]}
        {[Store].[USA].[WA].[Seattle].[Store 15]}
        {[Store].[USA].[WA].[Spokane].[Store 16]}
        {[Store].[USA].[WA].[Tacoma].[Store 17]}
        {[Store].[USA].[WA].[Walla Walla].[Store 22]}
        {[Store].[USA].[WA].[Yakima].[Store 23]}
        Axis #2:
        {[Time].[1st Half Sales]}
        {[Time].[2nd Half Sales]}
        {[Time].[Median]}
        Row #0: 20,801.04
        Row #0: 25,421.41
        Row #0: 26,275.11
        Row #0: 2,074.39
        Row #0: 28,519.18
        Row #0: 43,423.99
        Row #0: 2,140.99
        Row #0: 25,502.08
        Row #0: 25,293.50
        Row #0: 23,265.53
        Row #0: 34,926.91
        Row #0: 2,159.60
        Row #0: 12,490.89
        Row #1: 24,949.20
        Row #1: 29,123.87
        Row #1: 28,156.03
        Row #1: 2,366.79
        Row #1: 26,539.61
        Row #1: 43,794.29
        Row #1: 2,598.24
        Row #1: 27,394.22
        Row #1: 27,350.57
        Row #1: 26,368.93
        Row #1: 39,917.05
        Row #1: 2,546.37
        Row #1: 11,838.34
        Row #2: 4,577.35
        Row #2: 5,211.38
        Row #2: 4,722.87
        Row #2: 398.24
        Row #2: 5,039.50
        Row #2: 7,374.59
        Row #2: 410.22
        Row #2: 4,924.04
        Row #2: 4,569.13
        Row #2: 4,511.68
        Row #2: 6,630.91
        Row #2: 419.51
        Row #2: 2,169.48
      RESULT
    end
  end

  describe "Percentile" do
    # Java: FunctionTest#testPercentile
    it "computes percentile at various levels" do
      # same result as median
      assert_expression_returns @olap,
        "Percentile({[Store].[All Stores].[USA].children}, [Measures].[Store Sales], 50)",
        "159,167.84"

      # same result as min
      assert_expression_returns @olap,
        "Percentile({[Store].[All Stores].[USA].children}, [Measures].[Store Sales], 0)",
        "142,277.07"

      # same result as max
      assert_expression_returns @olap,
        "Percentile({[Store].[All Stores].[USA].children}, [Measures].[Store Sales], 100)",
        "263,793.22"

      # check some real percentile cases
      assert_expression_returns @olap,
        "Percentile({[Store].[All Stores].[USA].[WA].children}, [Measures].[Store Sales], 50)",
        "49,634.46"

      # the next two results correspond to MS Excel 2013.
      # See MONDRIAN-2343 jira issue.
      assert_expression_returns @olap,
        "Percentile({[Store].[All Stores].[USA].[WA].children}, [Measures].[Store Sales], 100/7*2)",
        "18,732.09"

      assert_expression_returns @olap,
        "Percentile({[Store].[All Stores].[USA].[WA].children}, [Measures].[Store Sales], 95)",
        "68,259.66"
    end

    # Java: FunctionTest#testPercentileBugMondrian1045
    # Bug MONDRIAN-1045: "When I use the Percentile function it cracks when
    # there's only 1 register"
    it "handles single member set" do
      assert_expression_returns @olap,
        "Percentile({[Store].[All Stores].[USA]}, [Measures].[Store Sales], 50)",
        "565,238.13"

      assert_expression_returns @olap,
        "Percentile({[Store].[All Stores].[USA]}, [Measures].[Store Sales], 40)",
        "565,238.13"

      assert_expression_returns @olap,
        "Percentile({[Store].[All Stores].[USA]}, [Measures].[Store Sales], 95)",
        "565,238.13"
    end
  end

  describe "Min" do
    # Java: FunctionTest#testMin
    it "computes min store sales across USA children" do
      assert_expression_returns @olap,
        "MIN({[Store].[All Stores].[USA].children},[Measures].[Store Sales])",
        "142,277.07"
    end

    # Java: FunctionTest#testMinTuple
    it "computes min with tuple expression" do
      assert_expression_returns @olap,
        "Min([Customers].[All Customers].[USA].Children, ([Measures].[Unit Sales], [Gender].[All Gender].[F]))",
        "33,036"
    end
  end

  describe "Stdev" do
    # Java: FunctionTest#testStdev
    it "computes standard deviation of store sales" do
      assert_expression_returns @olap,
        "STDEV({[Store].[All Stores].[USA].children},[Measures].[Store Sales])",
        "65,825.45"
    end

    # Java: FunctionTest#testStdevP
    it "computes population standard deviation of store sales" do
      assert_expression_returns @olap,
        "STDEVP({[Store].[All Stores].[USA].children},[Measures].[Store Sales])",
        "53,746.26"
    end
  end

  describe "Sum and Value" do
    # Java: FunctionTest#testSumNoExp
    it "sums without explicit expression" do
      assert_expression_returns @olap,
        "SUM({[Promotion Media].[Media Type].members})", "266,773"
    end

    # Java: FunctionTest#testValue
    it "returns VALUE property of a measure" do
      # VALUE is usually a cell property, not a member property.
      # We allow it because MS documents it as a function, <Member>.VALUE.
      assert_expression_returns @olap,
        "[Measures].[Store Sales].VALUE", "565,238.13"

      # The depends-on assertion for VALUE uses assertExprDependsOn which is not available

      # We do not allow FORMATTED_VALUE.
      assert_query_raises @olap,
        "WITH MEMBER [Measures].[_Expr] AS '[Measures].[Store Sales].FORMATTED_VALUE' SELECT {[Measures].[_Expr]} ON 0 FROM [Sales]",
        "mondrian gave exception while parsing query"

      assert_expression_returns @olap,
        "[Measures].[Store Sales].NAME", "Store Sales"

      # MS says that ID and KEY are standard member properties for
      # OLE DB for OLAP, but not for XML/A. We don't support them.
      assert_query_raises @olap,
        "WITH MEMBER [Measures].[_Expr] AS '[Measures].[Store Sales].ID' SELECT {[Measures].[_Expr]} ON 0 FROM [Sales]",
        "mondrian gave exception while parsing query"

      # Error for KEY is slightly different than for ID. It doesn't matter
      # very much.
      #
      # The error is different because KEY is registered as a Mondrian
      # builtin property, but ID isn't. KEY cannot be evaluated in
      # "<MEMBER>.KEY" syntax because there is not function defined. For
      # other builtin properties, such as NAME, CAPTION there is a builtin
      # function.
      assert_query_raises @olap,
        "WITH MEMBER [Measures].[_Expr] AS '[Measures].[Store Sales].KEY' SELECT {[Measures].[_Expr]} ON 0 FROM [Sales]",
        "mondrian gave exception while parsing query"

      assert_expression_returns @olap,
        "[Measures].[Store Sales].CAPTION", "Store Sales"
    end
  end

  describe "Var" do
    # Java: FunctionTest#testVar
    it "computes variance of store sales" do
      assert_expression_returns @olap,
        "VAR({[Store].[All Stores].[USA].children},[Measures].[Store Sales])",
        "4,332,990,493.69"
    end

    # Java: FunctionTest#testVarP
    it "computes population variance of store sales" do
      assert_expression_returns @olap,
        "VARP({[Store].[All Stores].[USA].children},[Measures].[Store Sales])",
        "2,888,660,329.13"
    end
  end
end
