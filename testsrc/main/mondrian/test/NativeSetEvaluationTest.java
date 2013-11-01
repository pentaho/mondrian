/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2005-2013 Pentaho
// All Rights Reserved.
*/
package mondrian.test;

import mondrian.olap.MondrianProperties;
import mondrian.rolap.BatchTestCase;
import mondrian.spi.Dialect.DatabaseProduct;

/**
 * Test native evaluation of supported set operations.
 *
 * <p>
 */
public class NativeSetEvaluationTest extends BatchTestCase {

    /**
     * Checks that a given MDX query results in a particular SQL statement
     * being generated.
     *
     * @param mdxQuery MDX query
     * @param patterns Set of patterns for expected SQL statements
     */
    protected void assertQuerySql(
        String mdxQuery,
        SqlPattern[] patterns)
    {
        assertQuerySqlOrNot(
            getTestContext(), mdxQuery, patterns, false, true, true);
    }

    /**
     *  we'll reuse this in a few variations
     */
    private static final class NativeTopCountWithAgg {
        static final String mysql =
            "select\n"
            + "    `product_class`.`product_family` as `c0`,\n"
            + "    `product_class`.`product_department` as `c1`,\n"
            + "    `product_class`.`product_category` as `c2`,\n"
            + "    `product_class`.`product_subcategory` as `c3`,\n"
            + "    `product`.`brand_name` as `c4`,\n"
            + "    `product`.`product_name` as `c5`,\n"
            + "    sum(`sales_fact_1997`.`store_sales`) as `c6`\n"
            + "from\n"
            + "    `product` as `product`,\n"
            + "    `product_class` as `product_class`,\n"
            + "    `sales_fact_1997` as `sales_fact_1997`,\n"
            + "    `time_by_day` as `time_by_day`\n"
            + "where\n"
            + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
            + "and\n"
            + "    `sales_fact_1997`.`product_id` = `product`.`product_id`\n"
            + "and\n"
            + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
            // aggregate set
            + "and\n"
            +    "    `time_by_day`.`the_year` = 1997\n"
            + "and\n"
            + "    `time_by_day`.`quarter` in ('Q1', 'Q2', 'Q3')\n"
            + "group by\n"
            + "    `product_class`.`product_family`,\n"
            + "    `product_class`.`product_department`,\n"
            + "    `product_class`.`product_category`,\n"
            + "    `product_class`.`product_subcategory`,\n"
            + "    `product`.`brand_name`,\n"
            + "    `product`.`product_name`\n"
            + "order by\n"
            // top count Measures.[Store Sales]
            + "    `c6` DESC,\n"
            + "    ISNULL(`product_class`.`product_family`) ASC, `product_class`.`product_family` ASC,\n"
            + "    ISNULL(`product_class`.`product_department`) ASC, `product_class`.`product_department` ASC,\n"
            + "    ISNULL(`product_class`.`product_category`) ASC, `product_class`.`product_category` ASC,\n"
            + "    ISNULL(`product_class`.`product_subcategory`) ASC, `product_class`.`product_subcategory` ASC,\n"
            + "    ISNULL(`product`.`brand_name`) ASC, `product`.`brand_name` ASC,\n"
            + "    ISNULL(`product`.`product_name`) ASC, `product`.`product_name` ASC";

        static final String mysqlAgg =
            "select\n"
            + "    `agg_c_14_sales_fact_1997`.`the_year` as `c0`,\n"
            + "    `agg_c_14_sales_fact_1997`.`quarter` as `c1`,\n"
            + "    `product`.`product_name` as `c2`,\n"
            + "    sum(`agg_c_14_sales_fact_1997`.`store_sales`) as `m0`\n"
            + "from\n"
            + "    `agg_c_14_sales_fact_1997` as `agg_c_14_sales_fact_1997`,\n"
            + "    `product` as `product`\n"
            + "where\n"
            + "    `agg_c_14_sales_fact_1997`.`the_year` = 1997\n"
            + "and\n"
            + "    `agg_c_14_sales_fact_1997`.`product_id` = `product`.`product_id`\n"
            + "and\n"
            + "    `product`.`product_name` in ('Hermanos Green Pepper', 'Hilltop Mint Mouthwash')\n"
            + "group by\n"
            + "    `agg_c_14_sales_fact_1997`.`the_year`,\n"
            + "    `agg_c_14_sales_fact_1997`.`quarter`,\n"
            + "    `product`.`product_name`";
        static final String result =
            "Axis #0:\n"
            + "{[Time].[x]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Store Sales]}\n"
            + "{[Measures].[x1]}\n"
            + "{[Measures].[x2]}\n"
            + "{[Measures].[x3]}\n"
            + "Axis #2:\n"
            + "{[Product].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Green Pepper]}\n"
            + "{[Product].[Non-Consumable].[Health and Hygiene].[Bathroom Products].[Mouthwash].[Hilltop].[Hilltop Mint Mouthwash]}\n"
            + "Row #0: 733.40\n"
            + "Row #0: 281.78\n"
            + "Row #0: 165.98\n"
            + "Row #0: 285.64\n"
            + "Row #1: 647.98\n"
            + "Row #1: 264.26\n"
            + "Row #1: 173.76\n"
            + "Row #1: 209.96\n";
    }

    /**
     * Simple enumerated aggregate.
     */
    public void _testNativeTopCountWithAggFlatSet() {
        // Disabled pending merge of changes from a3ff303
        final boolean useAgg =
            MondrianProperties.instance().UseAggregates.get()
            && MondrianProperties.instance().ReadAggregates.get();

        final String mdx =
            "with\n"
            + "member Time.Time.x as Aggregate({[Time].[1997].[Q1] , [Time].[1997].[Q2], [Time].[1997].[Q3]}, [Measures].[Store Sales])\n"
            + "member Measures.x1 as ([Time].[1997].[Q1], [Measures].[Store Sales])\n"
            + "member Measures.x2 as ([Time].[1997].[Q2], [Measures].[Store Sales])\n"
            + "member Measures.x3 as ([Time].[1997].[Q3], [Measures].[Store Sales])\n"
            + " set products as TopCount(Product.[Product Name].Members, 2, Measures.[Store Sales])\n"
            + " SELECT NON EMPTY products ON 1,\n"
            + "NON EMPTY {[Measures].[Store Sales], Measures.x1, Measures.x2, Measures.x3} ON 0\n"
            + "FROM [Sales] where Time.Time.x";

        propSaver.set(propSaver.props.GenerateFormattedSql, true);
        SqlPattern mysqlPattern = useAgg?
            new SqlPattern(
                DatabaseProduct.MYSQL,
                NativeTopCountWithAgg.mysqlAgg,
                NativeTopCountWithAgg.mysqlAgg)
            : new SqlPattern(
                DatabaseProduct.MYSQL,
                NativeTopCountWithAgg.mysql,
                NativeTopCountWithAgg.mysql);
        if (MondrianProperties.instance().EnableNativeTopCount.get()) {
            assertQuerySql(mdx, new SqlPattern[]{mysqlPattern});
        }
        assertQueryReturns(mdx, NativeTopCountWithAgg.result);
    }

    /**
     * Same as above, but using a named set
     */
    public void _testNativeTopCountWithAggMemberEnumSet() {
        // Disabled pending merge of changes from a3ff303
        final boolean useAgg =
            MondrianProperties.instance().UseAggregates.get()
            && MondrianProperties.instance().ReadAggregates.get();

        final String mdx =
            "with set TO_AGGREGATE as '{[Time].[1997].[Q1] , [Time].[1997].[Q2], [Time].[1997].[Q3]}'\n"
            + "member Time.Time.x as Aggregate(TO_AGGREGATE, [Measures].[Store Sales])\n"
            + "member Measures.x1 as ([Time].[1997].[Q1], [Measures].[Store Sales])\n"
            + "member Measures.x2 as ([Time].[1997].[Q2], [Measures].[Store Sales])\n"
            + "member Measures.x3 as ([Time].[1997].[Q3], [Measures].[Store Sales])\n"
            + " set products as TopCount(Product.[Product Name].Members, 2, Measures.[Store Sales])\n"
            + " SELECT NON EMPTY products ON 1,\n"
            + "NON EMPTY {[Measures].[Store Sales], Measures.x1, Measures.x2, Measures.x3} ON 0\n"
            + "FROM [Sales] where Time.Time.x";

        propSaver.set(propSaver.props.GenerateFormattedSql, true);
        SqlPattern mysqlPattern = useAgg?
            new SqlPattern(
                DatabaseProduct.MYSQL,
                NativeTopCountWithAgg.mysqlAgg,
                NativeTopCountWithAgg.mysqlAgg)
            : new SqlPattern(
                DatabaseProduct.MYSQL,
                NativeTopCountWithAgg.mysql,
                NativeTopCountWithAgg.mysql);
        if (propSaver.props.EnableNativeTopCount.get()
            && propSaver.props.EnableNativeNonEmpty.get())
        {
            assertQuerySql(mdx, new SqlPattern[]{mysqlPattern});
        }
        assertQueryReturns(mdx, NativeTopCountWithAgg.result);
    }

    /**
     * Same as above, defined as a range.
     */
    public void _testNativeTopCountWithAggMemberCMRange() {
        // Disabled pending merge of changes from a3ff303
        final boolean useAgg =
            MondrianProperties.instance().UseAggregates.get()
            && MondrianProperties.instance().ReadAggregates.get();
        final String mdx =
            "with set TO_AGGREGATE as '([Time].[1997].[Q1] : [Time].[1997].[Q3])'\n"
            + "member Time.Time.x as Aggregate(TO_AGGREGATE, [Measures].[Store Sales])\n"
            + "member Measures.x1 as ([Time].[1997].[Q1], [Measures].[Store Sales])\n"
            + "member Measures.x2 as ([Time].[1997].[Q2], [Measures].[Store Sales])\n"
            + "member Measures.x3 as ([Time].[1997].[Q3], [Measures].[Store Sales])\n"
            + " set products as TopCount(Product.[Product Name].Members, 2, Measures.[Store Sales])\n"
            + " SELECT NON EMPTY products ON 1,\n"
            + "NON EMPTY {[Measures].[Store Sales], Measures.x1, Measures.x2, Measures.x3} ON 0\n"
            + " FROM [Sales] where Time.Time.x";

          propSaver.set(propSaver.props.GenerateFormattedSql, true);
          SqlPattern mysqlPattern = useAgg?
              new SqlPattern(
                  DatabaseProduct.MYSQL,
                  NativeTopCountWithAgg.mysqlAgg,
                  NativeTopCountWithAgg.mysqlAgg)
              : new SqlPattern(
                  DatabaseProduct.MYSQL,
                  NativeTopCountWithAgg.mysql,
                  NativeTopCountWithAgg.mysql);
          if (propSaver.props.EnableNativeTopCount.get()
              && propSaver.props.EnableNativeNonEmpty.get())
          {
              assertQuerySql(mdx, new SqlPattern[]{mysqlPattern});
          }
          assertQueryReturns(mdx, NativeTopCountWithAgg.result);
    }

    public void _testNativeFilterWithAggDescendants() {
      // Disabled pending merge of changes from a3ff303
      final boolean useAgg =
          MondrianProperties.instance().UseAggregates.get()
          && MondrianProperties.instance().ReadAggregates.get();
      final String mdx =
          "with\n"
          + "  set QUARTERS as Descendants([Time].[1997], [Time].[Time].[Quarter])\n"
          + "  member Time.Time.x as Aggregate(QUARTERS, [Measures].[Store Sales])\n"
          + "  set products as Filter([Product].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].Children, [Measures].[Store Sales] > 700)\n"
          + "  SELECT NON EMPTY products ON 1,\n"
          + "  NON EMPTY {[Measures].[Store Sales]} ON 0\n"
          + "  FROM [Sales] where Time.Time.x";

        final String mysqlQuery =
            "select\n"
            + "    `product_class`.`product_family` as `c0`,\n"
            + "    `product_class`.`product_department` as `c1`,\n"
            + "    `product_class`.`product_category` as `c2`,\n"
            + "    `product_class`.`product_subcategory` as `c3`,\n"
            + "    `product`.`brand_name` as `c4`,\n"
            + "    `product`.`product_name` as `c5`\n"
            + "from\n"
            + "    `product` as `product`,\n"
            + "    `product_class` as `product_class`,\n"
            + (useAgg?
                "    `agg_c_14_sales_fact_1997` as `agg_c_14_sales_fact_1997`\n"
                + "where\n"
                + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
                + "and\n"
                + "    `agg_c_14_sales_fact_1997`.`product_id` = `product`.`product_id`\n"
                + "and\n"
                + "    `agg_c_14_sales_fact_1997`.`the_year` = 1997\n"
                + "and\n"
                + "    `agg_c_14_sales_fact_1997`.`quarter` in ('Q1', 'Q2', 'Q3', 'Q4')\n"
                : "    `sales_fact_1997` as `sales_fact_1997`,\n"
                  + "    `time_by_day` as `time_by_day`\n"
                  + "where\n"
                  + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
                  + "and\n"
                  + "    `sales_fact_1997`.`product_id` = `product`.`product_id`\n"
                  + "and\n"
                  + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
                  + "and\n"
            +    "    `time_by_day`.`the_year` = 1997\n"
            + "and\n"
                  + "    `time_by_day`.`quarter` in ('Q1', 'Q2', 'Q3', 'Q4')\n")
            + "and\n"
            + "    (`product`.`brand_name` = 'Hermanos' and `product_class`.`product_subcategory` = 'Fresh Vegetables' and `product_class`.`product_category` = 'Vegetables' and `product_class`.`product_department` = 'Produce' and `product_class`.`product_family` = 'Food')\n"
            + "group by\n"
            + "    `product_class`.`product_family`,\n"
            + "    `product_class`.`product_department`,\n"
            + "    `product_class`.`product_category`,\n"
            + "    `product_class`.`product_subcategory`,\n"
            + "    `product`.`brand_name`,\n"
            + "    `product`.`product_name`\n"
            + "having\n"
            + (useAgg?
                "    (sum(`agg_c_14_sales_fact_1997`.`store_sales`) > 700)\n"
                : "    (sum(`sales_fact_1997`.`store_sales`) > 700)\n")
            + "order by\n"
            + "    ISNULL(`product_class`.`product_family`) ASC, `product_class`.`product_family` ASC,\n"
            + "    ISNULL(`product_class`.`product_department`) ASC, `product_class`.`product_department` ASC,\n"
            + "    ISNULL(`product_class`.`product_category`) ASC, `product_class`.`product_category` ASC,\n"
            + "    ISNULL(`product_class`.`product_subcategory`) ASC, `product_class`.`product_subcategory` ASC,\n"
            + "    ISNULL(`product`.`brand_name`) ASC, `product`.`brand_name` ASC,\n"
            + "    ISNULL(`product`.`product_name`) ASC, `product`.`product_name` ASC";

        propSaver.set(propSaver.props.GenerateFormattedSql, true);
        SqlPattern mysqlPattern =
            new SqlPattern(
                DatabaseProduct.MYSQL,
                mysqlQuery,
                mysqlQuery);
        if (propSaver.props.EnableNativeFilter.get()
            && propSaver.props.EnableNativeNonEmpty.get())
        {
            assertQuerySql(mdx, new SqlPattern[]{mysqlPattern});
        }
        assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{[Time].[x]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Store Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Broccoli]}\n"
            + "{[Product].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Green Pepper]}\n"
            + "{[Product].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos New Potatos]}\n"
            + "{[Product].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Prepared Salad]}\n"
            + "Row #0: 742.73\n"
            + "Row #1: 922.54\n"
            + "Row #2: 703.80\n"
            + "Row #3: 718.08\n");
   }



    /**
     * Test case for <a href="http://jira.pentaho.com/browse/MONDRIAN-1426">
     * Mondrian-1426:</a> Native top count support for Member expressions
     * in Calculated member slicer
     */
    public void _testNativeTopCountWithMemberOnlySlicer() {
        propSaver.set(propSaver.props.GenerateFormattedSql, true);
        final boolean useAggregates =
            MondrianProperties.instance().UseAggregates.get()
            && MondrianProperties.instance().ReadAggregates.get();
        final String mdx =
            "WITH\n"
            + "  SET TC AS 'TopCount([Product].[Drink].[Alcoholic Beverages].Children, 3, [Measures].[Unit Sales] )'\n"
            + "  MEMBER [Time].[Slicer] as [Time].[1997]\n"
            + "  MEMBER [Store Type].[Slicer] as [Store Type].[Store Type].[Deluxe Supermarket]\n"
            + "\n"
            + "  SELECT NON EMPTY [Measures].[Unit Sales] on 0,\n"
            + "    TC ON 1 \n"
            + "  FROM [Sales] WHERE {([Time].[Slicer], [Store Type].[Slicer])}\n";

        String mysqlQuery =
              "select\n"
              + "    `product_class`.`product_family` as `c0`,\n"
              + "    `product_class`.`product_department` as `c1`,\n"
              + "    `product_class`.`product_category` as `c2`,\n"
              + (useAggregates
              ? ("    sum(`agg_c_14_sales_fact_1997`.`unit_sales`) as `c3`\n"
                + "from\n"
                + "    `product` as `product`,\n"
                + "    `product_class` as `product_class`,\n"
                + "    `agg_c_14_sales_fact_1997` as `agg_c_14_sales_fact_1997`,\n"
                + "    `store` as `store`\n"
                + "where\n"
                + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
                + "and\n"
                + "    `agg_c_14_sales_fact_1997`.`product_id` = `product`.`product_id`\n"
                + "and\n"
                + "    `agg_c_14_sales_fact_1997`.`store_id` = `store`.`store_id`\n"
                + "and\n"
                + "    `store`.`store_type` = 'Deluxe Supermarket'\n"
                + "and\n"
                + "    `agg_c_14_sales_fact_1997`.`the_year` = 1997\n")
              : ("    sum(`sales_fact_1997`.`unit_sales`) as `c3`\n"
                + "from\n"
                + "    `product` as `product`,\n"
                + "    `product_class` as `product_class`,\n"
                + "    `sales_fact_1997` as `sales_fact_1997`,\n"
                + "    `store` as `store`,\n"
                + "    `time_by_day` as `time_by_day`\n"
                + "where\n"
                + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
                + "and\n"
                + "    `sales_fact_1997`.`product_id` = `product`.`product_id`\n"
                + "and\n"
                + "    `sales_fact_1997`.`store_id` = `store`.`store_id`\n"
                + "and\n"
                + "    `store`.`store_type` = `Deluxe Supermarket`\n"
                + "and\n"
                + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
                + "and\n"
                + "    `time_by_day`.`the_year` = 1997\n"))
              + "and\n"
              + "    (`product_class`.`product_department` = 'Alcoholic Beverages' and `product_class`.`product_family` = 'Drink')\n"
              + "group by\n"
              + "    `product_class`.`product_family`,\n"
              + "    `product_class`.`product_department`,\n"
              + "    `product_class`.`product_category`\n"
              + "order by\n"
              + "    `c3` DESC,\n"
              + "    ISNULL(`product_class`.`product_family`) ASC, `product_class`.`product_family` ASC,\n"
              + "    ISNULL(`product_class`.`product_department`) ASC, `product_class`.`product_department` ASC,\n"
              + "    ISNULL(`product_class`.`product_category`) ASC, `product_class`.`product_category` ASC";

        SqlPattern mysqlPattern =
            new SqlPattern(
                DatabaseProduct.MYSQL,
                mysqlQuery,
                mysqlQuery.indexOf("("));
        if (MondrianProperties.instance().EnableNativeTopCount.get()) {
            assertQuerySql(mdx, new SqlPattern[]{mysqlPattern});
        }
        assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{[Time].[Slicer], [Store Type].[Slicer]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[Drink].[Alcoholic Beverages].[Beer and Wine]}\n"
            + "Row #0: 1,910\n");
    }

    /**
     * Test case for <a href="http://jira.pentaho.com/browse/MONDRIAN-1430">
     * Mondrian-1430:</a> Native top count support for + and
     * tuple (Parentheses) expressions in Calculated member slicer
     */
    public void _testNativeTopCountWithParenthesesMemberSlicer() {
        propSaver.set(propSaver.props.GenerateFormattedSql, true);

        final boolean useAggregates =
            MondrianProperties.instance().UseAggregates.get()
            && MondrianProperties.instance().ReadAggregates.get();
        final String mdx =
            "WITH\n"
            + "  SET TC AS 'TopCount([Product].[Drink].[Alcoholic Beverages].Children, 3, [Measures].[Unit Sales] )'\n"
            + "  MEMBER [Time].[Slicer] as [Time].[1997]\n"
            + "  MEMBER [Store Type].[Slicer] as ([Store Type].[Store Type].[Deluxe Supermarket])\n"
            + "\n"
            + "  SELECT NON EMPTY [Measures].[Unit Sales] on 0,\n"
            + "    TC ON 1 \n"
            + "  FROM [Sales] WHERE {([Time].[Slicer], [Store Type].[Slicer])}\n";

        String mysqlQuery =
              "select\n"
              + "    `product_class`.`product_family` as `c0`,\n"
              + "    `product_class`.`product_department` as `c1`,\n"
              + "    `product_class`.`product_category` as `c2`,\n"
              + (useAggregates
              ? ("    sum(`agg_c_14_sales_fact_1997`.`unit_sales`) as `c3`\n"
                + "from\n"
                + "    `product` as `product`,\n"
                + "    `product_class` as `product_class`,\n"
                + "    `agg_c_14_sales_fact_1997` as `agg_c_14_sales_fact_1997`,\n"
                + "    `store` as `store`\n"
                + "where\n"
                + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
                + "and\n"
                + "    `agg_c_14_sales_fact_1997`.`product_id` = `product`.`product_id`\n"
                + "and\n"
                + "    `agg_c_14_sales_fact_1997`.`store_id` = `store`.`store_id`\n"
                + "and\n"
                + "    `store`.`store_type` = 'Deluxe Supermarket'\n"
                + "and\n"
                + "    `agg_c_14_sales_fact_1997`.`the_year` = 1997\n")
              : ("    sum(`sales_fact_1997`.`unit_sales`) as `c3`\n"
                + "from\n"
                + "    `product` as `product`,\n"
                + "    `product_class` as `product_class`,\n"
                + "    `sales_fact_1997` as `sales_fact_1997`,\n"
                + "    `store` as `store`,\n"
                + "    `time_by_day` as `time_by_day`\n"
                + "where\n"
                + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
                + "and\n"
                + "    `sales_fact_1997`.`product_id` = `product`.`product_id`\n"
                + "and\n"
                + "    `sales_fact_1997`.`store_id` = `store`.`store_id`\n"
                + "and\n"
                + "    `store`.`store_type` = `Deluxe Supermarket`\n"
                + "and\n"
                + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
                + "and\n"
                + "    `time_by_day`.`the_year` = 1997\n"))
              + "and\n"
              + "    (`product_class`.`product_department` = 'Alcoholic Beverages' and `product_class`.`product_family` = 'Drink')\n"
              + "group by\n"
              + "    `product_class`.`product_family`,\n"
              + "    `product_class`.`product_department`,\n"
              + "    `product_class`.`product_category`\n"
              + "order by\n"
              + "    `c3` DESC,\n"
              + "    ISNULL(`product_class`.`product_family`) ASC, `product_class`.`product_family` ASC,\n"
              + "    ISNULL(`product_class`.`product_department`) ASC, `product_class`.`product_department` ASC,\n"
              + "    ISNULL(`product_class`.`product_category`) ASC, `product_class`.`product_category` ASC";

        SqlPattern mysqlPattern =
            new SqlPattern(
                DatabaseProduct.MYSQL,
                mysqlQuery,
                mysqlQuery.indexOf("("));
        if (MondrianProperties.instance().EnableNativeTopCount.get()) {
            assertQuerySql(mdx, new SqlPattern[]{mysqlPattern});
        }
        assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{[Time].[Slicer], [Store Type].[Slicer]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[Drink].[Alcoholic Beverages].[Beer and Wine]}\n"
            + "Row #0: 1,910\n");
    }

    /**
     * Test case for <a href="http://jira.pentaho.com/browse/MONDRIAN-1430">
     * Mondrian-1430:</a> Native top count support for + and
     * tuple (Parentheses) expressions in Calculated member slicer
     */
    public void _testNativeTopCountWithMemberSumSlicer() {
        propSaver.set(propSaver.props.GenerateFormattedSql, true);
        final boolean useAggregates =
            MondrianProperties.instance().UseAggregates.get()
            && MondrianProperties.instance().ReadAggregates.get();
        final String mdx =
            "WITH\n"
            + "  SET TC AS 'TopCount([Product].[Drink].[Alcoholic Beverages].Children, 3, [Measures].[Unit Sales] )'\n"
            + "  MEMBER [Time].[Slicer] as [Time].[1997]\n"
            + "  MEMBER [Store Type].[Slicer] as [Store Type].[Store Type].[Deluxe Supermarket] + [Store Type].[Store Type].[Gourmet Supermarket]\n"
            + "\n"
            + "  SELECT NON EMPTY [Measures].[Unit Sales] on 0,\n"
            + "    TC ON 1 \n"
            + "  FROM [Sales] WHERE {([Time].[Slicer], [Store Type].[Slicer])}\n";

        String mysqlQuery =
              "select\n"
              + "    `product_class`.`product_family` as `c0`,\n"
              + "    `product_class`.`product_department` as `c1`,\n"
              + "    `product_class`.`product_category` as `c2`,\n"
              + (useAggregates
              ? ("    sum(`agg_c_14_sales_fact_1997`.`unit_sales`) as `c3`\n"
                + "from\n"
                + "    `product` as `product`,\n"
                + "    `product_class` as `product_class`,\n"
                + "    `agg_c_14_sales_fact_1997` as `agg_c_14_sales_fact_1997`,\n"
                + "    `store` as `store`\n"
                + "where\n"
                + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
                + "and\n"
                + "    `agg_c_14_sales_fact_1997`.`product_id` = `product`.`product_id`\n"
                + "and\n"
                + "    `agg_c_14_sales_fact_1997`.`store_id` = `store`.`store_id`\n"
                + "and\n"
                + "    `store`.`store_type` in ('Deluxe Supermarket', 'Gourmet Supermarket')\n"
                + "and\n"
                + "    `agg_c_14_sales_fact_1997`.`the_year` = 1997\n")
              : ("    sum(`sales_fact_1997`.`unit_sales`) as `c3`\n"
                + "from\n"
                + "    `product` as `product`,\n"
                + "    `product_class` as `product_class`,\n"
                + "    `sales_fact_1997` as `sales_fact_1997`,\n"
                + "    `store` as `store`,\n"
                + "    `time_by_day` as `time_by_day`\n"
                + "where\n"
                + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
                + "and\n"
                + "    `sales_fact_1997`.`product_id` = `product`.`product_id`\n"
                + "and\n"
                + "    `sales_fact_1997`.`store_id` = `store`.`store_id`\n"
                + "and\n"
                + "    `store`.`store_type` in ('Deluxe Supermarket', 'Gourmet Supermarket')\n"
                + "and\n"
                + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
                + "and\n"
                + "    `time_by_day`.`the_year` = 1997\n"))
              + "and\n"
              + "    (`product_class`.`product_department` = 'Alcoholic Beverages' and `product_class`.`product_family` = 'Drink')\n"
              + "group by\n"
              + "    `product_class`.`product_family`,\n"
              + "    `product_class`.`product_department`,\n"
              + "    `product_class`.`product_category`\n"
              + "order by\n"
              + "    `c3` DESC,\n"
              + "    ISNULL(`product_class`.`product_family`) ASC, `product_class`.`product_family` ASC,\n"
              + "    ISNULL(`product_class`.`product_department`) ASC, `product_class`.`product_department` ASC,\n"
              + "    ISNULL(`product_class`.`product_category`) ASC, `product_class`.`product_category` ASC";

        if (MondrianProperties.instance().EnableNativeTopCount.get()) {
            SqlPattern mysqlPattern =
                new SqlPattern(
                    DatabaseProduct.MYSQL,
                    mysqlQuery,
                    mysqlQuery.indexOf("("));
            assertQuerySql(mdx, new SqlPattern[]{mysqlPattern});
        }
        assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{[Time].[Slicer], [Store Type].[Slicer]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[Drink].[Alcoholic Beverages].[Beer and Wine]}\n"
            + "Row #0: 2,435\n");
    }

    /**
     * Aggregate with default measure and TopCount without measure argument.
     */
    public void _testAggTCNoExplicitMeasure() {
        propSaver.set(propSaver.props.GenerateFormattedSql, true);
        final String mdx =
            "WITH\n"
            + "  SET TC AS 'TopCount([Product].[Drink].[Alcoholic Beverages].Children, 3)'\n"
            + "  MEMBER [Store Type].[Store Type].[Slicer] as Aggregate([Store Type].[Store Type].Members)\n"
            + "\n"
            + "  SELECT NON EMPTY [Measures].[Unit Sales] on 0,\n"
            + "    TC ON 1 \n"
            + "  FROM [Sales] WHERE [Store Type].[Slicer]\n";
        assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{[Store Type].[Slicer]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[Drink].[Alcoholic Beverages].[Beer and Wine]}\n"
            + "Row #0: 6,838\n");
    }

    /**
     * Crossjoin that uses same dimension as slicer but is independent from it,
     * evaluated via a named set. No loop should happen here.
     */
    public void _testCJSameDimAsSlicerNamedSet() {
        String mdx =
            "WITH\n"
            + "SET ST AS 'TopCount([Store Type].[Store Type].CurrentMember, 5)'\n"
            + "SET TOP_BEV AS 'TopCount([Product].[Drink].Children, 3, [Measures].[Unit Sales])'\n"
            + "SET TC AS TopCount(NonEmptyCrossJoin([Time].[Year].Members, TOP_BEV), 2, [Measures].[Unit Sales])\n"
            + "MEMBER [Product].[Top Drinks] as Aggregate(TC, [Measures].[Unit Sales]) \n"
            + "SET TOP_COUNTRY AS 'TopCount([Customers].[Country].Members, 1, [Measures].[Unit Sales])'\n"
            + "SELECT NON EMPTY [Measures].[Unit Sales] on 0,\n"
            + "  NON EMPTY TOP_COUNTRY ON 1 \n"
            + "FROM [Sales] WHERE [Product].[Top Drinks]";
        assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{[Product].[Products].[Top Drinks]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Customer].[Customers].[USA]}\n"
            + "Row #0: 20,411\n");
    }

    /**
     * Test evaluation loop detection still works after changes to
     * make it more permissable.
     */
    public void testLoopDetection() {
        if (!MondrianProperties.instance().EnableNativeTopCount.get()) {
            return;
        }
        final String mdx =
            "WITH\n"
            + "  SET CJ AS NonEmptyCrossJoin([Store Type].[Store Type].Members, {[Measures].[Unit Sales]})\n"
            + "  SET TC AS 'TopCount([Store Type].[Store Type].Members, 10, [Measures].[Unit Sales])'\n"
            + "  SET TIME_DEP AS 'Generate(CJ, {[Time].[Time].CurrentMember})' \n"
            + "  MEMBER [Time].[Time].[Slicer] as Aggregate(TIME_DEP)\n"
            + "\n"
            + "  SELECT NON EMPTY [Measures].[Unit Sales] on 0,\n"
            + "    TC ON 1 \n"
            + "  FROM [Sales] where [Time].[Slicer]\n";
        assertQueryThrows(
            mdx, "Infinite loop while evaluating calculated member");
    }

    /**
     * Check if getSlicerMembers in native evaluation context
     * doesn't break the results as in MONDRIAN-1187
     */
    public void _testSlicerTuplesPartialCrossJoin() {
        final String mdx =
            "with\n"
            + "set TSET as {NonEmptyCrossJoin({[Time].[1997].[Q1], [Time].[1997].[Q2]}, {[Store Type].[Supermarket]}),\n"
            + " NonEmptyCrossJoin({[Time].[1997].[Q1]}, {[Store Type].[Deluxe Supermarket], [Store Type].[Gourmet Supermarket]}) }\n"
            + " set products as TopCount(Product.[Product Name].Members, 2, Measures.[Store Sales])\n"
            + " SELECT NON EMPTY products ON 1,\n"
            + "NON EMPTY {[Measures].[Store Sales]} ON 0\n"
            + " FROM [Sales]\n"
            + "where TSET";

        final String result =
            "Axis #0:\n"
            + "{[Time].[1997].[Q1], [Store Type].[Supermarket]}\n"
            + "{[Time].[1997].[Q2], [Store Type].[Supermarket]}\n"
            + "{[Time].[1997].[Q1], [Store Type].[Deluxe Supermarket]}\n"
            + "{[Time].[1997].[Q1], [Store Type].[Gourmet Supermarket]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Store Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[Food].[Eggs].[Eggs].[Eggs].[Urban].[Urban Small Eggs]}\n"
            + "{[Product].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Green Pepper]}\n"
            + "Row #0: 332.86\n"
            + "Row #1: 343.54\n";

        assertQueryReturns(mdx, result);
    }

    /**
     * Same as before but without combinations missing in the crossjoin
     */
    public void _testSlicerTuplesFullCrossJoin() {
        final String mdx =
            "with\n"
            + "set TSET as NonEmptyCrossJoin({[Time].[1997].[Q1], [Time].[1997].[Q2]}, {[Store Type].[Supermarket], [Store Type].[Deluxe Supermarket], [Store Type].[Gourmet Supermarket]})\n"
            + " set products as TopCount(Product.[Product Name].Members, 2, Measures.[Store Sales])\n"
            + " SELECT NON EMPTY products ON 1,\n"
            + "NON EMPTY {[Measures].[Store Sales]} ON 0\n"
            + " FROM [Sales]\n"
            + "where TSET";

        String result =
            "Axis #0:\n"
            + "{[Time].[1997].[Q1], [Store Type].[Deluxe Supermarket]}\n"
            + "{[Time].[1997].[Q1], [Store Type].[Gourmet Supermarket]}\n"
            + "{[Time].[1997].[Q1], [Store Type].[Supermarket]}\n"
            + "{[Time].[1997].[Q2], [Store Type].[Deluxe Supermarket]}\n"
            + "{[Time].[1997].[Q2], [Store Type].[Gourmet Supermarket]}\n"
            + "{[Time].[1997].[Q2], [Store Type].[Supermarket]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Store Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[Food].[Eggs].[Eggs].[Eggs].[Urban].[Urban Small Eggs]}\n"
            + "{[Product].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Green Pepper]}\n"
            + "Row #0: 460.02\n"
            + "Row #1: 420.74\n";

        assertQueryReturns(mdx, result);
    }

    /**
     * Now that some native evaluation is supporting aggregated members, we
     * need to push that logic down to the AggStar selection
     */
    public void _testTopCountWithAggregatedMemberAggStar() {
        propSaver.set(
            propSaver.props.UseAggregates,
            true);
        propSaver.set(
            propSaver.props.ReadAggregates,
            true);
        propSaver.set(
            propSaver.props.GenerateFormattedSql,
            true);

        final String mdx =
            "with member [Time.Weekly].x as Aggregate([Time.Weekly].[1997].Children) "
            + "set products as "
            + "'TopCount([Product].[Product Department].Members, 2, "
            + "[Measures].[Store Sales])' "
            + "select NON EMPTY {[Measures].[Store Sales]} ON COLUMNS, "
            + "NON EMPTY [products] ON ROWS "
            + " from [Sales] where [Time.Weekly].[x]";

        final String mysql =
            "select\n"
            + "    `product_class`.`product_family` as `c0`,\n"
            + "    `product_class`.`product_department` as `c1`,\n"
            + "    sum(`agg_pl_01_sales_fact_1997`.`store_sales_sum`) as `c2`\n"
            + "from\n"
            + "    `product` as `product`,\n"
            + "    `product_class` as `product_class`,\n"
            + "    `agg_pl_01_sales_fact_1997` as `agg_pl_01_sales_fact_1997`,\n"
            + "    `time_by_day` as `time_by_day`\n"
            + "where\n"
            + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
            + "and\n"
            + "    `agg_pl_01_sales_fact_1997`.`product_id` = `product`.`product_id`\n"
            + "and\n"
            + "    `agg_pl_01_sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
            + "and\n"
            +    "    `time_by_day`.`the_year` = 1997\n"
            + "and\n"
            + "    `time_by_day`.`week_of_year` in (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52)\n"
            + "group by\n"
            + "    `product_class`.`product_family`,\n"
            + "    `product_class`.`product_department`\n"
            + "order by\n"
            + "    `c2` DESC,\n"
            + "    ISNULL(`product_class`.`product_family`) ASC, `product_class`.`product_family` ASC,\n"
            + "    ISNULL(`product_class`.`product_department`) ASC, `product_class`.`product_department` ASC";

        SqlPattern mysqlPattern =
            new SqlPattern(
                DatabaseProduct.MYSQL,
                mysql,
                mysql);

        if (MondrianProperties.instance().EnableNativeTopCount.get()) {
            assertQuerySql(mdx, new SqlPattern[]{mysqlPattern});
        }

        assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{[Time].[Weekly].[x]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Store Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[Food].[Produce]}\n"
            + "{[Product].[Food].[Snack Foods]}\n"
            + "Row #0: 82,248.42\n"
            + "Row #1: 67,609.82\n");
    }

    /**
     * Test case for <a href="http://jira.pentaho.com/browse/MONDRIAN-1291">
     * Mondrian-1291:</a> NPE on native set with at least two elements and
     * two all members for same dimension in slicer
     */
    public void _testMultipleAllWithInExpr() {
        // set up three hierarchies on same dimension
        final String multiHierarchyCube =
            " <Cube name=\"3StoreHCube\">\n"
            + " <Table name=\"sales_fact_1997\"/>\n"
            + " <Dimension name=\"AltStore\" foreignKey=\"store_id\">\n"
            + " <Hierarchy hasAll=\"true\" primaryKey=\"store_id\" allMemberName=\"All\">\n"
            + " <Table name=\"store\"/>\n"
            + " <Level name=\"Store Name\" column=\"store_name\" uniqueMembers=\"false\"/>\n"
            + " <Level name=\"Store City\" column=\"store_city\" uniqueMembers=\"false\"/>\n"
            + " </Hierarchy>\n"
            + " <Hierarchy name=\"City\" hasAll=\"true\" primaryKey=\"store_id\" allMemberName=\"All\">\n"
            + " <Table name=\"store\"/>\n"
            + " <Level name=\"Store City\" column=\"store_city\" uniqueMembers=\"false\"/>\n"
            + " <Level name=\"Store Name\" column=\"store_name\" uniqueMembers=\"false\"/>\n"
            + " </Hierarchy>\n"
            + " <Hierarchy name=\"State\" hasAll=\"true\" primaryKey=\"store_id\" allMemberName=\"All\">\n"
            + " <Table name=\"store\"/>\n"
            + " <Level name=\"Store State\" column=\"store_state\" uniqueMembers=\"false\"/>\n"
            + " <Level name=\"Store City\" column=\"store_city\" uniqueMembers=\"false\"/>\n"
            + " <Level name=\"Store Name\" column=\"store_name\" uniqueMembers=\"false\"/>\n"
            + " </Hierarchy>\n"
            + " </Dimension>\n"
            + " <DimensionUsage name=\"Time\" source=\"Time\" foreignKey=\"time_id\"/> \n"
            + " <DimensionUsage name=\"Product\" source=\"Product\" foreignKey=\"product_id\"/>\n"
            + " <Measure name=\"Store Sales\" column=\"store_sales\" aggregator=\"sum\" formatString=\"#,###.00\"/>\n"
            + " </Cube>";
        // slicer with multiple elements and two All members
        final String mdx =
            "with member [AltStore].[AllSlicer] as 'Aggregate({[AltStore].[All]})'\n"
            + " member [AltStore.City].[SetSlicer] as 'Aggregate({[AltStore.City].[San Francisco], [AltStore.City].[San Diego]})'\n"
            + " member [AltStore.State].[AllSlicer] as 'Aggregate({[AltStore.State].[All]})'\n"
            + "select {[Time].[1997].[Q1]} ON COLUMNS,\n"
            + " NON EMPTY TopCount(Product.[Product Name].Members, 2, Measures.[Store Sales]) ON ROWS\n"
            + "from [3StoreHCube]\n"
            + "where ([AltStore].[AllSlicer], [AltStore.City].[SetSlicer], [AltStore.State].[AllSlicer])\n";
        String result =
            "Axis #0:\n"
            + "{[AltStore].[AllSlicer], [AltStore.City].[SetSlicer], [AltStore.State].[AllSlicer]}\n"
            + "Axis #1:\n"
            + "{[Time].[1997].[Q1]}\n"
            + "Axis #2:\n"
            + "{[Product].[Food].[Deli].[Meat].[Bologna].[Red Spade].[Red Spade Low Fat Bologna]}\n"
            + "{[Product].[Non-Consumable].[Health and Hygiene].[Bathroom Products].[Mouthwash].[Hilltop].[Hilltop Mint Mouthwash]}\n"
            + "Row #0: 51.60\n"
            + "Row #1: 28.96\n";
        TestContext context =
            getTestContext().create(
                null,
                multiHierarchyCube,
                null,
                null,
                null,
                null);
        context.assertQueryReturns(
            mdx,
            result);
    }


    public void _testCompoundSlicerNativeEval() {
        // MONDRIAN-1404
        propSaver.set(
            propSaver.props.GenerateFormattedSql,
            true);
        propSaver.set(
            propSaver.props.UseAggregates,
            false);
        final String mdx =
            "select NON EMPTY [Customers].[USA].[CA].[San Francisco].Children ON COLUMNS \n"
            + "from [Sales] \n"
            + "where ([Time].[1997].[Q1] : [Time].[1997].[Q3]) \n";

        final String mysql =
            "select\n"
            + "    `customer`.`customer_id` as `c0`,\n"
            + "    CONCAT(`customer`.`fname`, ' ', `customer`.`lname`) as `c1`,\n"
            + "    CONCAT(`customer`.`fname`, ' ', `customer`.`lname`) as `c2`,\n"
            + "    `customer`.`gender` as `c3`,\n"
            + "    `customer`.`marital_status` as `c4`,\n"
            + "    `customer`.`education` as `c5`,\n"
            + "    `customer`.`yearly_income` as `c6`\n"
            + "from\n"
            + "    `time_by_day` as `time_by_day`,\n"
            + "    `sales_fact_1997` as `sales_fact_1997`,\n"
            + "    `customer` as `customer`\n"
            + "where\n"
            + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
            + "and\n"
            +    "    `time_by_day`.`the_year` = 1997\n"
            + "and\n"
            + "    `time_by_day`.`quarter` in ('Q1', 'Q2', 'Q3')\n"
            + "and\n"
            + "    `sales_fact_1997`.`customer_id` = `customer`.`customer_id`\n"
            + "and\n"
            + "    `customer`.`state_province` = 'CA'\n"
            + "and\n"
            + "    `customer`.`city` = 'San Francisco'\n"
            + "group by\n"
            + "    `customer`.`customer_id`,\n"
            + "    CONCAT(`customer`.`fname`, ' ', `customer`.`lname`),\n"
            + "    `customer`.`gender`,\n"
            + "    `customer`.`marital_status`,\n"
            + "    `customer`.`education`,\n"
            + "    `customer`.`yearly_income`\n"
            + "order by\n"
            + "    ISNULL(CONCAT(`customer`.`fname`, ' ', `customer`.`lname`)) ASC, CONCAT(`customer`.`fname`, ' ', `customer`.`lname`) ASC";
        SqlPattern mysqlPattern =
            new SqlPattern(
                DatabaseProduct.MYSQL,
                mysql,
                mysql);

        if (propSaver.props.EnableNativeNonEmpty.get()) {
            assertQuerySql(mdx, new SqlPattern[]{mysqlPattern});
        }

        assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{[Time].[1997].[Q1]}\n"
            + "{[Time].[1997].[Q2]}\n"
            + "{[Time].[1997].[Q3]}\n"
            + "Axis #1:\n"
            + "{[Customers].[USA].[CA].[San Francisco].[Dennis Messer]}\n"
            + "{[Customers].[USA].[CA].[San Francisco].[Esther Logsdon]}\n"
            + "{[Customers].[USA].[CA].[San Francisco].[Karen Moreland]}\n"
            + "{[Customers].[USA].[CA].[San Francisco].[Kent Brant]}\n"
            + "{[Customers].[USA].[CA].[San Francisco].[Louise Wakefield]}\n"
            + "{[Customers].[USA].[CA].[San Francisco].[Reta Mikalas]}\n"
            + "{[Customers].[USA].[CA].[San Francisco].[Tammy Mihalek]}\n"
            + "Row #0: 8\n"
            + "Row #0: 3\n"
            + "Row #0: 13\n"
            + "Row #0: 5\n"
            + "Row #0: 13\n"
            + "Row #0: 10\n"
            + "Row #0: 1\n");
    }

    public void _testSnowflakeDimInSlicerBug1407() {
        // MONDRIAN-1407
        propSaver.set(
            propSaver.props.GenerateFormattedSql,
            true);
        propSaver.set(
            propSaver.props.UseAggregates,
            false);
        final String mdx =
            "select TopCount([Customers].[Name].members, 5, measures.[unit sales]) ON COLUMNS \n"
            + "  from sales where \n"
            + " { [Time].[1997]} * {[Product].[All Products].[Drink], [Product].[All Products].[Food] }";

        final String mysql =
            "select\n"
            + "    `customer`.`country` as `c0`,\n"
            + "    `customer`.`state_province` as `c1`,\n"
            + "    `customer`.`city` as `c2`,\n"
            + "    `customer`.`customer_id` as `c3`,\n"
            + "    CONCAT(`customer`.`fname`, ' ', `customer`.`lname`) as `c4`,\n"
            + "    CONCAT(`customer`.`fname`, ' ', `customer`.`lname`) as `c5`,\n"
            + "    `customer`.`gender` as `c6`,\n"
            + "    `customer`.`marital_status` as `c7`,\n"
            + "    `customer`.`education` as `c8`,\n"
            + "    `customer`.`yearly_income` as `c9`,\n"
            + "    sum(`sales_fact_1997`.`unit_sales`) as `c10`\n"
            + "from\n"
            + "    `customer` as `customer`,\n"
            + "    `sales_fact_1997` as `sales_fact_1997`,\n"
            + "    `time_by_day` as `time_by_day`,\n"
            + "    `product_class` as `product_class`,\n"
            + "    `product` as `product`\n"
            + "where\n"
            + "    `sales_fact_1997`.`customer_id` = `customer`.`customer_id`\n"
            + "and\n"
            + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
            + "and\n"
            + "    `time_by_day`.`the_year` = 1997\n"
            + "and\n"
            + "    `sales_fact_1997`.`product_id` = `product`.`product_id`\n"
            + "and\n"
            + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
            + "and\n"
            + "    `product_class`.`product_family` in ('Drink', 'Food')\n"
            + "group by\n"
            + "    `customer`.`country`,\n"
            + "    `customer`.`state_province`,\n"
            + "    `customer`.`city`,\n"
            + "    `customer`.`customer_id`,\n"
            + "    CONCAT(`customer`.`fname`, ' ', `customer`.`lname`),\n"
            + "    `customer`.`gender`,\n"
            + "    `customer`.`marital_status`,\n"
            + "    `customer`.`education`,\n"
            + "    `customer`.`yearly_income`\n"
            + "order by\n"
            + "    `c10` DESC,\n"
            + "    ISNULL(`customer`.`country`) ASC, `customer`.`country` ASC,\n"
            + "    ISNULL(`customer`.`state_province`) ASC, `customer`.`state_province` ASC,\n"
            + "    ISNULL(`customer`.`city`) ASC, `customer`.`city` ASC,\n"
            + "    ISNULL(CONCAT(`customer`.`fname`, ' ', `customer`.`lname`)) ASC, CONCAT(`customer`.`fname`, ' ', `customer`.`lname`) ASC";
        SqlPattern mysqlPattern =
            new SqlPattern(
                DatabaseProduct.MYSQL,
                mysql,
                mysql);

        if (MondrianProperties.instance().EnableNativeTopCount.get()) {
            assertQuerySql(mdx, new SqlPattern[]{mysqlPattern});
        }

        assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{[Time].[1997], [Product].[Drink]}\n"
            + "{[Time].[1997], [Product].[Food]}\n"
            + "Axis #1:\n"
            + "{[Customers].[USA].[WA].[Spokane].[Mary Francis Benigar]}\n"
            + "{[Customers].[USA].[WA].[Spokane].[James Horvat]}\n"
            + "{[Customers].[USA].[WA].[Spokane].[Wildon Cameron]}\n"
            + "{[Customers].[USA].[WA].[Spokane].[Ida Rodriguez]}\n"
            + "{[Customers].[USA].[WA].[Spokane].[Joann Mramor]}\n"
            + "Row #0: 427\n"
            + "Row #0: 384\n"
            + "Row #0: 366\n"
            + "Row #0: 357\n"
            + "Row #0: 324\n");
    }

    public void _testCompoundSlicerNonUniqueMemberNames1413() {
        // MONDRIAN-1413
        propSaver.set(
            propSaver.props.GenerateFormattedSql,
            true);
        propSaver.set(
            propSaver.props.UseAggregates,
            false);
        final String mdx =
            "select TopCount([Customers].[Name].members, 5, "
            + "measures.[unit sales]) ON COLUMNS \n"
            + "  from sales where \n"
            + "  {[Time.Weekly].[1997].[48].[17] :[Time.Weekly].[1997].[48].[20]} ";

        final String mysql =
            "select\n"
            + "    `customer`.`country` as `c0`,\n"
            + "    `customer`.`state_province` as `c1`,\n"
            + "    `customer`.`city` as `c2`,\n"
            + "    `customer`.`customer_id` as `c3`,\n"
            + "    CONCAT(`customer`.`fname`, ' ', `customer`.`lname`) as `c4`,\n"
            + "    CONCAT(`customer`.`fname`, ' ', `customer`.`lname`) as `c5`,\n"
            + "    `customer`.`gender` as `c6`,\n"
            + "    `customer`.`marital_status` as `c7`,\n"
            + "    `customer`.`education` as `c8`,\n"
            + "    `customer`.`yearly_income` as `c9`,\n"
            + "    sum(`sales_fact_1997`.`unit_sales`) as `c10`\n"
            + "from\n"
            + "    `customer` as `customer`,\n"
            + "    `sales_fact_1997` as `sales_fact_1997`,\n"
            + "    `time_by_day` as `time_by_day`\n"
            + "where\n"
            + "    `sales_fact_1997`.`customer_id` = `customer`.`customer_id`\n"
            + "and\n"
            + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
            + "and\n"
            + "    `time_by_day`.`the_year` = 1997\n"
            + "and\n"
            + "    `time_by_day`.`week_of_year` = 48\n"
            + "and\n"
            + "    `time_by_day`.`day_of_month` in (17, 18, 19, 20)\n"
            + "group by\n"
            + "    `customer`.`country`,\n"
            + "    `customer`.`state_province`,\n"
            + "    `customer`.`city`,\n"
            + "    `customer`.`customer_id`,\n"
            + "    CONCAT(`customer`.`fname`, ' ', `customer`.`lname`),\n"
            + "    `customer`.`gender`,\n"
            + "    `customer`.`marital_status`,\n"
            + "    `customer`.`education`,\n"
            + "    `customer`.`yearly_income`\n"
            + "order by\n"
            + "    `c10` DESC,\n"
            + "    ISNULL(`customer`.`country`) ASC, `customer`.`country` ASC,\n"
            + "    ISNULL(`customer`.`state_province`) ASC, `customer`.`state_province` ASC,\n"
            + "    ISNULL(`customer`.`city`) ASC, `customer`.`city` ASC,\n"
            + "    ISNULL(CONCAT(`customer`.`fname`, ' ', `customer`.`lname`)) ASC, CONCAT(`customer`.`fname`, ' ', `customer`.`lname`) ASC";
        SqlPattern mysqlPattern =
            new SqlPattern(
                DatabaseProduct.MYSQL,
                mysql,
                mysql);

        if (MondrianProperties.instance().EnableNativeTopCount.get()) {
            assertQuerySql(mdx, new SqlPattern[]{mysqlPattern});
        }

        assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{[Time].[Weekly].[1997].[48].[17]}\n"
            + "{[Time].[Weekly].[1997].[48].[18]}\n"
            + "{[Time].[Weekly].[1997].[48].[19]}\n"
            + "{[Time].[Weekly].[1997].[48].[20]}\n"
            + "Axis #1:\n"
            + "{[Customers].[USA].[WA].[Yakima].[Joanne Skuderna]}\n"
            + "{[Customers].[USA].[WA].[Yakima].[Paula Stevens]}\n"
            + "{[Customers].[USA].[WA].[Everett].[Sarah Miller]}\n"
            + "{[Customers].[USA].[OR].[Albany].[Kathryn Chamberlin]}\n"
            + "{[Customers].[USA].[OR].[Salem].[Scott Pavicich]}\n"
            + "Row #0: 37\n"
            + "Row #0: 32\n"
            + "Row #0: 29\n"
            + "Row #0: 28\n"
            + "Row #0: 28\n");
    }

    /**
     * This is a test for
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-1630">MONDRIAN-1630</a>
     *
     * <p>The baseCube was taken out of the evaluator instead of being passed
     * by the caller, which caused the star column not to be found for the
     * level to evaluate natively as part of the set.
     */
    public void testNativeVirtualRestrictedSet() throws Exception {
        final TestContext ctx = getTestContext().legacy().create(
            null, null, null, null, null,
            "  <Role name=\"F-MIS-BE-CLIENT\">\n"
            + "    <SchemaGrant access=\"none\">\n"
            + "      <CubeGrant cube=\"Warehouse and Sales\" access=\"all\">\n"
            + "        <HierarchyGrant hierarchy=\"[Store]\" rollupPolicy=\"partial\" access=\"custom\">\n"
            + "          <MemberGrant member=\"[Store].[All Stores]\" access=\"none\">\n"
            + "          </MemberGrant>\n"
            + "          <MemberGrant member=\"[Store].[USA]\" access=\"all\">\n"
            + "          </MemberGrant>\n"
            + "        </HierarchyGrant>\n"
            + "      </CubeGrant>\n"
            + "      <CubeGrant cube=\"Warehouse\" access=\"all\">\n"
            + "        <HierarchyGrant hierarchy=\"[Store]\" rollupPolicy=\"partial\" access=\"custom\">\n"
            + "          <MemberGrant member=\"[Store].[All Stores]\" access=\"none\">\n"
            + "          </MemberGrant>\n"
            + "          <MemberGrant member=\"[Store].[USA]\" access=\"all\">\n"
            + "          </MemberGrant>\n"
            + "        </HierarchyGrant>\n"
            + "      </CubeGrant>\n"
            + "    </SchemaGrant>\n"
            + "  </Role>\n").withRole("F-MIS-BE-CLIENT");
        ctx.executeQuery(
            "With\n"
            + "Set [*NATIVE_CJ_SET] as 'NonEmptyCrossJoin([*BASE_MEMBERS_Store],[*BASE_MEMBERS_Warehouse])'\n"
            + "Set [*SORTED_ROW_AXIS] as 'Order([*CJ_ROW_AXIS],[Store].CurrentMember.OrderKey,BASC,[Warehouse].CurrentMember.OrderKey,BASC)'\n"
            + "Set [*BASE_MEMBERS_Warehouse] as '[Warehouse].[Country].Members'\n"
            + "Set [*BASE_MEMBERS_Store] as '[Store].[Store Country].Members'\n"
            + "Set [*BASE_MEMBERS_Measures] as '{[Measures].[*FORMATTED_MEASURE_0]}'\n"
            + "Set [*CJ_ROW_AXIS] as 'Generate([*NATIVE_CJ_SET], {([Store].currentMember,[Warehouse].currentMember)})'\n"
            + "Set [*CJ_COL_AXIS] as '[*NATIVE_CJ_SET]'\n"
            + "Member [Measures].[*FORMATTED_MEASURE_0] as '[Measures].[Store Invoice]', FORMAT_STRING = '#,###.00', SOLVE_ORDER=400\n"
            + "Select\n"
            + "[*BASE_MEMBERS_Measures] on columns,\n"
            + "Non Empty [*SORTED_ROW_AXIS] on rows\n"
            + "From [Warehouse and Sales]\n");
    }

    public void testNativeHonorsRoleRestrictions() {
        // NativeSetEvaluation pushes role restrictions to the where clause
        // (see SqlConstraintUtils.addRoleAccessConstraints) by
        // generating an IN expression based on accessible members.
        // If the number of accessible members in a hierarchy w/ CUSTOM
        // access exceeds MaxConstraints, it is not possible to
        // include the full role restriction in the IN clause.
        // This test verifies only permitted members are returned in this
        // case.

        // TODO:  Changes made with commit 51c1ac439 which allow pushdown of
        // some role access to native topcount/filter have not been merged
        // to lagunitas.  Without those changes native evaluation is skipped
        // for the test cases below.   This is covered w/ MONDRIAN-1461

        propSaver.set(MondrianProperties.instance().MaxConstraints, 4);
        String roleDef =
            "  <Role name=\"Test\">\n"
            + "    <SchemaGrant access=\"none\">\n"
            + "      <CubeGrant cube=\"Sales\" access=\"all\">\n"
            + "        <HierarchyGrant hierarchy=\"[Product]\" rollupPolicy=\"partial\" access=\"custom\">\n"
            + "          <MemberGrant member=\"[Product].[Non-Consumable].[Household].[Electrical].[Batteries].[Cormorant].[Cormorant AA-Size Batteries]\" access=\"all\" />\n"
            + "          <MemberGrant member=\"[Product].[Non-Consumable].[Household].[Electrical].[Batteries].[Cormorant].[Cormorant AA-Size Batteries]\" access=\"all\"/>\n"
            + "          <MemberGrant member=\"[Product].[Non-Consumable].[Household].[Electrical].[Batteries].[Cormorant].[Cormorant AAA-Size Batteries]\" access=\"all\"/>\n"
            + "          <MemberGrant member=\"[Product].[Non-Consumable].[Household].[Electrical].[Batteries].[Cormorant].[Cormorant C-Size Batteries]\" access=\"all\"/>\n"
            + "          <MemberGrant member=\"[Product].[Non-Consumable].[Household].[Electrical].[Batteries].[Denny].[Denny AA-Size Batteries]\" access=\"all\"/>\n"
            + "          <MemberGrant member=\"[Product].[Non-Consumable].[Household].[Electrical].[Batteries].[Denny].[Denny AAA-Size Batteries]\" access=\"all\"/>\n"
            + "        </HierarchyGrant>\n"
            + "      </CubeGrant>\n"
            + "    </SchemaGrant>\n"
            + "  </Role>";
        // The following queries should not include [Denny C-Size Batteries] or
        // [Denny D-Size Batteries]
        final TestContext ctx = getTestContext().legacy().create(
            null, null, null, null, null, roleDef).withRole("Test");
        verifySameNativeAndNot(
            "select non empty crossjoin([Store].[USA],[Product].[Product Name].members) on 0 from sales",
            "Native crossjoin mismatch", ctx);
        verifySameNativeAndNot(
            "select topcount([Product].[Product Name].members, 6, Measures.[Unit Sales]) on 0 from sales",
            "Native topcount mismatch", ctx);
        verifySameNativeAndNot(
            "select filter([Product].[Product Name].members, Measures.[Unit Sales] > 0) on 0 from sales",
            "Native native filter mismatch", ctx);
        propSaver.reset();
    }
}
// End NativeSetEvaluationTest.java
