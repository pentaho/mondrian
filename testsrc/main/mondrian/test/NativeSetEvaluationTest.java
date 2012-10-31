/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2012 Pentaho and others
// All Rights Reserved.
*/
package mondrian.test;

import mondrian.rolap.BatchTestCase;
import mondrian.spi.Dialect.DatabaseProduct;

/**
 * Test native evaluation of supported set operations.
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
        final static String mysql =
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
            //aggregate set
            + "and\n"
            + "    `time_by_day`.`quarter` in ('Q3', 'Q1', 'Q2') and `time_by_day`.`the_year` = 1997\n"
            + "group by\n"
            + "    `product_class`.`product_family`,\n"
            + "    `product_class`.`product_department`,\n"
            + "    `product_class`.`product_category`,\n"
            + "    `product_class`.`product_subcategory`,\n"
            + "    `product`.`brand_name`,\n"
            + "    `product`.`product_name`\n"
            + "order by\n"
            //top count Measures.[Store Sales]
            + "    `c6` DESC,\n"
            + "    ISNULL(`product_class`.`product_family`) ASC, `product_class`.`product_family` ASC,\n"
            + "    ISNULL(`product_class`.`product_department`) ASC, `product_class`.`product_department` ASC,\n"
            + "    ISNULL(`product_class`.`product_category`) ASC, `product_class`.`product_category` ASC,\n"
            + "    ISNULL(`product_class`.`product_subcategory`) ASC, `product_class`.`product_subcategory` ASC,\n"
            + "    ISNULL(`product`.`brand_name`) ASC, `product`.`brand_name` ASC,\n"
            + "    ISNULL(`product`.`product_name`) ASC, `product`.`product_name` ASC";
        final static String result =
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
    public void testNativeTopCountWithAggFlatSet() {
      final String mdx =
          "with\n"
          + "member Time.x as Aggregate({[Time].[1997].[Q1] , [Time].[1997].[Q2], [Time].[1997].[Q3]}, [Measures].[Store Sales])\n"
          + "member Measures.x1 as ([Time].[1997].[Q1], [Measures].[Store Sales])\n"
          + "member Measures.x2 as ([Time].[1997].[Q2], [Measures].[Store Sales])\n"
          + "member Measures.x3 as ([Time].[1997].[Q3], [Measures].[Store Sales])\n"
          + " set products as TopCount(Product.[Product Name].Members, 2, Measures.[Store Sales])\n"
          + " SELECT NON EMPTY products ON 1,\n"
          + "NON EMPTY {[Measures].[Store Sales], Measures.x1, Measures.x2, Measures.x3} ON 0\n"
          + "FROM [Sales] where Time.x";
      propSaver.set(propSaver.properties.GenerateFormattedSql, true);
      SqlPattern mysqlPattern =
          new SqlPattern(
              DatabaseProduct.MYSQL,
              NativeTopCountWithAgg.mysql,
              NativeTopCountWithAgg.mysql.indexOf("from"));
      assertQuerySql(mdx, new SqlPattern[]{mysqlPattern});
      assertQueryReturns(mdx, NativeTopCountWithAgg.result);
    }

    /**
     * Same as above, but using a named set
     */
    public void testNativeTopCountWithAggMemberEnumSet() {
        final String mdx =
            "with set TO_AGGREGATE as '{[Time].[1997].[Q1] , [Time].[1997].[Q2], [Time].[1997].[Q3]}'\n"
            + "member Time.x as Aggregate(TO_AGGREGATE, [Measures].[Store Sales])\n"
            + "member Measures.x1 as ([Time].[1997].[Q1], [Measures].[Store Sales])\n"
            + "member Measures.x2 as ([Time].[1997].[Q2], [Measures].[Store Sales])\n"
            + "member Measures.x3 as ([Time].[1997].[Q3], [Measures].[Store Sales])\n"
            + " set products as TopCount(Product.[Product Name].Members, 2, Measures.[Store Sales])\n"
            + " SELECT NON EMPTY products ON 1,\n"
            + "NON EMPTY {[Measures].[Store Sales], Measures.x1, Measures.x2, Measures.x3} ON 0\n"
            + "FROM [Sales] where Time.x";

        assertQueryReturns(mdx, NativeTopCountWithAgg.result);
    }

    /**
     * Same as above, defined as a range.
     */
    public void testNativeTopCountWithAggMemberCMRange() {
        final String mdx =
            "with set TO_AGGREGATE as '([Time].[1997].[Q1] : [Time].[1997].[Q3])'\n"
            + "member Time.x as Aggregate(TO_AGGREGATE, [Measures].[Store Sales])\n"
            + "member Measures.x1 as ([Time].[1997].[Q1], [Measures].[Store Sales])\n"
            + "member Measures.x2 as ([Time].[1997].[Q2], [Measures].[Store Sales])\n"
            + "member Measures.x3 as ([Time].[1997].[Q3], [Measures].[Store Sales])\n"
            + " set products as TopCount(Product.[Product Name].Members, 2, Measures.[Store Sales])\n"
            + " SELECT NON EMPTY products ON 1,\n"
            + "NON EMPTY {[Measures].[Store Sales], Measures.x1, Measures.x2, Measures.x3} ON 0\n"
            + " FROM [Sales] where Time.x";

          propSaver.set(propSaver.properties.GenerateFormattedSql, true);
          SqlPattern mysqlPattern =
              new SqlPattern(
                  DatabaseProduct.MYSQL,
                  NativeTopCountWithAgg.mysql,
                  NativeTopCountWithAgg.mysql.indexOf("from"));
          assertQuerySql(mdx, new SqlPattern[]{mysqlPattern});
          assertQueryReturns(mdx, NativeTopCountWithAgg.result);
    }

    public void testNativeFilterWithAggDescendants() {
      final String mdx =
          "with\n"
          + "  set QUARTERS as Descendants([Time].[1997], [Time].[Time].[Quarter])\n"
          + "  member Time.x as Aggregate(QUARTERS, [Measures].[Store Sales])\n"
          + "  set products as Filter([Product].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].Children, [Measures].[Store Sales] > 700)\n"
          + "  SELECT NON EMPTY products ON 1,\n"
          + "  NON EMPTY {[Measures].[Store Sales]} ON 0\n"
          + "  FROM [Sales] where Time.x";

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
            + "    `sales_fact_1997` as `sales_fact_1997`,\n"
            + "    `time_by_day` as `time_by_day`\n"
            + "where\n"
            + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
            + "and\n"
            + "    `sales_fact_1997`.`product_id` = `product`.`product_id`\n"
            + "and\n"
            + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
            + "and\n"
            + "    `time_by_day`.`quarter` in ('Q4', 'Q3', 'Q1', 'Q2')"
            + " and `time_by_day`.`the_year` = 1997\n"//slicer
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
            + "    (sum(`sales_fact_1997`.`store_sales`) > 700)\n"//filter exp
            + "order by\n"
            + "    ISNULL(`product_class`.`product_family`) ASC, `product_class`.`product_family` ASC,\n"
            + "    ISNULL(`product_class`.`product_department`) ASC, `product_class`.`product_department` ASC,\n"
            + "    ISNULL(`product_class`.`product_category`) ASC, `product_class`.`product_category` ASC,\n"
            + "    ISNULL(`product_class`.`product_subcategory`) ASC, `product_class`.`product_subcategory` ASC,\n"
            + "    ISNULL(`product`.`brand_name`) ASC, `product`.`brand_name` ASC,\n"
            + "    ISNULL(`product`.`product_name`) ASC, `product`.`product_name` ASC";

        propSaver.set(propSaver.properties.GenerateFormattedSql, true);
        SqlPattern mysqlPattern =
            new SqlPattern(
                DatabaseProduct.MYSQL,
                mysqlQuery,
                mysqlQuery.indexOf("from"));
        assertQuerySql(mdx, new SqlPattern[]{mysqlPattern});
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
     * Aggregate with default measure and TopCount without measure argument.
     */
    public void testAggTCNoExplicitMeasure() {
        propSaver.set(propSaver.properties.GenerateFormattedSql, true);
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
    public void testCJSameDimAsSlicerNamedSet() {
      String mdx = "WITH\n"
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
              + "{[Product].[Top Drinks]}\n"
              + "Axis #1:\n"
              + "{[Measures].[Unit Sales]}\n"
              + "Axis #2:\n"
              + "{[Customers].[USA]}\n"
              + "Row #0: 20,411\n");
    }

    /**
     * Test evaluation loop detection still works after changes to
     * make it more permissable.
     */
    public void testLoopDetection() {
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
        assertQueryThrows(mdx, "evaluating itself");
    }

    /**
     * Check if getSlicerMembers in native evaluation context
     * doesn't break the results as in MONDRIAN-1187
     */
    public void testSlicerTuplesPartialCrossJoin() {
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
    public void testSlicerTuplesFullCrossJoin() {
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

}
// End NativeSetEvaluationTest.java
