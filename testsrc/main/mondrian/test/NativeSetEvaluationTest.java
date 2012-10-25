package mondrian.test;

import mondrian.rolap.BatchTestCase;
import mondrian.spi.Dialect.DatabaseProduct;

/**
 * Test native evaluation of supported set operations.
 */
public class NativeSetEvaluationTest extends BatchTestCase {

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
            + "    (`time_by_day`.`quarter`, `time_by_day`.`the_year`) in (('Q1', 1997), ('Q2', 1997), ('Q3', 1997))\n"
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
          + "NON EMPTY {[Measures].[Store Sales], Measures.x1, Measures.x2} ON 0\n"
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
            + "NON EMPTY {[Measures].[Store Sales], Measures.x1, Measures.x2} ON 0\n"
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
            + "NON EMPTY {[Measures].[Store Sales], Measures.x1, Measures.x2} ON 0\n"
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
            + "    (`time_by_day`.`quarter`, `time_by_day`.`the_year`) in (('Q4', 1997), ('Q3', 1997), ('Q1', 1997), ('Q2', 1997))\n"//slicer
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
        assertQueryReturns(mdx,
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
    

}
