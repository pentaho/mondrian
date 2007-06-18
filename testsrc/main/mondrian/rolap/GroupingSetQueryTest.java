/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2004-2007 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import mondrian.rolap.agg.CellRequest;
import mondrian.rolap.agg.SegmentLoader;
import mondrian.olap.MondrianProperties;

/**
 * <p>Test for GroupingSets functionality</p>
 *
 * @author Thiyagu
 * @version $Id$
 * @since 08-Jun-2007
 */
public class GroupingSetQueryTest extends BatchTestCase {

    boolean useGroupingSets = false;
    private String cubeNameSales2 = "Sales 2";
    private final String measureStoreSales = "[Measures].[Store Sales]";
    private final String fieldNameMaritalStatus = "marital_status";
    private boolean formattedSql;


    protected void setUp() throws Exception {
        super.setUp();
        useGroupingSets = MondrianProperties.instance().useGroupingSets.get();
        formattedSql = MondrianProperties.instance().GenerateFormattedSql.get();
        MondrianProperties.instance().GenerateFormattedSql.set(false);
    }


    protected void tearDown() throws Exception {
        super.tearDown();
        MondrianProperties.instance().useGroupingSets.set(useGroupingSets);
        MondrianProperties.instance().useGroupingSets.set(formattedSql);
    }

    public void testGroupingSetForSingleColumnConstraint() {
        MondrianProperties.instance().useGroupingSets.set(true);
        CellRequest request1 = createRequest(cubeNameSales2,
            measureUnitSales, tableCustomer, fieldGender, "M");

        CellRequest request2 = createRequest(cubeNameSales2,
            measureUnitSales, tableCustomer, fieldGender, "F");

        CellRequest request3 = createRequest(cubeNameSales2,
            measureUnitSales, null, "", "");

        SqlPattern[] patternsWithGS = {new SqlPattern(SqlPattern.ORACLE_DIALECT,
            "select \"customer\".\"gender\" as \"c0\", sum(\"sales_fact_1997\".\"unit_sales\") as \"m0\", " +
                "grouping(\"customer\".\"gender\") as \"g0\" " +
                "from \"customer\" \"customer\", \"sales_fact_1997\" \"sales_fact_1997\" " +
                "where \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\" " +
                "group by grouping sets ((\"customer\".\"gender\"),())",
            26)
        };
        assertRequestSql(new CellRequest[]{request3, request1, request2},
            patternsWithGS, cubeNameSales2);

        MondrianProperties.instance().useGroupingSets.set(false);

        SqlPattern[] patternsWithoutGS =
            {new SqlPattern(SqlPattern.ACCESS_DIALECT,
                "select \"customer\".\"gender\" as \"c0\", sum(\"sales_fact_1997\".\"unit_sales\") as \"m0\" " +
                    "from \"customer\" as \"customer\", \"sales_fact_1997\" as \"sales_fact_1997\" " +
                    "where \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\" " +
                    "group by \"customer\".\"gender\"", 26),
                new SqlPattern(SqlPattern.ORACLE_DIALECT,
                    "select \"customer\".\"gender\" as \"c0\", sum(\"sales_fact_1997\".\"unit_sales\") as \"m0\" " +
                        "from \"customer\" \"customer\", \"sales_fact_1997\" \"sales_fact_1997\" " +
                        "where \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\" " +
                        "group by \"customer\".\"gender\"", 26)
            };
        assertRequestSql(new CellRequest[]{request3, request1, request2},
            patternsWithoutGS, cubeNameSales2);
    }

    public void testNotUsingGroupingSetWhenGroupUsesDifferentAggregateTable() {

        boolean useAggregates =
            MondrianProperties.instance().UseAggregates.get();
        boolean readAggregates =
            MondrianProperties.instance().ReadAggregates.get();
        MondrianProperties.instance().UseAggregates.set(true);
        MondrianProperties.instance().ReadAggregates.set(true);

        CellRequest request1 = createRequest(cubeNameSales,
            measureUnitSales, tableCustomer, fieldGender, "M");

        CellRequest request2 = createRequest(cubeNameSales,
            measureUnitSales, tableCustomer, fieldGender, "F");

        CellRequest request3 = createRequest(cubeNameSales,
            measureUnitSales, null, "", "");

        MondrianProperties.instance().useGroupingSets.set(true);

        SqlPattern[] patternsWithoutGS =
            {new SqlPattern(SqlPattern.ACCESS_DIALECT,
                "select \"agg_g_ms_pcat_sales_fact_1997\".\"gender\" as \"c0\", " +
                    "sum(\"agg_g_ms_pcat_sales_fact_1997\".\"unit_sales\") as \"m0\" " +
                    "from \"agg_g_ms_pcat_sales_fact_1997\" as \"agg_g_ms_pcat_sales_fact_1997\" " +
                    "group by \"agg_g_ms_pcat_sales_fact_1997\".\"gender\"",
                26),
                new SqlPattern(SqlPattern.ORACLE_DIALECT,
                    "select \"agg_g_ms_pcat_sales_fact_1997\".\"gender\" as \"c0\", " +
                        "sum(\"agg_g_ms_pcat_sales_fact_1997\".\"unit_sales\") as \"m0\" " +
                        "from \"agg_g_ms_pcat_sales_fact_1997\" \"agg_g_ms_pcat_sales_fact_1997\" " +
                        "group by \"agg_g_ms_pcat_sales_fact_1997\".\"gender\"",
                    26)
            };
        assertRequestSql(new CellRequest[]{request3, request1, request2},
            patternsWithoutGS, cubeNameSales);
        MondrianProperties.instance().UseAggregates.set(useAggregates);
        MondrianProperties.instance().ReadAggregates.set(readAggregates);
    }

    public void testNotUsingGroupingSet() {
        MondrianProperties.instance().useGroupingSets.set(true);
        CellRequest request1 = createRequest(cubeNameSales2,
            measureUnitSales, tableCustomer, fieldGender, "M");

        CellRequest request2 = createRequest(cubeNameSales2,
            measureUnitSales, tableCustomer, fieldGender, "F");

        SqlPattern[] patternsWithGSEnabled =
            {new SqlPattern(SqlPattern.ORACLE_DIALECT,
                "select \"customer\".\"gender\" as \"c0\", sum(\"sales_fact_1997\".\"unit_sales\") as \"m0\" " +
                    "from \"customer\" \"customer\", \"sales_fact_1997\" \"sales_fact_1997\" " +
                    "where \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\" " +
                    "group by \"customer\".\"gender\"", 72)
            };
        assertRequestSql(new CellRequest[]{request1, request2},
            patternsWithGSEnabled, cubeNameSales2);

        MondrianProperties.instance().useGroupingSets.set(false);

        SqlPattern[] patternsWithoutGS =
            {new SqlPattern(SqlPattern.ACCESS_DIALECT,
                "select \"customer\".\"gender\" as \"c0\", sum(\"sales_fact_1997\".\"unit_sales\") as \"m0\" " +
                    "from \"customer\" as \"customer\", \"sales_fact_1997\" as \"sales_fact_1997\" " +
                    "where \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\" " +
                    "group by \"customer\".\"gender\"", 72),
                new SqlPattern(SqlPattern.ORACLE_DIALECT,
                    "select \"customer\".\"gender\" as \"c0\", sum(\"sales_fact_1997\".\"unit_sales\") as \"m0\" " +
                        "from \"customer\" \"customer\", \"sales_fact_1997\" \"sales_fact_1997\" " +
                        "where \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\" " +
                        "group by \"customer\".\"gender\"", 72)
            };
        assertRequestSql(new CellRequest[]{request1, request2},
            patternsWithoutGS, cubeNameSales2);
    }

    public void testGroupingSetForMultipleMeasureAndSingleConstraint() {
        MondrianProperties.instance().useGroupingSets.set(true);

        CellRequest request1 = createRequest(cubeNameSales2,
            measureUnitSales, tableCustomer, fieldGender, "M");
        CellRequest request2 = createRequest(cubeNameSales2,
            measureUnitSales, tableCustomer, fieldGender, "F");
        CellRequest request3 = createRequest(cubeNameSales2,
            measureUnitSales, null, "", "");
        CellRequest request4 = createRequest(cubeNameSales2,
            measureStoreSales, tableCustomer, fieldGender, "M");
        CellRequest request5 = createRequest(cubeNameSales2,
            measureStoreSales, tableCustomer, fieldGender, "F");
        CellRequest request6 = createRequest(cubeNameSales2,
            measureStoreSales, null, "", "");

        SqlPattern[] patternsWithGS = {new SqlPattern(SqlPattern.ORACLE_DIALECT,
            "select \"customer\".\"gender\" as \"c0\", sum(\"sales_fact_1997\".\"unit_sales\") as \"m0\", " +
                "sum(\"sales_fact_1997\".\"store_sales\") as \"m1\", grouping(\"customer\".\"gender\") as \"g0\" " +
                "from \"customer\" \"customer\", \"sales_fact_1997\" \"sales_fact_1997\" " +
                "where \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\" " +
                "group by grouping sets ((\"customer\".\"gender\"),())",
            26)
        };
        assertRequestSql(new CellRequest[]{request1, request2, request3,
            request4, request5, request6},
            patternsWithGS, cubeNameSales2);

        MondrianProperties.instance().useGroupingSets.set(false);

        SqlPattern[] patternsWithoutGS =
            {new SqlPattern(SqlPattern.ACCESS_DIALECT,
                "select \"customer\".\"gender\" as \"c0\", sum(\"sales_fact_1997\".\"unit_sales\") as \"m0\", " +
                    "sum(\"sales_fact_1997\".\"store_sales\") as \"m1\" " +
                    "from \"customer\" as \"customer\", \"sales_fact_1997\" as \"sales_fact_1997\" " +
                    "where \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\" " +
                    "group by \"customer\".\"gender\"", 26),
                new SqlPattern(SqlPattern.ORACLE_DIALECT,
                    "select \"customer\".\"gender\" as \"c0\", sum(\"sales_fact_1997\".\"unit_sales\") as \"m0\", " +
                        "sum(\"sales_fact_1997\".\"store_sales\") as \"m1\" " +
                        "from \"customer\" \"customer\", \"sales_fact_1997\" \"sales_fact_1997\" " +
                        "where \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\" " +
                        "group by \"customer\".\"gender\"", 26)
            };
        assertRequestSql(new CellRequest[]{request1, request2, request3,
            request4, request5, request6},
            patternsWithoutGS, cubeNameSales2);
    }

    public void testGroupingSetForASummaryCanBeGroupedWith2DetailBatch() {
        MondrianProperties.instance().useGroupingSets.set(true);
        CellRequest request1 = createRequest(cubeNameSales2,
            measureUnitSales, tableCustomer, fieldGender, "M");
        CellRequest request2 = createRequest(cubeNameSales2,
            measureUnitSales, tableCustomer, fieldGender, "F");
        CellRequest request3 = createRequest(cubeNameSales2,
            measureUnitSales, null, "", "");
        CellRequest request4 = createRequest(cubeNameSales2,
            measureUnitSales, tableCustomer, fieldNameMaritalStatus, "M");
        CellRequest request5 = createRequest(cubeNameSales2,
            measureUnitSales, tableCustomer, fieldNameMaritalStatus, "S");
        CellRequest request6 = createRequest(cubeNameSales2,
            measureUnitSales, null, "", "");

        SqlPattern[] patternOfBatchGroupedWith1stGroup =
            {new SqlPattern(SqlPattern.ORACLE_DIALECT,
                "select \"customer\".\"gender\" as \"c0\", sum(\"sales_fact_1997\".\"unit_sales\") as \"m0\", " +
                    "grouping(\"customer\".\"gender\") as \"g0\" " +
                    "from \"customer\" \"customer\", \"sales_fact_1997\" \"sales_fact_1997\" " +
                    "where \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\" " +
                    "group by grouping sets ((\"customer\".\"gender\"),())",
                26)
            };
        assertRequestSql(new CellRequest[]{request1, request2, request3,
            request4, request5, request6},
            patternOfBatchGroupedWith1stGroup, cubeNameSales2);

        SqlPattern[] patternOfBatchNotGroupedTo2ndGroup =
            {new SqlPattern(SqlPattern.ORACLE_DIALECT,
                "select \"customer\".\"marital_status\" as \"c0\", sum(\"sales_fact_1997\".\"unit_sales\") as \"m0\" " +
                    "from \"customer\" \"customer\", \"sales_fact_1997\" \"sales_fact_1997\" " +
                    "where \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\" " +
                    "group by \"customer\".\"marital_status\"",
                26)
            };
        assertRequestSql(new CellRequest[]{request1, request2, request3,
            request4, request5, request6},
            patternOfBatchNotGroupedTo2ndGroup, cubeNameSales2);

        MondrianProperties.instance().useGroupingSets.set(false);

        SqlPattern[] patternsWithoutGS =
            {new SqlPattern(SqlPattern.ACCESS_DIALECT,
                "select sum(\"sales_fact_1997\".\"unit_sales\") as \"m0\" " +
                    "from \"sales_fact_1997\" as \"sales_fact_1997\"",
                40),
                new SqlPattern(SqlPattern.ORACLE_DIALECT,
                    "select sum(\"sales_fact_1997\".\"unit_sales\") as \"m0\" " +
                        "from \"sales_fact_1997\" \"sales_fact_1997\"",
                    40)
            };
        assertRequestSql(new CellRequest[]{request1, request2, request3,
            request4, request5, request6},
            patternsWithoutGS, cubeNameSales2);
    }

    public void testGroupingSetForMultipleColumnConstraint() {
        MondrianProperties.instance().useGroupingSets.set(true);
        CellRequest request1 = createRequest(cubeNameSales2,
            measureUnitSales, new String[]{tableCustomer, tableTime},
            new String[]{fieldGender, fieldYear},
            new String[]{"M", "1997"});

        CellRequest request2 = createRequest(cubeNameSales2,
            measureUnitSales, new String[]{tableCustomer, tableTime},
            new String[]{fieldGender, fieldYear},
            new String[]{"F", "1997"});

        CellRequest request3 = createRequest(cubeNameSales2,
            measureUnitSales, tableTime, fieldYear, "1997");

        SqlPattern[] patternsWithGS = {new SqlPattern(SqlPattern.ORACLE_DIALECT,
            "select \"time_by_day\".\"the_year\" as \"c0\", \"customer\".\"gender\" as \"c1\", " +
                "sum(\"sales_fact_1997\".\"unit_sales\") as \"m0\", grouping(\"customer\".\"gender\") as \"g0\" " +
                "from \"time_by_day\" \"time_by_day\", \"sales_fact_1997\" \"sales_fact_1997\", \"customer\" \"customer\" " +
                "where \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" and \"time_by_day\".\"the_year\" = 1997 " +
                "and \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\" " +
                "group by grouping sets ((\"time_by_day\".\"the_year\",\"customer\".\"gender\"),(\"time_by_day\".\"the_year\"))",
            150)
        };
        assertRequestSql(new CellRequest[]{request3, request1, request2},
            patternsWithGS, cubeNameSales2);

        MondrianProperties.instance().useGroupingSets.set(false);

        SqlPattern[] patternsWithoutGS =
            {new SqlPattern(SqlPattern.ACCESS_DIALECT,
                "select \"time_by_day\".\"the_year\" as \"c0\", \"customer\".\"gender\" as \"c1\", " +
                    "sum(\"sales_fact_1997\".\"unit_sales\") as \"m0\" " +
                    "from \"time_by_day\" as \"time_by_day\", \"sales_fact_1997\" as \"sales_fact_1997\", " +
                    "\"customer\" as \"customer\" " +
                    "where \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" and " +
                    "\"time_by_day\".\"the_year\" = 1997 and " +
                    "\"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\" " +
                    "group by \"time_by_day\".\"the_year\", \"customer\".\"gender\"",
                50),
                new SqlPattern(SqlPattern.ORACLE_DIALECT,
                    "select \"time_by_day\".\"the_year\" as \"c0\", \"customer\".\"gender\" as \"c1\", " +
                        "sum(\"sales_fact_1997\".\"unit_sales\") as \"m0\" " +
                        "from \"time_by_day\" \"time_by_day\", \"sales_fact_1997\" \"sales_fact_1997\", " +
                        "\"customer\" \"customer\" " +
                        "where \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" and " +
                        "\"time_by_day\".\"the_year\" = 1997 " +
                        "and \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\" " +
                        "group by \"time_by_day\".\"the_year\", \"customer\".\"gender\"",
                    50)
            };
        assertRequestSql(new CellRequest[]{request3, request1, request2},
            patternsWithoutGS, cubeNameSales2);
    }
}