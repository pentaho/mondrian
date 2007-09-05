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
import mondrian.olap.MondrianProperties;
import mondrian.test.SqlPattern;

/**
 * Test support for generating SQL queries with the <code>GROUPING SETS</code>
 * construct, if the DBMS supports it.
 *
 * @author Thiyagu
 * @version $Id$
 * @since 08-Jun-2007
 */
public class GroupingSetQueryTest extends BatchTestCase {

    private static final String cubeNameSales2 = "Sales 2";
    private static final String measureStoreSales = "[Measures].[Store Sales]";
    private static final String fieldNameMaritalStatus = "marital_status";

    private boolean useGroupingSets;
    private boolean formattedSql;

    protected void setUp() throws Exception {
        super.setUp();
        getTestContext().clearConnection();
        useGroupingSets = MondrianProperties.instance().EnableGroupingSets.get();
        formattedSql = MondrianProperties.instance().GenerateFormattedSql.get();
        MondrianProperties.instance().GenerateFormattedSql.set(false);
    }

    protected void tearDown() throws Exception {
        super.tearDown();
        MondrianProperties.instance().EnableGroupingSets.set(useGroupingSets);
        MondrianProperties.instance().GenerateFormattedSql.set(formattedSql);
    }

    public void testGroupingSetForSingleColumnConstraint() {
        final MondrianProperties properties = MondrianProperties.instance();
        properties.UseAggregates.setString("true");
        properties.ReadAggregates.setString("false");
        properties.ReadAggregates.setString("true");

        properties.ReadAggregates.setString("false");
        properties.UseAggregates.setString("false");
        properties.DisableCaching.setString("false");

        CellRequest request1 = createRequest(
            cubeNameSales2, measureUnitSales, tableCustomer, fieldGender, "M");

        CellRequest request2 = createRequest(
            cubeNameSales2, measureUnitSales, tableCustomer, fieldGender, "F");

        CellRequest request3 = createRequest(
            cubeNameSales2, measureUnitSales, null, "", "");

        SqlPattern[] patternsWithGsets = {
            new SqlPattern(
                SqlPattern.Dialect.ORACLE,
                "select \"customer\".\"gender\" as \"c0\", sum(\"sales_fact_1997\".\"unit_sales\") as \"m0\", " +
                "grouping(\"customer\".\"gender\") as \"g0\" " +
                "from \"customer\" \"customer\", \"sales_fact_1997\" \"sales_fact_1997\" " +
                "where \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\" " +
                "group by grouping sets ((\"customer\".\"gender\"),())",
            26)
        };

        // If aggregates are enabled, mondrian should use them. Results should
        // be the same with or without grouping sets enabled.
        SqlPattern[] patternsWithAggs = {
            new SqlPattern(
                SqlPattern.Dialect.ORACLE,
                "select sum(\"agg_c_10_sales_fact_1997\".\"unit_sales\") as \"m0\""
                    + " from \"agg_c_10_sales_fact_1997\" \"agg_c_10_sales_fact_1997\"",
                null),
            new SqlPattern(
                SqlPattern.Dialect.ORACLE,
                "select \"agg_g_ms_pcat_sales_fact_1997\".\"gender\" as \"c0\","
                    + " sum(\"agg_g_ms_pcat_sales_fact_1997\".\"unit_sales\") as \"m0\" "
                    + "from \"agg_g_ms_pcat_sales_fact_1997\" \"agg_g_ms_pcat_sales_fact_1997\" "
                    + "group by \"agg_g_ms_pcat_sales_fact_1997\".\"gender\"",
                null)
        };

        SqlPattern[] patternsWithoutGsets = {
            new SqlPattern(
                SqlPattern.Dialect.ACCESS,
                "select \"customer\".\"gender\" as \"c0\", sum(\"sales_fact_1997\".\"unit_sales\") as \"m0\" " +
                    "from \"customer\" as \"customer\", \"sales_fact_1997\" as \"sales_fact_1997\" " +
                    "where \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\" " +
                    "group by \"customer\".\"gender\"", 26),
            new SqlPattern(
                SqlPattern.Dialect.ORACLE,
                "select \"customer\".\"gender\" as \"c0\", sum(\"sales_fact_1997\".\"unit_sales\") as \"m0\" " +
                    "from \"customer\" \"customer\", \"sales_fact_1997\" \"sales_fact_1997\" " +
                    "where \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\" " +
                    "group by \"customer\".\"gender\"", 26)
        };

        properties.EnableGroupingSets.set(true);

        if (properties.ReadAggregates.get() && properties.UseAggregates.get()) {
            assertRequestSql(
                new CellRequest[] {request3, request1, request2},
                patternsWithAggs);
        } else {
            assertRequestSql(
                new CellRequest[] {request3, request1, request2},
                patternsWithGsets);
        }

        properties.EnableGroupingSets.set(false);

        if (properties.ReadAggregates.get() && properties.UseAggregates.get()) {
            assertRequestSql(
                new CellRequest[] {request3, request1, request2},
                patternsWithAggs);
        } else {
            assertRequestSql(
                new CellRequest[] {request3, request1, request2},
                patternsWithoutGsets);
        }
    }

    public void testNotUsingGroupingSetWhenGroupUsesDifferentAggregateTable() {
        final MondrianProperties properties = MondrianProperties.instance();
        if (!(properties.UseAggregates.get() &&
            properties.ReadAggregates.get())) {
            return;
        }

        CellRequest request1 = createRequest(cubeNameSales,
            measureUnitSales, tableCustomer, fieldGender, "M");

        CellRequest request2 = createRequest(cubeNameSales,
            measureUnitSales, tableCustomer, fieldGender, "F");

        CellRequest request3 = createRequest(cubeNameSales,
            measureUnitSales, null, "", "");

        properties.EnableGroupingSets.set(true);

        SqlPattern[] patternsWithoutGsets = {
            new SqlPattern(
                SqlPattern.Dialect.ACCESS,
                "select \"agg_g_ms_pcat_sales_fact_1997\".\"gender\" as \"c0\", " +
                    "sum(\"agg_g_ms_pcat_sales_fact_1997\".\"unit_sales\") as \"m0\" " +
                    "from \"agg_g_ms_pcat_sales_fact_1997\" as \"agg_g_ms_pcat_sales_fact_1997\" " +
                    "group by \"agg_g_ms_pcat_sales_fact_1997\".\"gender\"",
                26),
            new SqlPattern(
                SqlPattern.Dialect.ORACLE,
                "select \"agg_g_ms_pcat_sales_fact_1997\".\"gender\" as \"c0\", " +
                    "sum(\"agg_g_ms_pcat_sales_fact_1997\".\"unit_sales\") as \"m0\" " +
                    "from \"agg_g_ms_pcat_sales_fact_1997\" \"agg_g_ms_pcat_sales_fact_1997\" " +
                    "group by \"agg_g_ms_pcat_sales_fact_1997\".\"gender\"",
                26)
        };
        assertRequestSql(
            new CellRequest[] {request3, request1, request2},
            patternsWithoutGsets);
    }

    public void testNotUsingGroupingSet() {
        final MondrianProperties properties = MondrianProperties.instance();
        if (properties.ReadAggregates.get() && properties.UseAggregates.get()) {
            return;
        }
        properties.EnableGroupingSets.set(true);
        CellRequest request1 = createRequest(cubeNameSales2,
            measureUnitSales, tableCustomer, fieldGender, "M");

        CellRequest request2 = createRequest(cubeNameSales2,
            measureUnitSales, tableCustomer, fieldGender, "F");

        SqlPattern[] patternsWithGsets = {
            new SqlPattern(
                SqlPattern.Dialect.ORACLE,
                "select \"customer\".\"gender\" as \"c0\", sum(\"sales_fact_1997\".\"unit_sales\") as \"m0\" " +
                    "from \"customer\" \"customer\", \"sales_fact_1997\" \"sales_fact_1997\" " +
                    "where \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\" " +
                    "group by \"customer\".\"gender\"", 72)
            };
        assertRequestSql(
            new CellRequest[] {request1, request2},
            patternsWithGsets);

        properties.EnableGroupingSets.set(false);

        SqlPattern[] patternsWithoutGsets = {
            new SqlPattern(
                SqlPattern.Dialect.ACCESS,
                "select \"customer\".\"gender\" as \"c0\", sum(\"sales_fact_1997\".\"unit_sales\") as \"m0\" " +
                    "from \"customer\" as \"customer\", \"sales_fact_1997\" as \"sales_fact_1997\" " +
                    "where \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\" " +
                    "group by \"customer\".\"gender\"", 72),
            new SqlPattern(
                SqlPattern.Dialect.ORACLE,
                "select \"customer\".\"gender\" as \"c0\", sum(\"sales_fact_1997\".\"unit_sales\") as \"m0\" " +
                    "from \"customer\" \"customer\", \"sales_fact_1997\" \"sales_fact_1997\" " +
                    "where \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\" " +
                    "group by \"customer\".\"gender\"", 72)
        };
        assertRequestSql(
            new CellRequest[] {request1, request2},
            patternsWithoutGsets);
    }

    public void testGroupingSetForMultipleMeasureAndSingleConstraint() {
        final MondrianProperties properties = MondrianProperties.instance();
        if (properties.ReadAggregates.get() && properties.UseAggregates.get()) {
            return;
        }
        properties.EnableGroupingSets.set(true);

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

        SqlPattern[] patternsWithGsets = {
            new SqlPattern(
                SqlPattern.Dialect.ORACLE,
                "select \"customer\".\"gender\" as \"c0\", sum(\"sales_fact_1997\".\"unit_sales\") as \"m0\", " +
                    "sum(\"sales_fact_1997\".\"store_sales\") as \"m1\", grouping(\"customer\".\"gender\") as \"g0\" " +
                    "from \"customer\" \"customer\", \"sales_fact_1997\" \"sales_fact_1997\" " +
                    "where \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\" " +
                    "group by grouping sets ((\"customer\".\"gender\"),())",
                26)
        };
        assertRequestSql(
            new CellRequest[] {
                request1, request2, request3, request4, request5, request6},
            patternsWithGsets);

        properties.EnableGroupingSets.set(false);

        SqlPattern[] patternsWithoutGsets = {
            new SqlPattern(
                SqlPattern.Dialect.ACCESS,
                "select \"customer\".\"gender\" as \"c0\", sum(\"sales_fact_1997\".\"unit_sales\") as \"m0\", " +
                    "sum(\"sales_fact_1997\".\"store_sales\") as \"m1\" " +
                    "from \"customer\" as \"customer\", \"sales_fact_1997\" as \"sales_fact_1997\" " +
                    "where \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\" " +
                    "group by \"customer\".\"gender\"", 26),
            new SqlPattern(
                SqlPattern.Dialect.ORACLE,
                "select \"customer\".\"gender\" as \"c0\", sum(\"sales_fact_1997\".\"unit_sales\") as \"m0\", " +
                    "sum(\"sales_fact_1997\".\"store_sales\") as \"m1\" " +
                    "from \"customer\" \"customer\", \"sales_fact_1997\" \"sales_fact_1997\" " +
                    "where \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\" " +
                    "group by \"customer\".\"gender\"", 26)
        };
        assertRequestSql(
            new CellRequest[] {
                request1, request2, request3, request4, request5, request6},
            patternsWithoutGsets);
    }

    public void testGroupingSetForASummaryCanBeGroupedWith2DetailBatch(final MondrianProperties properties) {
        properties.EnableGroupingSets.set(true);
        if (properties.ReadAggregates.get() && properties.UseAggregates.get()) {
            return;
        }
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

        SqlPattern[] patternWithGsets = {
            new SqlPattern(
                SqlPattern.Dialect.ORACLE,
                "select \"customer\".\"gender\" as \"c0\", sum(\"sales_fact_1997\".\"unit_sales\") as \"m0\", " +
                    "grouping(\"customer\".\"gender\") as \"g0\" " +
                    "from \"customer\" \"customer\", \"sales_fact_1997\" \"sales_fact_1997\" " +
                    "where \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\" " +
                    "group by grouping sets ((\"customer\".\"gender\"),())",
                26),

            new SqlPattern(
                SqlPattern.Dialect.ORACLE,
                "select \"customer\".\"marital_status\" as \"c0\", sum(\"sales_fact_1997\".\"unit_sales\") as \"m0\" " +
                    "from \"customer\" \"customer\", \"sales_fact_1997\" \"sales_fact_1997\" " +
                    "where \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\" " +
                    "group by \"customer\".\"marital_status\"",
                26),
            };

        assertRequestSql(
            new CellRequest[] {
                request1, request2, request3, request4, request5, request6},
            patternWithGsets);

        properties.EnableGroupingSets.set(false);

        SqlPattern[] patternWithoutGsets = {
            new SqlPattern(
                SqlPattern.Dialect.ACCESS,
                "select sum(\"sales_fact_1997\".\"unit_sales\") as \"m0\" " +
                    "from \"sales_fact_1997\" as \"sales_fact_1997\"",
                40),
            new SqlPattern(
                SqlPattern.Dialect.ORACLE,
                "select sum(\"sales_fact_1997\".\"unit_sales\") as \"m0\" " +
                    "from \"sales_fact_1997\" \"sales_fact_1997\"",
                40)
        };

        assertRequestSql(
            new CellRequest[] {
                request1, request2, request3, request4, request5, request6},
            patternWithoutGsets);
    }

    public void testGroupingSetForMultipleColumnConstraint() {
        final MondrianProperties properties = MondrianProperties.instance();
        if (properties.ReadAggregates.get() && properties.UseAggregates.get()) {
            return;
        }
        properties.EnableGroupingSets.set(true);
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

        SqlPattern[] patternsWithGsets = {
            new SqlPattern(
                SqlPattern.Dialect.ORACLE,
                "select \"time_by_day\".\"the_year\" as \"c0\", \"customer\".\"gender\" as \"c1\", " +
                "sum(\"sales_fact_1997\".\"unit_sales\") as \"m0\", grouping(\"customer\".\"gender\") as \"g0\" " +
                "from \"time_by_day\" \"time_by_day\", \"sales_fact_1997\" \"sales_fact_1997\", \"customer\" \"customer\" " +
                "where \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" and \"time_by_day\".\"the_year\" = 1997 " +
                "and \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\" " +
                "group by grouping sets ((\"time_by_day\".\"the_year\",\"customer\".\"gender\"),(\"time_by_day\".\"the_year\"))",
            150)
        };

        // Sometimes this query causes Oracle 10.2 XE to give
        //   ORA-12516, TNS:listener could not find available handler with
        //   matching protocol stack
        //
        // You need to configure Oracle:
        //  $ su - oracle
        //  $ sqlplus / as sysdba
        //  SQL> ALTER SYSTEM SET sessions=320 SCOPE=SPFILE;
        //  SQL> SHUTDOWN
        assertRequestSql(
            new CellRequest[] {request3, request1, request2},
            patternsWithGsets);

        properties.EnableGroupingSets.set(false);

        SqlPattern[] patternsWithoutGsets = {
            new SqlPattern(
                SqlPattern.Dialect.ACCESS,
                "select \"time_by_day\".\"the_year\" as \"c0\", \"customer\".\"gender\" as \"c1\", " +
                    "sum(\"sales_fact_1997\".\"unit_sales\") as \"m0\" " +
                    "from \"time_by_day\" as \"time_by_day\", \"sales_fact_1997\" as \"sales_fact_1997\", " +
                    "\"customer\" as \"customer\" " +
                    "where \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" and " +
                    "\"time_by_day\".\"the_year\" = 1997 and " +
                    "\"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\" " +
                    "group by \"time_by_day\".\"the_year\", \"customer\".\"gender\"",
                50),
            new SqlPattern(
                SqlPattern.Dialect.ORACLE,
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
        assertRequestSql(
            new CellRequest[]{request3, request1, request2},
            patternsWithoutGsets);
    }
}

// End GroupingSetQueryTest.java
