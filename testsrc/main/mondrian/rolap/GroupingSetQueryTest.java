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

import java.util.ArrayList;
import java.util.List;

import mondrian.rolap.BatchTestCase.CellRequestConstraint;
import mondrian.rolap.agg.CellRequest;
import mondrian.rolap.sql.SqlQuery;
import mondrian.olap.MondrianProperties;
import mondrian.test.SqlPattern;
import mondrian.util.Bug;

/**
 * Test support for generating SQL queries with the <code>GROUPING SETS</code>
 * construct, if the DBMS supports it.
 *
 * @author Thiyagu
 * @version $Id$
 * @since 08-Jun-2007
 */
public class GroupingSetQueryTest extends BatchTestCase {

    private MondrianProperties prop = MondrianProperties.instance();
    
    private static final String cubeNameSales2 = "Sales 2";
    private static final String measureStoreSales = "[Measures].[Store Sales]";
    private static final String fieldNameMaritalStatus = "marital_status";
    private static final String measureCustomerCount = "[Measures].[Customer Count]";

    private boolean useGroupingSets;
    private boolean formattedSql;
    private String origWarnIfNoPatternForDialect;

    protected void setUp() throws Exception {
        super.setUp();
        getTestContext().clearConnection();
        useGroupingSets = prop.EnableGroupingSets.get();
        formattedSql = prop.GenerateFormattedSql.get();
        origWarnIfNoPatternForDialect = prop.WarnIfNoPatternForDialect.get();
        
        prop.GenerateFormattedSql.set(false);
        
        /*
         * This test warns of missing sql patterns for
         * 
         * ACCESS
         * ORACLE
         * 
         */
        final SqlQuery.Dialect dialect = getTestContext().getDialect();
        if (prop.WarnIfNoPatternForDialect.get().equals("ANY") ||
            (dialect.isAccess() || dialect.isOracle())) {
            prop.WarnIfNoPatternForDialect.set(
                SqlPattern.Dialect.get(dialect).toString());
        } else {
            /*
             * Do not warn unless the dialect is "ACCESS" or "ORACLE", or
             * if the test chooses to warn regardless of the dialect.
             */
            prop.WarnIfNoPatternForDialect.set("NONE");
        }
    }

    protected void tearDown() throws Exception {
        super.tearDown();
        prop.EnableGroupingSets.set(useGroupingSets);
        prop.GenerateFormattedSql.set(formattedSql);
        prop.WarnIfNoPatternForDialect.set(origWarnIfNoPatternForDialect);
    }

    public void testGroupingSetForSingleColumnConstraint() {

        if (!Bug.Bug1833526Fixed) {
            // flush cache, this method was failing within the suite because
            // SchemaTest.testMultipleDimensionUsages(), a new method,
            // alters the schema slightly. use this call eventually: 

            // getConnection().getCacheControl(null).flushSchemaCache();
    
            RolapSchema.clearCache();
        }
        
        prop.DisableCaching.setString("false");

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

        prop.EnableGroupingSets.set(true);

        if (prop.ReadAggregates.get() && prop.UseAggregates.get()) {
            assertRequestSql(
                new CellRequest[] {request3, request1, request2},
                patternsWithAggs);
        } else {
            assertRequestSql(
                new CellRequest[] {request3, request1, request2},
                patternsWithGsets);
        }

        prop.EnableGroupingSets.set(false);

        if (prop.ReadAggregates.get() && prop.UseAggregates.get()) {
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
        if (!(prop.UseAggregates.get() &&
            prop.ReadAggregates.get())) {
            return;
        }

        CellRequest request1 = createRequest(cubeNameSales,
            measureUnitSales, tableCustomer, fieldGender, "M");

        CellRequest request2 = createRequest(cubeNameSales,
            measureUnitSales, tableCustomer, fieldGender, "F");

        CellRequest request3 = createRequest(cubeNameSales,
            measureUnitSales, null, "", "");

        prop.EnableGroupingSets.set(true);

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
        if (prop.ReadAggregates.get() && prop.UseAggregates.get()) {
            return;
        }
        prop.EnableGroupingSets.set(true);
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

        prop.EnableGroupingSets.set(false);

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
        if (prop.ReadAggregates.get() && prop.UseAggregates.get()) {
            return;
        }
        prop.EnableGroupingSets.set(true);

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

        prop.EnableGroupingSets.set(false);

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

    public void testGroupingSetForASummaryCanBeGroupedWith2DetailBatch() {
        if (prop.ReadAggregates.get() && prop.UseAggregates.get()) {
            return;
        }        
        prop.EnableGroupingSets.set(true);
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

        prop.EnableGroupingSets.set(false);

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
        if (prop.ReadAggregates.get() && prop.UseAggregates.get()) {
            return;
        }
        prop.EnableGroupingSets.set(true);
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

        prop.EnableGroupingSets.set(false);

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
    
    public void testGroupingSetForMultipleColumnConstraintAndCompoundConstraint() {
        if (prop.ReadAggregates.get() && prop.UseAggregates.get()) {
            return;
        }
        List<String[]> compoundMembers = new ArrayList<String[]>();
        compoundMembers.add(new String[] {"Food", "Deli"});
        compoundMembers.add(new String[] {"Drink", "Beverages"});
        CellRequestConstraint constraint = 
            makeConstraintCountryState(compoundMembers);
        
        CellRequest request1 = createRequest(cubeNameSales2,
            measureCustomerCount, new String[]{tableCustomer, tableTime},
            new String[]{fieldGender, fieldYear},
            new String[]{"M", "1997"}, constraint);

        CellRequest request2 = createRequest(cubeNameSales2,
            measureCustomerCount, new String[]{tableCustomer, tableTime},
            new String[]{fieldGender, fieldYear},
            new String[]{"F", "1997"}, constraint);

        CellRequest request3 = createRequest(cubeNameSales2,
            measureCustomerCount, tableTime, fieldYear, "1997", constraint);

        String sqlWithoutGS =
            "select " +
            "\"time_by_day\".\"the_year\" as \"c0\", \"customer\".\"gender\" as \"c1\", " +
            "count(distinct \"sales_fact_1997\".\"customer_id\") as \"m0\" " +
            "from " +
            "\"time_by_day\" \"time_by_day\", \"sales_fact_1997\" \"sales_fact_1997\", " + 
            "\"customer\" \"customer\", \"store\" \"store\" " + 
            "where \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" and " +
            "\"time_by_day\".\"the_year\" = 1997 and " +
            "\"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\" and " + 
            "\"sales_fact_1997\".\"store_id\" = \"store\".\"store_id\" " +
            "and ((\"store\".\"store_country\" = 'Food' and \"store\".\"store_state\" = 'Deli') or " +
            "(\"store\".\"store_country\" = 'Drink' and \"store\".\"store_state\" = 'Beverages')) " +
            "group by \"time_by_day\".\"the_year\", \"customer\".\"gender\"";
            
        SqlPattern[] patternsGSDisabled = {
            new SqlPattern(SqlPattern.Dialect.ORACLE, sqlWithoutGS, sqlWithoutGS)
        };

        // Currently grouping sets are disabled for distinct count aggregates.
        // even without the compound constraints.
        SqlPattern[] patternsGSEnabled = patternsGSDisabled;

        prop.EnableGroupingSets.set(true);

        assertRequestSql(
            new CellRequest[] {request3, request1, request2}, patternsGSEnabled);

        prop.EnableGroupingSets.set(false);

        assertRequestSql(
            new CellRequest[]{request3, request1, request2}, patternsGSDisabled);
    }
}

// End GroupingSetQueryTest.java
