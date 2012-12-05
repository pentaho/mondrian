/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2004-2005 Julian Hyde
// Copyright (C) 2005-2012 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.rolap.agg.CellRequest;
import mondrian.spi.Dialect;
import mondrian.test.*;

import org.olap4j.impl.Olap4jUtil;

import java.util.*;

/**
 * Test support for generating SQL queries with the <code>GROUPING SETS</code>
 * construct, if the DBMS supports it.
 *
 * @author Thiyagu
 * @since 08-Jun-2007
 */
public class GroupingSetQueryTest extends BatchTestCase {
    private static final String cubeNameSales2 = "Sales 2";
    private static final String measureStoreSales = "[Measures].[Store Sales]";
    private static final String fieldNameMaritalStatus = "marital_status";
    private static final String measureCustomerCount =
        "[Measures].[Customer Count]";

    private static final Set<Dialect.DatabaseProduct> ORACLE_TERADATA =
        Olap4jUtil.enumSetOf(
            Dialect.DatabaseProduct.ORACLE,
            Dialect.DatabaseProduct.TERADATA);

    protected void setUp() throws Exception {
        super.setUp();

        propSaver.props.GenerateFormattedSql.set(false);

        // This test warns of missing sql patterns for
        //
        // ACCESS
        // ORACLE
        final Dialect dialect = getTestContext().getDialect();
        if (propSaver.props.WarnIfNoPatternForDialect.get().equals("ANY")
            || dialect.getDatabaseProduct() == Dialect.DatabaseProduct.ACCESS
            || dialect.getDatabaseProduct() == Dialect.DatabaseProduct.ORACLE)
        {
            propSaver.props.WarnIfNoPatternForDialect.set(
                dialect.getDatabaseProduct().toString());
        } else {
            // Do not warn unless the dialect is "ACCESS" or "ORACLE", or
            // if the test chooses to warn regardless of the dialect.
            propSaver.props.WarnIfNoPatternForDialect.set("NONE");
        }
    }

    public void testGroupingSetsWithAggregateOverDefaultMember() {
        // testcase for MONDRIAN-705
        if (getTestContext().getDialect().supportsGroupingSets()) {
            propSaver.set(propSaver.props.EnableGroupingSets, true);
        }
        assertQueryReturns(
            "with member [Gender].[agg] as ' "
            + "  Aggregate({[Gender].DefaultMember}, [Measures].[Store Cost])' "
            + "select "
            + "  {[Measures].[Store Cost]} ON COLUMNS, "
            + "  {[Gender].[Gender].Members, [Gender].[agg]} ON ROWS "
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Store Cost]}\n"
            + "Axis #2:\n"
            + "{[Customer].[Gender].[F]}\n"
            + "{[Customer].[Gender].[M]}\n"
            + "{[Customer].[Gender].[agg]}\n"
            + "Row #0: 111,777.48\n"
            + "Row #1: 113,849.75\n"
            + "Row #2: 225,627.23\n");
    }

    public void testGroupingSetForSingleColumnConstraint() {
        final TestContext testContext = getTestContext();
        propSaver.set(propSaver.props.DisableCaching, false);

        CellRequest request1 =
            createRequest(
                testContext,
                cubeNameSales2, measureUnitSales, tableCustomer, fieldGender,
                "M");

        CellRequest request2 =
            createRequest(
                testContext,
                cubeNameSales2, measureUnitSales, tableCustomer, fieldGender,
                "F");

        CellRequest request3 =
            createRequest(
                testContext,
                cubeNameSales2, measureUnitSales, null, "", "");

        SqlPattern[] patternsWithGsets = {
            new SqlPattern(
                ORACLE_TERADATA,
                "select \"customer\".\"gender\" as \"c0\", sum(\"sales_fact_1997\".\"unit_sales\") as \"m0\", "
                + "grouping(\"customer\".\"gender\") as \"g0\" "
                + "from \"customer\" =as= \"customer\", \"sales_fact_1997\" =as= \"sales_fact_1997\" "
                + "where \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\" "
                + "group by grouping sets ((\"customer\".\"gender\"), ())",
                26)
        };

        // If aggregates are enabled, mondrian should use them. Results should
        // be the same with or without grouping sets enabled.
        SqlPattern[] patternsWithAggs = {
            new SqlPattern(
                ORACLE_TERADATA,
                "select sum(\"agg_c_10_sales_fact_1997\".\"unit_sales\") as \"m0\""
                + " from \"agg_c_10_sales_fact_1997\" \"agg_c_10_sales_fact_1997\"",
                null),
            new SqlPattern(
                ORACLE_TERADATA,
                "select \"agg_g_ms_pcat_sales_fact_1997\".\"gender\" as \"c0\","
                + " sum(\"agg_g_ms_pcat_sales_fact_1997\".\"unit_sales\") as \"m0\" "
                + "from \"agg_g_ms_pcat_sales_fact_1997\" \"agg_g_ms_pcat_sales_fact_1997\" "
                + "group by \"agg_g_ms_pcat_sales_fact_1997\".\"gender\"",
                null)
        };

        SqlPattern[] patternsWithoutGsets = {
            new SqlPattern(
                Dialect.DatabaseProduct.ACCESS,
                "select \"customer\".\"gender\" as \"c0\", sum(\"sales_fact_1997\".\"unit_sales\") as \"m0\" "
                + "from \"customer\" as \"customer\", \"sales_fact_1997\" as \"sales_fact_1997\" "
                + "where \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\" "
                + "group by \"customer\".\"gender\"",
                26),
            new SqlPattern(
                ORACLE_TERADATA,
                "select \"customer\".\"gender\" as \"c0\", sum(\"sales_fact_1997\".\"unit_sales\") as \"m0\" "
                + "from \"customer\" =as= \"customer\", \"sales_fact_1997\" =as= \"sales_fact_1997\" "
                + "where \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\" "
                + "group by \"customer\".\"gender\"",
                26)
        };

        propSaver.set(propSaver.props.EnableGroupingSets, true);

        if (propSaver.props.ReadAggregates.get()
            && propSaver.props.UseAggregates.get())
        {
            assertRequestSql(
                testContext,
                new CellRequest[] {request3, request1, request2},
                patternsWithAggs);
        } else {
            assertRequestSql(
                testContext,
                new CellRequest[] {request3, request1, request2},
                patternsWithGsets);
        }

        propSaver.set(propSaver.props.EnableGroupingSets, false);

        if (propSaver.props.ReadAggregates.get()
            && propSaver.props.UseAggregates.get())
        {
            assertRequestSql(
                testContext,
                new CellRequest[] {request3, request1, request2},
                patternsWithAggs);
        } else {
            assertRequestSql(
                testContext,
                new CellRequest[] {request3, request1, request2},
                patternsWithoutGsets);
        }
    }
    public void testNotUsingGroupingSetWhenGroupUsesDifferentAggregateTable() {
        if (!(propSaver.props.UseAggregates.get()
              && propSaver.props.ReadAggregates.get()))
        {
            return;
        }

        final TestContext testContext = getTestContext();
        CellRequest request1 =
            createRequest(
                testContext,
                cubeNameSales,
                measureUnitSales, tableCustomer, fieldGender, "M");

        CellRequest request2 =
            createRequest(
                testContext,
                cubeNameSales,
                measureUnitSales, tableCustomer, fieldGender, "F");

        CellRequest request3 =
            createRequest(
                testContext,
                cubeNameSales,
                measureUnitSales, null, "", "");

        propSaver.set(propSaver.props.EnableGroupingSets, true);

        SqlPattern[] patternsWithoutGsets = {
            new SqlPattern(
                Dialect.DatabaseProduct.ACCESS,
                "select \"agg_g_ms_pcat_sales_fact_1997\".\"gender\" as \"c0\", "
                + "sum(\"agg_g_ms_pcat_sales_fact_1997\".\"unit_sales\") as \"m0\" "
                + "from \"agg_g_ms_pcat_sales_fact_1997\" as \"agg_g_ms_pcat_sales_fact_1997\" "
                + "group by \"agg_g_ms_pcat_sales_fact_1997\".\"gender\"",
                26),
            new SqlPattern(
                ORACLE_TERADATA,
                "select \"agg_g_ms_pcat_sales_fact_1997\".\"gender\" as \"c0\", "
                + "sum(\"agg_g_ms_pcat_sales_fact_1997\".\"unit_sales\") as \"m0\" "
                + "from \"agg_g_ms_pcat_sales_fact_1997\" \"agg_g_ms_pcat_sales_fact_1997\" "
                + "group by \"agg_g_ms_pcat_sales_fact_1997\".\"gender\"",
                26)
        };
        assertRequestSql(
            testContext,
            new CellRequest[] {request3, request1, request2},
            patternsWithoutGsets);
    }

    public void testNotUsingGroupingSet() {
        if (propSaver.props.ReadAggregates.get()
            && propSaver.props.UseAggregates.get())
        {
            return;
        }
        propSaver.set(propSaver.props.EnableGroupingSets, true);
        final TestContext testContext = getTestContext();
        CellRequest request1 =
            createRequest(
                testContext,
                cubeNameSales2,
                measureUnitSales, tableCustomer, fieldGender, "M");

        CellRequest request2 =
            createRequest(
                testContext,
                cubeNameSales2,
                measureUnitSales, tableCustomer, fieldGender, "F");

        SqlPattern[] patternsWithGsets = {
            new SqlPattern(
                ORACLE_TERADATA,
                "select \"customer\".\"gender\" as \"c0\", sum(\"sales_fact_1997\".\"unit_sales\") as \"m0\" "
                + "from \"customer\" =as= \"customer\", \"sales_fact_1997\" =as= \"sales_fact_1997\" "
                + "where \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\" "
                + "group by \"customer\".\"gender\"", 72)
            };
        assertRequestSql(
            testContext,
            new CellRequest[] {request1, request2},
            patternsWithGsets);

        propSaver.set(propSaver.props.EnableGroupingSets, false);

        SqlPattern[] patternsWithoutGsets = {
            new SqlPattern(
                Dialect.DatabaseProduct.ACCESS,
                "select \"customer\".\"gender\" as \"c0\", sum(\"sales_fact_1997\".\"unit_sales\") as \"m0\" "
                + "from \"customer\" as \"customer\", \"sales_fact_1997\" as \"sales_fact_1997\" "
                + "where \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\" "
                + "group by \"customer\".\"gender\"", 72),
            new SqlPattern(
                ORACLE_TERADATA,
                "select \"customer\".\"gender\" as \"c0\", sum(\"sales_fact_1997\".\"unit_sales\") as \"m0\" "
                + "from \"customer\" =as= \"customer\", \"sales_fact_1997\" =as= \"sales_fact_1997\" "
                + "where \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\" "
                + "group by \"customer\".\"gender\"", 72)
        };
        assertRequestSql(
            testContext,
            new CellRequest[] {request1, request2},
            patternsWithoutGsets);
    }

    public void testGroupingSetForMultipleMeasureAndSingleConstraint() {
        if (propSaver.props.ReadAggregates.get()
            && propSaver.props.UseAggregates.get())
        {
            return;
        }
        propSaver.set(propSaver.props.EnableGroupingSets, true);
        final TestContext testContext = getTestContext();

        CellRequest request1 =
            createRequest(
                testContext,
                cubeNameSales2,
                measureUnitSales, tableCustomer, fieldGender, "M");
        CellRequest request2 =
            createRequest(
                testContext,
                cubeNameSales2,
                measureUnitSales, tableCustomer, fieldGender, "F");
        CellRequest request3 =
            createRequest(
                testContext,
                cubeNameSales2,
                measureUnitSales, null, "", "");
        CellRequest request4 =
            createRequest(
                testContext,
                cubeNameSales2,
                measureStoreSales, tableCustomer, fieldGender, "M");
        CellRequest request5 =
            createRequest(
                testContext,
                cubeNameSales2,
                measureStoreSales, tableCustomer, fieldGender, "F");
        CellRequest request6 =
            createRequest(
                testContext,
                cubeNameSales2,
                measureStoreSales, null, "", "");

        SqlPattern[] patternsWithGsets = {
            new SqlPattern(
                ORACLE_TERADATA,
                "select \"customer\".\"gender\" as \"c0\", sum(\"sales_fact_1997\".\"unit_sales\") as \"m0\", "
                + "sum(\"sales_fact_1997\".\"store_sales\") as \"m1\", grouping(\"customer\".\"gender\") as \"g0\" "
                + "from \"customer\" =as= \"customer\", \"sales_fact_1997\" =as= \"sales_fact_1997\" "
                + "where \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\" "
                + "group by grouping sets ((\"customer\".\"gender\"), ())",
                26)
        };
        assertRequestSql(
            testContext,
            new CellRequest[] {
                request1, request2, request3, request4, request5, request6},
            patternsWithGsets);

        propSaver.set(propSaver.props.EnableGroupingSets, false);

        SqlPattern[] patternsWithoutGsets = {
            new SqlPattern(
                Dialect.DatabaseProduct.ACCESS,
                "select \"customer\".\"gender\" as \"c0\", sum(\"sales_fact_1997\".\"unit_sales\") as \"m0\", "
                + "sum(\"sales_fact_1997\".\"store_sales\") as \"m1\" "
                + "from \"customer\" as \"customer\", \"sales_fact_1997\" as \"sales_fact_1997\" "
                + "where \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\" "
                + "group by \"customer\".\"gender\"", 26),
            new SqlPattern(
                ORACLE_TERADATA,
                "select \"customer\".\"gender\" as \"c0\", sum(\"sales_fact_1997\".\"unit_sales\") as \"m0\", "
                + "sum(\"sales_fact_1997\".\"store_sales\") as \"m1\" "
                + "from \"customer\" =as= \"customer\", \"sales_fact_1997\" =as= \"sales_fact_1997\" "
                + "where \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\" "
                + "group by \"customer\".\"gender\"", 26)
        };
        assertRequestSql(
            testContext,
            new CellRequest[] {
                request1, request2, request3, request4, request5, request6},
            patternsWithoutGsets);
    }

    public void testGroupingSetForASummaryCanBeGroupedWith2DetailBatch() {
        if (propSaver.props.ReadAggregates.get()
            && propSaver.props.UseAggregates.get())
        {
            return;
        }
        propSaver.set(propSaver.props.EnableGroupingSets, true);
        final TestContext testContext = getTestContext();
        CellRequest request1 =
            createRequest(
                testContext,
                cubeNameSales2,
                measureUnitSales, tableCustomer, fieldGender, "M");
        CellRequest request2 =
            createRequest(
                testContext,
                cubeNameSales2,
                measureUnitSales, tableCustomer, fieldGender, "F");
        CellRequest request3 =
            createRequest(
                testContext,
                cubeNameSales2,
                measureUnitSales, null, "", "");
        CellRequest request4 =
            createRequest(
                testContext,
                cubeNameSales2,
                measureUnitSales, tableCustomer, fieldNameMaritalStatus, "M");
        CellRequest request5 =
            createRequest(
                testContext,
                cubeNameSales2,
                measureUnitSales, tableCustomer, fieldNameMaritalStatus, "S");
        CellRequest request6 =
            createRequest(
                testContext,
                cubeNameSales2,
                measureUnitSales, null, "", "");

        SqlPattern[] patternWithGsets = {
            new SqlPattern(
                ORACLE_TERADATA,
                "select \"customer\".\"gender\" as \"c0\", sum(\"sales_fact_1997\".\"unit_sales\") as \"m0\", "
                + "grouping(\"customer\".\"gender\") as \"g0\" "
                + "from \"customer\" =as= \"customer\", \"sales_fact_1997\" =as= \"sales_fact_1997\" "
                + "where \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\" "
                + "group by grouping sets ((\"customer\".\"gender\"), ())",
                26),

            new SqlPattern(
                ORACLE_TERADATA,
                "select \"customer\".\"marital_status\" as \"c0\", sum(\"sales_fact_1997\".\"unit_sales\") as \"m0\" "
                + "from \"customer\" =as= \"customer\", \"sales_fact_1997\" =as= \"sales_fact_1997\" "
                + "where \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\" "
                + "group by \"customer\".\"marital_status\"",
                26),
            };

        assertRequestSql(
            testContext,
            new CellRequest[] {
                request1, request2, request3, request4, request5, request6},
            patternWithGsets);

        propSaver.set(propSaver.props.EnableGroupingSets, false);

        SqlPattern[] patternWithoutGsets = {
            new SqlPattern(
                Dialect.DatabaseProduct.ACCESS,
                "select sum(\"sales_fact_1997\".\"unit_sales\") as \"m0\" "
                + "from \"sales_fact_1997\" as \"sales_fact_1997\"",
                40),
            new SqlPattern(
                ORACLE_TERADATA,
                "select sum(\"sales_fact_1997\".\"unit_sales\") as \"m0\" "
                + "from \"sales_fact_1997\" =as= \"sales_fact_1997\"",
                40)
        };

        assertRequestSql(
            testContext,
            new CellRequest[] {
                request1, request2, request3, request4, request5, request6},
            patternWithoutGsets);
    }

    public void testGroupingSetForMultipleColumnConstraint() {
        if (propSaver.props.ReadAggregates.get()
            && propSaver.props.UseAggregates.get())
        {
            return;
        }
        propSaver.set(propSaver.props.EnableGroupingSets, true);
        final TestContext testContext = getTestContext();
        CellRequest request1 =
            createRequest(
                testContext,
                cubeNameSales2,
                measureUnitSales,
                list(tableCustomer, tableTime),
                list(fieldGender, fieldYear),
                list("M", "1997"));

        CellRequest request2 =
            createRequest(
                testContext,
                cubeNameSales2,
                measureUnitSales,
                list(tableCustomer, tableTime),
                list(fieldGender, fieldYear),
                list("F", "1997"));

        CellRequest request3 =
            createRequest(
                testContext,
                cubeNameSales2,
                measureUnitSales, tableTime, fieldYear, "1997");

        SqlPattern[] patternsWithGsets = {
            new SqlPattern(
                ORACLE_TERADATA,
                "select \"time_by_day\".\"the_year\" as \"c0\", \"customer\".\"gender\" as \"c1\", "
                + "sum(\"sales_fact_1997\".\"unit_sales\") as \"m0\", grouping(\"customer\".\"gender\") as \"g0\" "
                + "from \"time_by_day\" =as= \"time_by_day\", \"sales_fact_1997\" =as= \"sales_fact_1997\", \"customer\" =as= \"customer\" "
                + "where \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" and \"time_by_day\".\"the_year\" = 1997 "
                + "and \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\" "
                + "group by grouping sets ((\"time_by_day\".\"the_year\", \"customer\".\"gender\"), (\"time_by_day\".\"the_year\"))",
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
            testContext,
            new CellRequest[] {request3, request1, request2},
            patternsWithGsets);

        propSaver.set(propSaver.props.EnableGroupingSets, false);

        SqlPattern[] patternsWithoutGsets = {
            new SqlPattern(
                Dialect.DatabaseProduct.ACCESS,
                "select \"time_by_day\".\"the_year\" as \"c0\", \"customer\".\"gender\" as \"c1\", "
                + "sum(\"sales_fact_1997\".\"unit_sales\") as \"m0\" "
                + "from \"time_by_day\" as \"time_by_day\", \"sales_fact_1997\" as \"sales_fact_1997\", "
                + "\"customer\" as \"customer\" "
                + "where \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" and "
                + "\"time_by_day\".\"the_year\" = 1997 and "
                + "\"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\" "
                + "group by \"time_by_day\".\"the_year\", \"customer\".\"gender\"",
                50),
            new SqlPattern(
                ORACLE_TERADATA,
                "select \"time_by_day\".\"the_year\" as \"c0\", \"customer\".\"gender\" as \"c1\", "
                + "sum(\"sales_fact_1997\".\"unit_sales\") as \"m0\" "
                + "from \"time_by_day\" =as= \"time_by_day\", \"sales_fact_1997\" =as= \"sales_fact_1997\", "
                + "\"customer\" =as= \"customer\" "
                + "where \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" and "
                + "\"time_by_day\".\"the_year\" = 1997 "
                + "and \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\" "
                + "group by \"time_by_day\".\"the_year\", \"customer\".\"gender\"",
                    50)
            };
        assertRequestSql(
            testContext,
            new CellRequest[]{request3, request1, request2},
            patternsWithoutGsets);
    }

    public void
        testGroupingSetForMultipleColumnConstraintAndCompoundConstraint()
    {
        if (propSaver.props.ReadAggregates.get()
            && propSaver.props.UseAggregates.get())
        {
            return;
        }
        final TestContext testContext = getTestContext();
        List<List<String>> compoundMembers = list(
            list("USA", "OR"),
            list("CANADA", "BC"));
        CellRequestConstraint constraint =
            makeConstraintCountryState(compoundMembers);

        CellRequest request1 =
            createRequest(
                testContext,
                cubeNameSales2,
                measureCustomerCount,
                list(tableCustomer, tableTime),
                list(fieldGender, fieldYear),
                list("M", "1997"),
                constraint);

        CellRequest request2 =
            createRequest(
                testContext,
                cubeNameSales2,
                measureCustomerCount,
                list(tableCustomer, tableTime),
                list(fieldGender, fieldYear),
                list("F", "1997"),
                constraint);

        CellRequest request3 =
            createRequest(
                testContext,
                cubeNameSales2,
                measureCustomerCount, tableTime, fieldYear, "1997", constraint);

        String sqlWithoutGS =
            "select \"time_by_day\".\"the_year\" as \"c0\", \"customer\".\"gender\" as \"c1\", "
            + "count(distinct \"sales_fact_1997\".\"customer_id\") as \"m0\" from \"time_by_day\" =as= \"time_by_day\", "
            + "\"sales_fact_1997\" =as= \"sales_fact_1997\", \"customer\" =as= \"customer\", \"store\" =as= \"store\" "
            + "where \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" and \"time_by_day\".\"the_year\" = 1997 "
            + "and \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\" and "
            + "\"sales_fact_1997\".\"store_id\" = \"store\".\"store_id\" and "
            + "((\"store\".\"store_country\" = 'USA' and \"store\".\"store_state\" = 'OR') or "
            + "(\"store\".\"store_country\" = 'CANADA' and \"store\".\"store_state\" = 'BC')) "
            + "group by \"time_by_day\".\"the_year\", \"customer\".\"gender\"";

        SqlPattern[] patternsGSDisabled = {
            new SqlPattern(ORACLE_TERADATA, sqlWithoutGS, sqlWithoutGS)
        };
        // as of change 12310 GS has been removed from distinct count queries,
        // since there is little or no performance benefit and there is a bug
        // related to it (2207515)
        SqlPattern[] patternsGSEnabled = patternsGSDisabled;

        propSaver.set(propSaver.props.EnableGroupingSets, true);

        assertRequestSql(
            testContext,
            new CellRequest[] {request3, request1, request2},
            patternsGSEnabled);

        propSaver.set(propSaver.props.EnableGroupingSets, false);

        assertRequestSql(
            testContext,
            new CellRequest[]{request3, request1, request2},
            patternsGSDisabled);
    }

    /**
     * Test case for
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-426">
     * bug MONDRIAN-426, "Except not working with grouping sets"</a>.
     */
    public void testBugMondrian426() {
        assertQueryReturns(
            "with member store.stores.allbutwallawalla as\n"
            + " 'aggregate(\n"
            + "    except(\n"
            + "        store.[store name].members,\n"
            + "        { [Store].[All Stores].[USA].[WA].[Walla Walla].[Store 22]}))'\n"
            + "select {\n"
            + "          store.[store name].members,\n"
            + "         store.allbutwallawalla,\n"
            + "         store.[all stores]} on 0,\n"
            + "  {measures.[customer count]} on 1\n"
            + "from sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store].[Stores].[Canada].[BC].[Vancouver].[Store 19]}\n"
            + "{[Store].[Stores].[Canada].[BC].[Victoria].[Store 20]}\n"
            + "{[Store].[Stores].[Mexico].[DF].[Mexico City].[Store 9]}\n"
            + "{[Store].[Stores].[Mexico].[DF].[San Andres].[Store 21]}\n"
            + "{[Store].[Stores].[Mexico].[Guerrero].[Acapulco].[Store 1]}\n"
            + "{[Store].[Stores].[Mexico].[Jalisco].[Guadalajara].[Store 5]}\n"
            + "{[Store].[Stores].[Mexico].[Veracruz].[Orizaba].[Store 10]}\n"
            + "{[Store].[Stores].[Mexico].[Yucatan].[Merida].[Store 8]}\n"
            + "{[Store].[Stores].[Mexico].[Zacatecas].[Camacho].[Store 4]}\n"
            + "{[Store].[Stores].[Mexico].[Zacatecas].[Hidalgo].[Store 12]}\n"
            + "{[Store].[Stores].[Mexico].[Zacatecas].[Hidalgo].[Store 18]}\n"
            + "{[Store].[Stores].[USA].[CA].[Alameda].[HQ]}\n"
            + "{[Store].[Stores].[USA].[CA].[Beverly Hills].[Store 6]}\n"
            + "{[Store].[Stores].[USA].[CA].[Los Angeles].[Store 7]}\n"
            + "{[Store].[Stores].[USA].[CA].[San Diego].[Store 24]}\n"
            + "{[Store].[Stores].[USA].[CA].[San Francisco].[Store 14]}\n"
            + "{[Store].[Stores].[USA].[OR].[Portland].[Store 11]}\n"
            + "{[Store].[Stores].[USA].[OR].[Salem].[Store 13]}\n"
            + "{[Store].[Stores].[USA].[WA].[Bellingham].[Store 2]}\n"
            + "{[Store].[Stores].[USA].[WA].[Bremerton].[Store 3]}\n"
            + "{[Store].[Stores].[USA].[WA].[Seattle].[Store 15]}\n"
            + "{[Store].[Stores].[USA].[WA].[Spokane].[Store 16]}\n"
            + "{[Store].[Stores].[USA].[WA].[Tacoma].[Store 17]}\n"
            + "{[Store].[Stores].[USA].[WA].[Walla Walla].[Store 22]}\n"
            + "{[Store].[Stores].[USA].[WA].[Yakima].[Store 23]}\n"
            + "{[Store].[Stores].[allbutwallawalla]}\n"
            + "{[Store].[Stores].[All Stores]}\n"
            + "Axis #2:\n"
            + "{[Measures].[Customer Count]}\n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: 1,059\n"
            + "Row #0: 1,147\n"
            + "Row #0: 962\n"
            + "Row #0: 296\n"
            + "Row #0: 563\n"
            + "Row #0: 474\n"
            + "Row #0: 190\n"
            + "Row #0: 179\n"
            + "Row #0: 906\n"
            + "Row #0: 84\n"
            + "Row #0: 278\n"
            + "Row #0: 96\n"
            + "Row #0: 95\n"
            + "Row #0: 5,485\n"
            + "Row #0: 5,581\n");
    }
}

// End GroupingSetQueryTest.java
