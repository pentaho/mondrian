/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2008-2011 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap.sql;

import mondrian.rolap.BatchTestCase;
import mondrian.spi.Dialect;
import mondrian.test.SqlPattern;
import mondrian.test.TestContext;

/**
 * Test that various values of {@link Dialect#allowsSelectNotInGroupBy}
 * produce correctly optimized SQL.
 *
 * @author Eric McDermid
 */
public class SelectNotInGroupByTest extends BatchTestCase {

    public static final String storeDimensionLevelIndependent =
        "<Dimension name=\"CustomStore\">\n"
        + "  <Hierarchy hasAll=\"true\" primaryKey=\"store_id\">\n"
        + "    <Table name=\"store\"/>\n"
        + "    <Level name=\"Store Country\" column=\"store_country\" uniqueMembers=\"true\"/>\n"
        + "    <Level name=\"Store City\" column=\"store_city\" uniqueMembers=\"false\">\n"
        + "      <Property name=\"Store State\" column=\"store_state\"/>\n"
        + "    </Level>\n"
        + "    <Level name=\"Store Name\" column=\"store_name\" uniqueMembers=\"true\"/>\n"
        + "  </Hierarchy>\n"
        + "</Dimension>";

    public static final String storeDimensionLevelDependent =
        "<Dimension name=\"CustomStore\">\n"
        + "  <Hierarchy hasAll=\"true\" primaryKey=\"store_id\">\n"
        + "    <Table name=\"store\"/>\n"
        + "    <Level name=\"Store Country\" column=\"store_country\" uniqueMembers=\"true\"/>\n"
        + "    <Level name=\"Store City\" column=\"store_city\" uniqueMembers=\"false\">\n"
        + "      <Property name=\"Store State\" column=\"store_state\" dependsOnLevelValue=\"true\"/>\n"
        + "    </Level>\n"
        + "    <Level name=\"Store Name\" column=\"store_name\" uniqueMembers=\"true\"/>\n"
        + "  </Hierarchy>\n"
        + "</Dimension>";

    public static final String storeDimensionUniqueLevelDependentProp =
        "<Dimension name=\"CustomStore\">\n"
        + "  <Hierarchy hasAll=\"true\" primaryKey=\"store_id\" uniqueKeyLevelName=\"Store Name\">\n"
        + "    <Table name=\"store\"/>\n"
        + "    <Level name=\"Store Country\" column=\"store_country\" uniqueMembers=\"true\"/>\n"
        + "    <Level name=\"Store City\" column=\"store_city\" uniqueMembers=\"false\">\n"
        + "      <Property name=\"Store State\" column=\"store_state\" dependsOnLevelValue=\"true\"/>\n"
        + "    </Level>\n"
        + "    <Level name=\"Store Name\" column=\"store_name\" uniqueMembers=\"true\"/>\n"
        + "  </Hierarchy>\n"
        + "</Dimension>";

    public static final String storeDimensionUniqueLevelIndependentProp =
        "<Dimension name=\"CustomStore\">\n"
        + "  <Hierarchy hasAll=\"true\" primaryKey=\"store_id\" uniqueKeyLevelName=\"Store Name\">\n"
        + "    <Table name=\"store\"/>\n"
        + "    <Level name=\"Store Country\" column=\"store_country\" uniqueMembers=\"true\"/>\n"
        + "    <Level name=\"Store City\" column=\"store_city\" uniqueMembers=\"false\">\n"
        + "      <Property name=\"Store State\" column=\"store_state\" dependsOnLevelValue=\"false\"/>\n"
        + "    </Level>\n"
        + "    <Level name=\"Store Name\" column=\"store_name\" uniqueMembers=\"true\"/>\n"
        + "  </Hierarchy>\n"
        + "</Dimension>";


    public static final String cubeA =
        "<Cube name=\"CustomSales\">\n"
        + "  <Table name=\"sales_fact_1997\"/>\n"
        + "  <DimensionUsage name=\"CustomStore\" source=\"CustomStore\" foreignKey=\"store_id\"/>\n"
        + "  <Measure name=\"Custom Store Sales\" column=\"store_sales\" aggregator=\"sum\" formatString=\"#,###.00\"/>\n"
        + "  <Measure name=\"Custom Store Cost\" column=\"store_cost\" aggregator=\"sum\"/>\n"
        + "  <Measure name=\"Sales Count\" column=\"product_id\" aggregator=\"count\"/>\n"
        + "</Cube>";

    public static final String queryCubeA =
        "select {[Measures].[Custom Store Sales],[Measures].[Custom Store Cost]} on columns, {[CustomStore].[Store Name].Members} on rows from CustomSales";

    public static final String sqlWithAllGroupBy =
        "select \n"
        + "    `store`.`store_country` as `c0`, \n"
        + "    `store`.`store_city` as `c1`, \n"
        + "    `store`.`store_state` as `c2`, \n"
        + "    `store`.`store_name` as `c3`\n"
        + "from \n"
        + "    `store` as `store`\n"
        + "group by \n"
        + "    `store`.`store_country`, \n"
        + "    `store`.`store_city`, \n"
        + "    `store`.`store_state`, \n"
        + "    `store`.`store_name`\n"
        + "order by \n"
        + "    ISNULL(`store`.`store_country`), `store`.`store_country` ASC, \n"
        + "    ISNULL(`store`.`store_city`), `store`.`store_city` ASC, \n"
        + "    ISNULL(`store`.`store_name`), `store`.`store_name` ASC\n";

    public static final String sqlWithNoGroupBy =
        "select \n"
        + "    `store`.`store_country` as `c0`, \n"
        + "    `store`.`store_city` as `c1`, \n"
        + "    `store`.`store_state` as `c2`, \n"
        + "    `store`.`store_name` as `c3`\n"
        + "from \n"
        + "    `store` as `store`\n"
        + "order by \n"
        + "    ISNULL(`store`.`store_country`), `store`.`store_country` ASC, \n"
        + "    ISNULL(`store`.`store_city`), `store`.`store_city` ASC, \n"
        + "    ISNULL(`store`.`store_name`), `store`.`store_name` ASC\n";

    public static final String sqlWithLevelGroupBy =
        "select \n"
        + "    `store`.`store_country` as `c0`, \n"
        + "    `store`.`store_city` as `c1`, \n"
        + "    `store`.`store_state` as `c2`, \n"
        + "    `store`.`store_name` as `c3`\n"
        + "from \n"
        + "    `store` as `store`\n"
        + "group by \n"
        + "    `store`.`store_country`, \n"
        + "    `store`.`store_city`, \n"
        + "    `store`.`store_name`\n"
        + "order by \n"
        + "    ISNULL(`store`.`store_country`), `store`.`store_country` ASC, \n"
        + "    ISNULL(`store`.`store_city`), `store`.`store_city` ASC, \n"
        + "    ISNULL(`store`.`store_name`), `store`.`store_name` ASC\n";


    public void testDependentPropertySkipped() {
        // Property group by should be skipped only if dialect supports it
        String sqlpat;
        if (dialectAllowsSelectNotInGroupBy()) {
            sqlpat = sqlWithLevelGroupBy;
        } else {
            sqlpat = sqlWithAllGroupBy;
        }
        SqlPattern[] sqlPatterns = {
            new SqlPattern(Dialect.DatabaseProduct.MYSQL, sqlpat, sqlpat)
        };

        // Use dimension with level-dependent property
        TestContext tc = TestContext.instance().create(
            storeDimensionLevelDependent,
            cubeA,
            null,
            null,
            null,
            null);
        assertQuerySqlOrNot(tc, queryCubeA, sqlPatterns, false, false, true);
    }

    public void testIndependentPropertyNotSkipped() {
        SqlPattern[] sqlPatterns = {
            new SqlPattern(
                Dialect.DatabaseProduct.MYSQL,
                sqlWithAllGroupBy,
                sqlWithAllGroupBy)
        };

        // Use dimension with level-independent property
        TestContext tc = TestContext.instance().create(
            storeDimensionLevelIndependent,
            cubeA,
            null,
            null,
            null,
            null);
        assertQuerySqlOrNot(tc, queryCubeA, sqlPatterns, false, false, true);
    }

    public void testGroupBySkippedIfUniqueLevel() {
        // If unique level is included and all properties are level
        // dependent, then group by can be skipped regardless of dialect
        SqlPattern[] sqlPatterns = {
            new SqlPattern(
                Dialect.DatabaseProduct.MYSQL,
                sqlWithNoGroupBy,
                sqlWithNoGroupBy)
        };

        // Use dimension with unique level & level-dependent properties
        TestContext tc = TestContext.instance().create(
            storeDimensionUniqueLevelDependentProp,
            cubeA,
            null,
            null,
            null,
            null);
        assertQuerySqlOrNot(tc, queryCubeA, sqlPatterns, false, false, true);
    }

    public void testGroupByNotSkippedIfIndependentProperty() {
        SqlPattern[] sqlPatterns = {
            new SqlPattern(
                Dialect.DatabaseProduct.MYSQL,
                sqlWithAllGroupBy,
                sqlWithAllGroupBy)
        };

        // Use dimension with unique level but level-indpendent property
        TestContext tc = TestContext.instance().create(
            storeDimensionUniqueLevelIndependentProp,
            cubeA,
            null,
            null,
            null,
            null);
        assertQuerySqlOrNot(tc, queryCubeA, sqlPatterns, false, false, true);
    }

    private boolean dialectAllowsSelectNotInGroupBy() {
        final Dialect dialect = getTestContext().getDialect();
        return dialect.allowsSelectNotInGroupBy();
    }
}

// End SelectNotInGroupByTest.java
