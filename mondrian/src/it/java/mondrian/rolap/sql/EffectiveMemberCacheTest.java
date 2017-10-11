/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2006-2017 Hitachi Vantara and others
// All Rights Reserved.
 */
package mondrian.rolap.sql;

import mondrian.rolap.*;
import mondrian.spi.Dialect;
import mondrian.test.*;

public class EffectiveMemberCacheTest extends BatchTestCase {

    TestContext testContext;

    @Override
    public void setUp() {
        clearCache();
        propSaver.set(propSaver.properties.GenerateFormattedSql, true);
        propSaver.set(propSaver.properties.EnableNativeNonEmpty, true);
    }

    public void testCachedLevelMembers() {
        // verify query for specific members can be fulfilled by members cached
        // from a level members query.
        String sql = "select\n"
                + "    `product`.`product_name` as `c0`\n"
                + "from\n"
                + "    `product` as `product`,\n"
                + "    `product_class` as `product_class`\n"
                + "where\n"
                + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
                + "and\n"
                + "    (`product`.`brand_name` = 'Hermanos' and `product_class`.`product_subcategory` = 'Fresh Fruit' and `product_class`.`product_category` = 'Fruit' and `product_class`.`product_department` = 'Produce' and `product_class`.`product_family` = 'Food')\n"
                + "and\n"
                + "    ( UPPER(`product`.`product_name`) IN (UPPER('Hermanos Fancy Plums'),UPPER('Hermanos Lemons'),UPPER('Hermanos Plums')))\n"
                + "group by\n"
                + "    `product`.`product_name`\n"
                + "order by\n"
                + (TestContext.instance().getDialect().requiresOrderByAlias()
                ? "    ISNULL(`c0`) ASC, `c0` ASC"
                : "    ISNULL(`product`.`product_name`) ASC, "
                + "`product`.`product_name` ASC");
        testWithAndWithoutCachedMembers(
            "select Product.[Product Name].members on 0 from sales",
            "select "
            + " { [Product].[Food].[Produce].[Fruit].[Fresh Fruit].[Hermanos].[Hermanos Fancy Plums], "
            + "[Product].[Food].[Produce].[Fruit].[Fresh Fruit].[Hermanos].[Hermanos Lemons],"
            + "[Product].[Food].[Produce].[Fruit].[Fresh Fruit].[Hermanos].[Hermanos Plums] }"
            + " on 0 from sales",
            new SqlPattern[]{
                new SqlPattern(
                    Dialect.DatabaseProduct.MYSQL, sql, null)}
        );
    }

    public void testCachedChildMembers() {
        // verify query for specific members can be fulfilled by members cached
        // from a child members query.
        String sql = "select\n"
                + "    `product`.`product_name` as `c0`\n"
                + "from\n"
                + "    `product` as `product`,\n"
                + "    `product_class` as `product_class`\n"
                + "where\n"
                + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
                + "and\n"
                + "    (`product`.`brand_name` = 'Hermanos' and `product_class`.`product_subcategory` = 'Fresh Fruit' and `product_class`.`product_category` = 'Fruit' and `product_class`.`product_department` = 'Produce' and `product_class`.`product_family` = 'Food')\n"
                + "and\n"
                + "    ( UPPER(`product`.`product_name`) IN "
                + "(UPPER('Hermanos Fancy Plums'),UPPER('Hermanos Lemons'),UPPER('Hermanos Plums')))\n"
                + "group by\n"
                + "    `product`.`product_name`\n"
                + "order by\n"
                + (TestContext.instance().getDialect().requiresOrderByAlias()
                ? "    ISNULL(`c0`) ASC, `c0` ASC"
                : "    ISNULL(`product`.`product_name`) ASC, "
                + "`product`.`product_name` ASC");
        testWithAndWithoutCachedMembers(
            "select [Product].[Food].[Produce].[Fruit].[Fresh Fruit].[Hermanos].Children on 0 from sales",
            "select "
            + " { [Product].[Food].[Produce].[Fruit].[Fresh Fruit].[Hermanos].[Hermanos Fancy Plums], "
            + "[Product].[Food].[Produce].[Fruit].[Fresh Fruit].[Hermanos].[Hermanos Lemons],"
            + "[Product].[Food].[Produce].[Fruit].[Fresh Fruit].[Hermanos].[Hermanos Plums] }"
            + " on 0 from sales",
            new SqlPattern[]{
                new SqlPattern(
                    Dialect.DatabaseProduct.MYSQL, sql, null) }
        );
    }

    public void testLevelPreCacheThreshold() {
        // [Store Type] members cardinality falls well below
        // LevelPreCacheThreshold.  All members should be loaded, not
        // just the 2 referenced.
        propSaver.set(propSaver.properties.LevelPreCacheThreshold, 300);
        String sql = "select\n"
                + "    `store`.`store_type` as `c0`\n"
                + "from\n"
                + "    `store` as `store`\n"
                + "group by\n"
                + "    `store`.`store_type`\n"
                + "order by\n"
                + (TestContext.instance().getDialect().requiresOrderByAlias()
                ? "    ISNULL(`c0`) ASC, `c0` ASC"
                : "    ISNULL(`store`.`store_type`) ASC, "
                + "`store`.`store_type` ASC");
        assertQuerySql(
            testContext,
            "select {[Store Type].[Gourmet Supermarket], "
            + "[Store Type].[HeadQuarters]} on 0 from sales",
            new SqlPattern[] {
                new SqlPattern(
                    Dialect.DatabaseProduct.MYSQL, sql, null)
            });
    }

    public void testLevelPreCacheThresholdDisabled() {
        // with LevelPreCacheThreshold set to 0, we should not load
        // all [store type] members, we should only retrieve the 2
        // specified.
        propSaver.set(propSaver.properties.LevelPreCacheThreshold, 0);
        String sql = "select\n"
                + "    `store`.`store_type` as `c0`\n"
                + "from\n"
                + "    `store` as `store`\n"
                + "where\n"
                + "    ( UPPER(`store`.`store_type`) IN "
                + "(UPPER('Gourmet Supermarket'),UPPER('HeadQuarters')))\n"
                + "group by\n"
                + "    `store`.`store_type`\n"
                + "order by\n"
                + (TestContext.instance().getDialect().requiresOrderByAlias()
                ? "    ISNULL(`c0`) ASC, `c0` ASC"
                : "    ISNULL(`store`.`store_type`) ASC, "
                + "`store`.`store_type` ASC");
        assertQuerySql(
            testContext,
            "select {[Store Type].[Gourmet Supermarket], "
            + "[Store Type].[HeadQuarters]} on 0 from sales",
            new SqlPattern[] {
                new SqlPattern(
                    Dialect.DatabaseProduct.MYSQL, sql, null)
            });
    }

    public void testLevelPreCacheThresholdParentDegenerate() {
        // we should avoid pulling all deg members, regardless of cardinality.
        // The cost of doing full scans of the fact table is assumed
        // to be too high.
        propSaver.set(propSaver.properties.LevelPreCacheThreshold, 1000);
        String sql = "select\n"
                + "    `store`.`coffee_bar` as `c0`\n"
                + "from\n"
                + "    `store` as `store`\n"
                + "where\n"
                + "    `store`.`coffee_bar` = false\n"
                + "group by\n"
                + "    `store`.`coffee_bar`\n"
                + "order by\n"
                + (TestContext.instance().getDialect().requiresOrderByAlias()
                ? "    ISNULL(`c0`) ASC, `c0` ASC"
                : "    ISNULL(`store`.`coffee_bar`) ASC, "
                + "`store`.`coffee_bar` ASC");
        assertQuerySql(
            testContext,
            "select {[Has coffee bar].[All Has coffee bars].[false]} on 0 from Store",
            new SqlPattern[]{
                new SqlPattern(
                    Dialect.DatabaseProduct.MYSQL, sql, null)});
    }


    /**
     * Execute testMdx both with and without running the cacheMdx first,
     * validating that sqlToLoadTestMdxMembers either fires or doesn't fire,
     * as appropriate.
     *
     * Assumption is that if the cacheMdx has fired, then members shoould
     * already be in cache and there is no need to load them.  If cacheMedx
     * is not fired we should see the sqlToLoadTestMdxMembers.
     */
    private void testWithAndWithoutCachedMembers(
        String cacheMdx, String testMdx, SqlPattern[] sqlToLoadTestMdxMembers)
    {
        for (boolean membersCached : new boolean[] {false, true}) {
            clearCache();
            if (membersCached) {
                testContext.executeQuery(cacheMdx);
            }
            assertQuerySqlOrNot(
                testContext,
                testMdx,
                sqlToLoadTestMdxMembers,
                membersCached, false, false);
        }
    }

    private void clearCache() {
        getTestContext().flushSchemaCache();
        testContext = getTestContext().withFreshConnection();
    }

}

// End EffectiveMemberCacheTest.java