/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2006-2015 Pentaho and others
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
        propSaver.set(propSaver.props.GenerateFormattedSql, true);
    }

    public void _testCachedLevelMembers() {
        // Disabled pending MONDRIAN-2341

        // verify query for specific members can be fulfilled by members cached
        // from a level members query.
        testWithAndWithoutCachedMembers(
            "select Product.[Product Name].members on 0 from sales",
            "select "
            + " { [Product].[Food].[Produce].[Fruit].[Fresh Fruit].[Hermanos].[Hermanos Fancy Plums], "
            + "[Product].[Food].[Produce].[Fruit].[Fresh Fruit].[Hermanos].[Hermanos Lemons],"
            + "[Product].[Food].[Produce].[Fruit].[Fresh Fruit].[Hermanos].[Hermanos Plums] }"
            + " on 0 from sales",
            new SqlPattern[]{
                new SqlPattern(
                    Dialect.DatabaseProduct.MYSQL,
                    "select\n"
                    + "    `product`.`product_name` as `c0`\n"
                    + "from\n"
                    + "    `product` as `product`,\n"
                    + "    `product_class` as `product_class`\n"
                    + "where\n"
                    + "    (`product_class`.`product_family` = 'Food' and `product_class`.`product_department` = 'Produce' and `product_class`.`product_category` = 'Fruit' and `product_class`.`product_subcategory` = 'Fresh Fruit' and `product`.`brand_name` = 'Hermanos')\n"
                    + "and\n"
                    + "    ( UPPER(`product`.`product_name`) IN (UPPER('Hermanos Fancy Plums'),UPPER('Hermanos Lemons'),UPPER('Hermanos Plums')))\n"
                    + "and\n"
                    + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
                    + "group by\n"
                    + "    `product`.`product_name`\n"
                    + "order by\n"
                    + "    `product`.`product_name` ASC", null)}
        );
    }

    public void testCachedChildMembers() {
        // verify query for specific members can be fulfilled by members cached
        // from a child members query.
        testWithAndWithoutCachedMembers(
            "select [Product].[Food].[Produce].[Fruit].[Fresh Fruit].[Hermanos].Children on 0 from sales",
            "select "
            + " { [Product].[Food].[Produce].[Fruit].[Fresh Fruit].[Hermanos].[Hermanos Fancy Plums], "
            + "[Product].[Food].[Produce].[Fruit].[Fresh Fruit].[Hermanos].[Hermanos Lemons],"
            + "[Product].[Food].[Produce].[Fruit].[Fresh Fruit].[Hermanos].[Hermanos Plums] }"
            + " on 0 from sales",
            new SqlPattern[]{
                new SqlPattern(
                    Dialect.DatabaseProduct.MYSQL,
                    "select\n"
                    + "    `product`.`product_name` as `c0`\n"
                    + "from\n"
                    + "    `product` as `product`,\n"
                    + "    `product_class` as `product_class`\n"
                    + "where\n"
                    + "    (`product_class`.`product_family` = 'Food' and `product_class`.`product_department` = 'Produce' and `product_class`.`product_category` = 'Fruit' and `product_class`.`product_subcategory` = 'Fresh Fruit' and `product`.`brand_name` = 'Hermanos')\n"
                    + "and\n"
                    + "    ( UPPER(`product`.`product_name`) IN (UPPER('Hermanos Fancy Plums'),UPPER('Hermanos Lemons'),UPPER('Hermanos Plums')))\n"
                    + "and\n"
                    + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
                    + "group by\n"
                    + "    `product`.`product_name`\n"
                    + "order by\n"
                    + "    `product`.`product_name` ASC", null) }
        );
    }

    public void testLevelPreCacheThreshold() {
        // [Store Type] members cardinality falls well below
        // LevelPreCacheThreshold.  All members should be loaded, not
        // just the 2 referenced.
        propSaver.set(propSaver.props.LevelPreCacheThreshold, 300);

        assertQuerySql(
            testContext,
            "select {[Store Type].[Gourmet Supermarket], "
            + "[Store Type].[HeadQuarters]} on 0 from sales",
            new SqlPattern[] {
                new SqlPattern(
                    Dialect.DatabaseProduct.MYSQL,
                    "select\n"
                    + "    `store`.`store_type` as `c0`\n"
                    + "from\n"
                    + "    `store` as `store`\n"
                    + "group by\n"
                    + "    `store`.`store_type`\n"
                    + "order by\n"
                    + "    `store`.`store_type` ASC", null)
            });
    }

    public void testLevelPreCacheThresholdDisabled() {
        propSaver.set(propSaver.props.LevelPreCacheThreshold, 0);

        // with LevelPreCacheThreshold set to 0, we should not load
        // all [store type] members, we should only retrieve the 2
        // specified.
        assertQuerySql(
            testContext.legacy(),
            "select {[Store Type].[Store Type].[Gourmet Supermarket], "
            + "[Store Type].[Store Type].[HeadQuarters]} on 0 from sales",
            new SqlPattern[] {
                new SqlPattern(
                    Dialect.DatabaseProduct.MYSQL,
                    "select\n"
                    + "    `store`.`store_type` as `c0`\n"
                    + "from\n"
                    + "    `store` as `store`\n"
                    + "where\n"
                    + "    ( UPPER(`store`.`store_type`) IN (UPPER('Gourmet Supermarket'),UPPER('HeadQuarters')))\n"
                    + "group by\n"
                    + "    `store`.`store_type`\n"
                    + "order by\n"
                    + "    `store`.`store_type` ASC", null)
            });
    }

    public void testLevelPreCacheThresholdParentDegenerate() {
        // we should avoid pulling all deg members, regardless of cardinality.
        // The cost of doing full scans of the fact table is assumed
        // to be too high.
        propSaver.set(propSaver.props.LevelPreCacheThreshold, 1000);
        assertQuerySql(
            testContext,
            "select {[Store Type].[Store Type].[Deluxe Supermarket]} on 0 from Store",
            new SqlPattern[]{
                new SqlPattern(
                    Dialect.DatabaseProduct.MYSQL,
                    "select\n"
                    + "    `store`.`store_type` as `c0`\n"
                    + "from\n"
                    + "    `store` as `store`\n"
                    + "where\n"
                    + "    UPPER(`store`.`store_type`) = UPPER('Deluxe Supermarket')\n"
                    + "group by\n"
                    + "    `store`.`store_type`\n"
                    + "order by\n"
                    + "    `store`.`store_type` ASC", null)});
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
        testContext = getTestContext().legacy().withFreshConnection();
    }

}

// End EffectiveMemberCacheTest.java