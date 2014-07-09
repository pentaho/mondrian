/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2005-2014 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.olap.*;
import mondrian.rolap.sql.SqlQueryBuilder;
import mondrian.rolap.sql.TupleConstraint;
import mondrian.spi.Dialect;
import mondrian.spi.impl.JdbcDialectImpl;
import mondrian.test.FoodMartTestCase;

import static org.mockito.Mockito.*;

public class SqlTupleReaderTest extends FoodMartTestCase {

    private final RolapMeasureGroup salesMeasureGroup = getCube("Sales")
        .getMeasureGroups().get(0);
    private SqlTupleReader.ColumnLayoutBuilder layoutBuilder;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        propSaver.set(MondrianProperties.instance().GenerateFormattedSql, true);
        layoutBuilder = new SqlTupleReader.ColumnLayoutBuilder();
    }

    @Override
    protected void tearDown() throws Exception {
        propSaver.reset();
    }

    public void testAddLevelMembersSqlOrderByAliasCalcExpr() {
        RolapStarSet starSet = new RolapStarSet(
            salesMeasureGroup.getStar(), salesMeasureGroup, null);
        SqlTupleReader sqlTupleReader = new SqlTupleReader(
            DefaultTupleConstraint.instance());
        Dialect spyDialect = spy(new JdbcDialectImpl());
        when(spyDialect.requiresOrderByAlias()).thenReturn(true);
        when(spyDialect.getDatabaseProduct()).thenReturn(
            Dialect.DatabaseProduct.MYSQL);

        final SqlQueryBuilder queryBuilder =
            new SqlQueryBuilder(spyDialect, "", layoutBuilder);
        sqlTupleReader.addLevelMemberSql(
            queryBuilder,
            getLevel(getCube("Sales"), "Customer", "Customers", "Name"),
            starSet, 0, 1);

        getTestContext().assertSqlEquals(
            "select\n"
            + "    customer.country as c0,\n"
            + "    customer.state_province as c1,\n"
            + "    customer.city as c2,\n"
            + "    CONCAT(customer.fname, ' ', customer.lname) as c3,\n"
            + "    customer.customer_id as c4\n"
            + "from\n"
            + "    customer as customer\n"
            + "group by\n"
            + "    customer.country,\n"
            + "    customer.state_province,\n"
            + "    customer.city,\n"
            + "    CONCAT(customer.fname, ' ', customer.lname),\n"
            + "    customer.customer_id\n"
            + "order by\n"
            + "    CASE WHEN c0 IS NULL THEN 1 ELSE 0 END, c0 ASC,\n"
            + "    CASE WHEN c1 IS NULL THEN 1 ELSE 0 END, c1 ASC,\n"
            + "    CASE WHEN c2 IS NULL THEN 1 ELSE 0 END, c2 ASC,\n"
            + "    CASE WHEN c3 IS NULL THEN 1 ELSE 0 END, c3 ASC,\n"
            + "    CASE WHEN c4 IS NULL THEN 1 ELSE 0 END, c4 ASC",
            queryBuilder.toSqlAndTypes().getKey(), -1);
    }

    public void testAddLevelMembersSqlWithOneLevel() {
        RolapStarSet starSet = new RolapStarSet(
            salesMeasureGroup.getStar(), salesMeasureGroup, null);
        SqlTupleReader sqlTupleReader = new SqlTupleReader(
            DefaultTupleConstraint.instance());
        Dialect spyDialect = spy(new JdbcDialectImpl());
        when(spyDialect.requiresOrderByAlias()).thenReturn(false);
        when(spyDialect.getDatabaseProduct()).thenReturn(
            Dialect.DatabaseProduct.MYSQL);

        final SqlQueryBuilder queryBuilder =
            new SqlQueryBuilder(spyDialect, "", layoutBuilder);
        sqlTupleReader.addLevelMemberSql(
            queryBuilder,
            getLevel(getCube("Sales"), "Time", "Time", "Quarter"),
            starSet, 0, 1);

        getTestContext().assertSqlEquals(
            "select\n"
            + "    time_by_day.the_year as c0,\n"
            + "    time_by_day.quarter as c1\n"
            + "from\n"
            + "    time_by_day as time_by_day\n"
            + "group by\n"
            + "    time_by_day.the_year,\n"
            + "    time_by_day.quarter\n"
            + "order by\n"
            + "    CASE WHEN time_by_day.the_year IS NULL THEN 1 ELSE 0 END, time_by_day.the_year ASC,\n"
            + "    CASE WHEN time_by_day.quarter IS NULL THEN 1 ELSE 0 END, time_by_day.quarter ASC",
            queryBuilder.toSqlAndTypes().getKey(), -1);
    }

    public void testAddLevelMembersSqlWithTwoLevels() {
        RolapStarSet starSet = new RolapStarSet(
            salesMeasureGroup.getStar(), salesMeasureGroup, null);
        SqlTupleReader sqlTupleReader = new SqlTupleReader(
            DefaultTupleConstraint.instance());
        Dialect spyDialect = spy(new JdbcDialectImpl());
        when(spyDialect.requiresOrderByAlias()).thenReturn(false);
        when(spyDialect.getDatabaseProduct()).thenReturn(
            Dialect.DatabaseProduct.MYSQL);

        final SqlQueryBuilder queryBuilder =
            new SqlQueryBuilder(spyDialect, "", layoutBuilder);
        sqlTupleReader.addLevelMemberSql(
            queryBuilder,
            getLevel(getCube("Sales"), "Time", "Time", "Quarter"),
            starSet, 0, 1);
        sqlTupleReader.addLevelMemberSql(
            queryBuilder,
            getLevel(getCube("Sales"), "Store", "Stores", "Store Name"),
            starSet, 0, 1);

        getTestContext().assertSqlEquals(
            "select\n"
            + "    time_by_day.the_year as c0,\n"
            + "    time_by_day.quarter as c1,\n"
            + "    store.store_country as c2,\n"
            + "    store.store_state as c3,\n"
            + "    store.store_city as c4,\n"
            + "    store.store_name as c5,\n"
            + "    store.store_type as c6,\n"
            + "    store.store_manager as c7,\n"
            + "    store.store_sqft as c8,\n"
            + "    store.grocery_sqft as c9,\n"
            + "    store.frozen_sqft as c10,\n"
            + "    store.meat_sqft as c11,\n"
            + "    store.coffee_bar as c12,\n"
            + "    store.store_street_address as c13\n"
            + "from\n"
            + "    time_by_day as time_by_day,\n"
            + "    store as store\n"
            + "group by\n"
            + "    time_by_day.the_year,\n"
            + "    time_by_day.quarter,\n"
            + "    store.store_country,\n"
            + "    store.store_state,\n"
            + "    store.store_city,\n"
            + "    store.store_name\n"
            + "order by\n"
            + "    CASE WHEN time_by_day.the_year IS NULL THEN 1 ELSE 0 END, time_by_day.the_year ASC,\n"
            + "    CASE WHEN time_by_day.quarter IS NULL THEN 1 ELSE 0 END, time_by_day.quarter ASC,\n"
            + "    CASE WHEN store.store_country IS NULL THEN 1 ELSE 0 END, store.store_country ASC,\n"
            + "    CASE WHEN store.store_state IS NULL THEN 1 ELSE 0 END, store.store_state ASC,\n"
            + "    CASE WHEN store.store_city IS NULL THEN 1 ELSE 0 END, store.store_city ASC,\n"
            + "    CASE WHEN store.store_name IS NULL THEN 1 ELSE 0 END, store.store_name ASC",
            queryBuilder.toSqlAndTypes().getKey(), -1);
    }

    public void testAddLevelMembersSqlWithOneLevelReqOrderAlias() {
        RolapStarSet starSet = new RolapStarSet(
            salesMeasureGroup.getStar(), salesMeasureGroup, null);
        SqlTupleReader sqlTupleReader = new SqlTupleReader(
            DefaultTupleConstraint.instance());
        Dialect spyDialect = spy(new JdbcDialectImpl());
        when(spyDialect.requiresOrderByAlias()).thenReturn(true);
        when(spyDialect.getDatabaseProduct()).thenReturn(
            Dialect.DatabaseProduct.HIVE);

        SqlQueryBuilder queryBuilder =
            new SqlQueryBuilder(spyDialect, "", layoutBuilder);
        sqlTupleReader.addLevelMemberSql(
            queryBuilder,
            getLevel(getCube("Sales"), "Time", "Time", "Quarter"),
            starSet, 0, 1);
        getTestContext().assertSqlEquals(
            "select\n"
            + "    time_by_day.the_year as c0,\n"
            + "    time_by_day.quarter as c1\n"
            + "from\n"
            + "    time_by_day as time_by_day\n"
            + "group by\n"
            + "    time_by_day.the_year,\n"
            + "    time_by_day.quarter\n"
            + "order by\n"
            + "    CASE WHEN c0 IS NULL THEN 1 ELSE 0 END, c0 ASC,\n"
            + "    CASE WHEN c1 IS NULL THEN 1 ELSE 0 END, c1 ASC",
            queryBuilder.toSqlAndTypes().getKey(), -1);
    }

    public void testAddLevelMembersSqlWithTwoLevelsReqOrderAlias() {
        RolapStarSet starSet = new RolapStarSet(
            salesMeasureGroup.getStar(), salesMeasureGroup, null);
        SqlTupleReader sqlTupleReader = new SqlTupleReader(
            DefaultTupleConstraint.instance());
        Dialect spyDialect = spy(new JdbcDialectImpl());
        when(spyDialect.requiresOrderByAlias()).thenReturn(true);
        when(spyDialect.getDatabaseProduct()).thenReturn(
            Dialect.DatabaseProduct.HIVE);
        final SqlQueryBuilder queryBuilder =
            new SqlQueryBuilder(spyDialect, "", layoutBuilder);
        sqlTupleReader.addLevelMemberSql(
            queryBuilder,
            getLevel(getCube("Sales"), "Time", "Time", "Quarter"),
            starSet, 0, 1);
        sqlTupleReader.addLevelMemberSql(
            queryBuilder,
            getLevel(getCube("Sales"), "Store", "Stores", "Store Name"),
            starSet, 0, 1);

        getTestContext().assertSqlEquals(
            "select\n"
            + "    time_by_day.the_year as c0,\n"
            + "    time_by_day.quarter as c1,\n"
            + "    store.store_country as c2,\n"
            + "    store.store_state as c3,\n"
            + "    store.store_city as c4,\n"
            + "    store.store_name as c5,\n"
            + "    store.store_type as c6,\n"
            + "    store.store_manager as c7,\n"
            + "    store.store_sqft as c8,\n"
            + "    store.grocery_sqft as c9,\n"
            + "    store.frozen_sqft as c10,\n"
            + "    store.meat_sqft as c11,\n"
            + "    store.coffee_bar as c12,\n"
            + "    store.store_street_address as c13\n"
            + "from\n"
            + "    time_by_day as time_by_day,\n"
            + "    store as store\n"
            + "group by\n"
            + "    time_by_day.the_year,\n"
            + "    time_by_day.quarter,\n"
            + "    store.store_country,\n"
            + "    store.store_state,\n"
            + "    store.store_city,\n"
            + "    store.store_name\n"
            + "order by\n"
            + "    CASE WHEN c0 IS NULL THEN 1 ELSE 0 END, c0 ASC,\n"
            + "    CASE WHEN c1 IS NULL THEN 1 ELSE 0 END, c1 ASC,\n"
            + "    CASE WHEN c2 IS NULL THEN 1 ELSE 0 END, c2 ASC,\n"
            + "    CASE WHEN c3 IS NULL THEN 1 ELSE 0 END, c3 ASC,\n"
            + "    CASE WHEN c4 IS NULL THEN 1 ELSE 0 END, c4 ASC,\n"
            + "    CASE WHEN c5 IS NULL THEN 1 ELSE 0 END, c5 ASC",
            queryBuilder.toSqlAndTypes().getKey(), -1);
    }


    public void testMakeLevelMembersSqlMultipleMeasureGroups() {
        TupleConstraint mockConstraint = mock(TupleConstraint.class);
        RolapCube warehouseSales = getCube("Warehouse and Sales");
        when(mockConstraint.getMeasureGroupList()).thenReturn(
            warehouseSales.getMeasureGroups());
        when(mockConstraint.isJoinRequired()).thenReturn(true);
        SqlTupleReader sqlTupleReader = new SqlTupleReader(mockConstraint);
        Dialect spyDialect = spy(new JdbcDialectImpl());
        // verify requireOrderByAlias in dialect doesn't screw up the
        // ORDER BY when ordinal should be used.
        when(spyDialect.requiresOrderByAlias()).thenReturn(true);
        when(spyDialect.getDatabaseProduct()).thenReturn(
            Dialect.DatabaseProduct.MYSQL);
        RolapCubeLevel level = getLevel(
            warehouseSales, "Time", "Time", "Quarter");
        MemberReader memberReader = RolapSchemaLoader.createMemberReader(
            level.getHierarchy(), getTestContext().getConnection().getRole());
        sqlTupleReader.addLevelMembers(
            level, memberReader.getMemberBuilder(), null);

        getTestContext().assertSqlEquals(
            "select\n"
            + "    time_by_day.the_year as c0,\n"
            + "    time_by_day.quarter as c1\n"
            + "from\n"
            + "    sales_fact_1997 as sales_fact_1997,\n"
            + "    time_by_day as time_by_day\n"
            + "where\n"
            + "    sales_fact_1997.time_id = time_by_day.time_id\n"
            + "group by\n"
            + "    time_by_day.the_year,\n"
            + "    time_by_day.quarter\n"
            + "union\n"
            + "select\n"
            + "    time_by_day.the_year as c0,\n"
            + "    time_by_day.quarter as c1\n"
            + "from\n"
            + "    inventory_fact_1997 as inventory_fact_1997,\n"
            + "    time_by_day as time_by_day\n"
            + "where\n"
            + "    inventory_fact_1997.time_id = time_by_day.time_id\n"
            + "group by\n"
            + "    time_by_day.the_year,\n"
            + "    time_by_day.quarter\n"
            + "order by\n"
            + "    1 ASC,\n"
            + "    2 ASC",
            sqlTupleReader.makeLevelMembersSql(spyDialect).getKey(), -1);
    }

    private RolapCube getCube(String name) {
        for (Cube cube
            : getTestContext().getConnection().getSchema().getCubes())
        {
            if (cube.getName().equals(name)) {
                return (RolapCube)cube;
            }
        }
        return null;
    }

    private RolapCubeLevel getLevel(
        RolapCube cube, String dimName, String hierName, String levelName)
    {
        for (Dimension dim : cube.getDimensionList()) {
            if (dim.getName().equals(dimName)) {
                for (Hierarchy hier : dim.getHierarchyList()) {
                    if (hier.getName().equals(hierName)) {
                        for (Level level : hier.getLevelList()) {
                            if (level.getName().equals(levelName)) {
                                return (RolapCubeLevel)level;
                            }
                        }
                    }
                }
            }
        }
        return null;
    }
}
// End SqlTupleReaderTest.java
