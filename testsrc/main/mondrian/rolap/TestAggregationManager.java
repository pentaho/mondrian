/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2002-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 28 September, 2002
*/
package mondrian.rolap;

import java.lang.reflect.Proxy;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

import javax.sql.DataSource;

import junit.framework.TestCase;
import mondrian.olap.Connection;
import mondrian.olap.Cube;
import mondrian.olap.Member;
import mondrian.olap.Util;
import mondrian.rolap.agg.AggregationManager;
import mondrian.rolap.agg.CellRequest;
import mondrian.test.TestContext;
import mondrian.util.DelegatingInvocationHandler;

/**
 * Unit test for {@link AggregationManager}.
 *
 * @author jhyde
 * @since 21 March, 2002
 * @version $Id$
 **/
public class TestAggregationManager extends TestCase {
    public TestAggregationManager(String name) {
        super(name);
    }

    public void testFemaleUnitSales() {
        CellRequest request = createRequest("Sales", "[Measures].[Unit Sales]", "customer", "gender", "F");
        final RolapAggregationManager aggMan = AggregationManager.instance();
        Object value = aggMan.getCellFromCache(request);
        assertNull(value); // before load, the cell is not found
        FastBatchingCellReader fbcr = 
                new FastBatchingCellReader(getCube("Sales"));
        fbcr.recordCellRequest(request);
        fbcr.loadAggregations();
        value = aggMan.getCellFromCache(request); // after load, cell is found
        assertTrue(value instanceof Number);
        assertEquals(131558, ((Number) value).intValue());
    }

    /**
     * Tests that a request for ([Measures].[Unit Sales], [Gender].[F])
     * generates the correct SQL.
     */
    public void testFemaleUnitSalesSql() {
        CellRequest request = createRequest("Sales", "[Measures].[Unit Sales]", "customer", "gender", "F");
        final String pattern = "select `customer`.`gender` as `c0`," +
                " sum(`sales_fact_1997`.`unit_sales`) as `m0` " +
                "from `customer` as `customer`," +
                " `sales_fact_1997` as `sales_fact_1997` " +
                "where `sales_fact_1997`.`customer_id` = `customer`.`customer_id` " +
                "and `customer`.`gender` = 'F' " +
                "group by `customer`.`gender`";
        assertRequestSql(new CellRequest[] {request}, pattern, "select `customer`.`gender`");
    }

    // todo: test multiple values, (UNit Sales, State={CA,OR})

    /**
     * Test a batch containing multiple measures:
     *   (store_state=CA, gender=F, measure=[UNit Sales])
     *   (store_state=CA, gender=M, measure=[Store Sales])
     *   (store_state=OR, gender=M, measure=[Unit Sales])
     */
    public void testMultipleMeasures() {
        CellRequest[] requests = new CellRequest[] {
            createRequest("Sales", "[Measures].[Unit Sales]",
                    new String[] {"customer", "store"},
                    new String[] {"gender", "store_state"},
                    new String[] {"F", "CA"}),
            createRequest("Sales", "[Measures].[Store Sales]",
                    new String[] {"customer", "store"},
                    new String[] {"gender", "store_state"},
                    new String[] {"M", "CA"}),
            createRequest("Sales", "[Measures].[Unit Sales]",
                    new String[] {"customer", "store"},
                    new String[] {"gender", "store_state"},
                    new String[] {"F", "OR"})};
        final String pattern = "select `customer`.`gender` as `c0`," +
                " `store`.`store_state` as `c1`," +
                " sum(`sales_fact_1997`.`unit_sales`) as `m0`," +
                " sum(`sales_fact_1997`.`store_sales`) as `m1` " +
                "from `customer` as `customer`, `sales_fact_1997` as `sales_fact_1997`," +
                " `store` as `store` " +
                "where `sales_fact_1997`.`customer_id` = `customer`.`customer_id`" +
                " and `sales_fact_1997`.`store_id` = `store`.`store_id`" +
                " and `store`.`store_state` in ('CA', 'OR') " +
                "group by `customer`.`gender`, `store`.`store_state`";
        assertRequestSql(requests, pattern, "select `customer`.`gender`");
    }

    /**
     */
    private CellRequest createMultipleMeasureCellRequest() {
        String cube = "Sales";
        String measure = "[Measures].[Unit Sales]";
        String table = "store";
        String column = "store_state";
        String value = "CA";
        final Connection connection = TestContext.instance().getFoodMartConnection(false);
        final boolean fail = true;
        Cube salesCube = connection.getSchema().lookupCube(cube, fail);
        Member storeSqftMeasure = salesCube.getSchemaReader(null).getMemberByUniqueName(
                Util.explode(measure), fail);
        RolapStar.Measure starMeasure = RolapStar.getStarMeasure(storeSqftMeasure);
        CellRequest request = new CellRequest(starMeasure);
        final RolapStar star = starMeasure.table.star;
        final RolapStar.Column storeTypeColumn = star.lookupColumn(
                table, column);
        request.addConstrainedColumn(storeTypeColumn, value);
        return request;
    }

    // todo: test unrestricted column, (Unit Sales, Gender=*)

    // todo: test one unrestricted, one restricted, (UNit Sales, Gender=*,
    //  State={CA, OR})

    // todo: test with 2 dimension columns on the same table, e.g.
    //  (Unit Sales, Gender={F}, MaritalStatus={S}) and make sure that the
    // table only appears once in the from clause.

    private void assertMatches(String s, String pattern) {
        assertTrue(s, s.indexOf(pattern) >= 0);
    }

    /**
     * If a hierarchy lives in the fact table, we should not generate a join.
     */
    public void testHierarchyInFactTable() {
        CellRequest request = createRequest("Store", "[Measures].[Store Sqft]", "store", "store_type", "Supermarket");
        final String pattern = "select `store`.`store_type` as `c0`," +
                " sum(`store`.`store_sqft`) as `m0` " +
                "from `store` as `store` " +
                "where `store`.`store_type` = 'Supermarket' " +
                "group by `store`.`store_type`";
        assertRequestSql(new CellRequest[] {request}, pattern, "select `store`.`store_type` as `c0`");
    }

    static class Bomb extends RuntimeException {
        String sql;
        Bomb(String sql) {
            this.sql = sql;
        }
    }

    private void assertRequestSql(CellRequest[] requests, final String pattern, final String trigger) {
        final RolapAggregationManager aggMan = AggregationManager.instance();
        RolapStar star = requests[0].getMeasure().table.star;
        String database = null;
        try {
            database = star.getJdbcConnection().getMetaData().getDatabaseProductName();
        } catch (SQLException e) {
        }
        if (!database.equals("ACCESS")) {
            return;
        }
        // Create a dummy DataSource which will throw a 'bomb' if it is asked
        // to execute a particular SQL statement, but will otherwise behave
        // exactly the same as the current DataSource.
        DataSource oldDataSource = star.getDataSource();
        final DataSource dataSource = (DataSource) Proxy.newProxyInstance(
                null,
                new Class[] {DataSource.class},
                new DataSourceHandler(oldDataSource, trigger));
        star.setDataSource(dataSource);
        Bomb bomb;
        try {
            FastBatchingCellReader fbcr = 
                new FastBatchingCellReader(getCube("Sales"));
            for (int i = 0; i < requests.length; i++)
                fbcr.recordCellRequest(requests[i]);
            fbcr.loadAggregations();
            bomb = null;
        } catch (Bomb e) {
            bomb = e;
        } finally {
            star.setDataSource(oldDataSource);
        }
        assertTrue(bomb != null);
        assertEquals(pattern, bomb.sql);
    }

    private CellRequest createRequest(final String cube, final String measure, final String table, final String column, final String value) {
        final Connection connection = TestContext.instance().getFoodMartConnection(false);
        final boolean fail = true;
        Cube salesCube = connection.getSchema().lookupCube(cube, fail);
        Member storeSqftMeasure = salesCube.getSchemaReader(null).getMemberByUniqueName(
                Util.explode(measure), fail);
        RolapStar.Measure starMeasure = RolapStar.getStarMeasure(storeSqftMeasure);
        CellRequest request = new CellRequest(starMeasure);
        final RolapStar star = starMeasure.table.star;
        final RolapStar.Column storeTypeColumn = star.lookupColumn(
                table, column);
        request.addConstrainedColumn(storeTypeColumn, value);
        return request;
    }

    private CellRequest createRequest(
            final String cube, final String measureName,
            final String[] tables, final String[] columns, final String[] values) {
        final Connection connection = TestContext.instance().getFoodMartConnection(false);
        final boolean fail = true;
        Cube salesCube = connection.getSchema().lookupCube(cube, fail);
        Member measure = salesCube.getSchemaReader(null).getMemberByUniqueName(
                Util.explode(measureName), fail);
        RolapStar.Measure starMeasure = RolapStar.getStarMeasure(measure);
        CellRequest request = new CellRequest(starMeasure);
        final RolapStar star = starMeasure.table.star;
        for (int i = 0; i < tables.length; i++) {
            String table = tables[i];
            String column = columns[i];
            String value = values[i];
            final RolapStar.Column storeTypeColumn = star.lookupColumn(
                    table, column);
            request.addConstrainedColumn(storeTypeColumn, value);
        }
        return request;
    }

    private RolapCube getCube(final String cube) {
        final Connection connection = TestContext.instance().getFoodMartConnection(false);
        final boolean fail = true;
        return (RolapCube) connection.getSchema().lookupCube(cube, fail);
    }


    public static class DataSourceHandler extends DelegatingInvocationHandler {
        private final DataSource dataSource;
        private final String trigger;

        public DataSourceHandler(DataSource dataSource, String trigger) {
            super(dataSource);
            this.dataSource = dataSource;
            this.trigger = trigger;
        }

        public java.sql.Connection getConnection() throws SQLException {
            final java.sql.Connection connection = dataSource.getConnection();
            return (java.sql.Connection) Proxy.newProxyInstance(
                    null,
                    new Class[]{java.sql.Connection.class},
                    new TriggerHandler(connection));
        }

        public class TriggerHandler extends DelegatingInvocationHandler {
            private final java.sql.Connection connection;

            public TriggerHandler(java.sql.Connection connection) {
                super(connection);
                this.connection = connection;
            }

            public Statement createStatement() throws SQLException {
                final Statement statement = connection.createStatement();
                return (Statement) Proxy.newProxyInstance(
                        null,
                        new Class[]{Statement.class},
                        new StatementHandler(statement));
            }
        }

        public class StatementHandler extends DelegatingInvocationHandler {
            private final Statement statement;

            public StatementHandler(Statement statement) {
                super(statement);
                this.statement = statement;
            }

            public ResultSet executeQuery(String sql) throws SQLException {
                if (trigger == null || sql.startsWith(trigger)) {
                    throw new Bomb(sql);
                } else {
                    return statement.executeQuery(sql);
                }
            }
        }
    }
}


// End TestAggregationManager.java
