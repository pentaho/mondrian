/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 28 September, 2002
*/
package mondrian.rolap.agg;

import junit.framework.TestCase;
import mondrian.olap.Connection;
import mondrian.olap.Cube;
import mondrian.olap.Member;
import mondrian.rolap.RolapAggregationManager;
import mondrian.rolap.RolapStar;
import mondrian.rolap.RolapUtil;
import mondrian.rolap.sql.DelegatingConnection;
import mondrian.rolap.sql.DelegatingStatement;
import mondrian.test.TestContext;

import java.util.ArrayList;
import java.util.Map;
import java.io.*;
import java.sql.*;

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
		CellRequest request = createFemaleUnitSalesCellRequest();
		final RolapAggregationManager aggMan = AggregationManager.instance();
		Object value = aggMan.getCellFromCache(request);
		assertNull(value); // before load, the cell is not found
		ArrayList pinnedSegments = new ArrayList();
		aggMan.loadAggregations(createSingletonBatch(request), pinnedSegments);
		value = aggMan.getCellFromCache(request); // after load, cell is found
		assertTrue(value instanceof Number);
		assertEquals(131558, ((Number) value).intValue());
	}

	/**
	 * Tests that a request for ([Measures].[Unit Sales], [Gender].[F])
	 * generates the correct SQL.
	 *
	 * <p> Currently only works if the current database is Access. todo: Rather
	 * than intercepting RolapUtil.debugOut, create a fake JDBC connection
	 * which pretends to be Access, and fails if it is not given a particular
	 * SQL statement.
	 */
	public void testFemaleUnitSalesSql() {
		CellRequest request = createFemaleUnitSalesCellRequest();
		final String pattern = "select `customer`.`gender` as `c0`, sum(`sales_fact_1997`.`unit_sales`) as `c1` from `customer` as `customer`, `sales_fact_1997` as `sales_fact_1997` where `sales_fact_1997`.`customer_id` = `customer`.`customer_id` group by `customer`.`gender`";
		assertRequestSql(request, pattern, null);
	}

	// todo: test multiple values, (UNit Sales, State={CA,OR})

	// todo: test multiple measures, ({Unit SAles,Store Sales}, Gender=F)
	//  -- NOT possible yet, since a batch can only contain one measure

	// todo: test unrestricted column, (Unit Sales, Gender=*)

	// todo: test one unrestricted, one restricted, (UNit Sales, Gender=*,
	//  State={CA, OR})

	// todo: test with 2 dimension columns on the same table, e.g.
	//  (Unit Sales, Gender={F}, MaritalStatus={S}) and make sure that the
	// table only appears once in the from clause.

	private void assertMatches(String s, String pattern) {
		assertTrue(s, s.indexOf(pattern) >= 0);
	}

	private ArrayList createSingletonBatch(CellRequest request) {
		ArrayList batches = new ArrayList();
		RolapAggregationManager.Batch batch = new RolapAggregationManager.Batch();
		batches.add(batch);
		batch.requests.add(request);
		return batches;
	}

	private CellRequest createFemaleUnitSalesCellRequest() {
		return createRequest("Sales", "[Measures].[Unit Sales]", "customer", "gender", "F");
	}

	/**
	 * If a hierarchy lives in the fact table, we should not generate a join.
	 */
	public void testHierarchyInFactTable() {
		CellRequest request = createRequest("Store", "[Measures].[Store Sqft]", "store", "store_type", "Supermarket");
		final String pattern = "select `store`.`store_type` as `c0`, sum(`store`.`store_sqft`) as `c1` from `store` as `store` group by `store`.`store_type`";
		assertRequestSql(request, pattern, "select `store`.`store_type` as `c0`");
	}

	private void assertRequestSql_old(CellRequest request, final String pattern) {
		final RolapAggregationManager aggMan = AggregationManager.instance();
		ArrayList pinnedSegments = new ArrayList();
		final RolapUtil.TeeWriter tw = RolapUtil.startTracing();
		aggMan.loadAggregations(createSingletonBatch(request), pinnedSegments);
		String s = tw.toString();
		assertMatches(s,pattern);
	}

	class Bomb extends RuntimeException {
		String sql;
		Bomb(String sql) {
			this.sql = sql;
		}
	};

	private void assertRequestSql(CellRequest request, final String pattern, final String trigger) {
		final RolapAggregationManager aggMan = AggregationManager.instance();
		ArrayList pinnedSegments = new ArrayList();
		RolapStar star = request.getMeasure().table.star;
		final java.sql.Connection originalConnection = star.getJdbcConnection();
		String database = null;
		try {
			database = originalConnection.getMetaData().getDatabaseProductName();
		} catch (SQLException e) {
		}
		if (!database.equals("ACCESS")) {
			return;
		}
		star.setJdbcConnection(
				new DelegatingConnection(originalConnection) {
					public Statement createStatement() throws SQLException {
						Statement statement = connection.createStatement();
						return new DelegatingStatement(statement, this) {
							public ResultSet executeQuery(String sql) throws SQLException {
								if (trigger == null || sql.startsWith(trigger)) {
									throw new Bomb(sql);
								} else {
									return super.executeQuery(sql);
								}
							}
							public boolean execute(String sql) throws SQLException {
								throw new Bomb(sql);
							}
						};
					}
				});
		Bomb bomb;
		try {
			aggMan.loadAggregations(createSingletonBatch(request), pinnedSegments);
			bomb = null;
		} catch (Bomb e) {
			bomb = e;
		} finally {
			star.setJdbcConnection(originalConnection);
		}
		assertTrue(bomb != null);
		assertEquals(pattern, bomb.sql);
	}

	private CellRequest createRequest(final String cube, final String measure, final String table, final String column, final String value) {
		final Connection connection = TestContext.instance().getFoodMartConnection();
		final boolean fail = true;
		Cube salesCube = connection.getSchema().lookupCube(cube, fail);
		Member storeSqftMeasure = salesCube.lookupMemberByUniqueName(
				measure, fail);
		RolapStar.Measure starMeasure = RolapStar.getStarMeasure(storeSqftMeasure);
		CellRequest request = new CellRequest(starMeasure);
		final RolapStar star = starMeasure.table.star;
		final RolapStar.Column storeTypeColumn = star.lookupColumn(
				table, column);
		request.addConstrainedColumn(storeTypeColumn, value);
		return request;
	}

}


class FakeConnection extends DelegatingConnection {
	FakeConnection(java.sql.Connection connection, String expectedSql) {
		super(connection);
	}

	public Statement createStatement() throws SQLException {
		return new DelegatingStatement(super.createStatement(), this);
	}
}



class AccessDatabaseMetaData extends DelegatingDatabaseMetaData {
	public AccessDatabaseMetaData(DatabaseMetaData meta) {
		super(meta);
	}

	public String getIdentifierQuoteString() throws SQLException {
		return "`";
	}
	public String getDatabaseProductName() throws SQLException {
		return "ACCESS";
	}
}

// End TestAggregationManager.java
