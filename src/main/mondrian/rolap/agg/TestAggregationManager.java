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
import mondrian.test.TestContext;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

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
		ArrayList pinnedSegments = new ArrayList();
		aggMan.loadAggregations(createBatch(new CellRequest[] {request}), pinnedSegments);
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
		final String pattern = "select `customer`.`gender` as `c0`, sum(`sales_fact_1997`.`unit_sales`) as `c1` from `customer` as `customer`, `sales_fact_1997` as `sales_fact_1997` where `sales_fact_1997`.`customer_id` = `customer`.`customer_id` group by `customer`.`gender`";
		assertRequestSql(new CellRequest[] {request}, pattern, null);
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
		final String pattern = "select `customer`.`gender` as `c0`, `store`.`store_state` as `c1`, sum(`sales_fact_1997`.`unit_sales`) as `c2`, sum(`sales_fact_1997`.`store_sales`) as `c3` from `customer` as `customer`, `sales_fact_1997` as `sales_fact_1997`, `store` as `store` where `sales_fact_1997`.`customer_id` = `customer`.`customer_id` and `sales_fact_1997`.`store_id` = `store`.`store_id` and `store`.`store_state` in ('CA', 'OR') group by `customer`.`gender`, `store`.`store_state`";
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

	// todo: test unrestricted column, (Unit Sales, Gender=*)

	// todo: test one unrestricted, one restricted, (UNit Sales, Gender=*,
	//  State={CA, OR})

	// todo: test with 2 dimension columns on the same table, e.g.
	//  (Unit Sales, Gender={F}, MaritalStatus={S}) and make sure that the
	// table only appears once in the from clause.

	private void assertMatches(String s, String pattern) {
		assertTrue(s, s.indexOf(pattern) >= 0);
	}

	private ArrayList createBatch(CellRequest[] requests) {
		ArrayList batches = new ArrayList();
		RolapAggregationManager.Batch batch = new RolapAggregationManager.Batch();
		batches.add(batch);
		for (int i = 0; i < requests.length; i++) {
			CellRequest request = requests[i];
			batch.requests.add(request);
		}
		return batches;
	}

	/**
	 * If a hierarchy lives in the fact table, we should not generate a join.
	 */
	public void testHierarchyInFactTable() {
		CellRequest request = createRequest("Store", "[Measures].[Store Sqft]", "store", "store_type", "Supermarket");
		final String pattern = "select `store`.`store_type` as `c0`, sum(`store`.`store_sqft`) as `c1` from `store` as `store` group by `store`.`store_type`";
		assertRequestSql(new CellRequest[] {request}, pattern, "select `store`.`store_type` as `c0`");
	}

	private void assertRequestSql_old(CellRequest request, final String pattern) {
		final RolapAggregationManager aggMan = AggregationManager.instance();
		ArrayList pinnedSegments = new ArrayList();
		final RolapUtil.TeeWriter tw = RolapUtil.startTracing();
		aggMan.loadAggregations(createBatch(new CellRequest[] {request}), pinnedSegments);
		String s = tw.toString();
		assertMatches(s,pattern);
	}

	class Bomb extends RuntimeException {
		String sql;
		Bomb(String sql) {
			this.sql = sql;
		}
	};

	private void assertRequestSql(CellRequest[] requests, final String pattern, final String trigger) {
		final RolapAggregationManager aggMan = AggregationManager.instance();
		ArrayList pinnedSegments = new ArrayList();
		RolapStar star = requests[0].getMeasure().table.star;
		final java.sql.Connection connection = star.getJdbcConnection();
		String database = null;
		try {
			database = connection.getMetaData().getDatabaseProductName();
		} catch (SQLException e) {
		}
		if (!database.equals("ACCESS")) {
			return;
		}
		star.setJdbcConnection(
				(java.sql.Connection) Proxy.newProxyInstance(
						null,
						new Class[]{java.sql.Connection.class},
						new DelegatingInvocationHandler(connection) {
							public Statement createStatement() throws SQLException {
								final Statement statement = connection.createStatement();
								return (Statement) Proxy.newProxyInstance(
										null,
										new Class[]{java.sql.Statement.class},
										new DelegatingInvocationHandler(statement) {
											public ResultSet executeQuery(String sql) throws SQLException {
												if (trigger == null || sql.startsWith(trigger)) {
													throw new Bomb(sql);
												} else {
													return statement.executeQuery(sql);
												}
											}
										});
							}
						}
				));
		Bomb bomb;
		try {
			aggMan.loadAggregations(createBatch(requests), pinnedSegments);
			bomb = null;
		} catch (Bomb e) {
			bomb = e;
		} finally {
			star.setJdbcConnection(connection);
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

	private CellRequest createRequest(
			final String cube, final String measureName,
			final String[] tables, final String[] columns, final String[] values) {
		final Connection connection = TestContext.instance().getFoodMartConnection();
		final boolean fail = true;
		Cube salesCube = connection.getSchema().lookupCube(cube, fail);
		Member measure = salesCube.lookupMemberByUniqueName(
				measureName, fail);
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
}

/**
 * A class derived from <code>DelegatingInvocationHandler</code> handles a
 * method call by looking for a method in itself with identical parameters. If
 * no such method is found, it forwards the call to a fallback object, which
 * must implement all of the interfaces which this proxy implements.
 *
 * <p> It is useful in creating a wrapper class around an interface which may
 * change over time.
 */
abstract class DelegatingInvocationHandler implements InvocationHandler {
	Object fallback;
	DelegatingInvocationHandler(Object fallback) {
		this.fallback = fallback;
	}
	public Object invoke(Object proxy, Method method, Object[] args)
			throws Throwable {
		Class clazz = getClass();
		Method matchingMethod;
		try {
			matchingMethod = clazz.getMethod(
					method.getName(), method.getParameterTypes());
		} catch (NoSuchMethodException e) {
			matchingMethod = null;
		} catch (SecurityException e) {
			matchingMethod = null;
		}
		try {
			if (matchingMethod != null) {
				// Invoke the method in the derived class.
				return matchingMethod.invoke(this, args);
			} else {
				// Invoke the method on the proxy.
				return method.invoke(fallback, args);
			}
		} catch (InvocationTargetException e) {
			throw e.getTargetException();
		}
	}
}

// End TestAggregationManager.java
