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

import java.util.ArrayList;
import java.io.*;

/**
 * Unit test for {@link AggregationManager}
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
		final RolapAggregationManager aggMan = AggregationManager.instance();
		ArrayList pinnedSegments = new ArrayList();
		final RolapUtil.TeeWriter tw = RolapUtil.startTracing();
		aggMan.loadAggregations(createSingletonBatch(request), pinnedSegments);
		String s = tw.toString();
		final String pattern = "executing sql [select `customer`.`gender` as `c0`, sum(`fact`.`unit_sales`) as `c1` from `customer` as `customer`, `sales_fact_1997` as `fact` where `fact`.`customer_id` = `customer`.`customer_id` group by `customer`.`gender`]";
		assertMatches(s,pattern);
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
		final Connection connection = TestContext.instance().getFoodMartConnection();
		final boolean fail = true;
		Cube salesCube = connection.lookupCube("Sales", fail);
		Member unitSalesMeasure = salesCube.lookupMemberByUniqueName(
				"[Measures].[Unit Sales]", fail);
		RolapStar.Measure starMeasure = RolapStar.getStarMeasure(unitSalesMeasure);
		CellRequest request = new CellRequest(starMeasure);
		final RolapStar star = starMeasure.table.star;
		final RolapStar.Column genderColumn = star.lookupColumn(
				"customer", "gender");
		request.addConstrainedColumn(genderColumn, "F");
		return request;
	}
}

// End TestAggregationManager.java
