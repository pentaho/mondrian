/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2003-2003 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Feb 21, 2003
*/
package mondrian.test;

import mondrian.olap.*;

/**
 * A <code>AccessControlTest</code> contains unit-tests for access-control
 *
 * @see Role
 *
 * @author jhyde
 * @since Feb 21, 2003
 * @version $Id$
 **/
public class AccessControlTest extends FoodMartTestCase {
	public AccessControlTest(String name) {
		super(name);
	}
	public void testGrantDimensionNone() {
		Connection connection = getConnection();
		Role role = connection.getRole().makeMutableClone();
		Schema schema = connection.getSchema();
		Cube salesCube = schema.lookupCube("Sales", true);
		// todo: add Schema.lookupDimension
		Dimension genderDimension = (Dimension) Util.lookupCompound(salesCube,
				new String[] {"Gender"}, salesCube, true);
		role.grant(genderDimension, Access.NONE);
		role.makeImmutable();
		connection.setRole(role);
		assertAxisThrows(connection, "[Gender].children", "MDX object '[Gender]' not found in cube 'Sales'");
	}
	public void testGrantHierarchy() {
		// assert: can access Mexico
		// assert: can access USA (rule 3 - parent of allowed member San Francisco)
		assertAxisReturns(getRestrictedConnection(), "[Store].children",
				"[Store].[All Stores].[Canada]" + nl +
				"[Store].[All Stores].[Mexico]" + nl +
				"[Store].[All Stores].[USA]");
	}
	public void testGrantHierarchy2() {
		// assert: can access California (parent of allowed member)
		final Connection restrictedConnection = getRestrictedConnection();
		assertAxisReturns(restrictedConnection, "[Store].[USA].children", "Ca");
		assertAxisReturns(restrictedConnection, "[Store].[USA].[CA].children", "San Francisco, Los Angeles");
	}
	public void testGrantHierarchy3() {
		// assert: can not access Washington (child of denied member)
		assertAxisThrows(getRestrictedConnection(), "[Store].[USA].[WA]", "not found");
	}
	public void testGrantHierarchy4() {
		// assert: can not access Oregon (rule 1 - order matters)
		assertAxisThrows(getRestrictedConnection(), "[Store].[USA].[OR].children", "not found");
	}
	public void testGrantHierarchy5() {
		// assert: can not access All (above top level)
		assertAxisThrows(getRestrictedConnection(), "[Store].[All Stores]", "not found");
		assertAxisReturns(getRestrictedConnection(), "[Store].members", "all,usa,ca,sf");
	}
	public void testGrantHierarchy6() {
		// assert: parent if at top level is null
		assertAxisReturns(getRestrictedConnection(), "[Customers].[USA].[CA].parent", "");
	}
	public void testGrantHierarchy7() {
		// assert: members above top level do not exist
		assertAxisThrows(getRestrictedConnection(), "[Customers].[Canada].children", "Canada not found");
	}
	public void testGrantHierarchy8() {
		// assert: can not access Catherine Abel in San Francisco (below bottom level)
		final Connection restrictedConnection = getRestrictedConnection();
		assertAxisThrows(restrictedConnection, "[Customers].[USA].[CA].[San Francisco].[Catherine Abel]", "not found");
		assertAxisReturns(restrictedConnection, "[Customers].[USA].[CA].[San Francisco].children", "");
		assertAxisReturns(restrictedConnection, "[Customers].members", "ca,altadena,...,woodland hills");
	}
	public void testGrantHierarchy9() {
		// assert: only aggregate over SF, LA, even when called from functions
		Result result = execute(getRestrictedConnection(),
				"with member [Measures].[California Unit Sales] as " +
				" 'Aggregate([Measures].[Unit Sales], [Store].[USA].[CA].children)'" + nl +
				"select {[Measures].[California Unit Sales]} on columns," + nl +
				" {[Gender].children} on rows" + nl +
				"from Sales" + nl +
				"where ([Marital Status].[S])");
		assertEquals("foo", toString(result));
	}
	public void testGrantHierarchyA() {
		// assert: totals for USA include missing cells
		Result result = execute(getRestrictedConnection(),
				"select {[Unit Sales]} on columns" + nl +
				"{[Customers].members} on rows" + nl +
				"from [Sales]");
		assertEquals("foo", toString(result));
	}

	private Connection getRestrictedConnection() {
		Connection connection = getConnection();
		Role role = new Role();
		Schema schema = connection.getSchema();
		final SchemaReader schemaReader = schema.getSchemaReader();
		final boolean fail = true;
		Cube salesCube = schema.lookupCube("Sales", fail);
		Hierarchy storeHierarchy = salesCube.lookupHierarchy("Store", false);
		Level nationLevel = storeHierarchy.lookupLevel("Nation");
		role.grant(storeHierarchy, Access.CUSTOM, nationLevel, null);
		role.grant(schemaReader.getMemberByUniqueName(storeHierarchy, Util.explode("[All Stores].[USA].[OR]"), fail), Access.ALL);
		role.grant(schemaReader.getMemberByUniqueName(storeHierarchy, Util.explode("[All Stores].[USA]"), fail), Access.NONE);
		role.grant(schemaReader.getMemberByUniqueName(storeHierarchy, Util.explode("[All Stores].[USA].[CA].[San Francisco]"), fail), Access.ALL);
		role.grant(schemaReader.getMemberByUniqueName(storeHierarchy, Util.explode("[All Stores].[USA].[CA].[Los Angeles]"), fail), Access.ALL);
		Hierarchy customersHierarchy = salesCube.lookupHierarchy("Customers", false);
		Level stateProvinceLevel = customersHierarchy.lookupLevel("State Province");
		Level customersCityLevel = customersHierarchy.lookupLevel("City");
		role.grant(customersHierarchy, Access.CUSTOM, stateProvinceLevel, customersCityLevel);
		role.makeImmutable();
		connection.setRole(role);
		return connection;
	}
	/* todo: test that access to restricted measure fails (will not work --
	have not fixed Cube.getMeasures) */
}

// End AccessControlTest.java