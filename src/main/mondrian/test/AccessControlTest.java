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
		Dimension genderDimension = (Dimension) Util.lookupCompound(
				salesCube.getSchemaReader(role), salesCube,
				new String[] {"Gender"}, true);
		role.grant(genderDimension, Access.NONE);
		role.makeImmutable();
		connection.setRole(role);
		assertAxisThrows(connection, "[Gender].children", "MDX object '[Gender]' not found in cube 'Sales'");
	}
	public void testRoleMemberAccess() {
		final Connection restrictedConnection = getRestrictedConnection();
		bar(restrictedConnection, Access.CUSTOM, "[Store].[USA]"); // because CA has access
		bar(restrictedConnection, Access.ALL, "[Store].[Mexico]");
		bar(restrictedConnection, Access.NONE, "[Store].[Canada]");
		bar(restrictedConnection, Access.NONE, "[Store].[Canada].[BC].[Vancouver]");
		bar(restrictedConnection, Access.ALL, "[Store].[USA].[CA].[Los Angeles]");
		bar(restrictedConnection, Access.NONE, "[Store].[USA].[CA].[San Diego]");
		bar(restrictedConnection, Access.NONE, "[Store].[USA].[OR].[Portland]"); // USA deny supercedes OR grant
		bar(restrictedConnection, Access.NONE, "[Store].[USA].[WA].[Seattle]");
		bar(restrictedConnection, Access.NONE, "[Store].[USA].[WA]");
		bar(restrictedConnection, Access.NONE, "[Store].[All Stores]"); // above top level
	}

	private void bar(final Connection restrictedConnection, int expectedAccess, String memberName) {
		final Role role = restrictedConnection.getRole(); // restricted
//		final SchemaReader schemaReader = connection.getSchema().getSchemaReader(); // unrestricted
		Schema schema = restrictedConnection.getSchema();
		final boolean fail = true;
		Cube salesCube = schema.lookupCube("Sales", fail);
		final SchemaReader schemaReader = salesCube.getSchemaReader(null); // unrestricted
		final Member member = schemaReader.getMemberByUniqueName(Util.explode(memberName),true);
		final int actualAccess = role.getAccess(member);
		assertEquals(memberName, Access.instance().getName(expectedAccess), Access.instance().getName(actualAccess));
	}

	public void testGrantHierarchy() {
		// assert: can access Mexico (explicitly granted)
		// assert: can not access Canada (explicitly denied)
		// assert: can access USA (rule 3 - parent of allowed member San Francisco)
		assertAxisReturns(getRestrictedConnection(), "[Store].children",
				"[Store].[All Stores].[Mexico]" + nl +
				"[Store].[All Stores].[USA]");
	}
	public void testGrantHierarchy2() {
		// assert: can access California (parent of allowed member)
		final Connection restrictedConnection = getRestrictedConnection();
		assertAxisReturns(restrictedConnection, "[Store].[USA].children", "[Store].[All Stores].[USA].[CA]");
		assertAxisReturns(restrictedConnection, "[Store].[USA].[CA].children",
				"[Store].[All Stores].[USA].[CA].[Los Angeles]" + nl +
				"[Store].[All Stores].[USA].[CA].[San Francisco]");
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
		assertAxisReturns(getRestrictedConnection(), "[Store].members",
				// note:
				// no: [All Stores] -- above top level
				// no: [Canada] -- not explicitly allowed
				// yes: [Mexico] -- explicitly allowed -- and all its children
				// yes: [USA] -- implicitly allowed
				// yes: [CA] -- implicitly allowed
				// no: [OR], [WA]
				// yes: [San Francisco] -- explicitly allowed
				// no: [San Diego]
				"[Store].[All Stores].[Mexico]" + nl +
				"[Store].[All Stores].[USA]" + nl +
				"[Store].[All Stores].[Mexico].[DF]" + nl +
				"[Store].[All Stores].[Mexico].[Guerrero]" + nl +
				"[Store].[All Stores].[Mexico].[Jalisco]" + nl +
				"[Store].[All Stores].[Mexico].[Veracruz]" + nl +
				"[Store].[All Stores].[Mexico].[Yucatan]" + nl +
				"[Store].[All Stores].[Mexico].[Zacatecas]" + nl +
				"[Store].[All Stores].[USA].[CA]" + nl +
				"[Store].[All Stores].[Mexico].[DF].[Mexico City]" + nl +
				"[Store].[All Stores].[Mexico].[DF].[San Andres]" + nl +
				"[Store].[All Stores].[Mexico].[Guerrero].[Acapulco]" + nl +
				"[Store].[All Stores].[Mexico].[Jalisco].[Guadalajara]" + nl +
				"[Store].[All Stores].[Mexico].[Veracruz].[Orizaba]" + nl +
				"[Store].[All Stores].[Mexico].[Yucatan].[Merida]" + nl +
				"[Store].[All Stores].[Mexico].[Zacatecas].[Camacho]" + nl +
				"[Store].[All Stores].[Mexico].[Zacatecas].[Hidalgo]" + nl +
				"[Store].[All Stores].[USA].[CA].[Los Angeles]" + nl +
				"[Store].[All Stores].[USA].[CA].[San Francisco]" + nl +
				"[Store].[All Stores].[Mexico].[DF].[Mexico City].[Store 9]" + nl +
				"[Store].[All Stores].[Mexico].[DF].[San Andres].[Store 21]" + nl +
				"[Store].[All Stores].[Mexico].[Guerrero].[Acapulco].[Store 1]" + nl +
				"[Store].[All Stores].[Mexico].[Jalisco].[Guadalajara].[Store 5]" + nl +
				"[Store].[All Stores].[Mexico].[Veracruz].[Orizaba].[Store 10]" + nl +
				"[Store].[All Stores].[Mexico].[Yucatan].[Merida].[Store 8]" + nl +
				"[Store].[All Stores].[Mexico].[Zacatecas].[Camacho].[Store 4]" + nl +
				"[Store].[All Stores].[Mexico].[Zacatecas].[Hidalgo].[Store 12]" + nl +
				"[Store].[All Stores].[Mexico].[Zacatecas].[Hidalgo].[Store 18]" + nl +
				"[Store].[All Stores].[USA].[CA].[Los Angeles].[Store 7]" + nl +
				"[Store].[All Stores].[USA].[CA].[San Francisco].[Store 14]");
	}
	public void testGrantHierarchy6() {
		// assert: parent if at top level is null
		assertAxisReturns(getRestrictedConnection(), "[Customers].[USA].[CA].parent", "");
	}
	public void testGrantHierarchy7() {
		// assert: members above top level do not exist
		assertAxisThrows(getRestrictedConnection(), "[Customers].[Canada].children",
				"MDX object '[Customers].[Canada]' not found in cube 'Sales'");
	}
	public void testGrantHierarchy8() {
		// assert: can not access Catherine Abel in San Francisco (below bottom level)
		final Connection restrictedConnection = getRestrictedConnection();
		assertAxisThrows(restrictedConnection, "[Customers].[USA].[CA].[San Francisco].[Catherine Abel]", "not found");
		assertAxisReturns(restrictedConnection, "[Customers].[USA].[CA].[San Francisco].children", "");
		Axis axis = executeAxis2(restrictedConnection, "[Customers].members");
		assertEquals(122, axis.positions.length); // 13 states, 109 cities
	}
	public void testGrantHierarchy9() {
		// assert: only aggregate over SF, LA, even when called from functions
		Result result = execute(getRestrictedConnection(),
				"with member [Measures].[California Unit Sales] as " +
				" 'Aggregate({[Store].[USA].[CA].children}, [Measures].[Unit Sales])'" + nl +
				"select {[Measures].[California Unit Sales]} on columns," + nl +
				" {[Gender].children} on rows" + nl +
				"from Sales" + nl +
				"where ([Marital Status].[S])");
		assertEquals("Axis #0:" + nl +
				"{[Marital Status].[All Marital Status].[S]}" + nl +
				"Axis #1:" + nl +
				"{[Measures].[California Unit Sales]}" + nl +
				"Axis #2:" + nl +
				"{[Gender].[All Gender].[F]}" + nl +
				"{[Gender].[All Gender].[M]}" + nl +
				"Row #0: 7104.0" + nl +
				"Row #1: 7055.0" + nl,
				toString(result));
	}
	public void testGrantHierarchyA() {
		// assert: totals for USA include missing cells
		Result result = execute(getRestrictedConnection(),
				"select {[Unit Sales]} on columns," + nl +
				"{[Store].[USA], [Store].[USA].children} on rows" + nl +
				"from [Sales]");
		assertEquals("Axis #0:" + nl +
				"{}" + nl +
				"Axis #1:" + nl +
				"{[Measures].[Unit Sales]}" + nl +
				"Axis #2:" + nl +
				"{[Store].[All Stores].[USA]}" + nl +
				"{[Store].[All Stores].[USA].[CA]}" + nl +
				"Row #0: 266,773" + nl +
				"Row #1: 74,748" + nl,
				toString(result));
	}

	private Connection getRestrictedConnection() {
		Connection connection = getConnection();
		Role role = new Role();
		Schema schema = connection.getSchema();
		final boolean fail = true;
		Cube salesCube = schema.lookupCube("Sales", fail);
		final SchemaReader schemaReader = salesCube.getSchemaReader(null);
		Hierarchy storeHierarchy = salesCube.lookupHierarchy("Store", false);
		role.grant(schema, Access.ALL_DIMENSIONS);
		role.grant(salesCube, Access.ALL);
		Level nationLevel = storeHierarchy.lookupLevel("Store Country");
		role.grant(storeHierarchy, Access.CUSTOM, nationLevel, null);
		role.grant(schemaReader.getMemberByUniqueName(Util.explode("[Store].[All Stores].[USA].[OR]"), fail), Access.ALL);
		role.grant(schemaReader.getMemberByUniqueName(Util.explode("[Store].[All Stores].[USA]"), fail), Access.NONE);
		role.grant(schemaReader.getMemberByUniqueName(Util.explode("[Store].[All Stores].[USA].[CA].[San Francisco]"), fail), Access.ALL);
		role.grant(schemaReader.getMemberByUniqueName(Util.explode("[Store].[All Stores].[USA].[CA].[Los Angeles]"), fail), Access.ALL);
		role.grant(schemaReader.getMemberByUniqueName(Util.explode("[Store].[All Stores].[Mexico]"), fail), Access.ALL);
		role.grant(schemaReader.getMemberByUniqueName(Util.explode("[Store].[All Stores].[Canada]"), fail), Access.NONE);
		Hierarchy customersHierarchy = salesCube.lookupHierarchy("Customers", false);
		Level stateProvinceLevel = customersHierarchy.lookupLevel("State Province");
		Level customersCityLevel = customersHierarchy.lookupLevel("City");
		role.grant(customersHierarchy, Access.CUSTOM, stateProvinceLevel, customersCityLevel);
		role.grant(schemaReader.getMemberByUniqueName(Util.explode("[Customers].[All Customers]"), fail), Access.ALL);
		role.makeImmutable();
		connection.setRole(role);
		return connection;
	}

	/* todo: test that access to restricted measure fails (will not work --
	have not fixed Cube.getMeasures) */
}

// End AccessControlTest.java