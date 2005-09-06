/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2003-2005 Julian Hyde <jhyde@users.sf.net>
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Feb 21, 2003
*/
package mondrian.test;

import junit.framework.Assert;
import mondrian.olap.*;

/**
 * <code>AccessControlTest</code> is a set of unit-tests for access-control.
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
        Connection connection = getConnection(true);
        Role role = connection.getRole().makeMutableClone();
        Schema schema = connection.getSchema();
        Cube salesCube = schema.lookupCube("Sales", true);
        // todo: add Schema.lookupDimension
        final SchemaReader schemaReader = salesCube.getSchemaReader(role);
        Dimension genderDimension = (Dimension) schemaReader.lookupCompound(
                salesCube, new String[] {"Gender"}, true, Category.Dimension);
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
//      final SchemaReader schemaReader = connection.getSchema().getSchemaReader(); // unrestricted
        Schema schema = restrictedConnection.getSchema();
        final boolean fail = true;
        Cube salesCube = schema.lookupCube("Sales", fail);
        final SchemaReader schemaReader = salesCube.getSchemaReader(null); // unrestricted
        final Member member = schemaReader.getMemberByUniqueName(Util.explode(memberName),true);
        final int actualAccess = role.getAccess(member);
        Assert.assertEquals(memberName, Access.instance().getName(expectedAccess), Access.instance().getName(actualAccess));
    }

    public void testGrantHierarchy1a() {
        // assert: can access Mexico (explicitly granted)
        // assert: can not access Canada (explicitly denied)
        // assert: can access USA (rule 3 - parent of allowed member San Francisco)
        assertAxisReturns(getRestrictedConnection(), "[Store].level.members",
                "[Store].[All Stores].[Mexico]" + nl +
                "[Store].[All Stores].[USA]");
    }

    public void testGrantHierarchy1aAllMembers() {
        // assert: can access Mexico (explicitly granted)
        // assert: can not access Canada (explicitly denied)
        // assert: can access USA (rule 3 - parent of allowed member San Francisco)
        assertAxisReturns(getRestrictedConnection(), "[Store].level.allmembers",
                "[Store].[All Stores].[Mexico]" + nl +
                "[Store].[All Stores].[USA]");
    }

    public void testGrantHierarchy1b() {
        // can access Mexico (explicitly granted) which is the first accessible one
        assertAxisReturns(getRestrictedConnection(), "[Store].defaultMember",
                "[Store].[All Stores].[Mexico]");
    }

    public void testGrantHierarchy1c() {
        // can access Mexico (explicitly granted) which is the first accessible one
        assertAxisReturns(getRestrictedConnection(), "[Customers].defaultMember",
                "[Customers].[All Customers].[Canada].[BC]");
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
        Assert.assertEquals(122, axis.positions.length); // 13 states, 109 cities
    }

    public void testGrantHierarchy8AllMembers() {
        // assert: can not access Catherine Abel in San Francisco (below bottom level)
        final Connection restrictedConnection = getRestrictedConnection();
        assertAxisThrows(restrictedConnection, "[Customers].[USA].[CA].[San Francisco].[Catherine Abel]", "not found");
        assertAxisReturns(restrictedConnection, "[Customers].[USA].[CA].[San Francisco].children", "");
        Axis axis = executeAxis2(restrictedConnection, "[Customers].allmembers");
        Assert.assertEquals(122, axis.positions.length); // 13 states, 109 cities
    }

    /** Test that we only aggregate over SF, LA, even when called from functions. */
    public void testGrantHierarchy9() {
        // Analysis services doesn't allow aggregation within calculated
        // measures, so use the following query to generate the results:
        //
        //   with member [Store].[SF LA] as
        //     'Aggregate({[USA].[CA].[San Francisco], [Store].[USA].[CA].[Los Angeles]})'
        //   select {[Measures].[Unit Sales]} on columns,
        //    {[Gender].children} on rows
        //   from Sales
        //   where ([Marital Status].[S], [Store].[SF LA])
        final TestContext tc = new RestrictedTestContext();
        tc.assertQueryReturns(
                "with member [Measures].[California Unit Sales] as " +
                " 'Aggregate({[Store].[USA].[CA].children}, [Measures].[Unit Sales])'" + nl +
                "select {[Measures].[California Unit Sales]} on columns," + nl +
                " {[Gender].children} on rows" + nl +
                "from Sales" + nl +
                "where ([Marital Status].[S])",
                "Axis #0:" + nl +
                "{[Marital Status].[All Marital Status].[S]}" + nl +
                "Axis #1:" + nl +
                "{[Measures].[California Unit Sales]}" + nl +
                "Axis #2:" + nl +
                "{[Gender].[All Gender].[F]}" + nl +
                "{[Gender].[All Gender].[M]}" + nl +
                "Row #0: 6,636" + nl +
                "Row #1: 7,329" + nl);
    }
    public void testGrantHierarchyA() {
        final TestContext tc = new RestrictedTestContext();
        // assert: totals for USA include missing cells
        tc.assertQueryReturns(
                "select {[Unit Sales]} on columns," + nl +
                "{[Store].[USA], [Store].[USA].children} on rows" + nl +
                "from [Sales]",
                "Axis #0:" + nl +
                "{}" + nl +
                "Axis #1:" + nl +
                "{[Measures].[Unit Sales]}" + nl +
                "Axis #2:" + nl +
                "{[Store].[All Stores].[USA]}" + nl +
                "{[Store].[All Stores].[USA].[CA]}" + nl +
                "Row #0: 266,773" + nl +
                "Row #1: 74,748" + nl);
    }

    private Connection getRestrictedConnection() {
        return getRestrictedConnection(true);
    }
    /**
     * @param restrictCustomers true to restrict access to the customers
     * dimension. This will change the defaultMember of the dimension,
     * all cell values will be null because there are no sales data
     * for Canada
     */
    private Connection getRestrictedConnection(boolean restrictCustomers) {
        Connection connection = getConnection(true);
        Role role = new Role();
        Schema schema = connection.getSchema();
        final boolean fail = true;
        Cube salesCube = schema.lookupCube("Sales", fail);
        final SchemaReader schemaReader = salesCube.getSchemaReader(null);
        Hierarchy storeHierarchy = salesCube.lookupHierarchy("Store", false);
        role.grant(schema, Access.ALL_DIMENSIONS);
        role.grant(salesCube, Access.ALL);
        Level nationLevel = Util.lookupHierarchyLevel(storeHierarchy, "Store Country");
        role.grant(storeHierarchy, Access.CUSTOM, nationLevel, null);
        role.grant(schemaReader.getMemberByUniqueName(Util.explode("[Store].[All Stores].[USA].[OR]"), fail), Access.ALL);
        role.grant(schemaReader.getMemberByUniqueName(Util.explode("[Store].[All Stores].[USA]"), fail), Access.NONE);
        role.grant(schemaReader.getMemberByUniqueName(Util.explode("[Store].[All Stores].[USA].[CA].[San Francisco]"), fail), Access.ALL);
        role.grant(schemaReader.getMemberByUniqueName(Util.explode("[Store].[All Stores].[USA].[CA].[Los Angeles]"), fail), Access.ALL);
        role.grant(schemaReader.getMemberByUniqueName(Util.explode("[Store].[All Stores].[Mexico]"), fail), Access.ALL);
        role.grant(schemaReader.getMemberByUniqueName(Util.explode("[Store].[All Stores].[Canada]"), fail), Access.NONE);
        if (restrictCustomers) {
            Hierarchy customersHierarchy = salesCube.lookupHierarchy("Customers", false);
            Level stateProvinceLevel = Util.lookupHierarchyLevel(customersHierarchy, "State Province");
            Level customersCityLevel = Util.lookupHierarchyLevel(customersHierarchy, "City");
            role.grant(customersHierarchy, Access.CUSTOM, stateProvinceLevel, customersCityLevel);
            role.grant(schemaReader.getMemberByUniqueName(Util.explode("[Customers].[All Customers]"), fail), Access.ALL);
        }
        role.makeImmutable();
        connection.setRole(role);
        return connection;
    }

    /* todo: test that access to restricted measure fails (will not work --
    have not fixed Cube.getMeasures) */
    private class RestrictedTestContext extends TestContext {
        public synchronized Connection getFoodMartConnection(boolean fresh) {
            return getRestrictedConnection(false);
        }
    }
}

// End AccessControlTest.java
