/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2003-2007 Julian Hyde
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
 * For these tests, all of the roles are of type RoleImpl.
 *
 * @see Role
 *
 * @author jhyde
 * @since Feb 21, 2003
 * @version $Id$
 */
public class AccessControlTest extends FoodMartTestCase {
    public AccessControlTest(String name) {
        super(name);
    }

    public void testGrantDimensionNone() {
        final Connection connection =
            getTestContext().getFoodMartConnection(false);
        TestContext testContext = getTestContext(connection);
        RoleImpl role = ((RoleImpl) connection.getRole()).makeMutableClone();
        Schema schema = connection.getSchema();
        Cube salesCube = schema.lookupCube("Sales", true);
        // todo: add Schema.lookupDimension
        final SchemaReader schemaReader = salesCube.getSchemaReader(role);
        Dimension genderDimension = (Dimension) schemaReader.lookupCompound(
                salesCube, Id.Segment.toList("Gender"), true, Category.Dimension);
        role.grant(genderDimension, Access.NONE);
        role.makeImmutable();
        connection.setRole(role);
        testContext.assertAxisThrows(
            "[Gender].children", "MDX object '[Gender]' not found in cube 'Sales'");
    }

    public void testRoleMemberAccess() {
        final Connection connection = getRestrictedConnection();
        assertMemberAccess(connection, Access.CUSTOM, "[Store].[USA]"); // because CA has access
        assertMemberAccess(connection, Access.ALL, "[Store].[Mexico]");
        assertMemberAccess(connection, Access.NONE, "[Store].[Mexico].[DF]");
        assertMemberAccess(connection, Access.NONE, "[Store].[Mexico].[DF].[Mexico City]");
        assertMemberAccess(connection, Access.NONE, "[Store].[Canada]");
        assertMemberAccess(connection, Access.NONE, "[Store].[Canada].[BC].[Vancouver]");
        assertMemberAccess(connection, Access.ALL, "[Store].[USA].[CA].[Los Angeles]");
        assertMemberAccess(connection, Access.NONE, "[Store].[USA].[CA].[San Diego]");
        assertMemberAccess(connection, Access.NONE, "[Store].[USA].[OR].[Portland]"); // USA deny supercedes OR grant
        assertMemberAccess(connection, Access.NONE, "[Store].[USA].[WA].[Seattle]");
        assertMemberAccess(connection, Access.NONE, "[Store].[USA].[WA]");
        assertMemberAccess(connection, Access.NONE, "[Store].[All Stores]"); // above top level
    }

    private void assertMemberAccess(
            final Connection connection,
            Access expectedAccess,
            String memberName) {
        final Role role = connection.getRole(); // restricted
        Schema schema = connection.getSchema();
        final boolean fail = true;
        Cube salesCube = schema.lookupCube("Sales", fail);
        final SchemaReader schemaReader = salesCube.getSchemaReader(null); // unrestricted
        final Member member =
            schemaReader.getMemberByUniqueName(
                Util.parseIdentifier(memberName),true);
        final Access actualAccess = role.getAccess(member);
        Assert.assertEquals(memberName, expectedAccess, actualAccess);
    }

    public void testGrantHierarchy1a() {
        // assert: can access Mexico (explicitly granted)
        // assert: can not access Canada (explicitly denied)
        // assert: can access USA (rule 3 - parent of allowed member San Francisco)
        getRestrictedTestContext().assertAxisReturns("[Store].level.members",
                "[Store].[All Stores].[Mexico]" + nl +
                "[Store].[All Stores].[USA]");
    }

    public void testGrantHierarchy1aAllMembers() {
        // assert: can access Mexico (explicitly granted)
        // assert: can not access Canada (explicitly denied)
        // assert: can access USA (rule 3 - parent of allowed member San Francisco)
        getRestrictedTestContext().assertAxisReturns("[Store].level.allmembers",
                "[Store].[All Stores].[Mexico]" + nl +
                "[Store].[All Stores].[USA]");
    }

    public void testGrantHierarchy1b() {
        // can access Mexico (explicitly granted) which is the first accessible one
        getRestrictedTestContext().assertAxisReturns("[Store].defaultMember",
                "[Store].[All Stores].[Mexico]");
    }

    public void testGrantHierarchy1c() {
        // can access Mexico (explicitly granted) which is the first accessible one
        getRestrictedTestContext().assertAxisReturns("[Customers].defaultMember",
                "[Customers].[All Customers].[Canada].[BC]");
    }

    public void testGrantHierarchy2() {
        // assert: can access California (parent of allowed member)
        final TestContext testContext = getRestrictedTestContext();
        testContext.assertAxisReturns("[Store].[All Stores].[USA].children", "[Store].[All Stores].[USA].[CA]");
        testContext.assertAxisReturns("[Store].[USA].children", "[Store].[All Stores].[USA].[CA]");
        testContext.assertAxisReturns("[Store].[USA].[CA].children",
                "[Store].[All Stores].[USA].[CA].[Los Angeles]" + nl +
                "[Store].[All Stores].[USA].[CA].[San Francisco]");
    }

    public void testGrantHierarchy3() {
        // assert: can not access Washington (child of denied member)
        final TestContext testContext = getRestrictedTestContext();
        testContext.assertAxisThrows("[Store].[USA].[WA]", "not found");
    }

    private TestContext getRestrictedTestContext() {
        return new DelegatingTestContext(getTestContext()) {
            public Connection getConnection() {
                return getRestrictedConnection();
            }
        };
    }

    public void testGrantHierarchy4() {
        // assert: can not access Oregon (rule 1 - order matters)
        final TestContext testContext = getRestrictedTestContext();
        testContext.assertAxisThrows("[Store].[USA].[OR].children", "not found");
    }

    public void testGrantHierarchy5() {
        // assert: can not access All (above top level)
        final TestContext testContext = getRestrictedTestContext();
        testContext.assertAxisThrows("[Store].[All Stores]", "not found");
        testContext.assertAxisReturns(
                "[Store].members",
                // note:
                // no: [All Stores] -- above top level
                // no: [Canada] -- not explicitly allowed
                // yes: [Mexico] -- explicitly allowed -- and all its children
                //      except [DF]
                // no: [Mexico].[DF]
                // yes: [USA] -- implicitly allowed
                // yes: [CA] -- implicitly allowed
                // no: [OR], [WA]
                // yes: [San Francisco] -- explicitly allowed
                // no: [San Diego]
                "[Store].[All Stores].[Mexico]" + nl +
                "[Store].[All Stores].[Mexico].[Guerrero]" + nl +
                "[Store].[All Stores].[Mexico].[Guerrero].[Acapulco]" + nl +
                "[Store].[All Stores].[Mexico].[Guerrero].[Acapulco].[Store 1]" + nl +
                "[Store].[All Stores].[Mexico].[Jalisco]" + nl +
                "[Store].[All Stores].[Mexico].[Jalisco].[Guadalajara]" + nl +
                "[Store].[All Stores].[Mexico].[Jalisco].[Guadalajara].[Store 5]" + nl +
                "[Store].[All Stores].[Mexico].[Veracruz]" + nl +
                "[Store].[All Stores].[Mexico].[Veracruz].[Orizaba]" + nl +
                "[Store].[All Stores].[Mexico].[Veracruz].[Orizaba].[Store 10]" + nl +
                "[Store].[All Stores].[Mexico].[Yucatan]" + nl +
                "[Store].[All Stores].[Mexico].[Yucatan].[Merida]" + nl +
                "[Store].[All Stores].[Mexico].[Yucatan].[Merida].[Store 8]" + nl +
                "[Store].[All Stores].[Mexico].[Zacatecas]" + nl +
                "[Store].[All Stores].[Mexico].[Zacatecas].[Camacho]" + nl +
                "[Store].[All Stores].[Mexico].[Zacatecas].[Camacho].[Store 4]" + nl +
                "[Store].[All Stores].[Mexico].[Zacatecas].[Hidalgo]" + nl +
                "[Store].[All Stores].[Mexico].[Zacatecas].[Hidalgo].[Store 12]" + nl +
                "[Store].[All Stores].[Mexico].[Zacatecas].[Hidalgo].[Store 18]" + nl +
                "[Store].[All Stores].[USA]" + nl +
                "[Store].[All Stores].[USA].[CA]" + nl +
                "[Store].[All Stores].[USA].[CA].[Los Angeles]" + nl +
                "[Store].[All Stores].[USA].[CA].[Los Angeles].[Store 7]" + nl +
                "[Store].[All Stores].[USA].[CA].[San Francisco]" + nl +
                "[Store].[All Stores].[USA].[CA].[San Francisco].[Store 14]");
    }

    public void testGrantHierarchy6() {
        // assert: parent if at top level is null
        getRestrictedTestContext().assertAxisReturns("[Customers].[USA].[CA].parent", "");
    }

    public void testGrantHierarchy7() {
        // assert: members above top level do not exist
        final TestContext testContext = getRestrictedTestContext();
        testContext.assertAxisThrows(
                "[Customers].[Canada].children",
                "MDX object '[Customers].[Canada]' not found in cube 'Sales'");
    }

    public void testGrantHierarchy8() {
        // assert: can not access Catherine Abel in San Francisco (below bottom level)
        final TestContext testContext = getRestrictedTestContext();
        testContext.assertAxisThrows("[Customers].[USA].[CA].[San Francisco].[Catherine Abel]", "not found");
        testContext.assertAxisReturns("[Customers].[USA].[CA].[San Francisco].children", "");
        Axis axis = testContext.executeAxis("[Customers].members");
        Assert.assertEquals(122, axis.getPositions().size()); // 13 states, 109 cities
    }

    public void testGrantHierarchy8AllMembers() {
        // assert: can not access Catherine Abel in San Francisco (below bottom level)
        final TestContext testContext = getRestrictedTestContext();
        testContext.assertAxisThrows("[Customers].[USA].[CA].[San Francisco].[Catherine Abel]", "not found");
        testContext.assertAxisReturns("[Customers].[USA].[CA].[San Francisco].children", "");
        Axis axis = testContext.executeAxis("[Customers].allmembers");
        Assert.assertEquals(122, axis.getPositions().size()); // 13 states, 109 cities
    }

    /**
     * Tests that we only aggregate over SF, LA, even when called from
     * functions.
     */
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

    public void _testSharedObjectsInGrantMappingsBug() {
        new TestContext() {
            public Connection getConnection() {
                boolean mustGet = true;
                Connection connection = super.getConnection();
                Schema schema = connection.getSchema();
                Cube salesCube = schema.lookupCube("Sales", mustGet);
                Cube warehouseCube = schema.lookupCube("Warehouse", mustGet);
                Hierarchy measuresInSales = salesCube.lookupHierarchy(
                        new Id.Segment("Measures", Id.Quoting.UNQUOTED), false);
                Hierarchy storeInWarehouse = warehouseCube.lookupHierarchy(
                        new Id.Segment("Store", Id.Quoting.UNQUOTED), false);

                RoleImpl role = new RoleImpl();
                role.grant(schema, Access.NONE);
                role.grant(salesCube, Access.NONE);
                // For using hierarchy Measures in #assertExprThrows
                Role.RollupPolicy rollupPolicy = Role.RollupPolicy.FULL;
                role.grant(measuresInSales, Access.ALL, null, null, rollupPolicy);
                role.grant(warehouseCube, Access.NONE);
                role.grant(storeInWarehouse.getDimension(), Access.ALL);

                role.makeImmutable();
                connection.setRole(role);
                return connection;
            }
        // Looking up default member on dimension Store in cube Sales should fail.
        }.assertExprThrows("[Store].DefaultMember", "'[Store]' not found in cube 'Sales'");
    }

    public void testNoAccessToCube() {
        final TestContext tc = new RestrictedTestContext();
        tc.assertThrows("select from [HR]", "MDX cube 'HR' not found");
    }

    private Connection getRestrictedConnection() {
        return getRestrictedConnection(true);
    }

    /**
     * Returns a connection with limited access to the schema.
     *
     * @param restrictCustomers true to restrict access to the customers
     * dimension. This will change the defaultMember of the dimension,
     * all cell values will be null because there are no sales data
     * for Canada
     *
     * @return restricted connection
     */
    private Connection getRestrictedConnection(boolean restrictCustomers) {
        Connection connection = getTestContext().getFoodMartConnection(false);
        RoleImpl role = new RoleImpl();
        Schema schema = connection.getSchema();
        final boolean fail = true;
        Cube salesCube = schema.lookupCube("Sales", fail);
        final SchemaReader schemaReader = salesCube.getSchemaReader(null);
        Hierarchy storeHierarchy = salesCube.lookupHierarchy(
                new Id.Segment("Store", Id.Quoting.UNQUOTED), false);
        role.grant(schema, Access.ALL_DIMENSIONS);
        role.grant(salesCube, Access.ALL);
        Level nationLevel = Util.lookupHierarchyLevel(storeHierarchy, "Store Country");
        Role.RollupPolicy rollupPolicy = Role.RollupPolicy.FULL;
        role.grant(storeHierarchy, Access.CUSTOM, nationLevel, null, rollupPolicy);
        role.grant(schemaReader.getMemberByUniqueName(Util.parseIdentifier("[Store].[All Stores].[USA].[OR]"), fail), Access.ALL);
        role.grant(schemaReader.getMemberByUniqueName(Util.parseIdentifier("[Store].[All Stores].[USA]"), fail), Access.NONE);
        role.grant(schemaReader.getMemberByUniqueName(Util.parseIdentifier("[Store].[All Stores].[USA].[CA].[San Francisco]"), fail), Access.ALL);
        role.grant(schemaReader.getMemberByUniqueName(Util.parseIdentifier("[Store].[All Stores].[USA].[CA].[Los Angeles]"), fail), Access.ALL);
        role.grant(schemaReader.getMemberByUniqueName(Util.parseIdentifier("[Store].[All Stores].[Mexico]"), fail), Access.ALL);
        role.grant(schemaReader.getMemberByUniqueName(Util.parseIdentifier("[Store].[All Stores].[Mexico].[DF]"), fail), Access.NONE);
        role.grant(schemaReader.getMemberByUniqueName(Util.parseIdentifier("[Store].[All Stores].[Canada]"), fail), Access.NONE);
        if (restrictCustomers) {
            Hierarchy customersHierarchy = salesCube.lookupHierarchy(
                    new Id.Segment("Customers", Id.Quoting.UNQUOTED), false);
            Level stateProvinceLevel = Util.lookupHierarchyLevel(customersHierarchy, "State Province");
            Level customersCityLevel = Util.lookupHierarchyLevel(customersHierarchy, "City");
            role.grant(customersHierarchy, Access.CUSTOM, stateProvinceLevel, customersCityLevel, rollupPolicy);
            role.grant(schemaReader.getMemberByUniqueName(
                    Util.parseIdentifier("[Customers].[All Customers]"), fail),
                    Access.ALL);
        }

        // No access to HR cube.
        Cube hrCube = schema.lookupCube("HR", fail);
        role.grant(hrCube, Access.NONE);

        role.makeImmutable();
        connection.setRole(role);
        return connection;
    }

    /* todo: test that access to restricted measure fails (will not work --
    have not fixed Cube.getMeasures) */
    private class RestrictedTestContext extends TestContext {
        public synchronized Connection getFoodMartConnection() {
            return getRestrictedConnection(false);
        }
    }

    /**
     * Test context where the [Store] hierarchy has restricted access
     * and cell values are rolled up with 'partial' policy.
     */
    private final TestContext rollupTestContext =
        TestContext.create(
            null, null, null, null, null,
            "<Role name=\"Role1\">\n"
                + "  <SchemaGrant access=\"none\">\n"
                + "    <CubeGrant cube=\"Sales\" access=\"all\">\n"
                + "      <HierarchyGrant hierarchy=\"[Store]\" access=\"custom\" rollupPolicy=\"partial\">\n"
                + "        <MemberGrant member=\"[Store].[USA]\" access=\"all\"/>\n"
                + "        <MemberGrant member=\"[Store].[USA].[CA]\" access=\"none\"/>\n"
                + "      </HierarchyGrant>\n"
                + "    </CubeGrant>\n"
                + "  </SchemaGrant>\n"
                + "</Role>")
            .withRole("Role1");

    /**
     * Basic test of partial rollup policy. [USA] = [OR] + [WA], not
     * the usual [CA] + [OR] + [WA].
     */
    public void testRollupPolicyBasic() {
        rollupTestContext.assertQueryReturns(
            "select {[Store].[USA], [Store].[USA].Children} on 0\n"
            + "from [Sales]",
            fold(
            "Axis #0:\n" +
            "{}\n" +
            "Axis #1:\n" +
            "{[Store].[All Stores].[USA]}\n" +
            "{[Store].[All Stores].[USA].[OR]}\n" +
            "{[Store].[All Stores].[USA].[WA]}\n" +
            "Row #0: 192,025\n" +
            "Row #0: 67,659\n" +
            "Row #0: 124,366\n"));
    }

    /**
     * The total for [Store].[All Stores] is similarly reduced. All
     * children of [All Stores] are visible, but one grandchild is not.
     * Normally the total is 266,773.
     */
    public void testRollupPolicyAll() {
        rollupTestContext.assertExprReturns(
            "([Store].[All Stores])", "192,025");
    }

    /**
     * Access [Store].[All Stores] implicitly as it is the default member
     * of the [Stores] hierarchy.
     */
    public void testRollupPolicyAllAsDefault() { 
        rollupTestContext.assertExprReturns("([Store])", "192,025");
    }

    /**
     * Access [Store].[All Stores] via the Parent relationship (to check
     * that this doesn't circumvent access control).
     */
    public void testRollupPolicyAllAsParent() {
        rollupTestContext.assertExprReturns(
            "([Store].[USA].Parent)", "192,025");
    }

    /**
     * Tests that members below bottom level are regarded as visible.
     */
    public void testRollupBottomLevel() {
        rollupPolicyBottom(Role.RollupPolicy.FULL, "74,748", "36,759", "266,773");
        rollupPolicyBottom(Role.RollupPolicy.PARTIAL, "72,739", "35,775", "264,764");
        rollupPolicyBottom(Role.RollupPolicy.HIDDEN, "", "", "");
    }

    private void rollupPolicyBottom(
        Role.RollupPolicy rollupPolicy,
        String v1,
        String v2,
        String v3)
    {
        TestContext testContext  =
            TestContext.create(
                null, null, null, null, null,
                "<Role name=\"Role1\">\n"
                    + "  <SchemaGrant access=\"none\">\n"
                    + "    <CubeGrant cube=\"Sales\" access=\"all\">\n"
                    + "      <HierarchyGrant hierarchy=\"[Customers]\" access=\"custom\" rollupPolicy=\""
                    + rollupPolicy
                    + "\" bottomLevel=\"[Customers].[City]\">\n"
                    + "        <MemberGrant member=\"[Customers].[USA]\" access=\"all\"/>\n"
                    + "        <MemberGrant member=\"[Customers].[USA].[CA].[Los Angeles]\" access=\"none\"/>\n"
                    + "      </HierarchyGrant>\n"
                    + "    </CubeGrant>\n"
                    + "  </SchemaGrant>\n"
                    + "</Role>")
                .withRole("Role1");
        // All of the children of [San Francisco] are invisible, because [City]
        // is the bottom level, but that shouldn't affect the total.
        testContext.assertExprReturns("([Customers].[USA].[CA].[San Francisco])", "88");
        testContext.assertExprThrows(
            "([Customers].[USA].[CA].[Los Angeles])",
            "MDX object '[Customers].[USA].[CA].[Los Angeles]' not found in cube 'Sales'");

        testContext.assertExprReturns("([Customers].[USA].[CA])", v1);
        testContext.assertExprReturns("([Customers].[USA].[CA], [Gender].[F])", v2);
        testContext.assertExprReturns("([Customers].[USA])", v3);
    }

    /**
     * Tests that a bad value for the rollupPolicy attribute gives the
     * appropriate error.
     */
    public void testRollupPolicyNegative() {
        TestContext testContext  =
            TestContext.create(
                null, null, null, null, null,
                "<Role name=\"Role1\">\n"
                    + "  <SchemaGrant access=\"none\">\n"
                    + "    <CubeGrant cube=\"Sales\" access=\"all\">\n"
                    + "      <HierarchyGrant hierarchy=\"[Customers]\" access=\"custom\" rollupPolicy=\"bad\" bottomLevel=\"[Customers].[City]\">\n"
                    + "        <MemberGrant member=\"[Customers].[USA]\" access=\"all\"/>\n"
                    + "        <MemberGrant member=\"[Customers].[USA].[CA].[Los Angeles]\" access=\"none\"/>\n"
                    + "      </HierarchyGrant>\n"
                    + "    </CubeGrant>\n"
                    + "  </SchemaGrant>\n"
                    + "</Role>")
                .withRole("Role1");
        testContext.assertThrows(
            "select from [Sales]",
            "Illegal rollupPolicy value 'bad'");
    }

    /**
     * Tests where all children are visible but a grandchild is not.
     */
    public void testRollupPolicyGreatGrandchildInvisible() {
        rollupPolicyGreatGrandchildInvisible(
            Role.RollupPolicy.FULL, "266,773", "74,748");
        rollupPolicyGreatGrandchildInvisible(
            Role.RollupPolicy.PARTIAL, "266,767", "74,742");
        rollupPolicyGreatGrandchildInvisible(
            Role.RollupPolicy.HIDDEN, "", "");
    }

    private void rollupPolicyGreatGrandchildInvisible(
        Role.RollupPolicy policy,
        String v1,
        String v2)
    {
        TestContext testContext  =
            TestContext.create(
                null, null, null, null, null,
                "<Role name=\"Role1\">\n"
                    + "  <SchemaGrant access=\"none\">\n"
                    + "    <CubeGrant cube=\"Sales\" access=\"all\">\n"
                    + "      <HierarchyGrant hierarchy=\"[Customers]\" access=\"custom\" rollupPolicy=\""
                    + policy
                    + "\">\n"
                    + "        <MemberGrant member=\"[Customers].[USA]\" access=\"all\"/>\n"
                    + "        <MemberGrant member=\"[Customers].[USA].[CA].[San Francisco].[Gladys Evans]\" access=\"none\"/>\n"
                    + "      </HierarchyGrant>\n"
                    + "    </CubeGrant>\n"
                    + "  </SchemaGrant>\n"
                    + "</Role>")
                .withRole("Role1");
        testContext.assertExprReturns("[Measures].[Unit Sales]", v1);
        testContext.assertExprReturns("([Measures].[Unit Sales], [Customers].[USA])", v1);
        testContext.assertExprReturns("([Measures].[Unit Sales], [Customers].[USA].[CA])", v2);
    }

    /**
     * Tests where two hierarchies are simultaneously access-controlled.
     */
    public void testRollupPolicySimultaneous() {
        // note that v2 is different for full vs partial, v3 is the same
        rollupPolicySimultaneous(
            Role.RollupPolicy.FULL, "266,773", "74,748", "25,635");
        rollupPolicySimultaneous(
            Role.RollupPolicy.PARTIAL, "72,631", "72,631", "25,635");
        rollupPolicySimultaneous(
            Role.RollupPolicy.HIDDEN, "", "", "");
    }

    private void rollupPolicySimultaneous(
        Role.RollupPolicy policy,
        String v1,
        String v2, String v3)
    {
        TestContext testContext  =
            TestContext.create(
                null, null, null, null, null,
                "<Role name=\"Role1\">\n"
                    + "  <SchemaGrant access=\"none\">\n"
                    + "    <CubeGrant cube=\"Sales\" access=\"all\">\n"
                    + "      <HierarchyGrant hierarchy=\"[Customers]\" access=\"custom\" rollupPolicy=\""
                    + policy
                    + "\">\n"
                    + "        <MemberGrant member=\"[Customers].[USA]\" access=\"all\"/>\n"
                    + "        <MemberGrant member=\"[Customers].[USA].[CA].[San Francisco].[Gladys Evans]\" access=\"none\"/>\n"
                    + "      </HierarchyGrant>\n"
                    + "      <HierarchyGrant hierarchy=\"[Store]\" access=\"custom\" rollupPolicy=\""
                    + policy
                    + "\">\n"
                    + "        <MemberGrant member=\"[Store].[USA].[CA]\" access=\"all\"/>\n"
                    + "        <MemberGrant member=\"[Store].[USA].[CA].[San Francisco].[Store 14]\" access=\"none\"/>\n"
                    + "      </HierarchyGrant>\n"
                    + "    </CubeGrant>\n"
                    + "  </SchemaGrant>\n"
                    + "</Role>")
                .withRole("Role1");
        testContext.assertExprReturns("[Measures].[Unit Sales]", v1);
        testContext.assertExprReturns("([Measures].[Unit Sales], [Customers].[USA])", v1);
        testContext.assertExprReturns("([Measures].[Unit Sales], [Customers].[USA].[CA])", v2);
        testContext.assertExprReturns("([Measures].[Unit Sales], [Customers].[USA].[CA], [Store].[USA].[CA])", v2);
        testContext.assertExprReturns("([Measures].[Unit Sales], [Customers].[USA].[CA], [Store].[USA].[CA].[San Diego])", v3);
    }

    // todo: performance test where 1 of 1000 children is not visible
}

// End AccessControlTest.java
