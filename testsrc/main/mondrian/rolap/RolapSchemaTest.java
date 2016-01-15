/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (c) 2015-2015 Pentaho Corporation..  All rights reserved.
*/
package mondrian.rolap;

import mondrian.olap.*;
import mondrian.resource.MondrianResource;
import mondrian.test.PropertyRestoringTestCase;
import mondrian.util.ByteString;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * @author Andrey Khayrutdinov
 */
public class RolapSchemaTest extends PropertyRestoringTestCase {

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }


    private RolapSchema createSchema() {
        SchemaKey key = new SchemaKey(
            mock(SchemaContentKey.class), mock(ConnectionKey.class));

        ByteString md5 = new ByteString("test schema".getBytes());
        //noinspection deprecation
        return new RolapSchema(key, md5, mock(RolapConnection.class));
    }

    private SchemaReader mockSchemaReader(int category, OlapElement element) {
        SchemaReader reader = mock(SchemaReader.class);
        when(reader.withLocus()).thenReturn(reader);
        when(reader.lookupCompound(
            any(OlapElement.class), anyListOf(Id.Segment.class),
            anyBoolean(), eq(category)))
            .thenReturn(element);
        return reader;
    }

    private RolapCube mockCube(RolapSchema schema) {
        RolapCube cube = mock(RolapCube.class);
        when(cube.getSchema()).thenReturn(schema);
        return cube;
    }


    public void testCreateUnionRole_ThrowsException_WhenSchemaGrantsExist() {
        MondrianDef.Role role = new MondrianDef.Role();
        role.schemaGrants =
            new MondrianDef.SchemaGrant[] {new MondrianDef.SchemaGrant()};
        role.union = new MondrianDef.Union();

        try {
            createSchema().createUnionRole(role);
        } catch (MondrianException ex) {
            assertMondrianException(
                MondrianResource.instance().RoleUnionGrants.ex(), ex);
            return;
        }
        fail("Should fail if union and schema grants exist simultaneously");
    }

    public void testCreateUnionRole_ThrowsException_WhenRoleNameIsUnknown() {
        final String roleName = "non-existing role name";
        MondrianDef.RoleUsage usage = new MondrianDef.RoleUsage();
        usage.roleName = roleName;

        MondrianDef.Role role = new MondrianDef.Role();
        role.union = new MondrianDef.Union();
        role.union.roleUsages = new MondrianDef.RoleUsage[] {usage};

        try {
            createSchema().createUnionRole(role);
        } catch (MondrianException ex) {
            assertMondrianException(
                MondrianResource.instance().UnknownRole.ex(roleName), ex);
            return;
        }
        fail("Should fail if union and schema grants exist simultaneously");
    }


    public void testHandleSchemaGrant() {
        RolapSchema schema = createSchema();
        schema = spy(schema);
        doNothing().when(schema)
            .handleCubeGrant(
                any(RoleImpl.class), any(MondrianDef.CubeGrant.class));

        MondrianDef.SchemaGrant grant = new MondrianDef.SchemaGrant();
        grant.access = Access.CUSTOM.toString();
        grant.cubeGrants =  new MondrianDef.CubeGrant[] {
            new MondrianDef.CubeGrant(), new MondrianDef.CubeGrant()};

        RoleImpl role = new RoleImpl();

        schema.handleSchemaGrant(role, grant);
        assertEquals(Access.CUSTOM, role.getAccess(schema));
        verify(schema, times(2))
            .handleCubeGrant(eq(role), any(MondrianDef.CubeGrant.class));
    }


    public void testHandleCubeGrant_ThrowsException_WhenCubeIsUnknown() {
        RolapSchema schema = createSchema();
        schema = spy(schema);
        doReturn(null).when(schema).lookupCube(anyString());

        MondrianDef.CubeGrant grant = new MondrianDef.CubeGrant();
        grant.cube = "cube";

        try {
            schema.handleCubeGrant(new RoleImpl(), grant);
        } catch (MondrianException e) {
            String message = e.getMessage();
            assertTrue(message, message.contains(grant.cube));
            return;
        }
        fail("Should fail if cube is unknown");
    }

    public void testHandleCubeGrant_GrantsCubeDimensionsAndHierarchies() {
        RolapSchema schema = createSchema();
        schema = spy(schema);
        doNothing().when(schema)
            .handleHierarchyGrant(
                any(RoleImpl.class),
                any(RolapCube.class),
                any(SchemaReader.class),
                any(MondrianDef.HierarchyGrant.class));

        final Dimension dimension = mock(Dimension.class);
        SchemaReader reader = mockSchemaReader(Category.Dimension, dimension);

        RolapCube cube = mockCube(schema);
        when(cube.getSchemaReader(any(Role.class))).thenReturn(reader);
        doReturn(cube).when(schema).lookupCube("cube");

        MondrianDef.DimensionGrant dimensionGrant =
            new MondrianDef.DimensionGrant();
        dimensionGrant.dimension = "dimension";
        dimensionGrant.access = Access.NONE.toString();

        MondrianDef.CubeGrant grant = new MondrianDef.CubeGrant();
        grant.cube = "cube";
        grant.access = Access.CUSTOM.toString();
        grant.dimensionGrants =
            new MondrianDef.DimensionGrant[] {dimensionGrant};
        grant.hierarchyGrants =
            new MondrianDef.HierarchyGrant[] {
                new MondrianDef.HierarchyGrant()};

        RoleImpl role = new RoleImpl();

        schema.handleCubeGrant(role, grant);

        assertEquals(Access.CUSTOM, role.getAccess(cube));
        assertEquals(Access.NONE, role.getAccess(dimension));
        verify(schema, times(1))
            .handleHierarchyGrant(
                eq(role),
                eq(cube),
                eq(reader),
                any(MondrianDef.HierarchyGrant.class));
    }


    public void testHandleHierarchyGrant_ValidMembers() {
        doTestHandleHierarchyGrant(Access.CUSTOM, Access.ALL);
    }

    public void testHandleHierarchyGrant_NoValidMembers() {
        doTestHandleHierarchyGrant(Access.NONE, null);
    }

    private void doTestHandleHierarchyGrant(
        Access expectedHierarchyAccess,
        Access expectedMemberAccess)
    {
        propSaver.set(propSaver.properties.IgnoreInvalidMembers, true);

        RolapSchema schema = createSchema();
        RolapCube cube = mockCube(schema);
        RoleImpl role = new RoleImpl();

        MondrianDef.MemberGrant memberGrant = new MondrianDef.MemberGrant();
        memberGrant.access = Access.ALL.toString();
        memberGrant.member = "member";

        MondrianDef.HierarchyGrant grant = new MondrianDef.HierarchyGrant();
        grant.access = Access.CUSTOM.toString();
        grant.rollupPolicy = Role.RollupPolicy.FULL.toString();
        grant.hierarchy = "hierarchy";
        grant.memberGrants = new MondrianDef.MemberGrant[] {memberGrant};

        Level level = mock(Level.class);
        Hierarchy hierarchy = mock(Hierarchy.class);
        when(hierarchy.getLevels()).thenReturn(new Level[]{level});
        when(level.getHierarchy()).thenReturn(hierarchy);

        Dimension dimension = mock(Dimension.class);
        when(hierarchy.getDimension()).thenReturn(dimension);

        SchemaReader reader = mockSchemaReader(Category.Hierarchy, hierarchy);

        Member member = mock(Member.class);
        when(member.getHierarchy()).thenReturn(hierarchy);
        when(member.getLevel()).thenReturn(level);

        if (expectedMemberAccess != null) {
            when(reader.getMemberByUniqueName(
                anyListOf(Id.Segment.class), anyBoolean())).thenReturn(member);
        }

        schema.handleHierarchyGrant(role, cube, reader, grant);
        assertEquals(expectedHierarchyAccess, role.getAccess(hierarchy));
        if (expectedMemberAccess != null) {
            assertEquals(expectedMemberAccess, role.getAccess(member));
        }
    }


    private void assertMondrianException(
        MondrianException expected,
        MondrianException actual)
    {
        assertEquals(expected.getMessage(), actual.getMessage());
    }
}

// End RolapSchemaTest.java
