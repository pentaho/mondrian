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
import mondrian.rolap.RolapSchema.RolapStarRegistry;
import mondrian.rolap.agg.AggregationManager;
import mondrian.rolap.agg.SegmentCacheManager;
import mondrian.test.PropertyRestoringTestCase;
import mondrian.util.ByteString;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

import java.lang.reflect.Field;
import java.util.List;

import org.eigenbase.xom.DOMWrapper;
import org.eigenbase.xom.Parser;
import org.eigenbase.xom.XOMException;
import org.eigenbase.xom.XOMUtil;

/**
 * @author Andrey Khayrutdinov
 */
public class RolapSchemaTest extends PropertyRestoringTestCase {
  private RolapSchema schemaSpy;
  private static RolapStar rlStarMock = mock(RolapStar.class);

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        schemaSpy = spy(createSchema());
    }


    private RolapSchema createSchema() {
        SchemaKey key = new SchemaKey(
            mock(SchemaContentKey.class), mock(ConnectionKey.class));

        ByteString md5 = new ByteString("test schema".getBytes());
        //noinspection deprecation
        //mock rolap connection to eliminate calls for cache loading
        MondrianServer mServerMock = mock(MondrianServer.class);
        RolapConnection rolapConnectionMock = mock(RolapConnection.class);
        AggregationManager aggManagerMock = mock(AggregationManager.class);
        SegmentCacheManager scManagerMock = mock(SegmentCacheManager.class);
        when(rolapConnectionMock.getServer()).thenReturn(mServerMock);
        when(mServerMock.getAggregationManager()).thenReturn(aggManagerMock);
        when(aggManagerMock.getCacheMgr()).thenReturn(scManagerMock);
        return new RolapSchema(key, md5, rolapConnectionMock);
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

    public void testEmptyRolapStarRegistryCreatedForTheNewSchema()
        throws Exception {
      RolapSchema schema = createSchema();
      RolapStarRegistry rolapStarRegistry = schema.getRolapStarRegistry();
      assertNotNull(rolapStarRegistry);
      assertTrue(rolapStarRegistry.getStars().isEmpty());
    }

    public void testGetOrCreateStar_StarCreatedAndUsed()
        throws Exception {
      //Create the test fact
      MondrianDef.Relation fact =
          new MondrianDef.Table(
              wrapStrSources(getFactTableWithSQLFilter()));
      List<String> rolapStarKey = RolapUtil.makeRolapStarKey(fact);
      //Expected result star
      RolapStar expectedStar = rlStarMock;
      RolapStarRegistry rolapStarRegistry =
          getStarRegistryLinkedToRolapSchemaSpy(schemaSpy, fact);


      //Test that a new rolap star has created and put to the registry
      RolapStar actualStar = rolapStarRegistry.getOrCreateStar(fact);
      assertSame(expectedStar, actualStar);
      assertEquals(1, rolapStarRegistry.getStars().size());
      assertEquals(expectedStar, rolapStarRegistry.getStar(rolapStarKey));
      verify(schemaSpy, times(1)).makeRolapStar(fact);
      //test that no new rolap star has created,
      //but extracted already existing one from the registry
      RolapStar actualStar2 = rolapStarRegistry.getOrCreateStar(fact);
      verify(schemaSpy, times(1)).makeRolapStar(fact);
      assertSame(expectedStar, actualStar2);
      assertEquals(1, rolapStarRegistry.getStars().size());
      assertEquals(expectedStar, rolapStarRegistry.getStar(rolapStarKey));
    }

    public void testGetStarFromRegistryByStarKey() throws Exception {
      //Create the test fact
      MondrianDef.Relation fact =
          new MondrianDef.Table(wrapStrSources(getFactTableWithSQLFilter()));
      List<String> rolapStarKey = RolapUtil.makeRolapStarKey(fact);
      //Expected result star
      RolapStarRegistry rolapStarRegistry =
          getStarRegistryLinkedToRolapSchemaSpy(schemaSpy, fact);
      //Put rolap star to the registry
      rolapStarRegistry.getOrCreateStar(fact);

      RolapStar actualStar = schemaSpy.getStar(rolapStarKey);
      assertSame(rlStarMock, actualStar);
    }

    public void testGetStarFromRegistryByFactTableName() throws Exception {
      //Create the test fact
      MondrianDef.Relation fact =
          new MondrianDef.Table(wrapStrSources(getFactTable()));
      //Expected result star
      RolapStarRegistry rolapStarRegistry =
          getStarRegistryLinkedToRolapSchemaSpy(schemaSpy, fact);
      //Put rolap star to the registry
      rolapStarRegistry.getOrCreateStar(fact);

      RolapStar actualStar = schemaSpy.getStar(fact.getAlias());
      assertSame(rlStarMock, actualStar);
    }

    private static RolapStarRegistry getStarRegistryLinkedToRolapSchemaSpy(
        RolapSchema schemaSpy, MondrianDef.Relation fact) throws Exception
    {
      //the rolap star registry is linked to the origin rolap schema,
      //not to the schemaSpy
      RolapStarRegistry rolapStarRegistry = schemaSpy.getRolapStarRegistry();
      //the star mock
      doReturn(rlStarMock).when(schemaSpy).makeRolapStar(fact);
      //Set the schema spy to be linked with the rolap star registry
      assertTrue(
          "For testing purpose object this$0 in the inner class "
          + "should be replaced to the rolap schema spy "
          + "but this not happend",
          replaceRolapSchemaLinkedToStarRegistry(
              rolapStarRegistry,
              schemaSpy));
      verify(schemaSpy, times(0)).makeRolapStar(fact);
      return rolapStarRegistry;
    }

     private static boolean replaceRolapSchemaLinkedToStarRegistry(
         RolapStarRegistry innerClass,
         RolapSchema sSpy) throws Exception
     {
       Field field = innerClass.getClass().getDeclaredField("this$0");
       if (field != null) {
         field.setAccessible(true);
         field.set(innerClass, sSpy);
         RolapSchema outerMocked = (RolapSchema) field.get(innerClass);
         return outerMocked == sSpy;
       }
       return false;
      }

    private static String getFactTableWithSQLFilter() {
      String fact =
          "<Table name=\"sales_fact_1997\" alias=\"TableAlias\">\n"
          + " <SQL dialect=\"mysql\">\n"
          + "     `TableAlias`.`promotion_id` = 112\n"
          + " </SQL>\n"
          + "</Table>";
      return fact;
    }

    private static String getFactTable() {
      String fact =
          "<Table name=\"sales_fact_1997\" alias=\"TableAlias\"/>";
      return fact;
    }

    private static DOMWrapper wrapStrSources(String resStr)
        throws XOMException {
      final Parser xmlParser = XOMUtil.createDefaultParser();
      final DOMWrapper def = xmlParser.parse(resStr);
      return def;
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
