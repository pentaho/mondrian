/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (c) 2002-2018 Hitachi Vantara..  All rights reserved.
*/
package mondrian.rolap;

import mondrian.olap.Access;
import mondrian.olap.Cube;
import mondrian.olap.Dimension;
import mondrian.olap.Hierarchy;
import mondrian.olap.Level;
import mondrian.olap.Role;
import mondrian.olap.Role.HierarchyAccess;
import mondrian.olap.Schema;
import mondrian.rolap.RestrictedMemberReader.MultiCardinalityDefaultMember;
import mondrian.rolap.sql.MemberChildrenConstraint;
import mondrian.test.FoodMartTestCase;

import junit.framework.Assert;

import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class RestrictedMemberReaderTest extends FoodMartTestCase {

  private RestrictedMemberReader rmr;
  private MemberReader mr;
  private Role role;

  private RolapMember anyRolapMember() {
    return Mockito.any(RolapMember.class);
  }

  private RolapMember mockMember() {
    return Mockito.mock(RolapMember.class);
  }

  public void testGetHierarchy_allAccess() {
    Schema schema = Mockito.mock(Schema.class);
    Dimension dimension = Mockito.mock(Dimension.class);
    RolapHierarchy hierarchy = Mockito.mock(RolapHierarchy.class);
    Level[] hierarchyAccessLevels = new Level[] { null };
    MemberReader delegateMemberReader = Mockito.mock(MemberReader.class);
    HierarchyAccess roleAccess = null;
    Role role = Mockito.mock(Role.class);

    Mockito.doReturn(schema).when(dimension).getSchema();
    Mockito.doReturn(dimension).when(hierarchy).getDimension();
    Mockito.doReturn(hierarchyAccessLevels).when(hierarchy).getLevels();
    Mockito.doReturn(true).when(hierarchy).isRagged();
    Mockito.doReturn(roleAccess).when(role)
        .getAccessDetails(Mockito.any(Hierarchy.class));
    Mockito.doReturn(hierarchy).when(delegateMemberReader).getHierarchy();

    rmr = new RestrictedMemberReader(delegateMemberReader, role);

    Assert.assertSame(hierarchy, rmr.getHierarchy());
  }

  public void testGetHierarchy_roleAccess() {
    RolapHierarchy hierarchy = Mockito.mock(RolapHierarchy.class);
    MemberReader delegateMemberReader = Mockito.mock(MemberReader.class);
    HierarchyAccess roleAccess = Mockito.mock(HierarchyAccess.class);
    Role role = Mockito.mock(Role.class);

    Mockito.doReturn(roleAccess).when(role)
        .getAccessDetails(Mockito.any(Hierarchy.class));
    Mockito.doReturn(hierarchy).when(delegateMemberReader).getHierarchy();

    rmr = new RestrictedMemberReader(delegateMemberReader, role);

    Assert.assertSame(hierarchy, rmr.getHierarchy());
  }

  public void testDefaultMember_allAccess() {
    Schema schema = Mockito.mock(Schema.class);
    Dimension dimension = Mockito.mock(Dimension.class);
    RolapHierarchy hierarchy = Mockito.mock(RolapHierarchy.class);
    Level[] hierarchyAccessLevels = new Level[] { null };
    MemberReader delegateMemberReader = Mockito.mock(MemberReader.class);
    HierarchyAccess roleAccess = null;
    Role role = Mockito.mock(Role.class);

    Mockito.doReturn(schema).when(dimension).getSchema();
    Mockito.doReturn(dimension).when(hierarchy).getDimension();
    Mockito.doReturn(hierarchyAccessLevels).when(hierarchy).getLevels();
    Mockito.doReturn(true).when(hierarchy).isRagged();
    Mockito.doReturn(roleAccess).when(role)
        .getAccessDetails(Mockito.any(Hierarchy.class));
    Mockito.doReturn(hierarchy).when(delegateMemberReader).getHierarchy();

    RolapMember hDefaultMember = mockMember();
    Mockito.doReturn(hDefaultMember).when(hierarchy).getDefaultMember();

    rmr = new RestrictedMemberReader(delegateMemberReader, role);

    Assert.assertSame(hDefaultMember, rmr.getDefaultMember());
  }

  public void testDefaultMember_roleAccess() {
    RolapHierarchy hierarchy = Mockito.mock(RolapHierarchy.class);
    MemberReader delegateMemberReader = Mockito.mock(MemberReader.class);
    HierarchyAccess roleAccess = Mockito.mock(HierarchyAccess.class);
    Role role = Mockito.mock(Role.class);
    RolapMember member0 = mockMember();
    List<RolapMember> rootMembers = Arrays.asList(new RolapMember[] {member0});
    RolapMember hierDefaultMember = member0;

    Mockito.doReturn(roleAccess).when(role)
        .getAccessDetails(Mockito.any(Hierarchy.class));
    Mockito.doReturn(hierarchy).when(delegateMemberReader).getHierarchy();

    Mockito.doReturn(hierDefaultMember).when(hierarchy).getDefaultMember();

    rmr = Mockito.spy(new RestrictedMemberReader(delegateMemberReader, role));
    Mockito.doReturn(rootMembers).when(rmr).getRootMembers();

    Mockito.doReturn(null).when(roleAccess).getAccess(anyRolapMember());
    Assert.assertSame(
        "on Access is null", hierDefaultMember, rmr.getDefaultMember());

    Mockito.doReturn(Access.ALL).when(roleAccess).getAccess(anyRolapMember());
    Assert.assertSame(
        "on Access.ALL", hierDefaultMember, rmr.getDefaultMember());

    Mockito.doReturn(Access.CUSTOM).when(roleAccess)
        .getAccess(anyRolapMember());
    Assert.assertSame(
        "on Access.CUSTOM", hierDefaultMember, rmr.getDefaultMember());

    Mockito.doReturn(Access.NONE).when(roleAccess).getAccess(anyRolapMember());

    Assert.assertNotSame(
        "on Access.NONE", hierDefaultMember, rmr.getDefaultMember());
    Assert.assertTrue(
        rmr.getDefaultMember() instanceof MultiCardinalityDefaultMember);
  }

  public void testDefaultMember_noDefaultMember_roleAccess() {
    RolapHierarchy hierarchy = Mockito.mock(RolapHierarchy.class);
    MemberReader delegateMemberReader = Mockito.mock(MemberReader.class);
    HierarchyAccess roleAccess = Mockito.mock(HierarchyAccess.class);
    Role role = Mockito.mock(Role.class);
    RolapMember member0 = mockMember();
    List<RolapMember> rootMembers = Arrays.asList(new RolapMember[] {member0});
    RolapMember hierDefaultMember = null;

    Mockito.doReturn(roleAccess).when(role)
        .getAccessDetails(Mockito.any(Hierarchy.class));
    Mockito.doReturn(hierarchy).when(delegateMemberReader).getHierarchy();

    Mockito.doReturn(hierDefaultMember).when(hierarchy).getDefaultMember();

    rmr = Mockito.spy(new RestrictedMemberReader(delegateMemberReader, role));
    Mockito.doReturn(rootMembers).when(rmr).getRootMembers();

    Mockito.doReturn(null).when(roleAccess).getAccess(anyRolapMember());
    Assert.assertSame(
        "on Access is null", member0, rmr.getDefaultMember());

    Mockito.doReturn(Access.ALL).when(roleAccess).getAccess(anyRolapMember());
    Assert.assertSame(
        "on Access.ALL", member0, rmr.getDefaultMember());

    Mockito.doReturn(Access.CUSTOM).when(roleAccess)
        .getAccess(anyRolapMember());
    Assert.assertSame(
        "on Access.CUSTOM", member0, rmr.getDefaultMember());

    Mockito.doReturn(Access.NONE).when(roleAccess).getAccess(anyRolapMember());

    Assert.assertNotSame(
        "on Access.NONE", member0, rmr.getDefaultMember());
    Assert.assertTrue(
        rmr.getDefaultMember() instanceof MultiCardinalityDefaultMember);
  }

  public void testDefaultMember_multiRoot() {
    RolapHierarchy hierarchy = Mockito.mock(RolapHierarchy.class);
    MemberReader delegateMemberReader = Mockito.mock(MemberReader.class);
    HierarchyAccess roleAccess = Mockito.mock(HierarchyAccess.class);
    Role role = Mockito.mock(Role.class);
    RolapMember member0 = mockMember();
    RolapMember member1 = mockMember();
    RolapMember member2 = mockMember();
    List<RolapMember> rootMembers = Arrays.asList(
        new RolapMember[] {member0, member1, member2});
    RolapMember hierDefaultMember = member1;

    Mockito.doReturn(roleAccess).when(role)
        .getAccessDetails(Mockito.any(Hierarchy.class));
    Mockito.doReturn(hierarchy).when(delegateMemberReader).getHierarchy();

    Mockito.doReturn(hierDefaultMember).when(hierarchy).getDefaultMember();

    rmr = Mockito.spy(new RestrictedMemberReader(delegateMemberReader, role));
    Mockito.doReturn(rootMembers).when(rmr).getRootMembers();

    Mockito.doReturn(Access.CUSTOM).when(roleAccess).getAccess(member0);
    Mockito.doReturn(Access.NONE).when(roleAccess).getAccess(member1);
    Mockito.doReturn(Access.NONE).when(roleAccess).getAccess(member2);
    Assert.assertSame(
        "on Access C-N-N", member0, rmr.getDefaultMember());

    Mockito.doReturn(Access.NONE).when(roleAccess).getAccess(member0);
    Mockito.doReturn(Access.NONE).when(roleAccess).getAccess(member1);
    Mockito.doReturn(Access.CUSTOM).when(roleAccess).getAccess(member2);
    Assert.assertSame(
        "on Access N-N-C", member2, rmr.getDefaultMember());

    Mockito.doReturn(Access.CUSTOM).when(roleAccess).getAccess(member0);
    Mockito.doReturn(Access.CUSTOM).when(roleAccess).getAccess(member1);
    Mockito.doReturn(Access.CUSTOM).when(roleAccess).getAccess(member2);
    Assert.assertSame(
        "on Access C-C-C", hierDefaultMember, rmr.getDefaultMember());

    Mockito.doReturn(Access.CUSTOM).when(roleAccess).getAccess(member0);
    Mockito.doReturn(Access.NONE).when(roleAccess).getAccess(member1);
    Mockito.doReturn(Access.CUSTOM).when(roleAccess).getAccess(member2);
    Assert.assertTrue(
        "on Access C-N-C",
        rmr.getDefaultMember() instanceof MultiCardinalityDefaultMember);

    Mockito.doReturn(Access.NONE).when(roleAccess).getAccess(anyRolapMember());
    Assert.assertTrue(
        "on Access.NONE",
        rmr.getDefaultMember() instanceof MultiCardinalityDefaultMember);
  }

  public void testDefaultMember_multiRootMeasure() {
    RolapHierarchy hierarchy = Mockito.mock(RolapHierarchy.class);
    MemberReader delegateMemberReader = Mockito.mock(MemberReader.class);
    HierarchyAccess roleAccess = Mockito.mock(HierarchyAccess.class);
    Role role = Mockito.mock(Role.class);
    RolapMember member0 = mockMember();
    RolapMember member1 = mockMember();
    RolapMember member2 = mockMember();
    List<RolapMember> rootMembers = Arrays.asList(
        new RolapMember[] {member0, member1, member2});
    RolapMember hierDefaultMember = member1;

    Mockito.doReturn(true).when(member0).isMeasure();
    Mockito.doReturn(true).when(member1).isMeasure();
    Mockito.doReturn(true).when(member1).isMeasure();

    Mockito.doReturn(roleAccess).when(role)
        .getAccessDetails(Mockito.any(Hierarchy.class));
    Mockito.doReturn(hierarchy).when(delegateMemberReader).getHierarchy();

    Mockito.doReturn(hierDefaultMember).when(hierarchy).getDefaultMember();

    rmr = Mockito.spy(new RestrictedMemberReader(delegateMemberReader, role));
    Mockito.doReturn(rootMembers).when(rmr).getRootMembers();

    Mockito.doReturn(Access.CUSTOM).when(roleAccess).getAccess(member0);
    Mockito.doReturn(Access.NONE).when(roleAccess).getAccess(member1);
    Mockito.doReturn(Access.NONE).when(roleAccess).getAccess(member2);
    Assert.assertSame(
        "on Access C-N-N", member0, rmr.getDefaultMember());

    Mockito.doReturn(Access.NONE).when(roleAccess).getAccess(member0);
    Mockito.doReturn(Access.NONE).when(roleAccess).getAccess(member1);
    Mockito.doReturn(Access.CUSTOM).when(roleAccess).getAccess(member2);
    Assert.assertSame(
        "on Access N-N-C", member2, rmr.getDefaultMember());

    Mockito.doReturn(Access.CUSTOM).when(roleAccess).getAccess(member0);
    Mockito.doReturn(Access.CUSTOM).when(roleAccess).getAccess(member1);
    Mockito.doReturn(Access.CUSTOM).when(roleAccess).getAccess(member2);
    Assert.assertSame(
        "on Access C-C-C", hierDefaultMember, rmr.getDefaultMember());

    Mockito.doReturn(Access.CUSTOM).when(roleAccess).getAccess(member0);
    Mockito.doReturn(Access.NONE).when(roleAccess).getAccess(member1);
    Mockito.doReturn(Access.CUSTOM).when(roleAccess).getAccess(member2);
    Assert.assertSame(
        "on Access C-N-C", member0, rmr.getDefaultMember());

    Mockito.doReturn(Access.NONE).when(roleAccess).getAccess(anyRolapMember());
    Assert.assertTrue(
        "on Access.NONE",
        rmr.getDefaultMember() instanceof MultiCardinalityDefaultMember);
  }

  private Cube findCubeByName(Cube[] cc, String cn) {
    for (Cube c : cc) {
      if (cn.equals(c.getName())) {
        return c;
      }
    }
    return null;
  }

  public void testProcessMemberChildren() {

      MemberReader delegateMemberReader = Mockito.mock(MemberReader.class);
      MemberChildrenConstraint constraint = Mockito.mock(MemberChildrenConstraint.class);
      Role role = Mockito.mock(Role.class);
      Schema schema = Mockito.mock(Schema.class);
      Dimension dimension = Mockito.mock(Dimension.class);
      RolapHierarchy hierarchy = Mockito.mock(RolapHierarchy.class);

      Level[] hierarchyAccessLevels = new Level[] { null };

      Mockito.doReturn(schema).when(dimension).getSchema();
      Mockito.doReturn(dimension).when(hierarchy).getDimension();
      Mockito.doReturn(hierarchyAccessLevels).when(hierarchy).getLevels();
      Mockito.doReturn(true).when(hierarchy).isRagged();
      Mockito.doReturn(hierarchy).when(delegateMemberReader).getHierarchy();

      List<RolapMember> children = new ArrayList<>();
      children.add(mockMember());

      List<RolapMember> fullChildren = new ArrayList<>();
      fullChildren.add(mockMember());
      fullChildren.add(mockMember());

      rmr = new RestrictedMemberReader(delegateMemberReader, role);
      final Map<RolapMember, Access> testResult = rmr.processMemberChildren(fullChildren, children, constraint);

      Assert.assertEquals(2, testResult.size());
      Assert.assertTrue(testResult.containsValue(Access.ALL));
  }
}
// End RestrictedMemberReaderTest.java
