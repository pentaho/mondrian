/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2006-2024 Hitachi Vantara and others
// All Rights Reserved.
 */
package mondrian.rolap;

import mondrian.rolap.sql.MemberChildrenConstraint;

import junit.framework.TestCase;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class MemberCacheHelperTest extends TestCase {

    @Mock
    private RolapMember parentMember;

    @Mock
    private ChildByNameConstraint childByNameConstraint;

    @Mock
    private MemberKey memberKey;

    private MemberChildrenConstraint defMemChildrenConstraint =
        DefaultMemberChildrenConstraint.instance();

    private List<RolapMember> children = new ArrayList<RolapMember>();

    private MemberCacheHelper cacheHelper = new MemberCacheHelper(null);

    @Override
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    public void testRoundtripChildrenUsingChildByNameConstraint() {
        List<String> childNames = fillChildren(children, 3);
        when(childByNameConstraint.getChildNames()).thenReturn(childNames);

        cacheHelper.putChildren(parentMember, childByNameConstraint, children);

        List<RolapMember> retrievedChildren =
            cacheHelper.getChildrenFromCache(
                parentMember, childByNameConstraint);

        assertEquals(children, retrievedChildren);
    }

    public void testCachedByDefaultConstraint() {
        List<String> childNames = fillChildren(children, 5);
        when(childByNameConstraint.getChildNames()).thenReturn(childNames);

        // cached under default constraint, but subsequent
        // retrieval with childByName should work since all children present.
        cacheHelper.putChildren(
            parentMember,
            defMemChildrenConstraint, children);

        List<RolapMember> retrievedChildren =
            cacheHelper.getChildrenFromCache(
                parentMember,
                childByNameConstraint);

        assertEquals(children, retrievedChildren);
    }

    public void testOnlyRequestedChildrenRetrieved() {
        // tests retrieval of a subset of children from
        // the cache with keyed with DefaultMemberChildrenConstraint
        List<String> childNames = fillChildren(children, 5);

        int FROM = 2;
        int TO = 5;
        // childByName constraint defined with member names in sublist
        when(childByNameConstraint.getChildNames()).thenReturn(
            childNames.subList(FROM, TO));

        // cached under default constraint, but subsequent
        // retrieval with childByName should work since all children present.
        cacheHelper.putChildren(
            parentMember,
            defMemChildrenConstraint, children);

        List<RolapMember> retrievedChildren =
            cacheHelper.getChildrenFromCache(
                parentMember,
                childByNameConstraint);

        assertEquals(
            "Expected children were not retrieved from cache.",
            children.subList(FROM, TO), retrievedChildren);
    }

    public void testMissingChildrenNotRetrievedDefaultConst() {
        runMissingChildrenNotRetrievedTest(defMemChildrenConstraint);
    }

    public void testMissingChildrenNotRetrievedChildByName() {
        runMissingChildrenNotRetrievedTest(childByNameConstraint);
    }

    public void runMissingChildrenNotRetrievedTest(
        MemberChildrenConstraint constraint)
    {
        fillChildren(children, 5);
        when(childByNameConstraint.getChildNames()).thenReturn(
            Arrays.asList(new String[]{ "Other Name", "Other Name2" }));

        cacheHelper.putChildren(
            parentMember, constraint, children);

        List<RolapMember> retrievedChildren =
            cacheHelper.getChildrenFromCache(
                parentMember, childByNameConstraint);

        assertEquals(
            "Not expecting to retrieve anything from cache",
            null, retrievedChildren);
    }


    public void testRemoveChildMemberPresentInNamedChildrenMap() {
        List<String> childNames = fillChildren(children, 3);
        when(childByNameConstraint.getChildNames()).thenReturn(
            childNames.subList(1, 3));
        List<MemberKey> childKeys = new ArrayList<MemberKey>();

        for (RolapMember member : children) {
            when(member.getParentMember()).thenReturn(parentMember);
            MemberKey key = mockMemberKey();
            childKeys.add(key);
            cacheHelper.putMember(key, member);
        }
        cacheHelper.putMember(memberKey, parentMember);
        cacheHelper.putChildren(parentMember, childByNameConstraint, children);

        cacheHelper.removeMember(childKeys.get(0));

        List<RolapMember> members =
            cacheHelper.getChildrenFromCache(
                parentMember, childByNameConstraint);

        assertEquals(
            "Retrieved children should not include the removed member",
            children.subList(1, 3), members);
    }

    private MemberKey mockMemberKey() {
        MemberKey mock = mock(MemberKey.class);
        when(mock.getLevel()).thenReturn(mock(RolapLevel.class));
        return mock;
    }

    private List<String> fillChildren(List<RolapMember> children, int count) {
        List<String> names = new ArrayList<String>();
        for (int i = 0; i < count; i++) {
            RolapMember member = mock(RolapMember.class);
            String name = "Member-" + i;
            names.add(name);
            when(member.getName()).thenReturn(name);
            // there's a bug in mockito which causes mock objects
            // .compareTo to always return 1
            // https://code.google.com/p/mockito/issues/detail?id=467
            // here's a workaround.
            when(member.compareTo(any(RolapMember.class))).thenAnswer(
                new Answer<Object>() {
                    public Object answer(InvocationOnMock invocation)
                        throws Throwable
                    {
                        return  ((RolapMember)invocation.getMock()).getName()
                            .compareTo(
                                ((RolapMember) invocation.getArguments()[0])
                                    .getName());
                    }
                }
            );
            children.add(member);
        }
        return names;
    }
}

// End MemberCacheHelperTest.java