/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2003-2005 Julian Hyde
// Copyright (C) 2005-2013 Pentaho
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.olap.*;
import mondrian.resource.MondrianResource;
import mondrian.rolap.sql.MemberChildrenConstraint;
import mondrian.rolap.sql.TupleConstraint;

import java.util.*;

/**
 * A <code>RestrictedMemberReader</code> reads only the members of a hierarchy
 * allowed by a role's access profile.
 *
 * @author jhyde
 * @since Feb 26, 2003
 */
class RestrictedMemberReader extends DelegatingMemberReader {

    private final Role.HierarchyAccess hierarchyAccess;
    private final boolean ragged;
    private final SqlConstraintFactory sqlConstraintFactory =
        SqlConstraintFactory.instance();

    /**
     * Creates a <code>RestrictedMemberReader</code>.
     *
     * <p>There's no filtering to be done unless
     * either the role has restrictions on this hierarchy,
     * or the hierarchy is ragged; there's a pre-condition to this effect.</p>
     *
     * @param memberReader Underlying (presumably unrestricted) member reader
     * @param role Role whose access profile to obey. The role must have
     *   restrictions on this hierarchy
     */
    RestrictedMemberReader(MemberReader memberReader, Role role) {
        super(memberReader);
        RolapHierarchy hierarchy = memberReader.getHierarchy();
        ragged = hierarchy.isRagged();
        if (role.getAccessDetails(hierarchy) == null) {
            assert ragged;
            hierarchyAccess = RoleImpl.createAllAccess(hierarchy);
        } else {
            hierarchyAccess = role.getAccessDetails(hierarchy);
        }
    }

    public boolean setCache(MemberCache cache) {
        // Don't support cache-writeback. It would confuse the cache!
        return false;
    }

    public RolapMember getLeadMember(RolapMember member, int n) {
        int i = 0;
        int increment = 1;
        if (n < 0) {
            increment = -1;
            n = -n;
        }
        while (i < n) {
            member = memberReader.getLeadMember(member, increment);
            if (member.isNull()) {
                return member;
            }
            if (canSee(member)) {
                ++i;
            }
        }
        return member;
    }

    public void getMemberChildren(
        RolapMember parentMember,
        List<RolapMember> children)
    {
        MemberChildrenConstraint constraint =
            sqlConstraintFactory.getMemberChildrenConstraint(null);
        getMemberChildren(parentMember, children, constraint);
    }

    public void getMemberChildren(
        RolapMember parentMember,
        List<RolapMember> children,
        MemberChildrenConstraint constraint)
    {
        List<RolapMember> fullChildren = new ArrayList<RolapMember>();
        memberReader.getMemberChildren(parentMember, fullChildren, constraint);
        processMemberChildren(fullChildren, children, constraint);
    }

    private void processMemberChildren(
        List<RolapMember> fullChildren,
        List<RolapMember> children,
        MemberChildrenConstraint constraint)
    {
        // todo: optimize if parentMember is beyond last level
        List<RolapMember> grandChildren = null;
        for (int i = 0; i < fullChildren.size(); i++) {
            RolapMember member = fullChildren.get(i);
            // If a child is hidden (due to raggedness) include its children.
            // This must be done before applying access-control.
            if (ragged) {
                if (member.isHidden()) {
                    // Replace this member with all of its children.
                    // They might be hidden too, but we'll get to them in due
                    // course. They also might be access-controlled; that's why
                    // we deal with raggedness before we apply access-control.
                    fullChildren.remove(i);
                    if (grandChildren == null) {
                        grandChildren = new ArrayList<RolapMember>();
                    } else {
                        grandChildren.clear();
                    }
                    memberReader.getMemberChildren(
                        member, grandChildren, constraint);
                    fullChildren.addAll(i, grandChildren);
                    // Step back to before the first child we just inserted,
                    // and go through the loop again.
                    --i;
                    continue;
                }
            }
            // Filter out children which are invisible because of
            // access-control.
            final Access access;
            if (hierarchyAccess != null) {
                access = hierarchyAccess.getAccess(member);
            } else {
                access = Access.ALL;
            }
            switch (access) {
            case NONE:
                break;
            default:
                children.add(member);
                break;
            }
        }
    }

    /**
     * Writes to members which we can see.
     * @param members Input list
     * @param filteredMembers Output list
     */
    private void filterMembers(
        List<RolapMember> members,
        List<RolapMember> filteredMembers)
    {
        for (RolapMember member : members) {
            if (canSee(member)) {
                filteredMembers.add(member);
            }
        }
    }

    private boolean canSee(final RolapMember member) {
        if (ragged && member.isHidden()) {
            return false;
        }
        if (hierarchyAccess != null) {
            final Access access = hierarchyAccess.getAccess(member);
            return access != Access.NONE;
        }
        return true;
    }

    public void getMemberChildren(
        List<RolapMember> parentMembers,
        List<RolapMember> children)
    {
        MemberChildrenConstraint constraint =
            sqlConstraintFactory.getMemberChildrenConstraint(null);
        getMemberChildren(parentMembers, children, constraint);
    }

    public synchronized void getMemberChildren(
        List<RolapMember> parentMembers,
        List<RolapMember> children,
        MemberChildrenConstraint constraint)
    {
//        for (Iterator i = parentMembers.iterator(); i.hasNext();) {
//            RolapMember parentMember = (RolapMember) i.next();
//            getMemberChildren(parentMember, children, constraint);
//        }
        List<RolapMember> fullChildren = new ArrayList<RolapMember>();
        super.getMemberChildren(parentMembers, fullChildren, constraint);
        processMemberChildren(fullChildren, children, constraint);
    }

    public List<RolapMember> getRootMembers() {
        final List<RolapMember> memberList;
        final int topLevelDepth = hierarchyAccess.getTopLevelDepth();
        if (topLevelDepth > 0) {
            RolapCubeLevel topLevel =
                getHierarchy().getLevelList().get(topLevelDepth);
            memberList = getMembersInLevel(topLevel);
        } else {
            List<RolapMember> unfilteredMemberList = super.getRootMembers();
            memberList = new ArrayList<RolapMember>();
            filterMembers(unfilteredMemberList, memberList);
        }
        if (memberList.isEmpty()) {
            throw MondrianResource.instance()
                .HierarchyHasNoAccessibleMembers.ex(
                    getHierarchy().getUniqueName());
        }
        return memberList;
    }

    public List<RolapMember> getMembersInLevel(
        RolapCubeLevel level)
    {
        TupleConstraint constraint =
            sqlConstraintFactory.getLevelMembersConstraint(null);
        return getMembersInLevel(level, constraint);
    }

    public List<RolapMember> getMembersInLevel(
        RolapCubeLevel level, TupleConstraint constraint)
    {
        if (hierarchyAccess != null) {
            final int depth = level.getDepth();
            if (depth < hierarchyAccess.getTopLevelDepth()) {
                return Collections.emptyList();
            }
            if (depth > hierarchyAccess.getBottomLevelDepth()) {
                return Collections.emptyList();
            }
        }
        final List<RolapMember> membersInLevel =
            memberReader.getMembersInLevel(
                level, constraint);
        List<RolapMember> filteredMembers = new ArrayList<RolapMember>();
        filterMembers(membersInLevel, filteredMembers);
        return filteredMembers;
    }

    public RolapMember getDefaultMember() {
        RolapMember defaultMember = getHierarchy().getDefaultMember();
        if (defaultMember != null) {
            Access i = hierarchyAccess.getAccess(defaultMember);
            if (i != Access.NONE) {
                return defaultMember;
            }
        }
        final List<RolapMember> rootMembers = getRootMembers();
        final RolapMember rootMember = rootMembers.get(0);
        if (rootMembers.size() == 1) {
            return rootMembers.get(0);
        } else {
            return new MultiCardinalityDefaultMember(rootMember);
        }
    }

    /**
     * Special subclass of {@link DelegatingRolapMember}, needed because
     * {@link Evaluator} doesn't support multi-cardinality default members.
     * {@link RolapHierarchy.LimitedRollupSubstitutingMemberReader}.substitute()
     * looks for this class and substitutes.
     *
     * <p>FIXME: If/when we refactor evaluator to support
     * multi-cardinality default members, we can remove this.
     */
    static class MultiCardinalityDefaultMember extends DelegatingRolapMember {
        protected MultiCardinalityDefaultMember(RolapMember member) {
            super(member);
        }
    }

    public RolapMember getMemberParent(RolapMember member) {
        RolapMember parentMember = member.getParentMember();
        // Skip over hidden parents.
        while (parentMember != null && parentMember.isHidden()) {
            parentMember = parentMember.getParentMember();
        }
        // Skip over non-accessible parents.
        if (parentMember != null) {
            if (hierarchyAccess.getAccess(parentMember) == Access.NONE) {
                return null;
            }
        }
        return parentMember;
    }
}

// End RestrictedMemberReader.java
