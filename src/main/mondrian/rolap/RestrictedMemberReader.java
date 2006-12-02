/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2003-2006 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Feb 26, 2003
*/
package mondrian.rolap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import mondrian.olap.*;
import mondrian.rolap.sql.TupleConstraint;
import mondrian.rolap.sql.MemberChildrenConstraint;

/**
 * A <code>RestrictedMemberReader</code> reads only the members of a hierarchy
 * allowed by a role's access profile.
 *
 * @author jhyde
 * @since Feb 26, 2003
 * @version $Id$
 */
class RestrictedMemberReader extends DelegatingMemberReader {

    private final Role.HierarchyAccess hierarchyAccess;
    private final boolean ragged;
    private final SqlConstraintFactory sqlConstraintFactory = SqlConstraintFactory.instance();

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
     * @pre role.getAccessDetails(memberReader.getHierarchy()) != null ||
     *   memberReader.getHierarchy().isRagged()
     */
    RestrictedMemberReader(MemberReader memberReader, Role role) {
        super(memberReader);
        RolapHierarchy hierarchy = memberReader.getHierarchy();
        hierarchyAccess = role.getAccessDetails(hierarchy);
        ragged = hierarchy.isRagged();
        Util.assertPrecondition(hierarchyAccess != null || ragged,
                "role.getAccessDetails(memberReader.getHierarchy()) != " +
                "null || hierarchy.isRagged()");
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
                    memberReader.getMemberChildren(member, grandChildren, constraint);
                    fullChildren.addAll(i, grandChildren);
                    // Step back to before the first child we just inserted,
                    // and go through the loop again.
                    --i;
                    continue;
                }
            }
            // Filter out children which are invisible because of
            // access-control.
            if (hierarchyAccess != null) {
                final int access = hierarchyAccess.getAccess(member);
                if (access == Access.NONE) {
                    continue;
                }
            }
            children.add(member);
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
            final int access = hierarchyAccess.getAccess(member);
            return access != Access.NONE;
        }
        return true;
    }

    public void getMemberChildren(List<RolapMember> parentMembers, List<RolapMember> children) {
        MemberChildrenConstraint constraint = sqlConstraintFactory.getMemberChildrenConstraint(null);
        getMemberChildren(parentMembers, children, constraint);
    }

    public synchronized void getMemberChildren(List<RolapMember> parentMembers, List<RolapMember> children, MemberChildrenConstraint constraint) {
//        for (Iterator i = parentMembers.iterator(); i.hasNext();) {
//            RolapMember parentMember = (RolapMember) i.next();
//            getMemberChildren(parentMember, children, constraint);
//        }
        List<RolapMember> fullChildren = new ArrayList<RolapMember>();
        super.getMemberChildren(parentMembers, fullChildren, constraint);
        processMemberChildren(fullChildren, children, constraint);
    }

    public List<RolapMember> getMembersInLevel(RolapLevel level,
                                  int startOrdinal,
                                  int endOrdinal) {
        TupleConstraint constraint = sqlConstraintFactory.getLevelMembersConstraint(null);
        return getMembersInLevel(level, startOrdinal, endOrdinal, constraint);
    }

    public List<RolapMember> getMembersInLevel(RolapLevel level,
                                int startOrdinal,
                                int endOrdinal,
                                TupleConstraint constraint) {
        if (hierarchyAccess != null) {
            final int depth = level.getDepth();
            if (hierarchyAccess.getTopLevel() != null &&
                    depth < hierarchyAccess.getTopLevel().getDepth()) {
                return Collections.emptyList();
            }
            if (hierarchyAccess.getBottomLevel() != null &&
                    depth > hierarchyAccess.getBottomLevel().getDepth()) {
                return Collections.emptyList();
            }
        }
        final List<RolapMember> membersInLevel = super.getMembersInLevel(level,
                startOrdinal, endOrdinal, constraint);
        List<RolapMember> filteredMembers = new ArrayList<RolapMember>();
        filterMembers(membersInLevel, filteredMembers);
        return filteredMembers;
    }

}

// End RestrictedMemberReader.java
