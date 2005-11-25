/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2003-2005 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Feb 24, 2003
*/
package mondrian.rolap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import mondrian.olap.Access;
import mondrian.olap.Cube;
import mondrian.olap.Evaluator;
import mondrian.olap.Exp;
import mondrian.olap.FunDef;
import mondrian.olap.Hierarchy;
import mondrian.olap.Level;
import mondrian.olap.Member;
import mondrian.olap.NamedSet;
import mondrian.olap.NativeEvaluator;
import mondrian.olap.OlapElement;
import mondrian.olap.Role;
import mondrian.olap.SchemaReader;
import mondrian.olap.Util;
import mondrian.rolap.sql.TupleConstraint;
import mondrian.rolap.sql.MemberChildrenConstraint;

import org.apache.log4j.Logger;

/**
 * A <code>RolapSchemaReader</code> allows you to read schema objects while
 * observing the access-control profile specified by a given role.
 *
 * @author jhyde
 * @since Feb 24, 2003
 * @version $Id$
 **/
public abstract class RolapSchemaReader implements SchemaReader {
    private final Role role;
    private final Map hierarchyReaders;
    private final RolapSchema schema;
    private final SqlConstraintFactory sqlConstraintFactory = SqlConstraintFactory.instance();
    private static final Logger LOGGER = Logger.getLogger(RolapSchemaReader.class);

    RolapSchemaReader(Role role, RolapSchema schema) {
        assert role != null : "precondition: role != null";

        this.role = role;
        this.schema = schema;
        this.hierarchyReaders = new HashMap();
    }

    public Role getRole() {
        return role;
    }

    public Member[] getHierarchyRootMembers(Hierarchy hierarchy) {
        final Role.HierarchyAccess hierarchyAccess = role.getAccessDetails(hierarchy);
        Level firstLevel;
        if (hierarchyAccess == null) {
            firstLevel = hierarchy.getLevels()[0];
        } else {
            firstLevel = hierarchyAccess.getTopLevel();
            if (firstLevel == null) {
                firstLevel = hierarchy.getLevels()[0];
            }
        }
        return getLevelMembers(firstLevel);
    }

    synchronized MemberReader getMemberReader(Hierarchy hierarchy) {
        MemberReader memberReader = (MemberReader) hierarchyReaders.get(hierarchy);
        if (memberReader == null) {
            memberReader = ((RolapHierarchy) hierarchy).getMemberReader(role);
            hierarchyReaders.put(hierarchy, memberReader);
        }
        return memberReader;
    }

    public void getMemberRange(Level level, Member startMember, Member endMember, List list) {
        getMemberReader(level.getHierarchy()).getMemberRange(
                (RolapLevel) level, (RolapMember) startMember,
                (RolapMember) endMember, list);
    }

    public int compareMembersHierarchically(Member m1, Member m2) {
        final RolapHierarchy hierarchy = (RolapHierarchy) m1.getHierarchy();
        Util.assertPrecondition(hierarchy == m2.getHierarchy());
        return getMemberReader(hierarchy).compare((RolapMember) m1, (RolapMember) m2, true);
    }

    public Member getMemberParent(Member member) {
        Member parentMember = member.getParentMember();
        // Skip over hidden parents.
        while (parentMember != null && parentMember.isHidden()) {
            parentMember = parentMember.getParentMember();
        }
        // Skip over non-accessible parents.
        if (parentMember != null) {
            final Role.HierarchyAccess hierarchyAccess =
                    role.getAccessDetails(member.getHierarchy());
            if (hierarchyAccess != null &&
                    hierarchyAccess.getAccess(parentMember) == Access.NONE) {
                return null;
            }
        }
        return parentMember;
    }

    public int getMemberDepth(Member member) {
        final Role.HierarchyAccess hierarchyAccess = role.getAccessDetails(member.getHierarchy());
        if (hierarchyAccess != null) {
            int memberDepth = member.getLevel().getDepth();
            final Level topLevel = hierarchyAccess.getTopLevel();
            if (topLevel != null) {
                memberDepth -= topLevel.getDepth();
            }
            return memberDepth;
        } else if (((RolapLevel) member.getLevel()).getParentExp() != null) {
            // For members of parent-child hierarchy, members in the same level may have
            // different depths.
            int depth = 0;
            for (Member m = member.getParentMember(); m != null; m = m.getParentMember()) {
                depth++;
            }
            return depth;
        } else {
            return member.getLevel().getDepth();
        }
    }


    public Member[] getMemberChildren(Member member) {
        return getMemberChildren(member, null);
    }

    public Member[] getMemberChildren(Member member, Evaluator context) {
        MemberChildrenConstraint constraint = sqlConstraintFactory.getMemberChildrenConstraint(context);
        return internalGetMemberChildren(member, constraint);
    }
    private Member[] internalGetMemberChildren(Member member, MemberChildrenConstraint constraint) {
        List children = new ArrayList();
        final Hierarchy hierarchy = member.getHierarchy();
        final MemberReader memberReader = getMemberReader(hierarchy);
        memberReader.getMemberChildren((RolapMember) member, children, constraint);
        return RolapUtil.toArray(children);
    }

    /**
     * check, whether members children are cached, and
     * if yes - return children count
     * if no  - return -1
     */
    public int getChildrenCountFromCache(Member member) {
        final Hierarchy hierarchy = member.getHierarchy();
        final MemberReader memberReader = getMemberReader(hierarchy);
        if( !(memberReader instanceof MemberCache)) {
            return -1;
        }
        List list = ((MemberCache)memberReader).getChildrenFromCache((RolapMember)member, null);
        if (list == null)
          return -1;
        return list.size();
    }

    /**
     * check, whether member reader is caching
     * if yes - return level member count
     * if no  - return -1
     */
    public int getLevelCardinalityFromCache(Level level) {
        final Hierarchy hierarchy = level.getHierarchy();
        final MemberReader memberReader = getMemberReader(hierarchy);
        if( !(memberReader instanceof MemberCache)) {
            return -1;
        }
        List list = ((MemberCache)memberReader).getLevelMembersFromCache((RolapLevel)level, null);
        if (list == null)
          return -1;
        return list.size();
    }

    public Member[] getMemberChildren(Member[] members) {
        return getMemberChildren(members, null);
    }

    public Member[] getMemberChildren(Member[] members, Evaluator context) {
        if (members.length == 0) {
            return RolapUtil.emptyMemberArray;
        } else {
            MemberChildrenConstraint constraint = sqlConstraintFactory.getMemberChildrenConstraint(context);
            final Hierarchy hierarchy = members[0].getHierarchy();
            final MemberReader memberReader = getMemberReader(hierarchy);
            List children = new ArrayList();
            memberReader.getMemberChildren(Arrays.asList(members), children, constraint);
            return RolapUtil.toArray(children);
        }
    }

    public abstract Cube getCube();

    public OlapElement getElementChild(OlapElement parent, String name) {
        return parent.lookupChild(this, name);
    }

    public Member getMemberByUniqueName(
            String[] uniqueNameParts, boolean failIfNotFound) {
        // In general, this schema reader doesn't have a cube, so we cannot
        // start looking up members.
        return null;
    }

    public OlapElement lookupCompound(
            OlapElement parent,
            String[] names,
            boolean failIfNotFound,
            int category) {
        return Util.lookupCompound(
                this, parent, names, failIfNotFound, category);
    }

    public Member lookupMemberChildByName(Member parent, String childName) {
        LOGGER.debug("looking for child \"" + childName + "\" of " + parent);
        try {
            MemberChildrenConstraint constraint =
                sqlConstraintFactory.getChildByNameConstraint((RolapMember)parent, childName);
            Member[] children = internalGetMemberChildren(parent, constraint);
            for (int i = 0; i < children.length; i++){
                final Member child = children[i];
                if (Util.equalName(child.getName(), childName)) {
                    return child;
                }
            }
        } catch (NumberFormatException e) {
            // this was thrown in SqlQuery#quote(boolean numeric, Object value). This happens when
            // Mondrian searches for unqualified Olap Elements like [Month], because it tries to look up
            // a member with that name in all dimensions. Then it generates for example
            // "select .. from time where year = Month" which will result in a NFE because
            // "Month" can not be parsed as a number. The real bug is probably, that Mondrian
            // looks at members at all.
            //
            // @see RolapCube#lookupChild()
            LOGGER.debug("NumberFormatException in lookupMemberChildByName for parent = \"" + parent + "\", childName=\"" + childName + "\", exception: " + e.getMessage());
        }
        return null;
    }

    public Member getCalculatedMember(String[] nameParts) {
        // There are no calculated members defined against a schema.
        return null;
    }

    public NamedSet getNamedSet(String[] nameParts) {
        if (nameParts.length != 1) {
            return null;
        }
        final String name = nameParts[0];
        return schema.getNamedSet(name);
    }

    public Member getLeadMember(Member member, int n) {
        return getMemberReader(member.getHierarchy()).getLeadMember((RolapMember) member, n);
    }

    public Member[] getLevelMembers(Level level) {
        return getLevelMembers(level, null);
    }

    public Member[] getLevelMembers(Level level, Evaluator context) {
        TupleConstraint constraint = sqlConstraintFactory.getLevelMembersConstraint(context);
        final MemberReader memberReader = getMemberReader(level.getHierarchy());
        final List membersInLevel = memberReader.getMembersInLevel(
                    (RolapLevel) level, 0, Integer.MAX_VALUE, constraint);
        return RolapUtil.toArray(membersInLevel);
    }

    public Level[] getHierarchyLevels(Hierarchy hierarchy) {
        Util.assertPrecondition(hierarchy != null, "hierarchy != null");
        final Role.HierarchyAccess hierarchyAccess = role.getAccessDetails(hierarchy);
        final Level[] levels = hierarchy.getLevels();
        if (hierarchyAccess == null) {
            return levels;
        }
        Level topLevel = hierarchyAccess.getTopLevel();
        Level bottomLevel = hierarchyAccess.getBottomLevel();
        if ((topLevel == null) && (bottomLevel == null)) {
            return levels;
        }
        if (topLevel == null) {
            topLevel = levels[0];
        }
        if (bottomLevel == null) {
            bottomLevel = levels[levels.length - 1];
        }
        final int levelCount = bottomLevel.getDepth() - topLevel.getDepth() + 1;
        Level[] restrictedLevels = new Level[levelCount];
        System.arraycopy(levels, topLevel.getDepth(), restrictedLevels, 0, levelCount);
        Util.assertPostcondition(restrictedLevels.length >= 1, "return.length >= 1");
        return restrictedLevels;
    }

    public Member getHierarchyDefaultMember(Hierarchy hierarchy) {
        Util.assertPrecondition(hierarchy != null, "hierarchy != null");
        // if we have no access to this hierarchy at all, we return
        // the unrestricted default member (e.g. All member)
        if (role.getAccess(hierarchy) == Access.NONE) {
            return hierarchy.getDefaultMember();
        }
        Member[] rootMembers = this.getHierarchyRootMembers(hierarchy);
        return (rootMembers.length > 0)
            ? rootMembers[0]
            : null;
    }

    public boolean isDrillable(Member member) {
        final RolapLevel level = (RolapLevel) member.getLevel();
        if (level.getParentExp() != null) {
            // This is a parent-child level, so its children, if any, come from
            // the same level.
            //
            // todo: More efficient implementation
            return getMemberChildren(member).length > 0;
        } else {
            // This is a regular level. It has children iff there is a lower
            // level.
            final Level childLevel = level.getChildLevel();
            return (childLevel != null) &&
                    (role.getAccess(childLevel) != Access.NONE);
        }
    }

    public boolean isVisible(Member member) {
        return !member.isHidden() && role.canAccess(member);
    }

    public Cube[] getCubes() {
        Cube[] cubes = schema.getCubes();
        List visibleCubes = new ArrayList(cubes.length);

        for (int idx = 0; idx < cubes.length; idx++) {
            if (role.canAccess(cubes[idx])) {
                visibleCubes.add(cubes[idx]);
            }
        }

        Cube[] result = new Cube[visibleCubes.size()];

        visibleCubes.toArray(result);

        return result;
    }

    public List getCalculatedMembers(Hierarchy hierarchy) {
        return Collections.EMPTY_LIST;
    }

    public List getCalculatedMembers(Level level) {
        return Collections.EMPTY_LIST;
    }

    public List getCalculatedMembers() {
        return Collections.EMPTY_LIST;
    }

    public NativeEvaluator getNativeSetEvaluator(FunDef fun, Evaluator evaluator, Exp[] args) {
        RolapEvaluator revaluator = (RolapEvaluator) evaluator;
        return schema.getNativeRegistry().createEvaluator(revaluator, fun, args);
    }

    DataSource getDataSource() {
        return schema.getInternalConnection().getDataSource();
    }

    RolapSchema getSchema() {
        return schema;
    }
}

// End RolapSchemaReader.java
