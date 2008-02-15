/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2003-2007 Julian Hyde
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

import mondrian.olap.*;
import mondrian.olap.type.StringType;
import mondrian.rolap.sql.TupleConstraint;
import mondrian.rolap.sql.MemberChildrenConstraint;
import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;
import mondrian.calc.DummyExp;
import mondrian.calc.impl.AbstractCalc;
import mondrian.calc.impl.GenericCalc;

import org.apache.log4j.Logger;
import org.eigenbase.util.property.Property;

/**
 * A <code>RolapSchemaReader</code> allows you to read schema objects while
 * observing the access-control profile specified by a given role.
 *
 * @author jhyde
 * @since Feb 24, 2003
 * @version $Id$
 */
public abstract class RolapSchemaReader
    implements SchemaReader, RolapNativeSet.SchemaReaderWithMemberReaderAvailable {
    private final Role role;
    private final Map<Hierarchy, MemberReader> hierarchyReaders =
        new HashMap<Hierarchy, MemberReader>();
    private final RolapSchema schema;
    private final SqlConstraintFactory sqlConstraintFactory =
        SqlConstraintFactory.instance();
    private static final Logger LOGGER =
        Logger.getLogger(RolapSchemaReader.class);

    RolapSchemaReader(Role role, RolapSchema schema) {
        assert role != null : "precondition: role != null";
        this.role = role;
        this.schema = schema;
    }

    public Role getRole() {
        return role;
    }

    public Member[] getHierarchyRootMembers(Hierarchy hierarchy)
    {
        final Role.HierarchyAccess hierarchyAccess =
            role.getAccessDetails(hierarchy);
        final Level[] levels = hierarchy.getLevels();
        final Level firstLevel;
        if (hierarchyAccess == null) {
            firstLevel = levels[0];
        } else {
            firstLevel = levels[hierarchyAccess.getTopLevelDepth()];
        }
        return getLevelMembers(firstLevel, true);
    }

    public synchronized MemberReader getMemberReader(Hierarchy hierarchy) {
        MemberReader memberReader = hierarchyReaders.get(hierarchy);
        if (memberReader == null) {
            memberReader = ((RolapHierarchy) hierarchy).createMemberReader(role);
            hierarchyReaders.put(hierarchy, memberReader);
        }
        return memberReader;
    }

    public Member substitute(Member member) {
        final MemberReader memberReader =
            getMemberReader(member.getHierarchy());
        return memberReader.substitute((RolapMember) member);
    }

    public void getMemberRange(
        Level level, Member startMember, Member endMember, List<Member> list)
    {
        getMemberReader(level.getHierarchy()).getMemberRange(
                (RolapLevel) level, (RolapMember) startMember,
                (RolapMember) endMember, (List) list);
    }

    public int compareMembersHierarchically(Member m1, Member m2) {
        RolapMember member1 = (RolapMember) m1;
        RolapMember member2 = (RolapMember) m2;
        final RolapHierarchy hierarchy = member1.getHierarchy();
        Util.assertPrecondition(hierarchy == m2.getHierarchy());
        return getMemberReader(hierarchy).compare(member1, member2, true);
    }

    public Member getMemberParent(Member member) {
        return getMemberReader(member.getHierarchy()).getMemberParent(
            (RolapMember) member);
    }

    public int getMemberDepth(Member member) {
        final Role.HierarchyAccess hierarchyAccess = role.getAccessDetails(member.getHierarchy());
        if (hierarchyAccess != null) {
            final int memberDepth = member.getLevel().getDepth();
            final int topLevelDepth = hierarchyAccess.getTopLevelDepth();
            return memberDepth - topLevelDepth;
        } else if (((RolapLevel) member.getLevel()).isParentChild()) {
            // For members of parent-child hierarchy, members in the same level
            // may have different depths.
            int depth = 0;
            for (Member m = member.getParentMember();
                 m != null;
                 m = m.getParentMember())
            {
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
        MemberChildrenConstraint constraint =
                sqlConstraintFactory.getMemberChildrenConstraint(context);
        List<RolapMember> memberList =
            internalGetMemberChildren(member, constraint);
        return memberList.toArray(new Member[memberList.size()]);
    }

    private List<RolapMember> internalGetMemberChildren(
            Member member, MemberChildrenConstraint constraint) {
        List<RolapMember> children = new ArrayList<RolapMember>();
        final Hierarchy hierarchy = member.getHierarchy();
        final MemberReader memberReader = getMemberReader(hierarchy);
        memberReader.getMemberChildren(
                (RolapMember) member, children, constraint);
        return children;
    }

    /**
     * check, whether members children are cached, and
     * if yes - return children count
     * if no  - return -1
     */
    public int getChildrenCountFromCache(Member member) {
        final Hierarchy hierarchy = member.getHierarchy();
        final MemberReader memberReader = getMemberReader(hierarchy);
        if (memberReader instanceof 
                RolapCubeHierarchy.RolapCubeHierarchyMemberReader) {
            List list = 
                ((RolapCubeHierarchy.RolapCubeHierarchyMemberReader)memberReader)
                    .getRolapCubeMemberCacheHelper()
                        .getChildrenFromCache((RolapMember)member, null);
            if (list == null) {
              return -1;
            }
            return list.size(); 
        }
        
        if (memberReader instanceof SmartMemberReader) {
            List list = ((SmartMemberReader)memberReader).getMemberCache()
                            .getChildrenFromCache((RolapMember)member, null);
            if (list == null) {
              return -1;
            }
            return list.size();
        }
        if( !(memberReader instanceof MemberCache)) {
            return -1;
        }
        List list = ((MemberCache)memberReader)
                        .getChildrenFromCache((RolapMember)member, null);
        if (list == null) {
          return -1;
        }
        return list.size();
    }

    /**
     * Returns number of members in a level,
     * if the information can be retrieved from cache.
     * Otherwise {@link Integer#MIN_VALUE}.
     *
     * @param level Level
     * @return number of members in level
     */
    private int getLevelCardinalityFromCache(Level level) {
        final Hierarchy hierarchy = level.getHierarchy();
        final MemberReader memberReader = getMemberReader(hierarchy);
        if (memberReader instanceof 
                RolapCubeHierarchy.RolapCubeHierarchyMemberReader) {
            List list = 
                ((RolapCubeHierarchy.RolapCubeHierarchyMemberReader)memberReader)
                    .getRolapCubeMemberCacheHelper()
                        .getLevelMembersFromCache((RolapLevel) level, null);
            if (list == null) {
                return Integer.MIN_VALUE;
            }
            return list.size();
        }
        
        if (memberReader instanceof SmartMemberReader) {
            List list = ((SmartMemberReader)memberReader).getMemberCache()
                            .getLevelMembersFromCache((RolapLevel) level, null);
                if (list == null) {
                    return Integer.MIN_VALUE;
                }
                return list.size();
        }
        
        if( !(memberReader instanceof MemberCache)) {
            return Integer.MIN_VALUE;
        }
        List list = ((MemberCache)memberReader).getLevelMembersFromCache(
            (RolapLevel) level, null);
        if (list == null) {
            return Integer.MIN_VALUE;
        }
        return list.size();
    }

    public int getLevelCardinality(
        Level level,
        boolean approximate,
        boolean materialize)
    {
        if (!this.role.canAccess(level)) {
            return 1;
        }

        int rowCount = Integer.MIN_VALUE;
        if (approximate) {
            // See if the schema has an approximation.
            rowCount = level.getApproxRowCount();
        }

        if (rowCount == Integer.MIN_VALUE) {
            // See if the precise row count is available in cache.
            rowCount = getLevelCardinalityFromCache(level);
        }

        if (rowCount == Integer.MIN_VALUE) {
            if (materialize) {
                // Either the approximate row count hasn't been set,
                // or they want the precise row count.
                final MemberReader memberReader =
                    getMemberReader(level.getHierarchy());
                rowCount =
                    memberReader.getLevelMemberCount((RolapLevel) level);
                // Cache it for future.
                ((RolapLevel) level).setApproxRowCount(rowCount);
            }
        }
        return rowCount;
    }

    public Member[] getMemberChildren(Member[] members) {
        return getMemberChildren(members, null);
    }

    public Member[] getMemberChildren(Member[] members, Evaluator context) {
        if (members.length == 0) {
            return RolapUtil.emptyMemberArray;
        } else {
            MemberChildrenConstraint constraint =
                    sqlConstraintFactory.getMemberChildrenConstraint(context);
            final Hierarchy hierarchy = members[0].getHierarchy();
            final MemberReader memberReader = getMemberReader(hierarchy);
            List<RolapMember> children = new ArrayList<RolapMember>();
            memberReader.getMemberChildren(
                (List) Arrays.asList(members),
                children,
                constraint);
            return RolapUtil.toArray(children);
        }
    }

    public abstract Cube getCube();

    public OlapElement getElementChild(OlapElement parent, Id.Segment name) {
        return getElementChild(parent, name, MatchType.EXACT);
    }

    public OlapElement getElementChild(
        OlapElement parent, Id.Segment name, MatchType matchType)
    {
        return parent.lookupChild(this, name, matchType);
    }

    public final Member getMemberByUniqueName(
        List<Id.Segment> uniqueNameParts,
        boolean failIfNotFound)
    {
        return getMemberByUniqueName(
            uniqueNameParts, failIfNotFound, MatchType.EXACT);
    }

    public Member getMemberByUniqueName(
        List<Id.Segment> uniqueNameParts,
        boolean failIfNotFound,
        MatchType matchType)
    {
        // In general, this schema reader doesn't have a cube, so we cannot
        // start looking up members.
        return null;
    }

    public OlapElement lookupCompound(
        OlapElement parent,
        List<Id.Segment> names,
        boolean failIfNotFound,
        int category)
    {
        return lookupCompound(
            parent, names, failIfNotFound, category, MatchType.EXACT);
    }

    public OlapElement lookupCompound(
        OlapElement parent,
        List<Id.Segment> names,
        boolean failIfNotFound,
        int category,
        MatchType matchType)
    {
        return Util.lookupCompound(
            this, parent, names, failIfNotFound, category, matchType);
    }

    public Member lookupMemberChildByName(Member parent, Id.Segment childName)
    {
        return lookupMemberChildByName(parent, childName, MatchType.EXACT);
    }

    public Member lookupMemberChildByName(
        Member parent, Id.Segment childName, MatchType matchType)
    {
        LOGGER.debug("looking for child \"" + childName + "\" of " + parent);
        assert !(parent instanceof RolapHierarchy.LimitedRollupMember);
        if (parent instanceof RolapHierarchy.LimitedRollupMember) {
            Util.deprecated("removeme");
            RolapHierarchy.LimitedRollupMember limitedRollupMember =
                (RolapHierarchy.LimitedRollupMember) parent;
            parent = limitedRollupMember.member;
        }
        try {
            MemberChildrenConstraint constraint;
            if (matchType == MatchType.EXACT) {
                constraint = sqlConstraintFactory.getChildByNameConstraint(
                    (RolapMember) parent, childName);
            } else {
                constraint =
                    sqlConstraintFactory.getMemberChildrenConstraint(null);
            }
            List<RolapMember> children =
                internalGetMemberChildren(parent, constraint);
            if (children.size() > 0) {
                return
                    RolapUtil.findBestMemberMatch(
                        children,
                        (RolapMember) parent,
                        children.get(0).getLevel(),
                        childName,
                        matchType,
                        true);
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

    public Member getCalculatedMember(List<Id.Segment> nameParts) {
        // There are no calculated members defined against a schema.
        return null;
    }

    public NamedSet getNamedSet(List<Id.Segment> nameParts) {
        if (nameParts.size() != 1) {
            return null;
        }
        final String name = nameParts.get(0).name;
        return schema.getNamedSet(name);
    }

    public Member getLeadMember(Member member, int n) {
        final MemberReader memberReader = getMemberReader(member.getHierarchy());
        return memberReader.getLeadMember((RolapMember) member, n);
    }

    public Member[] getLevelMembers(Level level, boolean includeCalculated) {
        return getLevelMembers(level, null);
    }

    public Member[] getLevelMembers(Level level, Evaluator context) {
        TupleConstraint constraint =
                sqlConstraintFactory.getLevelMembersConstraint(
                    context,
                    new Level [] { level });
        final MemberReader memberReader = getMemberReader(level.getHierarchy());
        final List<RolapMember> membersInLevel =
            memberReader.getMembersInLevel(
                (RolapLevel) level, 0, Integer.MAX_VALUE, constraint);
        return RolapUtil.toArray(membersInLevel);
    }

    public Level[] getHierarchyLevels(Hierarchy hierarchy) {
        assert hierarchy != null;
        final Role.HierarchyAccess hierarchyAccess = role.getAccessDetails(hierarchy);
        final Level[] levels = hierarchy.getLevels();
        if (hierarchyAccess == null) {
            return levels;
        }
        Level topLevel = levels[hierarchyAccess.getTopLevelDepth()];
        Level bottomLevel = levels[hierarchyAccess.getBottomLevelDepth()];
        final int levelCount = bottomLevel.getDepth() - topLevel.getDepth() + 1;
        Level[] restrictedLevels = new Level[levelCount];
        System.arraycopy(levels, topLevel.getDepth(), restrictedLevels, 0, levelCount);
        Util.assertPostcondition(restrictedLevels.length >= 1, "return.length >= 1");
        return restrictedLevels;
    }

    public Member getHierarchyDefaultMember(Hierarchy hierarchy) {
        assert hierarchy != null;
        // If the whole hierarchy is inaccessible, return the intrinsic default
        // member. This is important to construct a evaluator.
        if (role.getAccess(hierarchy) == Access.NONE) {
            return hierarchy.getDefaultMember();
        }
        return getMemberReader(hierarchy).getDefaultMember();
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
        List<RolapCube> cubes = schema.getCubeList();
        List<Cube> visibleCubes = new ArrayList<Cube>(cubes.size());

        for (Cube cube : cubes) {
            if (role.canAccess(cube)) {
                visibleCubes.add(cube);
            }
        }

        return visibleCubes.toArray(new Cube[visibleCubes.size()]);
    }

    public List<Member> getCalculatedMembers(Hierarchy hierarchy) {
        return Collections.emptyList();
    }

    public List<Member> getCalculatedMembers(Level level) {
        return Collections.emptyList();
    }

    public List<Member> getCalculatedMembers() {
        return Collections.emptyList();
    }

    public NativeEvaluator getNativeSetEvaluator(
            FunDef fun, Exp[] args, Evaluator evaluator, Calc calc) {
        RolapEvaluator revaluator = (RolapEvaluator)
                AbstractCalc.simplifyEvaluator(calc, evaluator);
        return schema.getNativeRegistry().createEvaluator(revaluator, fun, args);
    }

    public Parameter getParameter(String name) {
        // Scan through schema parameters.
        for (RolapSchemaParameter parameter : schema.parameterList) {
            if (Util.equalName(parameter.getName(), name)) {
                return parameter;
            }
        }

        // Scan through mondrian and system properties.
        List<Property> propertyList = MondrianProperties.instance().getPropertyList();
        for (Property property : propertyList) {
            if (property.getPath().equals(name)) {
                return new SystemPropertyParameter(name, false);
            }
        }
        if (System.getProperty(name) != null) {
            return new SystemPropertyParameter(name, true);
        }

        return null;
    }

    public DataSource getDataSource() {
        return schema.getInternalConnection().getDataSource();
    }

    RolapSchema getSchema() {
        return schema;
    }

    /**
     * Implementation of {@link Parameter} which is sourced from system
     * propertes (see {@link System#getProperties()} or mondrian properties
     * (see {@link MondrianProperties}.
     *
     * <p>The name of the property is the same as the key into the
     * {@link java.util.Properties} object; for example "java.version" or
     * "mondrian.trace.level".
     */
    private static class SystemPropertyParameter
        extends ParameterImpl
    {
        /**
         * true if source is a system property;
         * false if source is a mondrian property.
         */
        private final boolean system;
        /**
         * Definition of mondrian property, or null if system property.
         */
        private final Property propertyDefinition;

        public SystemPropertyParameter(String name, boolean system) {
            super(name,
                Literal.nullValue,
                "System property '" + name + "'",
                new StringType());
            this.system = system;
            this.propertyDefinition =
                system ? null :
                MondrianProperties.instance().getPropertyDefinition(name);
        }

        public Scope getScope() {
            return Scope.System;
        }

        public boolean isModifiable() {
            return false;
        }

        public Calc compile(ExpCompiler compiler) {
            return new GenericCalc(new DummyExp(getType())) {
                public Calc[] getCalcs() {
                    return new Calc[0];
                }

                public Object evaluate(Evaluator evaluator) {
                    if (system) {
                        final String name = SystemPropertyParameter.this.getName();
                        return System.getProperty(name);
                    } else {
                        return propertyDefinition.stringValue();
                    }
                }
            };
        }
    }
}

// End RolapSchemaReader.java
