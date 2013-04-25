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

import mondrian.calc.*;
import mondrian.calc.impl.AbstractCalc;
import mondrian.calc.impl.GenericCalc;
import mondrian.olap.*;
import mondrian.olap.type.StringType;
import mondrian.rolap.sql.MemberChildrenConstraint;
import mondrian.rolap.sql.TupleConstraint;

import org.apache.log4j.Logger;

import org.eigenbase.util.property.Property;

import org.olap4j.mdx.IdentifierSegment;

import java.util.*;
import javax.sql.DataSource;

/**
 * A <code>RolapSchemaReader</code> allows you to read schema objects while
 * observing the access-control profile specified by a given role.
 *
 * @author jhyde
 * @since Feb 24, 2003
 */
public class RolapSchemaReader
    implements SchemaReader,
        RolapNativeSet.SchemaReaderWithMemberReaderAvailable,
        NameResolver.Namespace
{
    protected final Role role;
    private final Map<Hierarchy, MemberReader> hierarchyReaders =
        new HashMap<Hierarchy, MemberReader>();
    protected final RolapSchema schema;
    private final SqlConstraintFactory sqlConstraintFactory =
        SqlConstraintFactory.instance();
    private static final Logger LOGGER =
        Logger.getLogger(RolapSchemaReader.class);

    /**
     * Creates a RolapSchemaReader.
     *
     * @param role Role for access control, must not be null
     * @param schema Schema
     */
    RolapSchemaReader(Role role, RolapSchema schema) {
        assert role != null : "precondition: role != null";
        assert schema != null;
        this.role = role;
        this.schema = schema;
    }

    public Role getRole() {
        return role;
    }

    public List<Member> getHierarchyRootMembers(Hierarchy hierarchy) {
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
            memberReader =
                ((RolapHierarchy) hierarchy).createMemberReader(role);
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
            (RolapMember) endMember, Util.<RolapMember>cast(list));
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
        final Role.HierarchyAccess hierarchyAccess =
            role.getAccessDetails(member.getHierarchy());
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


    public List<Member> getMemberChildren(Member member) {
        return getMemberChildren(member, null);
    }

    public List<Member> getMemberChildren(Member member, Evaluator context) {
        MemberChildrenConstraint constraint =
            sqlConstraintFactory.getMemberChildrenConstraint(context);
        List<RolapMember> memberList =
            internalGetMemberChildren(member, constraint);
        return Util.cast(memberList);
    }

    /**
     * Helper for getMemberChildren.
     *
     * @param member Member
     * @param constraint Constraint
     * @return List of children
     */
    private List<RolapMember> internalGetMemberChildren(
        Member member, MemberChildrenConstraint constraint)
    {
        List<RolapMember> children = new ArrayList<RolapMember>();
        final Hierarchy hierarchy = member.getHierarchy();
        final MemberReader memberReader = getMemberReader(hierarchy);
        memberReader.getMemberChildren(
            (RolapMember) member, children, constraint);
        return children;
    }

    public void getParentChildContributingChildren(
        Member dataMember,
        Hierarchy hierarchy,
        List<Member> list)
    {
        final List<RolapMember> rolapMemberList = Util.cast(list);
        list.add(dataMember);
        ((RolapHierarchy) hierarchy).getMemberReader().getMemberChildren(
            (RolapMember) dataMember, rolapMemberList);
    }

    public int getChildrenCountFromCache(Member member) {
        final Hierarchy hierarchy = member.getHierarchy();
        final MemberReader memberReader = getMemberReader(hierarchy);
        if (memberReader instanceof
            RolapCubeHierarchy.RolapCubeHierarchyMemberReader)
        {
            List list =
                ((RolapCubeHierarchy.RolapCubeHierarchyMemberReader)
                 memberReader)
                    .getRolapCubeMemberCacheHelper()
                    .getChildrenFromCache((RolapMember) member, null);
            if (list == null) {
                return -1;
            }
            return list.size();
        }

        if (memberReader instanceof SmartMemberReader) {
            List list = ((SmartMemberReader) memberReader).getMemberCache()
                .getChildrenFromCache((RolapMember) member, null);
            if (list == null) {
                return -1;
            }
            return list.size();
        }
        if (!(memberReader instanceof MemberCache)) {
            return -1;
        }
        List list = ((MemberCache) memberReader)
            .getChildrenFromCache((RolapMember) member, null);
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
            RolapCubeHierarchy.RolapCubeHierarchyMemberReader)
        {
            final MemberCacheHelper cache =
                ((RolapCubeHierarchy.RolapCubeHierarchyMemberReader)
                    memberReader).getRolapCubeMemberCacheHelper();
            if (cache == null) {
                return Integer.MIN_VALUE;
            }
            final List<RolapMember> list =
                cache.getLevelMembersFromCache(
                    (RolapLevel) level, null);
            if (list == null) {
                return Integer.MIN_VALUE;
            }
            return list.size();
        }

        if (memberReader instanceof SmartMemberReader) {
            List<RolapMember> list =
                ((SmartMemberReader) memberReader)
                    .getMemberCache()
                    .getLevelMembersFromCache(
                        (RolapLevel) level, null);
            if (list == null) {
                return Integer.MIN_VALUE;
            }
            return list.size();
        }

        if (memberReader instanceof MemberCache) {
            List<RolapMember> list =
                ((MemberCache) memberReader)
                    .getLevelMembersFromCache(
                        (RolapLevel) level, null);
            if (list == null) {
                return Integer.MIN_VALUE;
            }
            return list.size();
        }

        return Integer.MIN_VALUE;
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

    public List<Member> getMemberChildren(List<Member> members) {
        return getMemberChildren(members, null);
    }

    public List<Member> getMemberChildren(
        List<Member> members,
        Evaluator context)
    {
        if (members.size() == 0) {
            return Collections.emptyList();
        } else {
            MemberChildrenConstraint constraint =
                sqlConstraintFactory.getMemberChildrenConstraint(context);
            final Hierarchy hierarchy = members.get(0).getHierarchy();
            final MemberReader memberReader = getMemberReader(hierarchy);
            final List<RolapMember> rolapMemberList = Util.cast(members);
            final List<RolapMember> children = new ArrayList<RolapMember>();
            memberReader.getMemberChildren(
                rolapMemberList,
                children,
                constraint);
            return Util.cast(children);
        }
    }

    public void getMemberAncestors(Member member, List<Member> ancestorList) {
        Member parentMember = getMemberParent(member);
        while (parentMember != null) {
            ancestorList.add(parentMember);
            parentMember = getMemberParent(parentMember);
        }
    }

    public Cube getCube() {
        throw new UnsupportedOperationException();
    }

    public SchemaReader withoutAccessControl() {
        assert this.getClass() == RolapSchemaReader.class
            : "Subclass " + getClass() + " must override";
        if (role == schema.getDefaultRole()) {
            return this;
        }
        return new RolapSchemaReader(schema.getDefaultRole(), schema);
    }

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

    public final OlapElement lookupCompound(
        OlapElement parent,
        List<Id.Segment> names,
        boolean failIfNotFound,
        int category,
        MatchType matchType)
    {
        if (MondrianProperties.instance().SsasCompatibleNaming.get()) {
            return new NameResolver().resolve(
                parent,
                Util.toOlap4j(names),
                failIfNotFound,
                category,
                matchType,
                getNamespaces());
        }
        return lookupCompoundInternal(
            parent,
            names,
            failIfNotFound,
            category,
            matchType);
    }

    public final OlapElement lookupCompoundInternal(
        OlapElement parent,
        List<Id.Segment> names,
        boolean failIfNotFound,
        int category,
        MatchType matchType)
    {
        return Util.lookupCompound(
            this, parent, names, failIfNotFound, category, matchType);
    }

    public List<NameResolver.Namespace> getNamespaces() {
        return Collections.<NameResolver.Namespace>singletonList(this);
    }

    public OlapElement lookupChild(
        OlapElement parent,
        IdentifierSegment segment)
    {
        return lookupChild(parent, segment, MatchType.EXACT);
    }

    public OlapElement lookupChild(
        OlapElement parent,
        IdentifierSegment segment,
        MatchType matchType)
    {
        OlapElement element = getElementChild(
            parent,
            Util.convert(segment),
            matchType);
        if (element != null) {
            return element;
        }
        if (parent instanceof Cube) {
            // Named sets defined at the schema level do not, of course, belong
            // to a cube. But if parent is a cube, this indicates that the name
            // has not been qualified.
            element = schema.getNamedSet(segment);
        }
        return element;
    }

    public Member lookupMemberChildByName(
        Member parent, Id.Segment childName, MatchType matchType)
    {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                "looking for child \"" + childName + "\" of " + parent);
        }
        assert !(parent instanceof RolapHierarchy.LimitedRollupMember);
        try {
            MemberChildrenConstraint constraint;
            if (childName instanceof Id.NameSegment
                && matchType.isExact())
            {
                constraint = sqlConstraintFactory.getChildByNameConstraint(
                    (RolapMember) parent, (Id.NameSegment) childName);
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
                        matchType);
            }
        } catch (NumberFormatException e) {
            // this was thrown in SqlQuery#quote(boolean numeric, Object
            // value). This happens when Mondrian searches for unqualified Olap
            // Elements like [Month], because it tries to look up a member with
            // that name in all dimensions. Then it generates for example
            // "select .. from time where year = Month" which will result in a
            // NFE because "Month" can not be parsed as a number. The real bug
            // is probably, that Mondrian looks at members at all.
            //
            // @see RolapCube#lookupChild()
            LOGGER.debug(
                "NumberFormatException in lookupMemberChildByName "
                + "for parent = \"" + parent
                + "\", childName=\"" + childName
                + "\", exception: " + e.getMessage());
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
        if (!(nameParts.get(0) instanceof Id.NameSegment)) {
            return null;
        }
        final String name = ((Id.NameSegment) nameParts.get(0)).name;
        return schema.getNamedSet(name);
    }

    public Member getLeadMember(Member member, int n) {
        final MemberReader memberReader =
            getMemberReader(member.getHierarchy());
        return memberReader.getLeadMember((RolapMember) member, n);
    }

    public List<Member> getLevelMembers(Level level, boolean includeCalculated)
    {
        List<Member> members = getLevelMembers(level, null);
        if (!includeCalculated) {
            members = SqlConstraintUtils.removeCalculatedMembers(members);
        }
        return members;
    }

    public List<Member> getLevelMembers(Level level, Evaluator context) {
        TupleConstraint constraint =
            sqlConstraintFactory.getLevelMembersConstraint(
                context,
                new Level[] {level});
        final MemberReader memberReader =
            getMemberReader(level.getHierarchy());
        List<RolapMember> membersInLevel =
            memberReader.getMembersInLevel(
                (RolapLevel) level, constraint);
        return Util.cast(membersInLevel);
    }

    public List<Dimension> getCubeDimensions(Cube cube) {
        assert cube != null;
        final List<Dimension> dimensions = new ArrayList<Dimension>();
        for (Dimension dimension : cube.getDimensions()) {
            switch (role.getAccess(dimension)) {
            case NONE:
                continue;
            default:
                dimensions.add(dimension);
                break;
            }
        }
        return dimensions;
    }

    public List<Hierarchy> getDimensionHierarchies(Dimension dimension) {
        assert dimension != null;
        final List<Hierarchy> hierarchies = new ArrayList<Hierarchy>();
        for (Hierarchy hierarchy : dimension.getHierarchies()) {
            switch (role.getAccess(hierarchy)) {
            case NONE:
                continue;
            default:
                hierarchies.add(hierarchy);
                break;
            }
        }
        return hierarchies;
    }

    public List<Level> getHierarchyLevels(Hierarchy hierarchy) {
        assert hierarchy != null;
        final Role.HierarchyAccess hierarchyAccess =
            role.getAccessDetails(hierarchy);
        final Level[] levels = hierarchy.getLevels();
        if (hierarchyAccess == null) {
            return Arrays.asList(levels);
        }
        Level topLevel = levels[hierarchyAccess.getTopLevelDepth()];
        Level bottomLevel = levels[hierarchyAccess.getBottomLevelDepth()];
        List<Level> restrictedLevels =
            Arrays.asList(levels).subList(
                topLevel.getDepth(), bottomLevel.getDepth() + 1);
        assert restrictedLevels.size() >= 1 : "postcondition";
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
            return getMemberChildren(member).size() > 0;
        } else {
            // This is a regular level. It has children iff there is a lower
            // level.
            final Level childLevel = level.getChildLevel();
            return (childLevel != null)
                && (role.getAccess(childLevel) != Access.NONE);
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
        FunDef fun, Exp[] args, Evaluator evaluator, Calc calc)
    {
        RolapEvaluator revaluator = (RolapEvaluator)
            AbstractCalc.simplifyEvaluator(calc, evaluator);
        if (evaluator.nativeEnabled()) {
            return schema.getNativeRegistry().createEvaluator(
                revaluator, fun, args);
        }
        return null;
    }

    public Parameter getParameter(String name) {
        // Scan through schema parameters.
        for (RolapSchemaParameter parameter : schema.parameterList) {
            if (Util.equalName(parameter.getName(), name)) {
                return parameter;
            }
        }

        // Scan through mondrian and system properties.
        List<Property> propertyList =
            MondrianProperties.instance().getPropertyList();
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

    public RolapSchema getSchema() {
        return schema;
    }

    public SchemaReader withLocus() {
        return RolapUtil.locusSchemaReader(
            schema.getInternalConnection(),
            this);
    }

    public Map<? extends Member, Access> getMemberChildrenWithDetails(
        Member member,
        Evaluator evaluator)
    {
        MemberChildrenConstraint constraint =
            sqlConstraintFactory.getMemberChildrenConstraint(evaluator);
        final Hierarchy hierarchy = member.getHierarchy();
        final MemberReader memberReader = getMemberReader(hierarchy);
        final ArrayList<RolapMember> memberChildren =
            new ArrayList<RolapMember>();

        return memberReader.getMemberChildren(
            (RolapMember) member,
            memberChildren,
            constraint);
    }

    /**
     * Implementation of {@link Parameter} which is sourced from system
     * propertes (see {@link System#getProperties()} or mondrian properties
     * (see {@link MondrianProperties}.
     * <p/>
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
            super(
                name,
                Literal.nullValue,
                "System property '" + name + "'",
                new StringType());
            this.system = system;
            this.propertyDefinition =
                system
                ? null
                : MondrianProperties.instance().getPropertyDefinition(name);
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
                        final String name =
                            SystemPropertyParameter.this.getName();
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
