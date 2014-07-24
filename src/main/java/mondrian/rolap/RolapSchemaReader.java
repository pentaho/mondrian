/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2003-2005 Julian Hyde
// Copyright (C) 2005-2014 Pentaho
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
import java.util.concurrent.ConcurrentHashMap;
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
        new ConcurrentHashMap<Hierarchy, MemberReader>();
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
        final List<? extends Level> levels = hierarchy.getLevelList();
        final Level firstLevel;
        if (hierarchyAccess == null) {
            firstLevel = levels.get(0);
        } else {
            firstLevel = levels.get(hierarchyAccess.getTopLevelDepth());
        }
        return getLevelMembers(firstLevel, true);
    }


    /**
     * This method uses a double-checked locking idiom to avoid making the
     * method fully synchronized, or potentially creating the same MemberReader
     * more than once.  Double-checked locking can cause issues if
     * a second thread accesses the field without either a shared lock in
     * place or the field being specified as volatile.
     * In this case, hierarchyReaders is a ConcurrentHashMap,
     * which internally uses volatile load semantics for read operations.
     * This assures values written by one thread will be visible when read by
     * others.
     * http://en.wikipedia.org/wiki/Double-checked_locking
     */
    public MemberReader getMemberReader(RolapCubeHierarchy hierarchy) {
        MemberReader memberReader = hierarchyReaders.get(hierarchy);
        if (memberReader == null) {
            synchronized (this) {
                memberReader = hierarchyReaders.get(hierarchy);
                if (memberReader == null) {
                    memberReader =
                        RolapSchemaLoader.createMemberReader(hierarchy, role);
                    assert memberReader != null : hierarchy;
                    hierarchyReaders.put(hierarchy, memberReader);
                }
            }
        }
        return memberReader;
    }

    public Member substitute(Member _member) {
        final RolapMember member = (RolapMember) _member;
        final MemberReader memberReader =
            getMemberReader(member.getHierarchy());
        return memberReader.substitute(member);
    }

    public void getMemberRange(
        Level _level, Member _startMember, Member _endMember, List<Member> list)
    {
        final RolapCubeLevel level = (RolapCubeLevel) _level;
        final RolapMember startMember = (RolapMember) _startMember;
        final RolapMember endMember = (RolapMember) _endMember;
        getMemberReader(level.getHierarchy()).getMemberRange(
            level, startMember, endMember, Util.<RolapMember>cast(list));
    }

    public int compareMembersHierarchically(Member m1, Member m2) {
        RolapMember member1 = (RolapMember) m1;
        RolapMember member2 = (RolapMember) m2;
        final RolapCubeHierarchy hierarchy = member1.getHierarchy();
        Util.assertPrecondition(hierarchy == m2.getHierarchy());
        return getMemberReader(hierarchy).compare(member1, member2, true);
    }

    public RolapMember getMemberParent(Member _member) {
        final RolapMember member = (RolapMember) _member;
        return getMemberReader(member.getHierarchy()).getMemberParent(member);
    }

    public int getMemberDepth(Member _member) {
        final RolapMember member = (RolapMember) _member;
        final Role.HierarchyAccess hierarchyAccess =
            role.getAccessDetails(member.getHierarchy());
        if (hierarchyAccess != null) {
            final int memberDepth = member.getLevel().getDepth();
            final int topLevelDepth = hierarchyAccess.getTopLevelDepth();
            return memberDepth - topLevelDepth;
        } else if (member.getLevel().isParentChild()) {
            // For members of parent-child hierarchy, members in the same level
            // may have different depths.
            int depth = 0;
            for (RolapMember m = member.getParentMember();
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

    public List<Member> getMemberChildren(Member _member, Evaluator _context) {
        final RolapMember member = (RolapMember) _member;
        final RolapEvaluator context = (RolapEvaluator) _context;
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
        RolapMember member, MemberChildrenConstraint constraint)
    {
        List<RolapMember> children = new ArrayList<RolapMember>();
        final MemberReader memberReader =
            getMemberReader(member.getHierarchy());
        memberReader.getMemberChildren(member, children, constraint);
        return children;
    }

    public void getParentChildContributingChildren(
        Member _dataMember,
        Hierarchy _hierarchy,
        List<Member> _list)
    {
        final RolapMember dataMember = (RolapMember) _dataMember;
        final RolapCubeHierarchy hierarchy = (RolapCubeHierarchy) _hierarchy;
        final List<RolapMember> list = Util.cast(_list);
        list.add(dataMember);
        hierarchy.getMemberReader().getMemberChildren(dataMember, list);
    }

    public int getChildrenCountFromCache(Member _member) {
        final RolapMember member = (RolapMember) _member;
        final MemberReader memberReader =
            getMemberReader(member.getHierarchy());
        if (memberReader instanceof SmartMemberReader) {
            List list = ((SmartMemberReader) memberReader).getMemberCache()
                .getChildrenFromCache(member, null);
            if (list == null) {
                return -1;
            }
            return list.size();
        }
        if (!(memberReader instanceof MemberCache)) {
            return -1;
        }
        List list = ((MemberCache) memberReader)
            .getChildrenFromCache(member, null);
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
    private int getLevelCardinalityFromCache(RolapCubeLevel level) {
        final RolapCubeHierarchy hierarchy = level.getHierarchy();
        final MemberReader memberReader = getMemberReader(hierarchy);
        if (memberReader instanceof SmartMemberReader) {
            List<RolapMember> list =
                ((SmartMemberReader) memberReader)
                    .getMemberCache()
                    .getLevelMembersFromCache(level, null);
            if (list == null) {
                return Integer.MIN_VALUE;
            }
            return list.size();
        }

        if (memberReader instanceof MemberCache) {
            List<RolapMember> list =
                ((MemberCache) memberReader)
                    .getLevelMembersFromCache(level, null);
            if (list == null) {
                return Integer.MIN_VALUE;
            }
            return list.size();
        }

        return Integer.MIN_VALUE;
    }

    public int getLevelCardinality(
        Level _level,
        boolean approximate,
        boolean materialize)
    {
        final RolapCubeLevel level = (RolapCubeLevel) _level;
        if (!this.role.canAccess(_level)) {
            return 1;
        }

        int rowCount = Integer.MIN_VALUE;
        if (approximate) {
            // See if the schema has an approximation.
            rowCount = level.getApproxRowCount();
        }

        if (rowCount == Integer.MIN_VALUE) {
            rowCount = level.getAttribute().getApproxRowCount();
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
                rowCount = memberReader.getLevelMemberCount(level);
                // Cache it for future.
                level.setApproxRowCount(rowCount);
            }
        }
        return rowCount;
    }

    public List<Member> getMemberChildren(List<Member> members) {
        return getMemberChildren(members, null);
    }

    public List<Member> getMemberChildren(
        List<Member> _members,
        Evaluator _context)
    {
        final List<RolapMember> members = Util.cast(_members);
        if (members.size() == 0) {
            return Collections.emptyList();
        }
        final RolapEvaluator context = (RolapEvaluator) _context;
        MemberChildrenConstraint constraint =
            sqlConstraintFactory.getMemberChildrenConstraint(context);
        final MemberReader memberReader =
            getMemberReader(members.get(0).getHierarchy());
        final List<RolapMember> children = new ArrayList<RolapMember>();
        memberReader.getMemberChildren(
            members,
            children,
            constraint);
        return Util.cast(children);
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
        if (role == schema.rootRole) {
            return this;
        }
        return new RolapSchemaReader(schema.rootRole, schema);
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
        return new NameResolver().resolve(
            parent,
            Util.toOlap4j(names),
            failIfNotFound,
            category,
            matchType,
            getNamespaces());
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
        Member _parent, Id.Segment childName, MatchType matchType)
    {
        final RolapMember parent = (RolapMember) _parent;
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
                    parent, (Id.NameSegment) childName);
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
                        parent,
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

    public Member getLeadMember(Member _member, int n) {
        final RolapMember member = (RolapMember) _member;
        final MemberReader memberReader =
            getMemberReader(member.getHierarchy());
        return memberReader.getLeadMember(member, n);
    }

    public List<Member> getLevelMembers(Level level, boolean includeCalculated)
    {
        List<Member> members = getLevelMembers(level, null);
        if (!includeCalculated) {
            members = SqlConstraintUtils.removeCalculatedMembers(members);
        }
        return members;
    }

    public List<Member> getLevelMembers(Level _level, Evaluator _context) {
        final RolapCubeLevel level = (RolapCubeLevel) _level;
        final RolapEvaluator context = (RolapEvaluator) _context;
        TupleConstraint constraint =
            sqlConstraintFactory.getLevelMembersConstraint(
                context,
                Collections.singletonList(level));
        final MemberReader memberReader =
            getMemberReader(level.getHierarchy());
        List<RolapMember> membersInLevel =
            memberReader.getMembersInLevel(level, constraint);
        return Util.cast(membersInLevel);
    }

    public List<Dimension> getCubeDimensions(Cube cube) {
        assert cube != null;
        final List<Dimension> dimensions = new ArrayList<Dimension>();
        for (Dimension dimension : cube.getDimensionList()) {
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
        for (Hierarchy hierarchy : dimension.getHierarchyList()) {
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
        final List<Level> levels = Util.cast(hierarchy.getLevelList());
        if (hierarchyAccess == null) {
            return levels;
        }
        Level topLevel = levels.get(hierarchyAccess.getTopLevelDepth());
        Level bottomLevel = levels.get(hierarchyAccess.getBottomLevelDepth());
        List<Level> restrictedLevels =
            levels.subList(
                topLevel.getDepth(), bottomLevel.getDepth() + 1);
        assert restrictedLevels.size() >= 1 : "postcondition";
        return restrictedLevels;
    }

    public Member getHierarchyDefaultMember(Hierarchy _hierarchy) {
        final RolapCubeHierarchy hierarchy = (RolapCubeHierarchy) _hierarchy;
        assert _hierarchy != null;
        // If the whole hierarchy is inaccessible, return the intrinsic default
        // member. This is important to construct a evaluator.
        if (role.getAccess(hierarchy) == Access.NONE) {
            return hierarchy.getDefaultMember();
        }
        return getMemberReader(hierarchy).getDefaultMember();
    }

    public boolean isDrillable(Member member) {
        final RolapLevel level = (RolapLevel) member.getLevel();
        if (level.isParentChild()) {
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

        // Scan through mondrian properties.
        List<Property> propertyList =
            MondrianProperties.instance().getPropertyList();
        for (Property property : propertyList) {
            if (property.getPath().equals(name)) {
                return new SystemPropertyParameter(name, false);
            }
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

    /**
     * Implementation of {@link Parameter} which is sourced from mondrian
     * properties (see {@link MondrianProperties}.
     * <p/>
     * <p>The name of the property is the same as the key into the
     * {@link java.util.Properties} object; for example "mondrian.trace.level".
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
