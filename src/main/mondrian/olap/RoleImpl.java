/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2002-2002 Kana Software, Inc.
// Copyright (C) 2002-2007 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Oct 5, 2002
*/

package mondrian.olap;

import java.util.*;

/**
 * <code>RoleImpl</code> is Mondrian's default implementation for the
 * <code>Role</code> interface.
 *
 * @author jhyde
 * @since Oct 5, 2002
 * @version $Id$
 */
public class RoleImpl implements Role {
    private boolean mutable = true;
    private final Map<Schema, Access> schemaGrants =
        new HashMap<Schema, Access>();
    private final Map<Cube, Access> cubeGrants =
        new HashMap<Cube, Access>();
    private final Map<Dimension, Access> dimensionGrants =
        new HashMap<Dimension, Access>();
    private final Map<Hierarchy, HierarchyAccessImpl> hierarchyGrants =
        new HashMap<Hierarchy, HierarchyAccessImpl>();

    /**
     * Creates a role with no permissions.
     */
    public RoleImpl() {}

    protected RoleImpl clone() {
        RoleImpl role = new RoleImpl();
        role.mutable = mutable;
        role.schemaGrants.putAll(schemaGrants);
        role.cubeGrants.putAll(cubeGrants);
        role.dimensionGrants.putAll(dimensionGrants);
        for (Map.Entry<Hierarchy, HierarchyAccessImpl> entry :
                hierarchyGrants.entrySet()) {
            role.hierarchyGrants.put(
                entry.getKey(),
                (HierarchyAccessImpl) entry.getValue().clone());
        }
        return role;
    }

    /**
     * Returns a copy of this <code>Role</code> which can be modified.
     */
    public RoleImpl makeMutableClone() {
        RoleImpl role = clone();
        role.mutable = true;
        return role;
    }

    /**
     * Prevents any further modifications.
     * @post !isMutable()
     */
    public void makeImmutable() {
        mutable = false;
    }

    /**
     * Returns whether modifications are possible.
     */
    public boolean isMutable() {
        return mutable;
    }

    /**
     * Defines access to all cubes and dimensions in a schema.
     *
     * @param schema Schema whose access to grant/deny.
     * @param access An {@link Access access code}
     *
     * @pre schema != null
     * @pre access == Access.ALL || access == Access.NONE || access == Access.ALL_DIMENSIONS
     * @pre isMutable()
     */
    public void grant(Schema schema, Access access) {
        assert schema != null;
        assert access == Access.ALL || access == Access.NONE || access == Access.ALL_DIMENSIONS;
        assert isMutable();
        schemaGrants.put(schema, access);
    }

    public Access getAccess(Schema schema) {
        assert schema != null;
        return toAccess(schemaGrants.get(schema));
    }

    private static Access toAccess(Access access) {
        return access == null ? Access.NONE : access;
    }

    /**
     * Defines access to a cube.
     *
     * @param cube Cube whose access to grant/deny.
     * @param access An {@link Access access code}
     *
     * @pre cube != null
     * @pre access == Access.ALL || access == Access.NONE
     * @pre isMutable()
     */
    public void grant(Cube cube, Access access) {
        Util.assertPrecondition(cube != null, "cube != null");
        assert access == Access.ALL || access == Access.NONE;
        Util.assertPrecondition(isMutable(), "isMutable()");
        cubeGrants.put(cube, access);
    }

    public Access getAccess(Cube cube) {
        assert cube != null;
        Access access = cubeGrants.get(cube);
        if (access == null) {
            access = schemaGrants.get(cube.getSchema());
        }
        return toAccess(access);
    }

    /**
     * Removes the upper level restriction of each hierarchy in this role.
     * This will allow member names to be resolved even if the upper levels
     * are not visible.
     *
     * <p>For example, it should be possible to resolve
     * [Store].[USA].[CA].[San Francisco] even if the role cannot see the
     * nation level.
     */
    public void removeTopLevels() {
        for (Map.Entry<Hierarchy,HierarchyAccessImpl> entry
            : hierarchyGrants.entrySet())
        {
            final HierarchyAccessImpl hierarchyAccess = entry.getValue();
            if (hierarchyAccess.topLevel != null) {
                final HierarchyAccessImpl hierarchyAccessClone =
                    new HierarchyAccessImpl(
                        hierarchyAccess.hierarchy,
                        hierarchyAccess.access,
                        null,
                        hierarchyAccess.bottomLevel,
                        hierarchyAccess.rollupPolicy);
                hierarchyAccessClone.memberGrants.putAll(
                    hierarchyAccess.memberGrants);
                entry.setValue(hierarchyAccessClone);
            }
        }
    }

    /**
     * Defines access to a dimension.
     *
     * @param dimension Hierarchy whose access to grant/deny.
     * @param access An {@link Access access code}
     *
     * @pre dimension != null
     * @pre access == Access.ALL || access == Access.NONE
     * @pre isMutable()
     */
    public void grant(Dimension dimension, Access access) {
        assert dimension != null;
        assert access == Access.ALL || access == Access.NONE;
        Util.assertPrecondition(isMutable(), "isMutable()");
        dimensionGrants.put(dimension, access);
    }

    public Access getAccess(Dimension dimension) {
        assert dimension != null;
        Access access = dimensionGrants.get(dimension);
        if (access != null) {
            return toAccess(access);
        }
        // If the role has access to a cube this dimension is part of, that's
        // good enough.
        for (Map.Entry<Cube,Access> cubeGrant : cubeGrants.entrySet()) {
            access = toAccess(cubeGrant.getValue());
            if (access == Access.NONE) {
                continue;
            }
            final Dimension[] dimensions = cubeGrant.getKey().getDimensions();
            for (Dimension dimension1 : dimensions) {
                if (dimension1 == dimension) {
                    return access;
                }
            }
        }
        // Check access at the schema level.
        switch (getAccess(dimension.getSchema())) {
        case ALL:
        case ALL_DIMENSIONS:
            return Access.ALL;
        default:
            return Access.NONE;
        }
    }

    /**
     * Defines access to a hierarchy.
     *
     * @param hierarchy Hierarchy whose access to grant/deny.
     * @param access An {@link Access access code}
     * @param topLevel Top-most level which can be accessed, or null if the
     *     highest level. May only be specified if <code>access</code> is
     *    {@link mondrian.olap.Access#CUSTOM}.
     * @param bottomLevel Bottom-most level which can be accessed, or null if
     *     the lowest level. May only be specified if <code>access</code> is
     *    {@link mondrian.olap.Access#CUSTOM}.
     *
     * @param rollupPolicy
     * @pre hierarchy != null
     * @pre Access.instance().isValid(access)
     * @pre (access == Access.CUSTOM) || (topLevel == null && bottomLevel == null)
     * @pre topLevel == null || topLevel.getHierarchy() == hierarchy
     * @pre bottomLevel == null || bottomLevel.getHierarchy() == hierarchy
     * @pre isMutable()
     */
    public void grant(
        Hierarchy hierarchy,
        Access access,
        Level topLevel,
        Level bottomLevel,
        RollupPolicy rollupPolicy)
    {
        assert hierarchy != null;
        assert access != null;
        assert (access == Access.CUSTOM)
            || (topLevel == null && bottomLevel == null);
        assert topLevel == null || topLevel.getHierarchy() == hierarchy;
        assert bottomLevel == null || bottomLevel.getHierarchy() == hierarchy;
        assert isMutable();
        assert rollupPolicy != null;
        hierarchyGrants.put(
            hierarchy,
            new HierarchyAccessImpl(
                hierarchy, access, topLevel, bottomLevel, rollupPolicy));
    }

    public Access getAccess(Hierarchy hierarchy) {
        assert hierarchy != null;
        HierarchyAccessImpl hierarchyAccess = hierarchyGrants.get(hierarchy);
        if (hierarchyAccess != null) {
            return hierarchyAccess.access;
        }
        return getAccess(hierarchy.getDimension());
    }

    public HierarchyAccess getAccessDetails(Hierarchy hierarchy) {
        Util.assertPrecondition(hierarchy != null, "hierarchy != null");
        return hierarchyGrants.get(hierarchy);
    }

    public Access getAccess(Level level) {
        assert level != null;
        HierarchyAccessImpl hierarchyAccess =
                hierarchyGrants.get(level.getHierarchy());
        if (hierarchyAccess != null) {
            if (hierarchyAccess.topLevel != null &&
                    level.getDepth() < hierarchyAccess.topLevel.getDepth()) {
                return Access.NONE;
            }
            if (hierarchyAccess.bottomLevel != null &&
                    level.getDepth() > hierarchyAccess.bottomLevel.getDepth()) {
                return Access.NONE;
            }
            return hierarchyAccess.access;
        }
        return getAccess(level.getDimension());
    }

    /**
     * Defines access to a member in a hierarchy.
     *
     * <p>Notes:<ol>
     * <li>The order of grants matters. If you grant/deny access to a
     *     member, previous grants/denials to its descendants are ignored.</li>
     * <li>Member grants do not supersde top/bottom levels set using
     *     {@link #grant(Hierarchy, Access, Level, Level, mondrian.olap.Role.RollupPolicy)}.
     * <li>If you have access to a member, then you can see its ancestors
     *     <em>even those explicitly denied</em>, up to the top level.
     * </ol>
     *
     * @pre member != null
     * @pre Access.instance().isValid(access)
     * @pre isMutable()
     * @pre getAccess(member.getHierarchy()) == Access.CUSTOM
     */
    public void grant(Member member, Access access) {
        Util.assertPrecondition(member != null, "member != null");
        assert Util.isValid(Access.class, access);
        assert isMutable();
        assert getAccess(member.getHierarchy()) == Access.CUSTOM;
        HierarchyAccessImpl hierarchyAccess = hierarchyGrants.get(member.getHierarchy());
        assert hierarchyAccess != null;
        assert hierarchyAccess.access == Access.CUSTOM;
        hierarchyAccess.grant(member, access);
    }

    public Access getAccess(Member member) {
        assert member != null;
        HierarchyAccessImpl hierarchyAccess =
            hierarchyGrants.get(member.getHierarchy());
        if (hierarchyAccess != null) {
            return hierarchyAccess.getAccess(member);
        }
        return getAccess(member.getDimension());
    }

    public Access getAccess(NamedSet set) {
        Util.assertPrecondition(set != null, "set != null");
        return Access.ALL;
    }

    public boolean canAccess(OlapElement olapElement) {
        Util.assertPrecondition(olapElement != null, "olapElement != null");
        if (olapElement instanceof Member) {
            return getAccess((Member) olapElement) != Access.NONE;
        } else if (olapElement instanceof Level) {
            return getAccess((Level) olapElement) != Access.NONE;
        } else if (olapElement instanceof NamedSet) {
            return getAccess((NamedSet) olapElement) != Access.NONE;
        } else if (olapElement instanceof Hierarchy) {
            return getAccess((Hierarchy) olapElement) != Access.NONE;
        } else if (olapElement instanceof Cube) {
            return getAccess((Cube) olapElement) != Access.NONE;
        } else if (olapElement instanceof Dimension) {
            return getAccess((Dimension) olapElement) != Access.NONE;
        } else {
            return false;
        }
    }

    /**
     * Creates an element which represents all access to a hierarchy.
     *
     * @param hierarchy Hierarchy
     * @return element representing all access to a given hierarchy
     */
    public static HierarchyAccess createAllAccess(Hierarchy hierarchy) {
        final Level[] levels = hierarchy.getLevels();
        return new HierarchyAccessImpl(
            hierarchy, Access.ALL, levels[0],
            levels[levels.length - 1], Role.RollupPolicy.FULL);
    }

    public static Role union(final List<Role> roleList) {
        assert roleList.size() > 0;
        return new UnionRoleImpl(roleList);
    }

    // ~ Inner classes --------------------------------------------------------

    /**
     * Represents the access that a role has to a particular hierarchy.
     */
    private static class HierarchyAccessImpl implements Role.HierarchyAccess {
        private final Hierarchy hierarchy;
        private final Level topLevel;
        private final Access access;
        private final Level bottomLevel;
        private final Map<Member, Access> memberGrants =
            new HashMap<Member, Access>();
        private final RollupPolicy rollupPolicy;

        /**
         * Creates a <code>HierarchyAccessImpl</code>
         */
        HierarchyAccessImpl(
            Hierarchy hierarchy,
            Access access,
            Level topLevel,
            Level bottomLevel,
            RollupPolicy rollupPolicy)
        {
            assert access != null;
            this.hierarchy = hierarchy;
            this.access = access;
            final Level[] levels = hierarchy.getLevels();
            this.topLevel = (topLevel == null)
                    ? levels[0] : topLevel;
            this.bottomLevel = (bottomLevel == null)
                    ? levels[levels.length-1] : bottomLevel;
            assert rollupPolicy != null;
            this.rollupPolicy = rollupPolicy;
        }

        public HierarchyAccess clone() {
            HierarchyAccessImpl hierarchyAccess =
                new HierarchyAccessImpl(
                    hierarchy, access, topLevel, bottomLevel, rollupPolicy);
            hierarchyAccess.memberGrants.putAll(memberGrants);
            return hierarchyAccess;
        }

        void grant(Member member, Access access) {
            Util.assertTrue(member.getHierarchy() == hierarchy);
            // Remove any existing grants to descendants of "member"
            for (Iterator<Member> memberIter =
                    memberGrants.keySet().iterator(); memberIter.hasNext();) {
                Member m = memberIter.next();
                if (m.isChildOrEqualTo(member)) {
                    memberIter.remove();
                }
            }

            memberGrants.put(member, access);

            if (access == Access.NONE) {
                // If an ancestor of this member has any children with 'All'
                // access, set them to Custom.
                loop:
                for (Member m = member.getParentMember();
                     m != null;
                     m = m.getParentMember()) {
                    final Access memberAccess = memberGrants.get(m);
                    if (memberAccess == null) {
                        if (childGrantsExist(m)) {
                            memberGrants.put(m, Access.CUSTOM);
                        } else {
                            break;
                        }
                    } else if (memberAccess == Access.CUSTOM) {
                        // Ancestor does not inherit access, but used to have
                        // at least one child with access. See if it still
                        // does...
                        if (childGrantsExist(m)) {
                            memberGrants.put(m, Access.CUSTOM);
                        } else {
                            break;
                        }
                    } else if (memberAccess == Access.NONE) {
                        // Ancestor is explicitly marked having no access.
                        // Leave it that way.
                        break;
                    } else if (memberAccess == Access.ALL) {
                        // Ancestor is explicitly marked having all access.
                        // Leave it that way.
                        break;
                    }
                }

            } else {

                // Create 'custom' access for any ancestors of 'member' which
                // do not have explicit access but which have at least one
                // child visible.
                for (Member m = member.getParentMember();
                     m != null;
                     m = m.getParentMember()) {
                    switch (toAccess(memberGrants.get(m))) {
                    case NONE:
                        memberGrants.put(m, Access.CUSTOM);
                        break;
                    default:
                        // Existing access (All or Custom) is OK.
                        break;
                    }
                }
            }
        }

        private boolean childGrantsExist(Member parent) {
            for (Map.Entry<Member, Access> entry : memberGrants.entrySet()) {
                final Member member = entry.getKey();
                if (member.getParentMember() == parent) {
                    final Access access = toAccess(entry.getValue());
                    if (access != Access.NONE) {
                        return true;
                    }
                }
            }
            return false;
        }

        public Access getAccess(Member member) {
            if (this.access != Access.CUSTOM) {
                return this.access;
            }
            if (member.getLevel().getDepth() < getTopLevelDepth()) {
                // no access
                return Access.NONE;
            } else if (member.getLevel().getDepth() > getBottomLevelDepth()) {
                // no access
                return Access.NONE;
            } else {
                // Check whether there is an explicit grant for the member or
                // an ancestor.
                for (Member m = member; m != null; m = m.getParentMember()) {
                    final Access memberAccess = memberGrants.get(m);
                    if (memberAccess == null) {
                        continue;
                    }
                    if (memberAccess == Access.CUSTOM &&
                            m != member) {
                        // If member's ancestor has custom access, that
                        // means that member has no access.
                        return Access.NONE;
                    }
                    return memberAccess;
                }
                // If there is no inherited access, check for implicit access.
                // A member is implicitly visible if one of its descendants is
                // visible.
                for (Map.Entry<Member, Access> entry : memberGrants.entrySet()) {
                    final Member grantedMember = entry.getKey();
                    switch (entry.getValue()) {
                    case NONE:
                        continue;
                    }
                    for (Member m = grantedMember; m != null; m = m.getParentMember()) {
                        if (m == member) {
                            return Access.CUSTOM;
                        }
                        if (m != grantedMember && memberGrants.get(m) != null) {
                            break;
                        }
                    }
                }
                return Access.NONE;
            }
        }

        public final int getTopLevelDepth() {
            return topLevel.getDepth();
        }

        public final int getBottomLevelDepth() {
            return bottomLevel.getDepth();
        }

        public RollupPolicy getRollupPolicy() {
            return rollupPolicy;
        }

        public boolean hasInaccessibleDescendants(Member member) {
            for (Map.Entry<Member,Access> entry : memberGrants.entrySet()) {
                switch (entry.getValue()) {
                case NONE:
                    Member grantedMember = entry.getKey();
                    for (Member m = grantedMember;
                         m != null; m = m.getParentMember())
                    {
                        if (m.equals(member)) {
                            // We have proved that this granted member is a
                            // descendant of 'member'.
                            return true;
                        }
                    }
                }
            }
            // All descendants are accessible.
            return false;
        }
    }


}

// End RoleImpl.java
