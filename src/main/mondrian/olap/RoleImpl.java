/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2002-2005 Julian Hyde
// Copyright (C) 2005-2013 Pentaho and others
// All Rights Reserved.
*/
package mondrian.olap;

import mondrian.rolap.RolapCube;
import mondrian.rolap.RolapCubeDimension;
import mondrian.rolap.RolapCubeHierarchy;

import java.util.*;

/**
 * Default implementation of the {@link Role} interface.
 *
 * @author jhyde, lucboudreau
 * @since Oct 5, 2002
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
    public RoleImpl() {
    }

    protected RoleImpl clone() {
        RoleImpl role = new RoleImpl();
        role.mutable = mutable;
        role.schemaGrants.putAll(schemaGrants);
        role.cubeGrants.putAll(cubeGrants);
        role.dimensionGrants.putAll(dimensionGrants);
        for (Map.Entry<Hierarchy, HierarchyAccessImpl> entry
            : hierarchyGrants.entrySet())
        {
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
     * @pre access == Access.ALL || access == Access.NONE
     * || access == Access.ALL_DIMENSIONS
     * @pre isMutable()
     */
    public void grant(Schema schema, Access access) {
        assert schema != null;
        assert isMutable();
        schemaGrants.put(schema, access);
    }

    public Access getAccess(Schema schema) {
        assert schema != null;
        final Access schemaAccess = schemaGrants.get(schema);
        if (schemaAccess == null) {
            // No specific rules means full access.
            return Access.CUSTOM;
        } else {
            return schemaAccess;
        }
    }

    /**
     * Converts a null Access object to {@link Access#NONE}.
     *
     * @param access Access object or null
     * @return Access object, never null
     */
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
        assert access == Access.ALL
            || access == Access.NONE
            || access == Access.CUSTOM;
        Util.assertPrecondition(isMutable(), "isMutable()");
        cubeGrants.put(cube, access);
        // Set the schema's access to 'custom' if no rules already exist.
        final Access schemaAccess =
            getAccess(cube.getSchema());
        if (schemaAccess == Access.NONE) {
            grant(cube.getSchema(), Access.CUSTOM);
        }
    }

    public Access getAccess(Cube cube) {
        assert cube != null;
        // Check for explicit rules.
        // Both 'custom' and 'all' are good enough
        Access access = cubeGrants.get(cube);
        if (access != null) {
            return access;
        }
        // Check for inheritance from the parent schema
        // 'All Dimensions' and 'custom' are not good enough
        access = schemaGrants.get(cube.getSchema());
        if (access == Access.ALL) {
            return Access.ALL;
        }
        // Deny access
        return Access.NONE;
    }

    /**
     * Defines access to a dimension.
     *
     * @param dimension Dimension whose access to grant/deny.
     * @param access An Access instance
     *
     * @pre dimension != null
     * @pre access == Access.ALL || access == Access.CUSTOM
     * || access == Access.NONE
     * @pre isMutable()
     */
    public void grant(Dimension dimension, Access access) {
        assert dimension != null;
        assert access == Access.ALL
            || access == Access.NONE
            || access == Access.CUSTOM;
        Util.assertPrecondition(isMutable(), "isMutable()");
        dimensionGrants.put(dimension, access);
        // Dimension grants do not cascade to the parent cube automatically.
        // We always figure out the inheritance at runtime since the place
        // where the dimension is used (either inside of a virtual cube,
        // a shared dimension or a cube) will influence on the decision.
    }

    public Access getAccess(Dimension dimension) {
        assert dimension != null;
        // Check for explicit rules.
        Access access = getDimensionGrant(dimension);
        if (access == Access.CUSTOM) {
            // For legacy reasons, if there are no accessible hierarchies
            // and the dimension has an access level of custom, we deny.
            Util.deprecated("TODO Remove for Mondrian 4.0", false);
            boolean canAccess = false;
            for (Hierarchy hierarchy : dimension.getHierarchyList()) {
                final HierarchyAccessImpl hierarchyAccess =
                    hierarchyGrants.get(hierarchy);
                if (hierarchyAccess != null
                    && hierarchyAccess.access != Access.NONE)
                {
                    canAccess = true;
                }
            }
            if (canAccess) {
                return Access.CUSTOM;
            } else {
                return Access.NONE;
            }
        } else if (access != null) {
            return access;
        }
        // Check if this dimension inherits the cube's access rights.
        // 'Custom' level is not good enough for inherited access.
        access = checkDimensionAccessByCubeInheritance(dimension);
        if (access != Access.NONE) {
            return access;
        }
        // Check access at the schema level.
        // Levels of 'custom' and 'none' are not good enough.
        switch (getAccess(dimension.getSchema())) {
        case ALL:
            return Access.ALL;
        case ALL_DIMENSIONS:
            // For all_dimensions to work, the cube access must be
            // at least 'custom' level
            if (access != Access.NONE) {
                return Access.ALL;
            } else {
                return Access.NONE;
            }
        default:
            return Access.NONE;
        }
    }

    private Access getDimensionGrant(final Dimension dimension) {
        if (dimension.isMeasures()) {
            for (Dimension key : dimensionGrants.keySet()) {
                if (key == dimension) {
                    return dimensionGrants.get(key);
                }
            }
            return null;
        } else {
            return dimensionGrants.get(dimension);
        }
    }

    /**
     * This method is used to check if the access rights over a dimension
     * that might be inherited from the parent cube.
     * <p>It only checks for inherited access, and it presumes that there
     * are no dimension grants currently given to the dimension passed as an
     * argument.
     */
    private Access checkDimensionAccessByCubeInheritance(Dimension dimension) {
        assert dimensionGrants.containsKey(dimension) == false
               || dimension.isMeasures();
        for (Map.Entry<Cube, Access> cubeGrant : cubeGrants.entrySet()) {
            final Access access = toAccess(cubeGrant.getValue());
            // The 'none' and 'custom' access level are not good enough
            if (access == Access.NONE || access == Access.CUSTOM) {
                continue;
            }
            for (Dimension dimension1 : cubeGrant.getKey().getDimensionList()) {
                // If the dimensions have the same identity,
                // we found an access rule.
                if (dimension == dimension1) {
                    return cubeGrant.getValue();
                }
                // If the passed dimension argument is of class
                // RolapCubeDimension, we must validate the cube
                // assignment and make sure the cubes are the same.
                // If not, skip to the next grant.
                if (dimension instanceof RolapCubeDimension
                    && dimension.equals(dimension1)
                    && !((RolapCubeDimension)dimension1)
                        .getCube()
                            .equals(cubeGrant.getKey()))
                {
                    continue;
                }
                // Last thing is to allow for equality correspondences
                // to work with virtual cubes.
                if (cubeGrant.getKey() instanceof RolapCube
                    && ((RolapCube)cubeGrant.getKey()).isVirtual()
                    && dimension.equals(dimension1))
                {
                    return cubeGrant.getValue();
                }
            }
        }
        return Access.NONE;
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
     * @param rollupPolicy Rollup policy
     *
     * @pre hierarchy != null
     * @pre Access.instance().isValid(access)
     * @pre (access == Access.CUSTOM)
     *      || (topLevel == null &amp;&amp; bottomLevel == null)
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
                this, hierarchy, access, topLevel, bottomLevel, rollupPolicy));

        // Cascade the access right to 'custom' on the parent dim if necessary.
        Access dimAccess = dimensionGrants.get(hierarchy.getDimension());
        if (dimAccess == Access.NONE) {
            dimAccess = Access.CUSTOM;
        }
        if (dimAccess == null && hierarchy instanceof RolapCubeHierarchy) {
            RolapCube cube = ((RolapCubeHierarchy) hierarchy).getCube();
            dimAccess = cubeGrants.get(cube);
        }
        if (dimAccess == null) {
            dimAccess = Access.ALL;
        }
        grant(hierarchy.getDimension(), dimAccess);
    }

    public Access getAccess(Hierarchy hierarchy) {
        assert hierarchy != null;
        HierarchyAccessImpl hierarchyAccess = hierarchyGrants.get(hierarchy);
        if (hierarchyAccess != null) {
            return hierarchyAccess.access;
        }
        // There was no explicit rule for this particular hierarchy.
        // Let's check the parent dimension.
        Access access = getAccess(hierarchy.getDimension());
        if (access == Access.ALL) {
            // Access levels of 'none' and 'custom' are not enough.
            return Access.ALL;
        }
        // Access denied, since we know that the dimension check has
        // checked for its parents as well.
        return Access.NONE;
    }

    public HierarchyAccess getAccessDetails(Hierarchy hierarchy) {
        Util.assertPrecondition(hierarchy != null, "hierarchy != null");
        if (hierarchyGrants.containsKey(hierarchy)) {
            return hierarchyGrants.get(hierarchy);
        }
        final Access hierarchyAccess;
        final Access schemaGrant =
            schemaGrants.get(hierarchy.getDimension().getSchema());
        if (schemaGrant != null) {
            if (schemaGrant == Access.ALL) {
                hierarchyAccess = Access.ALL;
            } else {
                hierarchyAccess = Access.NONE;
            }
        } else {
            hierarchyAccess = Access.ALL;
        }
        return new HierarchyAccessImpl(
            this,
            hierarchy,
            hierarchyAccess,
            hierarchy.getLevelList().get(0),
            Util.last(hierarchy.getLevelList()),
            RollupPolicy.HIDDEN);
    }

    public Access getAccess(Level level) {
        assert level != null;
        HierarchyAccessImpl hierarchyAccess =
                hierarchyGrants.get(level.getHierarchy());
        if (hierarchyAccess != null
            && hierarchyAccess.access != Access.NONE)
        {
            if (checkLevelIsOkWithRestrictions(
                    hierarchyAccess,
                    level))
            {
                // We're good. Let it through.
                return hierarchyAccess.access;
            }
        }
        // No information could be deducted from the parent hierarchy.
        // Let's use the parent dimension.
        return getAccess(level.getDimension());
    }

    private static boolean checkLevelIsOkWithRestrictions(
        HierarchyAccessImpl hierarchyAccess,
        Level level)
    {
        // Check if this level is explicitly excluded by top/bototm
        // level restrictions.
        if (level.getDepth() < hierarchyAccess.topLevel.getDepth()) {
            return false;
        }
        if (level.getDepth() > hierarchyAccess.bottomLevel.getDepth()) {
            return false;
        }
        return true;
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
     * @pre isMutable()
     * @pre getAccess(member.getHierarchy()) == Access.CUSTOM
     */
    public void grant(Member member, Access access) {
        Util.assertPrecondition(member != null, "member != null");
        assert isMutable();
        assert getAccess(member.getHierarchy()) == Access.CUSTOM;
        HierarchyAccessImpl hierarchyAccess =
            hierarchyGrants.get(member.getHierarchy());
        assert hierarchyAccess != null;
        assert hierarchyAccess.access == Access.CUSTOM;
        hierarchyAccess.grant(this, member, access);
    }

    public Access getAccess(Member member) {
        assert member != null;
        // Always allow access to calculated members.
        if (member.isCalculatedInQuery()) {
            return Access.ALL;
        }
        // Check if the parent hierarchy has any access
        // rules for this.
        final HierarchyAccessImpl hierarchyAccess =
            hierarchyGrants.get(member.getHierarchy());
        if (hierarchyAccess != null) {
            return hierarchyAccess.getAccess(member);
        }
        // Then let's check ask the parent level.
        return getAccess(member.getLevel());
    }

    public Access getAccess(NamedSet set) {
        Util.assertPrecondition(set != null, "set != null");
        // TODO Named sets cannot be secured at the moment.
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
        final List<? extends Level> levels = hierarchy.getLevelList();
        return new HierarchyAccessImpl(
            Util.createRootRole(hierarchy.getDimension().getSchema()),
            hierarchy, Access.ALL, levels.get(0),
            Util.last(levels), Role.RollupPolicy.FULL);
    }

    /**
     * Returns a role that is the union of the given roles.
     *
     * @param roleList List of roles
     * @return Union role
     */
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
        private final Map<String, MemberAccess> memberGrants =
            new HashMap<String, MemberAccess>();
        private final RollupPolicy rollupPolicy;
        private final Role role;

        /**
         * Creates a <code>HierarchyAccessImpl</code>.
         * @param role A role this access belongs to.
         * @param hierarchy A hierarchy this object describes.
         * @param access The access granted to this role for this hierarchy.
         * @param topLevel The top level to restrict the role to, or null to
         * grant access up top the top level of the hierarchy parameter.
         * @param bottomLevel The bottom level to restrict the role to, or null
         * to grant access down to the bottom level of the hierarchy parameter.
         * @param rollupPolicy The rollup policy to apply.
         */
        HierarchyAccessImpl(
            Role role,
            Hierarchy hierarchy,
            Access access,
            Level topLevel,
            Level bottomLevel,
            RollupPolicy rollupPolicy)
        {
            assert role != null;
            assert hierarchy != null;
            assert access != null;
            assert rollupPolicy != null;
            this.role = role;
            this.hierarchy = hierarchy;
            this.access = access;
            this.rollupPolicy = rollupPolicy;
            this.topLevel = topLevel == null
                ? hierarchy.getLevelList().get(0)
                : topLevel;
            this.bottomLevel = bottomLevel == null
                ? Util.last(hierarchy.getLevelList())
                : bottomLevel;
        }

        public HierarchyAccess clone() {
            HierarchyAccessImpl hierarchyAccess =
                new HierarchyAccessImpl(
                    role, hierarchy, access, topLevel,
                    bottomLevel, rollupPolicy);
            hierarchyAccess.memberGrants.putAll(memberGrants);
            return hierarchyAccess;
        }

        /**
         * Grants access to a member.
         *
         * @param member Member
         * @param access Access
         */
        void grant(RoleImpl role, Member member, Access access) {
            Util.assertTrue(member.getHierarchy() == hierarchy);

            // Remove any existing grants to descendants of "member"

            for (Iterator<MemberAccess> memberIter =
                memberGrants.values().iterator(); memberIter.hasNext();)
            {
                Member m = memberIter.next().member;
                if (m.isChildOrEqualTo(member)) {
                    memberIter.remove();
                }
            }

            memberGrants.put(
                member.getUniqueName(),
                new MemberAccess(member, access));

            if (access == Access.NONE) {
                // Since we're denying access, the immediate parent
                // must have an access level of at least 'custom'
                for (Member m = member.getParentMember();
                     m != null;
                     m = m.getParentMember())
                {
                    MemberAccess mAccess =
                        memberGrants.get(m.getUniqueName());
                    final Access memberAccess =
                        mAccess == null ? null : mAccess.access;
                    // If no current access is allowed, upgrade to "custom"
                    if (memberAccess == Access.NONE
                        && checkLevelIsOkWithRestrictions(
                            this,
                            m.getLevel()))
                    {
                        memberGrants.put(
                            m.getUniqueName(),
                            new MemberAccess(m, Access.CUSTOM));
                    }
                }
            } else {
                // Create 'custom' access for any ancestors of 'member' which
                // do not have explicit access but which have at least one
                // child visible.
                for (Member m = member.getParentMember();
                     m != null;
                     m = m.getParentMember())
                {
                    if (checkLevelIsOkWithRestrictions(
                            this,
                            m.getLevel()))
                    {
                        MemberAccess mAccess =
                            memberGrants.get(m.getUniqueName());
                        final Access parentAccess =
                            toAccess(mAccess == null ? null : mAccess.access);
                        if (parentAccess == Access.NONE) {
                            memberGrants.put(
                                m.getUniqueName(),
                                new MemberAccess(m, Access.CUSTOM));
                        }
                    }
                }
                // Also set custom access for the parent hierarchy.
                final Access hierarchyAccess =
                    role.getAccess(member.getLevel().getHierarchy());
                if (hierarchyAccess == Access.NONE) {
                    // Upgrade to CUSTOM level.
                    role.grant(
                        hierarchy,
                        Access.CUSTOM,
                        topLevel,
                        bottomLevel,
                        rollupPolicy);
                }
            }
        }

        public Access getAccess(Member member) {
            if (this.access != Access.CUSTOM) {
                return this.access;
            }

            MemberAccess mAccess =
                memberGrants.get(member.getUniqueName());
            Access access = mAccess == null ? null : mAccess.access;

            // Check for an explicit deny.
            if (access == Access.NONE) {
                return Access.NONE;
            }
            // Check for explicit grant
            if (access == Access.ALL || access == Access.CUSTOM) {
                return access;
            }
            // Check if the member is out of the bounds
            // defined by topLevel and bottomLevel
            if (!checkLevelIsOkWithRestrictions(this, member.getLevel())) {
                return Access.NONE;
            }
            // Nothing was explicitly defined for this member.
            // Check for grants on its parents
            for (Member m = member.getParentMember();
                m != null;
                m = m.getParentMember())
            {
                MemberAccess pAccess =
                    memberGrants.get(m.getUniqueName());
                final Access parentAccess = pAccess == null
                    ? null
                    : pAccess.access;
                if (parentAccess == null) {
                    // No explicit rules for this parent
                    continue;
                }
                // Check for parent deny
                if (parentAccess == Access.NONE
                    || parentAccess == Access.CUSTOM)
                {
                    return Access.NONE;
                }
                return Access.ALL;
            }
            // Check for inherited access from ancestors.
            // "Custom" is not good enough. We are looking for "all" access.
            access = role.getAccess(member.getLevel());
            if (access == Access.ALL) {
                return Access.ALL;
            }
            // This member might be at a level allowed by the
            // topLevel/bottomLevel attributes. If there are no explicit
            // member grants defined at this level but the member fits
            // those bounds, we give access.
            if (memberGrants.size() == 0
                && checkLevelIsOkWithRestrictions(
                    this,
                    member.getLevel()))
            {
                return Access.ALL;
            }
            // No access
            return Access.NONE;
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

        /**
         * Tells whether a given member has some of its children being
         * restricted by the access controls of this role instance.
         */
        public boolean hasInaccessibleDescendants(Member member) {
            for (MemberAccess access : memberGrants.values()) {
                switch (access.access) {
                case NONE:
                case CUSTOM:
                    if (access.isSubGrant(member)) {
                        // At least one of the limited member is
                        // part of the descendants of this member.
                        return true;
                    }
                }
            }
            // All descendants are accessible.
            return false;
        }
    }

    /**
     * A MemberAccess contains information about a grant applied
     * to a member for a given role. It is only an internal data
     * structure and should not be exposed via the API.
     */
    private static class MemberAccess {
        private final Member member;
        private final Access access;
        private final Map<String, Boolean> parentsCache =
            new HashMap<String, Boolean>();
        public MemberAccess(
            Member member,
            Access access)
        {
                this.member = member;
                this.access = access;
        }

        /**
         * Tells whether the member concerned by this grant object
         * is a children of a given member. The result of the computation
         * is cached for faster results, since this might get called
         * very often.
         */
        private boolean isSubGrant(Member parentMember) {
            boolean disableCaching = MondrianProperties
                .instance().DisableCaching.get();
            if (!disableCaching
                && parentsCache.containsKey(parentMember.getUniqueName()))
            {
                return parentsCache.get(parentMember.getUniqueName());
            }
            boolean foundParent = false;
            for (Member m = member; m != null; m = m.getParentMember()) {
                if (m.equals(parentMember)) {
                    // We have proved that this granted member is a
                    // descendant of 'member'.
                    foundParent = true;
                    break;
                }
            }
            // Not a parent. Cache it and return.
            if (!disableCaching) {
                parentsCache.put(
                    parentMember.getUniqueName(), foundParent);
            }
            return foundParent;
        }
    }

    /**
     * Implementation of {@link mondrian.olap.Role.HierarchyAccess} that
     * delegates all methods to an underlying hierarchy access.
     */
    public static abstract class DelegatingHierarchyAccess
        implements HierarchyAccess
    {
        protected final HierarchyAccess hierarchyAccess;

        /**
         * Creates a DelegatingHierarchyAccess.
         *
         * @param hierarchyAccess Underlying hierarchy access
         */
        public DelegatingHierarchyAccess(HierarchyAccess hierarchyAccess) {
            assert hierarchyAccess != null;
            this.hierarchyAccess = hierarchyAccess;
        }

        public Access getAccess(Member member) {
            return hierarchyAccess.getAccess(member);
        }

        public int getTopLevelDepth() {
            return hierarchyAccess.getTopLevelDepth();
        }

        public int getBottomLevelDepth() {
            return hierarchyAccess.getBottomLevelDepth();
        }

        public RollupPolicy getRollupPolicy() {
            return hierarchyAccess.getRollupPolicy();
        }

        public boolean hasInaccessibleDescendants(Member member) {
            return hierarchyAccess.hasInaccessibleDescendants(member);
        }
    }

    /**
     * Implementation of {@link mondrian.olap.Role.HierarchyAccess} that caches
     * the access of each member and level.
     *
     * <p>This reduces the number of calls to the underlying HierarchyAccess,
     * which is particularly useful for a union role which is based on many
     * roles.
     *
     * <p>Caching uses two {@link java.util.WeakHashMap} objects, so should
     * release resources if memory is scarce. However, it may use up memory and
     * cause segments etc. to be removed from the cache when GC is triggered.
     * For this reason, you should only use this wrapper for a HierarchyAccess
     * which would otherwise have poor performance; currently used for union
     * roles with 5 or more member roles.
     */
    static class CachingHierarchyAccess
        extends DelegatingHierarchyAccess
    {
        private final Map<Member, Access> memberAccessMap =
            new WeakHashMap<Member, Access>();
        private RollupPolicy rollupPolicy;
        private Map<Member, Boolean> inaccessibleDescendantsMap =
            new WeakHashMap<Member, Boolean>();
        private Integer topLevelDepth;
        private Integer bottomLevelDepth;

        /**
         * Creates a CachingHierarchyAccess.
         *
         * @param hierarchyAccess Underlying hierarchy access
         */
        public CachingHierarchyAccess(HierarchyAccess hierarchyAccess) {
            super(hierarchyAccess);
        }

        @Override
        public Access getAccess(Member member) {
            Access access = memberAccessMap.get(member);
            if (access != null) {
                return access;
            }
            access = hierarchyAccess.getAccess(member);
            memberAccessMap.put(member, access);
            return access;
        }

        @Override
        public int getTopLevelDepth() {
            if (topLevelDepth == null) {
                topLevelDepth = hierarchyAccess.getTopLevelDepth();
            }
            return topLevelDepth;
        }

        @Override
        public int getBottomLevelDepth() {
            if (bottomLevelDepth == null) {
                bottomLevelDepth = hierarchyAccess.getBottomLevelDepth();
            }
            return bottomLevelDepth;
        }

        @Override
        public RollupPolicy getRollupPolicy() {
            if (rollupPolicy == null) {
                rollupPolicy = hierarchyAccess.getRollupPolicy();
            }
            return rollupPolicy;
        }

        @Override
        public boolean hasInaccessibleDescendants(Member member) {
            Boolean b = inaccessibleDescendantsMap.get(member);
            if (b == null) {
                b = hierarchyAccess.hasInaccessibleDescendants(member);
                inaccessibleDescendantsMap.put(member, b);
            }
            return b;
        }
    }
}

// End RoleImpl.java
