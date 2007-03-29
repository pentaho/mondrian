/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2002-2002 Kana Software, Inc.
// Copyright (C) 2002-2006 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Oct 5, 2002
*/

package mondrian.olap;

import java.util.*;

/**
 * A <code>Role</code> is a collection of access rights to cubes, permissions,
 * and so forth.
 *
 * <p>At present, the only way to create a role is programmatically. You then
 * add appropriate permissions, and associate the role with a connection.
 * Queries executed for the duration of the connection will b.
 *
 * <p>Mondrian does not have any notion of a 'user'. It is the client
 * application's responsibility to create a role appropriate for the user who
 * is establishing the connection.
 *
 * @author jhyde
 * @since Oct 5, 2002
 * @version $Id$
 */
public class Role {
    private boolean mutable = true;
    private final Map<Schema, Access> schemaGrants =
        new HashMap<Schema, Access>();
    private final Map<Cube, Access> cubeGrants =
        new HashMap<Cube, Access>();
    private final Map<Dimension, Access> dimensionGrants =
        new HashMap<Dimension, Access>();
    private final Map<Hierarchy, HierarchyAccess> hierarchyGrants =
        new HashMap<Hierarchy, HierarchyAccess>();

    /**
     * Creates a role with no permissions.
     */
    public Role() {}

    protected Role clone() {
        Role role = new Role();
        role.mutable = mutable;
        role.schemaGrants.putAll(schemaGrants);
        role.cubeGrants.putAll(cubeGrants);
        role.dimensionGrants.putAll(dimensionGrants);
        for (Map.Entry<Hierarchy, HierarchyAccess> entry :
            hierarchyGrants.entrySet()) {
            role.hierarchyGrants.put(
                entry.getKey(),
                entry.getValue().clone());
        }
        return role;
    }

    /**
     * Returns a copy of this <code>Role</code> which can be modified.
     */
    public Role makeMutableClone() {
        Role role = clone();
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

    /**
     * Returns the access this role has to a given schema.
     *
     * @pre schema != null
     * @post return == Access.ALL || return == Access.NONE || return == Access.ALL_DIMENSIONS
     */
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

    /**
     * Returns the access this role has to a given cube.
     *
     * @pre cube != null
     * @post return == Access.ALL || return == Access.NONE
     */
    public Access getAccess(Cube cube) {
        assert cube != null;
        Access access = cubeGrants.get(cube);
        if (access == null) {
            access = schemaGrants.get(cube.getSchema());
        }
        return toAccess(access);
    }

    /**
     * Represents the access that a role has to a particular hierarchy.
     */
    public static class HierarchyAccess {
        private final Hierarchy hierarchy;
        private final Level topLevel;
        private final Access access;
        private final Level bottomLevel;
        private final Map<Member, Access> memberGrants =
            new HashMap<Member, Access>();

        /**
         * Creates a <code>HierarchyAccess</code>
         */
        HierarchyAccess(
                Hierarchy hierarchy,
                Access access,
                Level topLevel,
                Level bottomLevel) {
            assert access != null;
            this.hierarchy = hierarchy;
            this.access = access;
            this.topLevel = topLevel;
            this.bottomLevel = bottomLevel;
        }

        public HierarchyAccess clone() {
            HierarchyAccess hierarchyAccess = new HierarchyAccess(
                    hierarchy, access, topLevel, bottomLevel);
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
            for (final Member member : memberGrants.keySet()) {
                if (member.getParentMember() == parent) {
                    final Access access = toAccess(memberGrants.get(member));
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
            if (topLevel != null &&
                    member.getLevel().getDepth() < topLevel.getDepth()) {
                // no access
                return Access.NONE;
            } else if (bottomLevel != null &&
                    member.getLevel().getDepth() > bottomLevel.getDepth()) {
                // no access
                return Access.NONE;
            } else {
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
                return Access.NONE;
            }
        }

        public Hierarchy getHierarchy() {
            return hierarchy;
        }

        public Level getTopLevel() {
            return topLevel;
        }

        public Level getBottomLevel() {
            return bottomLevel;
        }

        public Map<Member, Access> getMemberGrants() {
            return Collections.unmodifiableMap(memberGrants);
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

    /**
     * Returns the access this role has to a given dimension.
     *
     * @pre dimension != null
     * @post Access.instance().isValid(return)
     */
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
            Level bottomLevel) {
        Util.assertPrecondition(hierarchy != null, "hierarchy != null");
        assert access != null;
        Util.assertPrecondition((access == Access.CUSTOM) || (topLevel == null && bottomLevel == null), "access == Access.CUSTOM) || (topLevel == null && bottomLevel == null)");
        Util.assertPrecondition(topLevel == null || topLevel.getHierarchy() == hierarchy, "topLevel == null || topLevel.getHierarchy() == hierarchy");
        Util.assertPrecondition(bottomLevel == null || bottomLevel.getHierarchy() == hierarchy, "bottomLevel == null || bottomLevel.getHierarchy() == hierarchy");
        Util.assertPrecondition(isMutable(), "isMutable()");
        hierarchyGrants.put(
            hierarchy,
            new HierarchyAccess(hierarchy, access, topLevel, bottomLevel));
    }

    /**
     * Returns the access this role has to a given hierarchy.
     *
     * @pre hierarchy != null
     * @post return == Access.NONE || return == Access.ALL || return == Access.CUSTOM
     */
    public Access getAccess(Hierarchy hierarchy) {
        assert hierarchy != null;
        HierarchyAccess access = hierarchyGrants.get(hierarchy);
        if (access != null) {
            return access.access;
        }
        return getAccess(hierarchy.getDimension());
    }

    /**
     * Returns the details of this hierarchy's access, or null if the hierarchy
     * has not been given explicit access.
     *
     * @pre hierarchy != null
     */
    public HierarchyAccess getAccessDetails(Hierarchy hierarchy) {
        Util.assertPrecondition(hierarchy != null, "hierarchy != null");
        return hierarchyGrants.get(hierarchy);
    }

    /**
     * Returns the access this role has to a given level.
     *
     * @pre level != null
     * @post Access.instance().isValid(return)
     */
    public Access getAccess(Level level) {
        assert level != null;
        HierarchyAccess access = hierarchyGrants.get(level.getHierarchy());
        if (access != null) {
            if (access.topLevel != null &&
                    level.getDepth() < access.topLevel.getDepth()) {
                return Access.NONE;
            }
            if (access.bottomLevel != null &&
                    level.getDepth() > access.bottomLevel.getDepth()) {
                return Access.NONE;
            }
            return access.access;
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
     *     {@link #grant(Hierarchy,Access,Level,Level)}.
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
        HierarchyAccess hierarchyAccess = hierarchyGrants.get(member.getHierarchy());
        assert hierarchyAccess != null;
        assert hierarchyAccess.access == Access.CUSTOM;
        hierarchyAccess.grant(member, access);
    }

    /**
     * Returns the access this role has to a given member.
     *
     * @pre member != null
     * @pre isMutable()
     * @post return == Access.NONE || return == Access.ALL || return == Access.CUSTOM
     */
    public Access getAccess(Member member) {
        assert member != null;
        HierarchyAccess hierarchyAccess =
            hierarchyGrants.get(member.getHierarchy());
        if (hierarchyAccess != null) {
            return hierarchyAccess.getAccess(member);
        }
        return getAccess(member.getDimension());
    }

    /**
     * Returns the access this role has to a given named set.
     *
     * @pre set != null
     * @pre isMutable()
     * @post return == Access.NONE || return == Access.ALL
     */
    public Access getAccess(NamedSet set) {
        Util.assertPrecondition(set != null, "set != null");
        return Access.ALL;
    }

    /**
     * Returns whether this role is allowed to see a given element.
     * @pre olapElement != null
     */
    public boolean canAccess(OlapElement olapElement) {
        Util.assertPrecondition(olapElement != null, "olapElement != null");
        if (olapElement instanceof Cube) {
            return getAccess((Cube) olapElement) != Access.NONE;
        } else if (olapElement instanceof Dimension) {
            return getAccess((Dimension) olapElement) != Access.NONE;
        } else if (olapElement instanceof Hierarchy) {
            return getAccess((Hierarchy) olapElement) != Access.NONE;
        } else if (olapElement instanceof Level) {
            return getAccess((Level) olapElement) != Access.NONE;
        } else if (olapElement instanceof Member) {
            return getAccess((Member) olapElement) != Access.NONE;
        } else if (olapElement instanceof NamedSet) {
            return getAccess((NamedSet) olapElement) != Access.NONE;
        } else {
            return false;
        }
    }
}

// End Role.java
