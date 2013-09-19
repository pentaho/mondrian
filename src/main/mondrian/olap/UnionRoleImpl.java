/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.olap;

import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of {@link Role} which combines the privileges of several
 * roles and has the superset of their privileges.
 *
 * @see mondrian.olap.RoleImpl#union(java.util.List)
 *
 * @author jhyde
 * @since Nov 26, 2007
 */
class UnionRoleImpl implements Role {
    private static final Logger LOGGER =
        Logger.getLogger(UnionRoleImpl.class);
    private final List<Role> roleList;

    /**
     * Creates a UnionRoleImpl.
     *
     * @param roleList List of constituent roles
     */
    UnionRoleImpl(List<Role> roleList) {
        this.roleList = new ArrayList<Role>(roleList);
    }

    public Access getAccess(Schema schema) {
        Access access = Access.NONE;
        for (Role role : roleList) {
            access = max(access, role.getAccess(schema));
            if (access == Access.ALL) {
                break;
            }
        }
        LOGGER.debug(
            "Access level " + access
            + " granted to schema " + schema.getName()
            + " because of a union of roles.");
        return access;
    }

    /**
     * Returns the larger of two enum values. Useful if the enums are sorted
     * so that more permissive values come after less permissive values.
     *
     * @param t1 First value
     * @param t2 Second value
     * @return larger of the two values
     */
    private static <T extends Enum<T>> T max(T t1, T t2) {
        if (t1.ordinal() > t2.ordinal()) {
            return t1;
        } else {
            return t2;
        }
    }

    public Access getAccess(Cube cube) {
        Access access = Access.NONE;
        for (Role role : roleList) {
            access = max(access, role.getAccess(cube));
            if (access == Access.ALL) {
                break;
            }
        }
        LOGGER.debug(
            "Access level " + access
            + " granted to cube " + cube.getName()
            + " because of a union of roles.");
        return access;
    }

    public Access getAccess(Dimension dimension) {
        Access access = Access.NONE;
        for (Role role : roleList) {
            access = max(access, role.getAccess(dimension));
            if (access == Access.ALL) {
                break;
            }
        }
        LOGGER.debug(
            "Access level " + access
            + " granted to dimension " + dimension.getUniqueName()
            + " because of a union of roles.");
        return access;
    }

    public Access getAccess(Hierarchy hierarchy) {
        Access access = Access.NONE;
        for (Role role : roleList) {
            access = max(access, role.getAccess(hierarchy));
            if (access == Access.ALL) {
                break;
            }
        }
        LOGGER.debug(
            "Access level " + access
            + " granted to hierarchy " + hierarchy.getUniqueName()
            + " because of a union of roles.");
        return access;
    }

    public HierarchyAccess getAccessDetails(final Hierarchy hierarchy) {
        List<HierarchyAccess> list = new ArrayList<HierarchyAccess>();
        for (Role role : roleList) {
            final HierarchyAccess accessDetails =
                role.getAccessDetails(hierarchy);
            if (accessDetails != null) {
                list.add(accessDetails);
            }
        }
        // If none of the roles call out access details, we shouldn't either.
        if (list.isEmpty()) {
            return null;
        }
        HierarchyAccess hierarchyAccess =
            new UnionHierarchyAccessImpl(hierarchy, list);
        if (list.size() > 5) {
            hierarchyAccess =
                new RoleImpl.CachingHierarchyAccess(hierarchyAccess);
        }
        return hierarchyAccess;
    }

    public Access getAccess(Level level) {
        Access access = Access.NONE;
        for (Role role : roleList) {
            access = max(access, role.getAccess(level));
            if (access == Access.ALL) {
                break;
            }
        }
        LOGGER.debug(
            "Access level " + access
            + " granted to level " + level.getUniqueName()
            + " because of a union of roles.");
        return access;
    }

    public Access getAccess(Member member) {
        assert member != null;
        HierarchyAccess hierarchyAccess =
            getAccessDetails(member.getHierarchy());
        if (hierarchyAccess != null) {
            return hierarchyAccess.getAccess(member);
        }
        final Access access = getAccess(member.getDimension());
        LOGGER.debug(
            "Access level " + access
            + " granted to member " + member.getUniqueName()
            + " because of a union of roles.");
        return access;
    }

    public Access getAccess(NamedSet set) {
        Access access = Access.NONE;
        for (Role role : roleList) {
            access = max(access, role.getAccess(set));
            if (access == Access.ALL) {
                break;
            }
        }
        LOGGER.debug(
            "Access level " + access
            + " granted to set " + set.getUniqueName()
            + " because of a union of roles.");
        return access;
    }

    public boolean canAccess(OlapElement olapElement) {
        for (Role role : roleList) {
            if (role.canAccess(olapElement)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Implementation of {@link mondrian.olap.Role.HierarchyAccess} that
     * gives access to an object if any one of the constituent hierarchy
     * accesses has access to that object.
     */
    private class UnionHierarchyAccessImpl implements HierarchyAccess {
        private final List<HierarchyAccess> list;
        private final Hierarchy hierarchy;

        /**
         * Creates a UnionHierarchyAccessImpl.
         *
         * @param hierarchy Hierarchy
         * @param list List of underlying hierarchy accesses
         */
        UnionHierarchyAccessImpl(
            Hierarchy hierarchy,
            List<HierarchyAccess> list)
        {
            this.hierarchy = hierarchy;
            this.list = list;
        }

        public Access getAccess(Member member) {
            Access access = Access.NONE;
            final int roleCount = roleList.size();
            for (int i = 0; i < roleCount; i++) {
                Role role = roleList.get(i);
                access = max(access, role.getAccess(member));
                if (access == Access.ALL) {
                    break;
                }
            }
            LOGGER.debug(
                "Access level " + access
                + " granted to member " + member.getUniqueName()
                + " because of a union of roles.");
            return access;
        }

        public int getTopLevelDepth() {
            if (!isTopLeveRestricted()) {
                // We don't restrict the top level.
                // Return 0 for root.
                return 0;
            }
            int access = Integer.MAX_VALUE;
            for (HierarchyAccess hierarchyAccess : list) {
                if (hierarchyAccess.getTopLevelDepth() == 0) {
                    // No restrictions. Skip.
                    continue;
                }
                access =
                    Math.min(
                        access,
                        hierarchyAccess.getTopLevelDepth());
                if (access == 0) {
                    break;
                }
            }
            return access;
        }

        public int getBottomLevelDepth() {
            if (!isBottomLeveRestricted()) {
                // We don't restrict the bottom level.
                return list.get(0).getBottomLevelDepth();
            }
            int access = -1;
            for (HierarchyAccess hierarchyAccess : list) {
                if (hierarchyAccess.getBottomLevelDepth()
                    == hierarchy.getLevels().length)
                {
                    // No restrictions. Skip.
                    continue;
                }
                access =
                    Math.max(
                        access,
                        hierarchyAccess.getBottomLevelDepth());
            }
            return access;
        }

        public RollupPolicy getRollupPolicy() {
            RollupPolicy rollupPolicy = RollupPolicy.HIDDEN;
            for (HierarchyAccess hierarchyAccess : list) {
                rollupPolicy =
                    max(
                        rollupPolicy,
                        hierarchyAccess.getRollupPolicy());
                if (rollupPolicy == RollupPolicy.FULL) {
                    break;
                }
            }
            return rollupPolicy;
        }

        public boolean hasInaccessibleDescendants(Member member) {
            // If any of the roles return all the members,
            // we assume that all descendants are accessible when
            // we create a union of these roles.
            final Access unionAccess = getAccess(member);
            if (unionAccess == Access.ALL) {
                return false;
            }
            if (unionAccess == Access.NONE) {
                return true;
            }
            for (HierarchyAccess hierarchyAccess : list) {
                if (hierarchyAccess.getAccess(member) == Access.CUSTOM
                    && !hierarchyAccess.hasInaccessibleDescendants(member))
                {
                    return false;
                }
            }
            // All of the roles have restricted the descendants in
            // some way.
            return true;
        }

        private boolean isTopLeveRestricted() {
            for (HierarchyAccess hierarchyAccess : list) {
                if (hierarchyAccess.getTopLevelDepth() > 0) {
                    return true;
                }
            }
            return false;
        }

        private boolean isBottomLeveRestricted() {
            for (HierarchyAccess hierarchyAccess : list) {
                if (hierarchyAccess.getBottomLevelDepth()
                    == hierarchy.getLevels().length)
                {
                    return true;
                }
            }
            return false;
        }
    }
}

// End UnionRoleImpl.java
