/*
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2007-2007 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap;

import java.util.List;
import java.util.ArrayList;

/**
 * Implementation of {@link Role} which combines the privileges of several
 * roles and has the superset of their privileges.
 *
 * @see mondrian.olap.RoleImpl#union(java.util.List)
 *
 * @author jhyde
 * @version $Id$
 * @since Nov 26, 2007
 */
class UnionRoleImpl implements Role {
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
        return new UnionHierarchyAccessImpl(hierarchy, list);
    }

    public Access getAccess(Level level) {
        Access access = Access.NONE;
        for (Role role : roleList) {
            access = max(access, role.getAccess(level));
            if (access == Access.ALL) {
                break;
            }
        }
        return access;
    }

    public Access getAccess(Member member) {
        assert member != null;
        HierarchyAccess hierarchyAccess =
            getAccessDetails(member.getHierarchy());
        if (hierarchyAccess != null) {
            return hierarchyAccess.getAccess(member);
        }
        return getAccess(member.getDimension());
    }

    public Access getAccess(NamedSet set) {
        Access access = Access.NONE;
        for (Role role : roleList) {
            access = max(access, role.getAccess(set));
            if (access == Access.ALL) {
                break;
            }
        }
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

    private class UnionHierarchyAccessImpl implements HierarchyAccess {
        private final List<HierarchyAccess> list;

        UnionHierarchyAccessImpl(
            Hierarchy hierarchy,
            List<HierarchyAccess> list)
        {
            Util.discard(hierarchy);
            this.list = list;
        }

        public Access getAccess(Member member) {
            Access access = Access.NONE;
            for (Role role : roleList) {
                access = max(access, role.getAccess(member));
                if (access == Access.ALL) {
                    break;
                }
            }
            return access;
        }

        public int getTopLevelDepth() {
            int access = Integer.MAX_VALUE;
            for (HierarchyAccess hierarchyAccess : list) {
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
            int access = -1;
            for (HierarchyAccess hierarchyAccess : list) {
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
            for (HierarchyAccess hierarchyAccess : list) {
                switch (hierarchyAccess.getAccess(member)) {
                case NONE:
                    continue;
                case CUSTOM:
                    return true;
                case ALL:
                    if (!hierarchyAccess.hasInaccessibleDescendants(member)) {
                        return false;
                    }
                }
            }
            return true;
        }
    }
}

// End UnionRoleImpl.java
