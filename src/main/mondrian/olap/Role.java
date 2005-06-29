/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright 2002-2005 (C) Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Oct 5, 2002
*/

package mondrian.olap;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

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
 * @testcase {@link mondrian.test.AccessControlTest}
 *
 * @author jhyde
 * @since Oct 5, 2002
 * @version $Id$
 **/
public class Role {
    private boolean mutable = true;
    /** Maps {@link Schema} to {@link Integer},
     * {@link Cube} to {@link Integer},
     * {@link Dimension} to {@link Integer},
     * {@link Hierarchy} to {@link HierarchyAccess}. */
    private Map grants = new HashMap();
    private static Integer integers[] = {
        new Integer(0),
        new Integer(1),
        new Integer(2),
        new Integer(3),
        new Integer(4),
    };

    /**
     * Creates a role with no permissions.
     */
    public Role() {}

    protected Object clone() {
        Role role = new Role();
        role.mutable = mutable;
        role.grants.putAll(grants);
        for (Iterator iter = grants.values().iterator(); iter.hasNext();) {
            Object value = iter.next();
            if (value instanceof HierarchyAccess) {
                final HierarchyAccess hierarchyAccess = (HierarchyAccess) value;
                role.grants.put(hierarchyAccess.hierarchy, hierarchyAccess.clone());
            }
        }
        return role;
    }

    /**
     * Returns a copy of this <code>Role</code> which can be modified.
     */
    public Role makeMutableClone() {
        Role role = (Role) clone();
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
    public void grant(Schema schema, int access) {
        Util.assertPrecondition(schema != null, "schema != null");
        Util.assertPrecondition(access == Access.ALL || access == Access.NONE || access == Access.ALL_DIMENSIONS, "access == Access.ALL || access == Access.NONE");
        Util.assertPrecondition(isMutable(), "isMutable()");
        grants.put(schema, toInteger(access));
    }

    private static Integer toInteger(int access) {
        return integers[access];
    }

    /**
     * Returns the access this role has to a given schema.
     *
     * @pre schema != null
     * @post return == Access.ALL || return == Access.NONE || return == Access.ALL_DIMENSIONS
     */
    public int getAccess(Schema schema) {
        Util.assertPrecondition(schema != null, "schema != null");
        return toAccess((Integer) grants.get(schema));
    }

    private static int toAccess(Integer i) {
        return i == null ? Access.NONE : i.intValue();
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
    public void grant(Cube cube, int access) {
        Util.assertPrecondition(cube != null, "cube != null");
        Util.assertPrecondition(access == Access.ALL || access == Access.NONE, "access == Access.ALL || access == Access.NONE");
        Util.assertPrecondition(isMutable(), "isMutable()");
        grants.put(cube, toInteger(access));
    }

    /**
     * Returns the access this role has to a given cube.
     *
     * @pre cube != null
     * @post return == Access.ALL || return == Access.NONE
     */
    public int getAccess(Cube cube) {
        Util.assertPrecondition(cube != null, "cube != null");
        Integer access = (Integer) grants.get(cube);
        if (access == null) {
            access = (Integer) grants.get(cube.getSchema());
        }
        return toAccess(access);
    }

    /**
     * Represents the access that a role has to a particular hierarchy.
     */
    public static class HierarchyAccess {
        private final Hierarchy hierarchy;
        private final Level topLevel;
        private final int access;
        private final Level bottomLevel;
        private final Map memberGrants = new HashMap();

        /**
         * Creates a <code>HierarchyAccess</code>
         *
         * @pre Access.instance().isValid(access)
         */
        HierarchyAccess(Hierarchy hierarchy,
                        int access,
                        Level topLevel,
                        Level bottomLevel) {
            this.hierarchy = hierarchy;
            this.access = access;
            this.topLevel = topLevel;
            this.bottomLevel = bottomLevel;
            Util.assertPrecondition(Access.instance().isValid(access));
        }

        public Object clone() {
            HierarchyAccess hierarchyAccess = new HierarchyAccess(
                    hierarchy, access, topLevel, bottomLevel);
            hierarchyAccess.memberGrants.putAll(memberGrants);
            return hierarchyAccess;
        }

        void grant(Member member, int access) {
            Util.assertTrue(member.getHierarchy() == hierarchy);
            // Remove any existing grants to descendants of "member"
            for (Iterator membersIter = memberGrants.keySet().iterator();
                 membersIter.hasNext();) {
                Member m = (Member) membersIter.next();
                if (m.isChildOrEqualTo(member)) {
                    membersIter.remove();
                }
            }
            memberGrants.put(member, toInteger(access));
        }

        public int getAccess(Member member) {
            int access = this.access;
            if (access == Access.CUSTOM) {
                access = Access.NONE;
                if (topLevel != null &&
                        member.getLevel().getDepth() < topLevel.getDepth()) {
                    // no access
                } else if (bottomLevel != null &&
                        member.getLevel().getDepth() > bottomLevel.getDepth()) {
                    // no access
                } else {
                    for (Iterator membersIter = memberGrants.keySet().iterator(); membersIter.hasNext();) {
                        Member m = (Member) membersIter.next();
                        final int memberAccess = toAccess((Integer) memberGrants.get(m));
                        if (member.isChildOrEqualTo(m)) {
                            // A member has access if it has been granted access,
                            // or if any of its ancestors have.
                            access = Math.max(access, memberAccess);
                        } else if (m.isChildOrEqualTo(member) &&
                                memberAccess != Access.NONE) {
                            // A member has CUSTOM access if any of its descendants
                            // has access.
                            access = Math.max(access, Access.CUSTOM);
                        }
                    }
                }
            }
            return access;
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

        public Map getMemberGrants() {
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
    public void grant(Dimension dimension, int access) {
        Util.assertPrecondition(dimension != null, "dimension != null");
        Util.assertPrecondition(access == Access.ALL || access == Access.NONE, "access == Access.ALL || access == Access.NONE");
        Util.assertPrecondition(isMutable(), "isMutable()");
        grants.put(dimension, toInteger(access));
    }

    /**
     * Returns the access this role has to a given dimension.
     *
     * @pre dimension != null
     * @post Access.instance().isValid(return)
     */
    public int getAccess(Dimension dimension) {
        Util.assertPrecondition(dimension != null, "dimension != null");
        Integer i = (Integer) grants.get(dimension);
        if (i != null) {
            return toAccess(i);
        }
        // If the role has access to a cube this dimension is part of, that's
        // good enough.
        for (Iterator grantsIter = grants.keySet().iterator(); grantsIter.hasNext();) {
            Object object = grantsIter.next();
            if (!(object instanceof Cube)) {
                continue;
            }
            final int access = toAccess((Integer) grants.get(object));
            if (access == Access.NONE) {
                continue;
            }
            final Dimension[] dimensions = ((Cube) object).getDimensions();
            for (int j = 0; j < dimensions.length; j++) {
                if (dimensions[j] == dimension) {
                    return access;
                }
            }
        }
        // Check access at the schema level.
        switch (getAccess(dimension.getSchema())) {
        case Access.ALL:
        case Access.ALL_DIMENSIONS:
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
     *    {@link Access#CUSTOM}.
     * @param bottomLevel Bottom-most level which can be accessed, or null if
     *     the lowest level. May only be specified if <code>access</code> is
     *    {@link Access#CUSTOM}.
     *
     * @pre hierarchy != null
     * @pre Access.instance().isValid(access)
     * @pre (access == Access.CUSTOM) || (topLevel == null && bottomLevel == null)
     * @pre topLevel == null || topLevel.getHierarchy() == hierarchy
     * @pre bottomLevel == null || bottomLevel.getHierarchy() == hierarchy
     * @pre isMutable()
     */
    public void grant(Hierarchy hierarchy,
                      int access,
                      Level topLevel,
                      Level bottomLevel) {
        Util.assertPrecondition(hierarchy != null, "hierarchy != null");
        Util.assertPrecondition(Access.instance().isValid(access));
        Util.assertPrecondition((access == Access.CUSTOM) || (topLevel == null && bottomLevel == null), "access == Access.CUSTOM) || (topLevel == null && bottomLevel == null)");
        Util.assertPrecondition(topLevel == null || topLevel.getHierarchy() == hierarchy, "topLevel == null || topLevel.getHierarchy() == hierarchy");
        Util.assertPrecondition(bottomLevel == null || bottomLevel.getHierarchy() == hierarchy, "bottomLevel == null || bottomLevel.getHierarchy() == hierarchy");
        Util.assertPrecondition(isMutable(), "isMutable()");
        grants.put(hierarchy, new HierarchyAccess(hierarchy, access, topLevel, bottomLevel));
    }

    /**
     * Returns the access this role has to a given hierarchy.
     *
     * @pre hierarchy != null
     * @post return == Access.NONE || return == Access.ALL || return == Access.CUSTOM
     */
    public int getAccess(Hierarchy hierarchy) {
        Util.assertPrecondition(hierarchy != null, "hierarchy != null");
        HierarchyAccess access = (HierarchyAccess) grants.get(hierarchy);
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
        return (HierarchyAccess) grants.get(hierarchy);
    }

    /**
     * Returns the access this role has to a given level.
     *
     * @pre level != null
     * @post Access.instance().isValid(return)
     */
    public int getAccess(Level level) {
        Util.assertPrecondition(level != null, "level != null");
        HierarchyAccess access = (HierarchyAccess) grants.get(level.getHierarchy());
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
     * <li>Member grants do not supersde top/bottom levels set using {@link
     *     #grant(Hierarchy,int,Level,Level)}.
     * <li>If you have access to a member, then you can see its ancestors
     *     <em>even those explicitly denied</em>, up to the top level.
     * </ol>
     *
     * @pre member != null
     * @pre Access.instance().isValid(access)
     * @pre isMutable()
     * @pre getAccess(member.getHierarchy()) == Access.CUSTOM
     */
    public void grant(Member member, int access) {
        Util.assertPrecondition(member != null, "member != null");
        Util.assertPrecondition(Access.instance().isValid(access), "Access.instance().isValid(access)");
        Util.assertPrecondition(isMutable(), "isMutable()");
        Util.assertPrecondition(getAccess(member.getHierarchy()) == Access.CUSTOM, "getAccess(member.getHierarchy()) == Access.CUSTOM");
        HierarchyAccess hierarchyAccess = (HierarchyAccess) grants.get(member.getHierarchy());
        Util.assertTrue(hierarchyAccess != null && hierarchyAccess.access == Access.CUSTOM);
        hierarchyAccess.grant(member, access);
    }

    /**
     * Returns the access this role has to a given member.
     *
     * @pre member != null
     * @pre isMutable()
     * @post return == Access.NONE || return == Access.ALL || return == Access.CUSTOM
     */
    public int getAccess(Member member) {
        Util.assertPrecondition(member != null, "member != null");
        HierarchyAccess hierarchyAccess = (HierarchyAccess)
                grants.get(member.getHierarchy());
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
    public int getAccess(NamedSet set) {
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
