/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2002-2005 Julian Hyde
// Copyright (C) 2005-2009 Pentaho and others
// All Rights Reserved.
//
// jhyde, Oct 5, 2002
*/

package mondrian.olap;

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
 */
public interface Role {

    /**
     * Returns the access this role has to a given schema.
     *
     * @pre schema != null
     * @post return == Access.ALL
     * || return == Access.NONE
     * || return == Access.ALL_DIMENSIONS
     */
    Access getAccess(Schema schema);

    /**
     * Returns the access this role has to a given cube.
     *
     * @pre cube != null
     * @post return == Access.ALL || return == Access.NONE
     */
    Access getAccess(Cube cube);

    /**
     * Represents the access that a role has to a particular hierarchy.
     */
    public interface HierarchyAccess {
        /**
         * Returns the access the current role has to a given member.
         *
         * <p>Visibility is:<ul>
         * <li>{@link Access#NONE} if member is not visible,
         * <li>{@link Access#ALL} if member and all children are visible,
         * <li>{@link Access#CUSTOM} if some of the children are not visible.
         * </ul></p>
         *
         * <p>For these purposes, children which are below the bottom level are
         * regarded as visible.</p>
         *
         * @param member Member
         * @return current role's access to member
         */
        Access getAccess(Member member);

        /**
         * Returns the depth of the highest level to which the current Role has
         * access. The 'all' level, if present, has a depth of zero.
         *
         * @return depth of the highest accessible level
         */
        int getTopLevelDepth();

        /**
         * Returns the depth of the lowest level to which the current Role has
         * access. The 'all' level, if present, has a depth of zero.
         *
         * @return depth of the lowest accessible level
         */
        int getBottomLevelDepth();

        /**
         * Returns the policy by which cell values are calculated if not all
         * of a member's children are visible.
         *
         * @return rollup policy
         */
        RollupPolicy getRollupPolicy();

        /**
         * Returns <code>true</code> if at least one of the descendants of the
         * given Member is inaccessible to this Role.
         *
         * <p>Descendants which are inaccessible because they are below the
         * bottom level are ignored.
         *
         * @param member Member
         * @return whether a descendant is inaccessible
         */
        boolean hasInaccessibleDescendants(Member member);
    }

    /**
     * Returns the access this role has to a given dimension.
     *
     * @pre dimension != null
     * @post Access.instance().isValid(return)
     */
    Access getAccess(Dimension dimension);

    /**
     * Returns the access this role has to a given hierarchy.
     *
     * @pre hierarchy != null
     * @post return == Access.NONE
     *   || return == Access.ALL
     *   || return == Access.CUSTOM
     */
    Access getAccess(Hierarchy hierarchy);

    /**
     * Returns the details of this hierarchy's access, or null if the hierarchy
     * has not been given explicit access.
     *
     * @pre hierarchy != null
     */
    HierarchyAccess getAccessDetails(Hierarchy hierarchy);

    /**
     * Returns the access this role has to a given level.
     *
     * @pre level != null
     * @post Access.instance().isValid(return)
     */
    Access getAccess(Level level);

    /**
     * Returns the access this role has to a given member.
     *
     * @pre member != null
     * @pre isMutable()
     * @post return == Access.NONE
     *    || return == Access.ALL
     *    || return == Access.CUSTOM
     */
    Access getAccess(Member member);

    /**
     * Returns the access this role has to a given named set.
     *
     * @pre set != null
     * @pre isMutable()
     * @post return == Access.NONE || return == Access.ALL
     */
    Access getAccess(NamedSet set);

    /**
     * Returns whether this role is allowed to see a given element.
     * @pre olapElement != null
     */
    boolean canAccess(OlapElement olapElement);

    /**
     * Enumeration of the policies by which a cell is calculated if children
     * of a member are not accessible.
     */
    enum RollupPolicy {
        /**
         * The value of the cell is null if any of the children are
         * inaccessible.
         */
        HIDDEN,

        /**
         * The value of the cell is obtained by rolling up the values of
         * accessible children.
         */
        PARTIAL,

        /**
         * The value of the cell is obtained by rolling up the values of all
         * children.
         */
        FULL,
    }
}

// End Role.java
