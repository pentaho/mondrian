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
public interface Role {

    /**
     * Returns the access this role has to a given schema.
     *
     * @pre schema != null
     * @post return == Access.ALL || return == Access.NONE || return == Access.ALL_DIMENSIONS
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
        Access getAccess(Member member);
        int getTopLevelDepth();
        int getBottomLevelDepth();
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
     * @post return == Access.NONE || return == Access.ALL || return == Access.CUSTOM
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
     * @post return == Access.NONE || return == Access.ALL || return == Access.CUSTOM
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
}

// End Role.java
