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

/**
 * <code>DelegatingRole</code> implements {@link Role} by
 * delegating all methods to an underlying {@link Role}.
 *
 * It is a convenient base class if you want to override just a few of
 * {@link Role}'s methods.
 *
 * @author Richard M. Emberson
 * @since Mar 29 2007
 * @version $Id$
 */
public class DelegatingRole implements Role {
    protected final Role role;

    public DelegatingRole(Role role) {
        this.role = role;
    }

    public Access getAccess(Schema schema) {
        return role.getAccess(schema);
    }

    public Access getAccess(Cube cube) {
        return role.getAccess(cube);
    }

    public Access getAccess(Dimension dimension) {
        return role.getAccess(dimension);
    }

    public Access getAccess(Hierarchy hierarchy) {
        return role.getAccess(hierarchy);
    }

    public HierarchyAccess getAccessDetails(Hierarchy hierarchy) {
        return role.getAccessDetails(hierarchy);
    }

    public Access getAccess(Level level) {
        return role.getAccess(level);
    }

    public Access getAccess(Member member) {
        return role.getAccess(member);
    }

    public Access getAccess(NamedSet set) {
        return role.getAccess(set);
    }

    public boolean canAccess(OlapElement olapElement) {
        return role.canAccess(olapElement);
    }
}

// End DelegatingRole.java
