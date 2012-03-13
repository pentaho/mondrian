/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2007-2009 Pentaho and others
// All Rights Reserved.
*/
package mondrian.olap;

/**
 * <code>DelegatingRole</code> implements {@link Role} by
 * delegating all methods to an underlying {@link Role}.
 *
 * <p>It is a convenient base class if you want to override just a few of
 * {@link Role}'s methods.
 *
 * @author Richard M. Emberson
 * @since Mar 29 2007
 */
public class DelegatingRole implements Role {
    protected final Role role;

    /**
     * Creates a DelegatingRole.
     *
     * @param role Underlying role
     */
    public DelegatingRole(Role role) {
        assert role != null;
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

    /**
     * {@inheritDoc}
     *
     * <p>This implementation returns the same access as the underlying role.
     * Derived class may choose to refine access by creating a subclass of
     * {@link mondrian.olap.RoleImpl.DelegatingHierarchyAccess}.
     */
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
