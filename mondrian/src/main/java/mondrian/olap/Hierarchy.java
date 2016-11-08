/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 1999-2005 Julian Hyde
// Copyright (C) 2005-2011 Pentaho and others
// All Rights Reserved.
*/

package mondrian.olap;

/**
 * A <code>Hierarchy</code> is a set of members, organized into levels.
 */
public interface Hierarchy extends OlapElement, Annotated {
    /**
     * Returns the dimension this hierarchy belongs to.
     */
    Dimension getDimension();
    /**
     * Returns the levels in this hierarchy.
     *
     * <p>If a hierarchy is subject to access-control, some of the levels may
     * not be visible; use {@link SchemaReader#getHierarchyLevels} instead.
     *
     * @post return != null
     */
    Level[] getLevels();
    /**
     * Returns the default member of this hierarchy.
     *
     * <p>If a hierarchy is subject to access-control, the default member may
     * not be visible, so use {@link SchemaReader#getHierarchyDefaultMember}.
     *
     * @post return != null
     */
    Member getDefaultMember();
    /**
     * Returns the "All" member of this hierarchy.
     *
     * @post return != null
     */
    Member getAllMember();
    /**
     * Returns a special member representing the "null" value. This never
     * occurs on an axis, but may occur if functions such as <code>Lead</code>,
     * <code>NextMember</code> and <code>ParentMember</code> walk off the end
     * of the hierarchy.
     *
     * @post return != null
     */
    Member getNullMember();

    boolean hasAll();

    /**
     * Creates a member of this hierarchy. If this is the measures hierarchy, a
     * calculated member is created, and <code>formula</code> must not be null.
     */
    Member createMember(
        Member parent, Level level, String name, Formula formula);

    /**
     * Returns the unique name of this hierarchy, always including the dimension
     * name, e.g. "[Time].[Time]", regardless of whether
     * {@link MondrianProperties#SsasCompatibleNaming} is enabled.
     *
     * @deprecated Will be removed in mondrian-4.0, when
     * {@link #getUniqueName()} will have this behavior.
     *
     * @return Unique name of hierarchy.
     */
    String getUniqueNameSsas();
}

// End Hierarchy.java
