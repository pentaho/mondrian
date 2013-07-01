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

import mondrian.spi.MemberFormatter;

/**
 * A <code>Level</code> is a group of {@link Member}s in a {@link Hierarchy},
 * all with the same attributes and at the same depth in the hierarchy.
 *
 * @author jhyde, 1 March, 1999
 */
public interface Level extends OlapElement, Annotated {

    /**
     * Returns the depth of this level.
     *
     * <p>Note #1: In an access-controlled context, the first visible level of
     * a hierarchy (as returned by {@link SchemaReader#getHierarchyLevels}) may
     * not have a depth of 0.</p>
     *
     * <p>Note #2: In a parent-child hierarchy, the depth of a member (as
     * returned by {@link SchemaReader#getMemberDepth}) may not be the same as
     * the depth of its level.
     */
    int getDepth();
    Hierarchy getHierarchy();

    Level getChildLevel();
    Level getParentLevel();
    boolean isAll();
    boolean areMembersUnique();
    org.olap4j.metadata.Level.Type getLevelType();

    /** Returns properties defined against this level. */
    Property[] getProperties();
    /** Returns properties defined against this level and parent levels. */
    Property[] getInheritedProperties();

    /**
      * Returns the object that is used to format members of this level.
      */
    MemberFormatter getMemberFormatter();

    /**
     * Returns the approximate number of members in this level, or
     * {@link Integer#MIN_VALUE} if no approximation is known.
     */
    int getApproxRowCount();
}

// End Level.java
