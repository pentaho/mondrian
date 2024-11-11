/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/


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
    LevelType getLevelType();

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
