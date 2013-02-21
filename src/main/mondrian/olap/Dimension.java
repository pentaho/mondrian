/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 1999-2005 Julian Hyde
// Copyright (C) 2005-2013 Pentaho and others
// All Rights Reserved.
*/
package mondrian.olap;

import org.olap4j.impl.Named;

import java.util.List;

/**
 * A <code>Dimension</code> represents a dimension of a cube.
 *
 * @author jhyde, 1 March, 1999
 */
public interface Dimension extends OlapElement, Annotated, Named {
    final String MEASURES_UNIQUE_NAME = "[Measures]";
    final String MEASURES_NAME = "Measures";

    /**
     * {@inheritDoc}
     *
     * @deprecated Uses of this method are suspect, if a dimension has more than
     * one hierarchy. Hierarchy.getDimension().getHierarchy() will give you back
     * a different hierarchy. If you have a specific hierarchy, use it.
     */
    Hierarchy getHierarchy();

    /**
     * Returns an array of the hierarchies which belong to this dimension.
     *
     * @deprecated Use {@link #getHierarchyList()}.
     */
    Hierarchy[] getHierarchies();

    /**
     * Returns a list of hierarchies in this dimension.
     */
    List<? extends Hierarchy> getHierarchyList();

    /**
     * Returns whether this is the <code>[Measures]</code> dimension.
     */
    boolean isMeasures();

    /**
     * Returns the type of this dimension.
     */
    org.olap4j.metadata.Dimension.Type getDimensionType();

    /**
     * Returns the schema this dimension belongs to.
     */
    Schema getSchema();
}

// End Dimension.java
