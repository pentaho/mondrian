/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 1999-2005 Julian Hyde
// Copyright (C) 2005-2009 Pentaho and others
// All Rights Reserved.
*/
package mondrian.olap;

/**
 * A <code>Dimension</code> represents a dimension of a cube.
 *
 * @author jhyde, 1 March, 1999
 */
public interface Dimension extends OlapElement, Annotated {
    final String MEASURES_UNIQUE_NAME = "[Measures]";
    final String MEASURES_NAME = "Measures";

    /**
     * Returns an array of the hierarchies which belong to this dimension.
     */
    Hierarchy[] getHierarchies();

    /**
     * Returns whether this is the <code>[Measures]</code> dimension.
     */
    boolean isMeasures();

    /**
     * Returns the type of this dimension
     * ({@link DimensionType#StandardDimension} or
     * {@link DimensionType#TimeDimension}
     */
    DimensionType getDimensionType();

    /**
     * Returns the schema this dimension belongs to.
     */
    Schema getSchema();

    /**
     * Returns whether the dimension should be considered as a "high
     * cardinality" or "low cardinality" according to cube definition.
     *
     * Mondrian tends to evaluate high cardinality dimensions using
     * iterators rather than lists, avoiding instantiating the dimension in
     * memory.
     *
     * @return whether this dimension is high-cardinality
     */
    boolean isHighCardinality();
}

// End Dimension.java
