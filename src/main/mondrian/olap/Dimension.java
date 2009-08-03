/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 1999-2002 Kana Software, Inc.
// Copyright (C) 2001-2008 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 1 March, 1999
*/

package mondrian.olap;

/**
 * A <code>Dimension</code> represents a dimension of a cube.
 */
public interface Dimension extends OlapElement {
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
     * Returns dimension's ordinal within a given cube.
     * The <code>[Measures]</code> always has ordinal 0.
     */
    int getOrdinal(Cube cube);

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
