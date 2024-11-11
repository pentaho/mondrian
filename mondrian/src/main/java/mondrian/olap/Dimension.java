/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package mondrian.olap;

/**
 * A <code>Dimension</code> represents a dimension of a cube.
 *
 * @author jhyde, 1 March, 1999
 */
public interface Dimension extends OlapElement, Annotated {
  String MEASURES_UNIQUE_NAME = "[Measures]";
  String MEASURES_NAME = "Measures";

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
   * <p>
   * Mondrian tends to evaluate high cardinality dimensions using
   * iterators rather than lists, avoiding instantiating the dimension in
   * memory.
   *
   * @return whether this dimension is high-cardinality
   */
  boolean isHighCardinality();
}
