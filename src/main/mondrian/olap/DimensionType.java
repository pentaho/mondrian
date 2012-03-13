/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2004-2005 Galt Johnson
// Copyright (C) 2004-2005 Julian Hyde
// Copyright (C) 2005-2007 Pentaho and others
// All Rights Reserved.
*/
package mondrian.olap;

/**
 * Enumerates the types of dimensions.
 *
 * @author Galt Johnson
 * @since 5 April, 2004
 */
public enum DimensionType {
    /**
     * Indicates that the dimension is not related to time.
     */
    StandardDimension,

    /**
     * Indicates that a dimension is a time dimension.
     */
    TimeDimension,

    /**
     * Indicates the a dimension is the measures dimension.
     */
    MeasuresDimension,
}

// End DimensionType.java
