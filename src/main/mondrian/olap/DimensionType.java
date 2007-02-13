/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2004-2005 Galt Johnson
// Copyright (C) 2004-2006 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap;

/**
 * Enumerates the types of dimensions.
 *
 * @author Galt Johnson
 * @since 5 April, 2004
 * @version $Id$
 */
public enum DimensionType {
    /**
     * Indicates that the dimension is not related to time.
     */
    StandardDimension,

    /**
     * Indicates that a dimension is a time dimension.
     */
    TimeDimension
}

// End DimensionType.java
