/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 - 2026 by Pentaho Canada Inc. : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2030-06-15
 ******************************************************************************/



package mondrian.olap;

/**
 * User-defined property on a metadata element.
 *
 * @see mondrian.olap.Annotated
 *
 * @author jhyde
 */
public interface Annotation {
    /**
     * Returns the name of this annotation. Must be unique within its element.
     *
     * @return Annotation name
     */
    String getName();

    /**
     * Returns the value of this annotation. Usually a string.
     *
     * @return Annotation value
     */
    Object getValue();
}

// End Annotation.java
