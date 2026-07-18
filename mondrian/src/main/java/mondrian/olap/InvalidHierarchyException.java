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
 * Exception which indicates that a Cube is invalid
 * because there is a hierarchy with no members.
 */
public class InvalidHierarchyException extends MondrianException {
    /**
     * Creates a InvalidHierarchyException.
     *
     * @param message Localized error message
     */
    public InvalidHierarchyException(String message) {
        super(message);
    }
}

// End InvalidHierarchyException.java
