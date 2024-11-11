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

/**
 * Exception which indicates that an argument is invalid
 *
 * @author Thiyagu
 * @since April 5, 2007
 */
public class InvalidArgumentException extends MondrianException {
    /**
     * Creates a InvalidArgumentException.
     *
     * @param message Localized error message
     */
    public InvalidArgumentException(String message) {
        super(message);
    }
}

// End InvalidArgumentException.java
