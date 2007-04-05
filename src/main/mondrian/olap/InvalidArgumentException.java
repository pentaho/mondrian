/*
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2005-2007 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap;

/**
 * Exception which indicates that an argument is invalid
 *
 * @author Thiyagu
 * @since April 5, 2007
 * @version $Id$
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