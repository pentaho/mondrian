/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2002-2005 Julian Hyde
// Copyright (C) 2005-2005 Pentaho and others
// All Rights Reserved.
*/

package mondrian.olap;

/**
 * Instances of this class are thrown for all exceptions that Mondrian
 * generates through as a result of known error conditions. It is used in the
 * resource classes generated from mondrian.resource.MondrianResource.xml.
 *
 * @author Galt Johnson (gjabx)
 * @see org.eigenbase.xom
 */
public class MondrianException extends RuntimeException {
    public MondrianException() {
        super();
    }

    public MondrianException(Throwable cause) {
        super(cause);
    }

    public MondrianException(String message) {
        super(message);
    }

    public MondrianException(String message, Throwable cause) {
        super(message, cause);
    }

    public String getLocalizedMessage() {
        return getMessage();
    }

    public String getMessage() {
        return "Mondrian Error:" + super.getMessage();
    }
}

// End MondrianException.java
