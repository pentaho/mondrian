/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2004-2002 Kana Software, Inc.
// Copyright (C) 2004-2005 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/

package mondrian.olap;

/**
 * Instances of this class are thrown for all exceptions that Mondrian
 * generates through as a result of known error conditions. It is used in the
 * resource classes generated from mondrian.resource.MondrianResource.xml.
 *
 * @author Galt Johnson (gjabx)
 * @version $Id$
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
