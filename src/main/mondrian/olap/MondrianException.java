/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2004 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
*/

package mondrian.olap;

/**
 * Instances of this class are thrown for all exceptions that Mondrian generates
 * through as a result of known error conditions. It is used in the resource classes
 * generated from mondrian.olap.MondrianResource.xml.
 * @author Galt Johnson (gjabx)
 * @see mondrian.xom
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
