/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 18 June, 2001
*/

package mondrian.xom;

/**
 * XOMException extends Exception and provides detailed error messages for
 * xom-specific exceptions.
 */
public class XOMException extends Exception {

    /**
     * Constructs a mining exception with no message.
     */
    public XOMException()
    {
        super(null,null);
    }

    /**
     * Constructs an exception with a detailed message.
     *
     *@param s - a detailed message describing the specific error
     */
    public XOMException(String s)
    {
        super(s,null);
    }

    /**
     * Constructs an exception based on another exception, so that
     * the exceptions may be chained.
     * @param cause the exception on which this one is based.
     * @param s a message for this portion of the exception.
     */
    public XOMException(Throwable cause, String s)
    {
        super(s,cause);
    }
}

// End XOMException.java
