/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2003 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.resource;

/**
 * @deprecated use {@link RuntimeException}
 **/
public class ChainableRuntimeException extends RuntimeException {
    public ChainableRuntimeException(String message) {
        super(message);
    }

    public ChainableRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }

    public ChainableRuntimeException(Throwable cause) {
        super(cause);
    }
}

// End ChainableRuntimeException.java
