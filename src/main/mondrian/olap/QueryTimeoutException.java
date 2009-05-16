/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2004-2005 TONBELLER AG
// Copyright (C) 2005-2007 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap;

/**
 * Exception which indicates that a query executed for longer than its allowed
 * time and was automatically canceled.
 *
 * @version $Id$
 */
public class QueryTimeoutException extends ResultLimitExceededException {
    /**
     * Creates a QueryTimeoutException.
     *
     * @param message Localized error message
     */
    public QueryTimeoutException(String message) {
        super(message);
    }
}

// End QueryTimeoutException.java
