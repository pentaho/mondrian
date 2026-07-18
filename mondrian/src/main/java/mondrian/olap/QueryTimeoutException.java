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
 * Exception which indicates that a query executed for longer than its allowed
 * time and was automatically canceled.
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
