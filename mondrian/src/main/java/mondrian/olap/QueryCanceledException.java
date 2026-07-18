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
 * Exception which indicates that a query was canceled by an end-user.
 *
 * <p>See also {@link mondrian.olap.QueryTimeoutException}, which indicates that
 * a query was canceled automatically due to a timeout.
 */
public class QueryCanceledException extends ResultLimitExceededException {
    /**
     * Creates a QueryCanceledException.
     *
     * @param message Localized error message
     */
    public QueryCanceledException(String message) {
        super(message);
    }
}

// End QueryCanceledException.java
