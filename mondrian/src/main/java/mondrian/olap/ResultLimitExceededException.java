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
 * Abstract base class for exceptions that indicate some limit was exceeded.
 */
public abstract class ResultLimitExceededException extends MondrianException {

    /**
     * Creates a ResultLimitExceededException.
     *
     * @param message Localized message
     */
    public ResultLimitExceededException(String message) {
        super(message);
    }
}

// End ResultLimitExceededException.java
