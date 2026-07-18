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
 * Exception which indicates that native evaluation of a function
 * was enabled but not supported, and
 * {@link MondrianProperties#AlertNativeEvaluationUnsupported} was
 * set to <code>ERROR</code>.
 *
 * @author John Sichi
 */
public class NativeEvaluationUnsupportedException
    extends ResultLimitExceededException
{

    /**
     * Creates a NativeEvaluationUnsupportedException.
     *
     * @param message Localized error message
     */
    public NativeEvaluationUnsupportedException(String message) {
        super(message);
    }
}

// End NativeEvaluationUnsupportedException.java
