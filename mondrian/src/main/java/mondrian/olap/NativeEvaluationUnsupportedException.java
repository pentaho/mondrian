/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

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
