/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2007-2009 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap;

/**
 * Exception which indicates that native evaluation of a function
 * was enabled but not supported, and
 * {@link MondrianProperties#AlertNativeEvaluationUnsupported} was
 * set to <code>ERROR</code>.
 *
 * @author John Sichi
 * @version $Id$
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
