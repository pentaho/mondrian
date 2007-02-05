/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2007-2007 Julian Hyde and others
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
    extends ResultLimitExceededException {
    
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
