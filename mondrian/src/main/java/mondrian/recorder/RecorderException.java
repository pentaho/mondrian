/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2005-2005 Julian Hyde
// Copyright (C) 2005-2006 Pentaho and others
// All Rights Reserved.
*/

package mondrian.recorder;

import mondrian.olap.MondrianException;

/**
 * Exception thrown by {@link MessageRecorder} when too many errors
 * have been reported.
 *
 * @author Richard M. Emberson
 */
public final class RecorderException extends MondrianException {
     protected RecorderException(String msg) {
        super(msg);
    }
}

// End RecorderException.java
