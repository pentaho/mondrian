/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2005-2005 Julian Hyde and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.recorder;

import org.apache.log4j.Logger;

/**
 * Implementation of {@link MessageRecorder} that writes to a
 * {@link Logger log4j logger}.
 *
 * @author <a>Richard M. Emberson</a>
 * @version $Id$
 */
public class LoggerRecorder extends AbstractRecorder {
    private final Logger logger;

    public LoggerRecorder(final Logger logger) {
        this.logger = logger;
    }

    protected void recordMessage(
            final String msg,
            final Object info,
            final int msgType) {
        String context = getContext();
        logMessage(context, msg, msgType, logger);
    }
}

// End LoggerRecorder.java
