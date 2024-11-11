/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package mondrian.recorder;

import org.apache.logging.log4j.Logger;

/**
 * Implementation of {@link MessageRecorder} that writes to a
 * {@link Logger log4j logger}.
 *
 * @author Richard M. Emberson
 */
public class LoggerRecorder extends AbstractRecorder {
    private final Logger logger;

    public LoggerRecorder(final Logger logger) {
        this.logger = logger;
    }

    protected void recordMessage(
        final String msg,
        final Object info,
        final MsgType msgType)
    {
        String context = getContext();
        logMessage(context, msg, msgType, logger);
    }
}

// End LoggerRecorder.java
