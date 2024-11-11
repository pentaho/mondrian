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

import mondrian.olap.Util;

import java.io.PrintStream;

/**
 * Implementation of {@link MessageRecorder} simply writes messages to
 * PrintStreams.
 */
public class PrintStreamRecorder extends AbstractRecorder {
    private final PrintStream err;
    private final PrintStream out;

    public PrintStreamRecorder() {
        this(System.out, System.err);
    }

    public PrintStreamRecorder(final PrintStream out, final PrintStream err) {
        this.out = out;
        this.err = err;
    }

    protected void recordMessage(
        final String msg,
        final Object info,
        final MsgType msgType)
    {
        PrintStream ps;
        String prefix;
        switch (msgType) {
        case INFO:
            prefix = "INFO: ";
            ps = out;
            break;
        case WARN:
            prefix = "WARN: ";
            ps = out;
            break;
        case ERROR:
            prefix = "ERROR: ";
            ps = err;
            break;
        default:
            throw Util.unexpected(msgType);
        }
        String context = getContext();

        ps.print(prefix);
        ps.print(context);
        ps.print(": ");
        ps.println(msg);
    }
}

// End PrintStreamRecorder.java
