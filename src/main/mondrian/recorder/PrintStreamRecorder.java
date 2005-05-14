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
            final int msgType) {
        PrintStream ps = null;
        String prefix = null;
        switch (msgType) {
        case INFO_MSG_TYPE :
            prefix = "INFO: ";
            ps = out;
            break;
        case WARN_MSG_TYPE :
            prefix = "WARN: ";
            ps = out;
            break;
        case ERROR_MSG_TYPE :
            prefix = "ERROR: ";
            ps = err;
            break;
        default :
            prefix = "UNKNOWN: ";
        }
        String context = getContext();

        ps.print(prefix);
        ps.print(context);
        ps.print(": ");
        ps.println(msg);
    }
}

// End PrintStreamRecorder.java
