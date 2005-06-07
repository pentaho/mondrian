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

import java.util.*;

/**
 * Implementation of {@link MessageRecorder} that records each message
 * in a {@link List}. The calling code can then access the list and take
 * actions as needed.
 */
public class ListRecorder extends AbstractRecorder {

    private final List errorList;
    private final List warnList;
    private final List infoList;

    public ListRecorder() {
        errorList = new ArrayList();
        warnList = new ArrayList();
        infoList = new ArrayList();
    }
    public void clear() {
        super.clear();
        errorList.clear();
        warnList.clear();
        infoList.clear();
    }
    public Iterator getErrorEntries() {
        return errorList.iterator();
    }
    public Iterator getWarnEntries() {
        return warnList.iterator();
    }
    public Iterator getInfoEntries() {
        return infoList.iterator();
    }
    protected void recordMessage(final String msg,
                                 final Object info,
                                 final int msgType) {
        String context = getContext();

        Entry e = new Entry(context, msg, msgType, info);
        switch (msgType) {
        case INFO_MSG_TYPE :
            infoList.add(e);
            break;
        case WARN_MSG_TYPE :
            warnList.add(e);
            break;
        case ERROR_MSG_TYPE :
            errorList.add(e);
            break;
        default :
            e = new Entry(
                context,
                "Unknown message type enum \"" +
                msgType +
                "\" for message: " + msg,
                WARN_MSG_TYPE,
                info);
            warnList.add(e);
        }
    }

    public void logInfoMessage(final Logger logger) {
        if (hasInformation()) {
            logMessage(getInfoEntries(), logger);
        }
    }

    public void logWarningMessage(final Logger logger) {
        if (hasWarnings()) {
            logMessage(getWarnEntries(), logger);
        }
    }

    public void logErrorMessage(final Logger logger) {
        if (hasErrors()) {
            logMessage(getErrorEntries(), logger);
        }
    }

    static void logMessage(Iterator it, Logger logger) {
        while (it.hasNext()) {
            Entry e = (Entry) it.next();
            logMessage(e, logger);
        }
    }

    static void logMessage(
            final Entry e,
            final Logger logger) {
        logMessage(e.getContext(), e.getMessage(), e.getMsgType(), logger);
    }

    /**
     * Entry is a Info, Warning or Error message. This is the object stored
     * in the Lists MessageRecorder's info, warning and error message lists.
     */
    public static class Entry {
        private final String context;
        private final String msg;
        private final int msgType;
        private final Object info;

        private Entry(final String context,
                      final String msg,
                      final int msgType,
                      final Object info) {
            this.context = context;
            this.msg = msg;
            this.msgType = msgType;
            this.info = info;
        }
        public String getContext() {
            return context;
        }
        public String getMessage() {
            return msg;
        }
        public int getMsgType() {
            return msgType;
        }
        public Object getInfo() {
            return info;
        }
    }
}

// End ListRecorder.java
