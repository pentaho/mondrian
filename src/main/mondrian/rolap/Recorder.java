/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2001-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 12 August, 2001
*/

package mondrian.rolap;

import mondrian.olap.Util;
import java.io.PrintStream;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

/** 
 * This class provides both an abstract implemention of the MessageRecorder
 * interface as well as three concrete implementions.
 * 
 * @author <a>Richard M. Emberson</a>
 * @version 
 */
public abstract class Recorder implements MessageRecorder {
    /** 
     * Simply writes messages to PrintStreams.
     */
    public static class Std extends Recorder {
        private final PrintStream err;
        private final PrintStream out;
        public Std() {
            this(System.out, System.err);
        }
        public Std(final PrintStream out, final PrintStream err) {
            this.out = out;
            this.err = err;
        }
        protected void recordMessage(final String msg, 
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

    /** 
     * Helper method to format a message and write to logger.
     */
    public static void logMessage(final String context, 
                                  final String msg,
                                  final int msgType,
                                  final org.apache.log4j.Logger logger) {
            StringBuffer buf = new StringBuffer(64);
            buf.append(context);
            buf.append(": ");
            buf.append(msg);

            switch (msgType) {
            case INFO_MSG_TYPE :
                logger.info(buf.toString());
                break;
            case WARN_MSG_TYPE :
                logger.warn(buf.toString());
                break;
            case ERROR_MSG_TYPE :
                logger.error(buf.toString());
                break;
            default :
                logger.warn(
                    "Unknown message type enum \"" +
                    msgType +
                    "\" for message: " +
                    buf.toString()
                );
            }
    }

    /** 
     * MessageRecorder that writes to a logger.
     */
    public static class Logger extends Recorder {
        private final org.apache.log4j.Logger logger;

        public Logger(final org.apache.log4j.Logger logger) {
            this.logger = logger;
        }
        protected void recordMessage(final String msg, 
                                     final Object info,
                                     final int msgType) {
            String context = getContext();

            logMessage(context, msg, msgType, logger);
        }
    }

    public static void logMessage(final Lists.Entry e,
                                  final org.apache.log4j.Logger logger) {
        logMessage(e.getContext(), e.getMessage(), e.getMsgType(), logger);
    }

    /** 
     * MessageRecorder that records all of the messages in lists allowing the
     * calling code to access the list and take actions as needed.
     */
    public static class Lists extends Recorder {

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

        private final List errorList;
        private final List warnList;
        private final List infoList;

        public Lists() {
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
        public void logInfoMessage(final org.apache.log4j.Logger logger) {
            if (hasInformation()) {
                logMessage(getInfoEntries(), logger);
            }
        }
        public void logWarningMessage(final org.apache.log4j.Logger logger) {
            if (hasWarnings()) {
                logMessage(getWarnEntries(), logger);
            }
        }
        public void logErrorMessage(final org.apache.log4j.Logger logger) {
            if (hasErrors()) {
                logMessage(getErrorEntries(), logger);
            }
        }
        protected void logMessage(final Iterator it, 
                                  final org.apache.log4j.Logger logger) {
            while (it.hasNext()) {
                Recorder.Lists.Entry e = (Recorder.Lists.Entry) it.next();
                Recorder.logMessage(e, logger);
            }
        }
    }
    public static final int INFO_MSG_TYPE       = 1;
    public static final int WARN_MSG_TYPE       = 2;
    public static final int ERROR_MSG_TYPE      = 3;

    public static final int DEFAULT_MSG_LIMIT = 10;

    private final int errorMsgLimit;
    private final List contexts;
    private int errorMsgCount;
    private int warningMsgCount;
    private int infoMsgCount;
    private String contextMsgCache;
    private long startTime;

    protected Recorder() {
        this(DEFAULT_MSG_LIMIT);
    }
    protected Recorder(final int errorMsgLimit) {
        this.errorMsgLimit = errorMsgLimit;
        this.contexts = new ArrayList();
        this.startTime = System.currentTimeMillis();
    }

    /** 
     * Reset this MessageRecorder. 
     */
    public void clear() {
        errorMsgCount = 0;
        warningMsgCount = 0;
        infoMsgCount = 0;
        contextMsgCache = null;
        contexts.clear();
        this.startTime = System.currentTimeMillis();
    }
    public long getStartTimeMillis() {
        return this.startTime;
    }

    public long getRunTimeMillis() {
        return (System.currentTimeMillis() - this.startTime);
    }

    public boolean hasInformation() {
        return (infoMsgCount > 0);
    }
    public boolean hasWarnings() {
        return (warningMsgCount > 0);
    }
    public boolean hasErrors() {
        return (errorMsgCount > 0);
    }
    public int getInfoCount() {
        return infoMsgCount;
    }
    public int getWarningCount() {
        return warningMsgCount;
    }
    public int getErrorCount() {
        return errorMsgCount;
    }

    public String getContext() {
        // heavy weight
        if (contextMsgCache == null) {
            final StringBuffer buf = new StringBuffer();
            for (Iterator it = contexts.iterator(); it.hasNext();) {
                String name = (String) it.next();
                buf.append(name);
                if (it.hasNext()) {
                    buf.append(':');
                }
            }
            contextMsgCache = buf.toString();
        }
        return contextMsgCache;
    }
    public void pushContextName(final String name) {
        // light weight
        contexts.add(name);
        contextMsgCache = null;
    }
    public void popContextName() {
        // light weight
        contexts.remove(contexts.size()-1);
        contextMsgCache = null;
    }

    public void throwRTException() throws RTException {
        if (hasErrors()) {
            final String errorMsg =
                    Util.getRes().getForceMessageRecorderError(
                                        getContext(),
                                        new Integer(errorMsgCount));
            throw new MessageRecorder.RTException(errorMsg);
        }
    }

    public void reportError(final Exception ex) 
                    throws MessageRecorder.RTException {
        reportError(ex, null);
    }

    public void reportError(final Exception ex, final Object info) 
                    throws MessageRecorder.RTException {
        reportError(ex.toString(), info);
    }

    public void reportError(final String msg)
                    throws MessageRecorder.RTException {
        reportError(msg, null);
    }
    public void reportError(final String msg, final Object info)
                    throws MessageRecorder.RTException {
        errorMsgCount++;
        recordMessage(msg, info, ERROR_MSG_TYPE);

        if (errorMsgCount >= errorMsgLimit) {
            final String errorMsg =
                    Util.getRes().getTooManyMessageRecorderErrors(
                                        getContext(),
                                        new Integer(errorMsgCount));
            throw new MessageRecorder.RTException(errorMsg);
        }
    }

    public void reportWarning(final String msg) {
        reportWarning(msg, null);
    }
    public void reportWarning(final String msg, final Object info) {
        warningMsgCount++;
        recordMessage(msg, info, WARN_MSG_TYPE);
    }

    public void reportInfo(final String msg) {
        reportInfo(msg, null);
    }
    public void reportInfo(final String msg, final Object info) {
        infoMsgCount++;
        recordMessage(msg, info, INFO_MSG_TYPE);
    }
    
    /** 
     * Classes implementing this abstract class must provide an implemention
     * of this method which receives all warning/error messages.
     * 
     * @param msg the error or warning message.
     * @param info the information Object which might be null.
     * @param msgType one of the message type enum values
     */
    protected abstract void recordMessage(final String msg, 
                                          final Object info,
                                          final int msgType);
}
