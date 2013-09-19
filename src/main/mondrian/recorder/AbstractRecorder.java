/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2005-2005 Julian Hyde
// Copyright (C) 2005-2011 Pentaho and others
// All Rights Reserved.
*/

package mondrian.recorder;

import mondrian.resource.MondrianResource;

import java.util.ArrayList;
import java.util.List;

/**
 * Abstract implemention of the {@link MessageRecorder} interface.
 *
 * @author Richard M. Emberson
 */
public abstract class AbstractRecorder implements MessageRecorder {

    /**
     * Helper method to format a message and write to logger.
     */
    public static void logMessage(
        final String context,
        final String msg,
        final MsgType msgType,
        final org.apache.log4j.Logger logger)
    {
        StringBuilder buf = new StringBuilder(64);
        buf.append(context);
        buf.append(": ");
        buf.append(msg);

        switch (msgType) {
        case INFO:
            logger.info(buf.toString());
            break;
        case WARN:
            logger.warn(buf.toString());
            break;
        case ERROR:
            logger.error(buf.toString());
            break;
        default:
            logger.warn(
                "Unknown message type enum \""
                + msgType + "\" for message: " + buf.toString());
        }
    }

    enum MsgType {
        INFO,
        WARN,
        ERROR
    }

    public static final int DEFAULT_MSG_LIMIT = 10;

    private final int errorMsgLimit;
    private final List<String> contexts;
    private int errorMsgCount;
    private int warningMsgCount;
    private int infoMsgCount;
    private String contextMsgCache;
    private long startTime;

    protected AbstractRecorder() {
        this(DEFAULT_MSG_LIMIT);
    }

    protected AbstractRecorder(final int errorMsgLimit) {
        this.errorMsgLimit = errorMsgLimit;
        this.contexts = new ArrayList<String>();
        this.startTime = System.currentTimeMillis();
    }

    /**
     * Resets this MessageRecorder.
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
            final StringBuilder buf = new StringBuilder();
            int k = 0;
            for (String name : contexts) {
                if (k++ > 0) {
                    buf.append(':');
                }
                buf.append(name);
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
        contexts.remove(contexts.size() - 1);
        contextMsgCache = null;
    }

    public void throwRTException() throws RecorderException {
        if (hasErrors()) {
            final String errorMsg =
                MondrianResource.instance().ForceMessageRecorderError.str(
                    getContext(),
                    errorMsgCount);
            throw new RecorderException(errorMsg);
        }
    }

    public void reportError(final Exception ex)
        throws RecorderException
    {
        reportError(ex, null);
    }

    public void reportError(final Exception ex, final Object info)
        throws RecorderException
    {
        reportError(ex.toString(), info);
    }

    public void reportError(final String msg)
        throws RecorderException
    {
        reportError(msg, null);
    }

    public void reportError(final String msg, final Object info)
        throws RecorderException
    {
        errorMsgCount++;
        recordMessage(msg, info, MsgType.ERROR);

        if (errorMsgCount >= errorMsgLimit) {
            final String errorMsg =
                MondrianResource.instance().TooManyMessageRecorderErrors.str(
                    getContext(),
                    errorMsgCount);
            throw new RecorderException(errorMsg);
        }
    }

    public void reportWarning(final String msg) {
        reportWarning(msg, null);
    }

    public void reportWarning(final String msg, final Object info) {
        warningMsgCount++;
        recordMessage(msg, info, MsgType.WARN);
    }

    public void reportInfo(final String msg) {
        reportInfo(msg, null);
    }

    public void reportInfo(final String msg, final Object info) {
        infoMsgCount++;
        recordMessage(msg, info, MsgType.INFO);
    }

    /**
     * Handles a message.
     * Classes implementing this abstract class must provide an implemention
     * of this method; it receives all warning/error messages.
     *
     * @param msg the error or warning message.
     * @param info the information Object which might be null.
     * @param msgType one of the message type enum values
     */
    protected abstract void recordMessage(
        String msg,
        Object info,
        MsgType msgType);
}

// End AbstractRecorder.java
