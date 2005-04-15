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
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

public abstract class AbstractRecorder implements MessageRecorder {
    public static final int DEFAULT_MSG_LIMIT = 10;

    private final int errorMsgLimit;
    private final List contexts;
    private int errorMsgCount;
    private int warningMsgCount;
    private String contextMsgCache;

    protected AbstractRecorder() {
        this(DEFAULT_MSG_LIMIT);
    }
    protected AbstractRecorder(final int errorMsgLimit) {
        this.errorMsgLimit = errorMsgLimit;
        this.contexts = new ArrayList();
    }

    public void clear() {
        errorMsgCount = 0;
        warningMsgCount = 0;
        contextMsgCache = null;
        contexts.clear();
    }
    public boolean hasWarnings() {
        return (warningMsgCount > 0);
    }
    public boolean hasErrors() {
        return (errorMsgCount > 0);
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
            Iterator it = contexts.iterator();
            while (it.hasNext()) {
                String name = (String) it.next();
                buf.append(name);
                if (it.hasNext()) {
                    buf.append('.');
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
        if (errorMsgCount < errorMsgLimit) {
            recordMessage(msg, info, true);
        } else {
            final String errorMsg =
                    Util.getRes().getTooManyMessageRecorderErrors(
                                        getContext(),
                                        new Integer(errorMsgCount));
            recordMessage(errorMsg, info, true);
            throw new MessageRecorder.RTException(errorMsg);
        }
    }

    public void reportWarning(final String msg) {
        reportWarning(msg, null);
    }
    public void reportWarning(final String msg, final Object info) {
        warningMsgCount++;
        recordMessage(msg, info, false);
    }
    
    /** 
     * Classes implementing this abstract class must provide an implemention
     * of this method which receives all warning/error messages.
     * 
     * @param msg the error or warning message.
     * @param info the information Object which might be null.
     * @param isError if true, it is an error otherwise it is a warning
     */
    protected abstract void recordMessage(final String msg, 
                                          final Object info,
                                          final boolean isError);
}
