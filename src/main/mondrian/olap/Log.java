/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2005-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/

package mondrian.olap;


import java.io.StringWriter;
import java.io.PrintWriter;

/** 
 * Simple and fast logger.
 * 
 * @author Richard M. Emberson
 * @version 
 */
public class Log {
    public static final int BAD_LEVEL       = -1;
    public static final int NONE_LEVEL      = 0;

    public static final int CRITICAL_LEVEL  = 1;
    public static final int ERROR_LEVEL     = 2;
    public static final int WARN_LEVEL      = 3;
    public static final int LOG_LEVEL       = 4;
    public static final int DEBUG_LEVEL     = 5;
    public static final int TRACE_LEVEL     = 6;

    public static final String UNKNOWN_STR   = "UNKNOWN";
    public static final String BAD_STR       = "BAD";
    public static final String NONE_STR      = "NONE";

    public static final String CRITICAL_STR  = "CRITICAL";
    public static final String ERROR_STR     = "ERROR";
    public static final String WARN_STR      = "WARN";
    public static final String LOG_STR       = "LOG";
    public static final String DEBUG_STR     = "DEBUG";
    public static final String TRACE_STR     = "TRACE";


    // these MUST be before the static block below
    private static int level = WARN_LEVEL;
    private static final StringBuffer buf = new StringBuffer(2048);
    private static final StringWriter sw = new StringWriter(4096);
    private static String LOG_PROP = "mondrian.log.level";

    static {
        // set log level via property
        String levelStr = System.getProperty(LOG_PROP);
        if (levelStr != null) {
            setLevel(levelStr);
        }
    }

    /** 
     * Given the name of a log level, return the associated value. 
     * 
     * @param levelStr 
     * @return 
     */
    public static int lookupLevel(String levelStr) {
        if (levelStr == null) {
            return Log.BAD_LEVEL;
        } else {
            if (levelStr.equalsIgnoreCase(Log.NONE_STR)) {
                return Log.NONE_LEVEL;
            } else if (levelStr.equalsIgnoreCase(Log.CRITICAL_STR)) {
                return Log.CRITICAL_LEVEL;
            } else if (levelStr.equalsIgnoreCase(Log.ERROR_STR)) {
                return Log.ERROR_LEVEL;
            } else if (levelStr.equalsIgnoreCase(Log.WARN_STR)) {
                return Log.WARN_LEVEL;
            } else if (levelStr.equalsIgnoreCase(Log.LOG_STR)) {
                return Log.LOG_LEVEL;
            } else if (levelStr.equalsIgnoreCase(Log.DEBUG_STR)) {
                return Log.DEBUG_LEVEL;
            } else if (levelStr.equalsIgnoreCase(Log.TRACE_STR)) {
                return Log.TRACE_LEVEL;
            } else {
                return Log.BAD_LEVEL;
            }
        }
    }
    
    /** 
     * Given a log level value gets its name.
     * 
     * @param level 
     * @return 
     */
    public static String lookupLevelName(int level) {
        String levelStr = null;
        switch (level) {
            case Log.BAD_LEVEL :
                levelStr = BAD_STR;
                break;
            case Log.NONE_LEVEL :
                levelStr = NONE_STR;
                break;
            case Log.CRITICAL_LEVEL :
                levelStr = CRITICAL_STR;
                break;
            case Log.ERROR_LEVEL :
                levelStr = ERROR_STR;
                break;
            case Log.WARN_LEVEL :
                levelStr = WARN_STR;
                break;
            case Log.LOG_LEVEL :
                levelStr = LOG_STR;
                break;
            case Log.DEBUG_LEVEL :
                levelStr = DEBUG_STR;
                break;
            case Log.TRACE_LEVEL :
                levelStr = TRACE_STR;
                break;
            default :
                levelStr = UNKNOWN_STR;
        }
        return levelStr;
    }
    public static int getLevel() {
        return Log.level;
    }
    public static void setLevel(String levelStr) {
        int level = lookupLevel(levelStr);
        if (level == Log.BAD_LEVEL) {
            print(WARN_STR, 
                "Log.setLevel: Bad log level name \"" +levelStr +"\"");
        } else {
            setLevel(level);
        }
    }
    public static void setLevel(int level) {
        if (level < Log.NONE_LEVEL) {
            print(Log.WARN_STR, "Log.setLevel: Bad log level=" +level);
        } else if (level > Log.TRACE_LEVEL) {
            print(Log.WARN_STR, "Log.setLevel: Bad log level=" +level);
        } else {
            Log.level = level;
        }

    }

    /////////////////////////////////////////////////////////////////////////
    // trace
    /////////////////////////////////////////////////////////////////////////
    public static boolean isTrace() {
        return (Log.level >= Log.TRACE_LEVEL);
    }
    public static void trace(String msg) {
        if (isTrace()) {
            print(Log.TRACE_STR, msg);
        }
    }
    public static void trace(String msg, Throwable t) {
        if (isTrace()) {
            print(Log.TRACE_STR, msg, t);
        }
    }

    /////////////////////////////////////////////////////////////////////////
    // debug
    /////////////////////////////////////////////////////////////////////////
    public static boolean isDebug() {
        return (Log.level >= Log.DEBUG_LEVEL);
    }
    public static void debug(String msg) {
        if (isDebug()) {
            print(Log.DEBUG_STR, msg);
        }
    }
    public static void debug(String msg, Throwable t) {
        if (isDebug()) {
            print(Log.DEBUG_STR, msg, t);
        }
    }

    /////////////////////////////////////////////////////////////////////////
    // log
    /////////////////////////////////////////////////////////////////////////
    public static boolean isLog() {
        return (Log.level >= Log.LOG_LEVEL);
    }
    public static void log(String msg) {
        if (isLog()) {
            print(Log.LOG_STR, msg);
        }
    }
    public static void log(String msg, Throwable t) {
        if (isLog()) {
            print(Log.LOG_STR, msg, t);
        }
    }

    /////////////////////////////////////////////////////////////////////////
    // warn
    /////////////////////////////////////////////////////////////////////////
    public static boolean isWarn() {
        return (Log.level >= Log.WARN_LEVEL);
    }
    public static void warn(String msg) {
        if (isWarn()) {
            print(Log.WARN_STR, msg);
        }
    }
    public static void warn(String msg, Throwable t) {
        if (isWarn()) {
            print(Log.WARN_STR, msg, t);
        }
    }

    /////////////////////////////////////////////////////////////////////////
    // error
    /////////////////////////////////////////////////////////////////////////
    public static boolean isError() {
        return (Log.level >= Log.ERROR_LEVEL);
    }
    public static void error(String msg) {
        if (isError()) {
            print(Log.ERROR_STR, msg);
        }
    }
    public static void error(String msg, Throwable t) {
        if (isError()) {
            print(Log.ERROR_STR, msg, t);
        }
    }

    /////////////////////////////////////////////////////////////////////////
    // critical
    /////////////////////////////////////////////////////////////////////////
    public static boolean isCritical() {
        return (Log.level >= Log.CRITICAL_LEVEL);
    }
    public static void critial(String msg) {
        if (isCritical()) {
            print(Log.CRITICAL_STR, msg);
        }
    }
    public static void critial(String msg, Throwable t) {
        if (isCritical()) {
            print(Log.CRITICAL_STR, msg, t);
        }
    }

    /////////////////////////////////////////////////////////////////////////
    // implementation (simple)
    /////////////////////////////////////////////////////////////////////////
    private static void print(String levelStr, String msg) {
        print(levelStr, msg, null);
    }
    
    /** 
     * Synchronized so that a single StringBuffer can be used/reused. 
     * 
     * @param levelStr 
     * @param msg 
     * @param t 
     * @return 
     */
    private synchronized static void print(String levelStr, 
                                           String msg, 
                                           Throwable t) {
        // reset buffers
        Log.buf.setLength(0);
        Log.sw.getBuffer().setLength(0);

        Log.buf.append(levelStr);
        Log.buf.append(": ");
        if (msg != null) {
            Log.buf.append(msg);
        }
        if (t != null) {
            formatError(Log.buf, t);

            PrintWriter pw = new PrintWriter(Log.sw);
            t.printStackTrace(pw);
            pw.flush();
            Log.buf.append(Log.sw.toString());
        }

        System.out.println(Log.buf.toString());

    }
	private static void formatError(StringBuffer buf, Throwable t) {
		String message = t.getMessage();
        buf.append(message);

		if ((t.getCause() != null) && (t.getCause() != t)) {
            buf.append('\n');
            formatError(buf, t.getCause());
        }
	}

    private Log() {}
}
