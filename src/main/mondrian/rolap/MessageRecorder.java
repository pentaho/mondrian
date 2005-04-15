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

/** 
 * Implemetations of this type are used to record warnings and errors
 * during the processing of a task. Contexts can be added and removed.
 * This allows one to collect more than one warning/error, keep processing, 
 * and then the code that initiated the processing can determine what to do 
 * with the warnings/errors if they exist.
 * <p>
 * A typical usage might be:
 * <pre><code>
 *    void process(MessageRecorder msgRecorder) {
 *      msgRecorder.pushContextName(getName());
 *      try {
 *          // prcess task
 *          ....
 *          // need to generate warning message
 *          String msg = ... 
 *          msgRecorder.reportWarning(msg);
 *          ....
 *      } finally {
 *          msgRecorder.popContextName();
 *      }
 *    }
 * <code></pre>
 * <p>
 * Implementations must provide the means for extracting the error/warning
 * messages.
 * <p>
 * Code that is processing should not catch the MessageRecorder.RTException.
 * This Exception is thrown by the MessageRecorder when too many errors have
 * been seen. Throwing this Exception is the mechanism used to stop processing
 * and return to the initiating code. The initiating code should expect to 
 * catch the MessageRecorder.RTException Exception.
 * <pre><code>
 *    void initiatingCode(MessageRecorder msgRecorder) {
 *      // get MessageRecorder implementation
 *      MessageRecorder msgRecorder = .... 
 *      try {
 *          processingCode(msgRecorder);
 *      } catch (MessageRecorder.RTException mrex) {
 *          // empty 
 *      }
 *      if (msgRecorder.hasErrors()) {
 *          // handle errors
 *      } else if (msgRecorder.hasWarnings()) {
 *          // handle warnings
 *      }
 *    }
 * <code></pre>
 * <p>
 * Lastly, the reporting methods all have variations that take an "info" Object.
 * This can be used to pass something, beyond a text message, from the point 
 * of warning/error to the initiating code.
 * 
 * @author <a>Richard M. Emberson</a>
 * @version 
 */
public interface MessageRecorder  {
    
    /** 
     * Exception thrown by MessageRecorder when too many errors have been
     * reported.
     */
    public final class RTException extends RuntimeException { 
         protected RTException(String msg) {
            super(msg);
        }
    }

    /** 
     * Clear all context, warnings and errors from the MessageRecorder. 
     * After calling this method the MessageRecorder implemenation should
     * be in the same state as if it were just constructed.
     */
    void clear();

    /** 
     * Returns true if there are one or moer warning messages.  
     * 
     * @return true if there are one or more warnings.
     */
    boolean hasWarnings();

    /** 
     * Returns true if there are one or moer error messages.  
     * 
     * @return true if there are one or more errors.
     */
    boolean hasErrors();

    /** 
     * Get the current context string. 
     * 
     * @return the context string.
     */
    String getContext();

    /** 
     * Add the name parameter to the current context. 
     * 
     * @param name 
     */
    void pushContextName(final String name);
    
    /** 
     * Remove the last context name added. 
     */
    void popContextName();

    /** 
     * Add an Exception.  
     * 
     * @param ex the Exception added.
     * @throws RTException if too many error messages have been added.
     */
    void reportError(final Exception ex) throws RTException;

    /** 
     * Add an Exception and extra informaton. 
     * 
     * @param ex the Exception added.
     * @param info extra information
     * @throws RTException if too many error messages have been added.
     */
    void reportError(final Exception ex, final Object info) throws RTException;

    /** 
     * Add an error message. 
     * 
     * @param msg  the text of the error message.
     * @throws RTException if too many error messages have been added.
     */
    void reportError(final String msg) throws RTException;

    /** 
     * Add an error message and extra information. 
     * 
     * @param msg  the text of the error message.
     * @param info extra information
     * @throws RTException if too many error messages have been added.
     */
    void reportError(final String msg, final Object info) throws RTException;

    /** 
     * Add a warning message. 
     * 
     * @param msg  the text of the warning message.
     */
    void reportWarning(final String msg);

    /** 
     * Add a warning message and extra information. 
     * 
     * @param msg  the text of the warning message.
     * @param info extra information
     */
    void reportWarning(final String msg, final Object info);
}
