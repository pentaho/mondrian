/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2002-2002 Kana Software, Inc.
// Copyright (C) 2002-2006 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
*/
package mondrian.util;

/** 
 * The <code>MemoryMonitor</code> interface defines the API for
 * Mondrian's memory monitors. For Java4, the available monitors
 * do nothing since there is no reliable way of detecting that
 * memory is running low using such a JVM (you are welcome to
 * try to create one, but I believe you will fail - some such candidates
 * only make it more likely that an OutOfMemory condition will occur). 
 * For Java5 one
 * can optionally enable a monitor which is based upon the Java5
 * memory classes locate in java.lang.management.
 * <p>
 * Clients implement the <code>MemoryMonitor.Listener</code> interface and 
 * register with the <code>MemoryMonitor</code>.
 * <p>
 * The <code>MemoryMonitor</code> supports having multiple 
 * <code>Listener</code> clients. The clients can have the same 
 * threshold percentage or different values. The threshold percentage value
 * is used by the <code>MemoryMonitor</code> to determine when to
 * notify a client. It is the percentage of the total memory,
 * 100 * free-memory / total-memory (0 $lt;= free-memory $lt;= total-memory). 
 * 
 * @author <a>Richard M. Emberson</a>
 * @since Feb 01 2007
 * @version $Id$
 */
public interface MemoryMonitor {
    
    /** 
     * A <code>MemoryMonitor</code> client implements the <code>Listener</code>
     * interface and registers with the <code>MemoryMonitor</code>.
     * When the <code>MemoryMonitor</code> detects that free memory is 
     * low, it notifies the client by calling the client's
     * <code>memoryUsageNotification</code> method. It is important 
     * that the client quickly return from this call, that the
     * <code>memoryUsageNotification</code> method does not do a lot of
     * work. It is best if it simply sets a flag. The flag should be
     * polled by an application thread and when it detects that the
     * flag was set, it should take immediate memory relinquishing operations.
     * In the case of Mondrian, the present query is aborted.
     */
    interface Listener {
        
        /** 
         * When the <code>MemoryMonitor</code> determines that the 
         * <code>Listener</code>'s threshold is equal to or less than
         * the current available memory (post garbage collection),
         * then this method is called with the current memory usage,
         * <code>usedMemory</code>, and the maximum memory (which
         * is a constant per JVM invocation).
         * <p>
         * This method is called (in the case of Java5) by a system
         * thread associated with the garbage collection activity.
         * When this method is called, the client should quickly do what
         * it needs to to communicate with an application thread and
         * then return. Generally, quickly de-referencing some big objects
         * and setting a flag is the most that should be done by
         * implementations of this method. If the implementor chooses to
         * de-reference some objects, then the application code must
         * be written so that if will not throw a NullPointerException
         * when such de-referenced objects are accessed. If a flag
         * is set, then the application must be written to check the
         * flag periodically.
         * 
         * @param usedMemory the current memory used.
         * @param maxMemory the maximum available memory.
         */
        void memoryUsageNotification(long usedMemory, long maxMemory);
    }

    /** 
     * Add a <code>Listener</code> to the <code>MemoryMonitor</code> with
     * a given threshold percentage.
     * If the threshold percentage value is below the system's current 
     * value, then the * <code>Listener</code> will have its notification 
     * callback called * while in this method - so a client should 
     * always check if its notification method was called immediately 
     * after calling this method.
     * 
     * @param listener the <code>Listener</code> being added.
     * @param thresholdPercentage the notification threshold percentage.
     * @return 
     */
    boolean addListener(Listener listener, int thresholdPercentage);

    /** 
     * Add a <code>Listener</code> to the <code>MemoryMonitor</code> and
     * use the default threshold percentage.
     * If the default threshold percentage value is below the system's current 
     * value, then the * <code>Listener</code> will have its notification 
     * callback called * while in this method - so a client should 
     * always check if its notification method was called immediately 
     * after calling this method.
     * 
     * @param listener the <code>Listener</code> being added.
     * @return 
     */
    boolean addListener(final Listener listener);

    /** 
     * For the given <code>Listener</code> change its threshold percentage.
     * If the new value is below the system's current value, then the
     * <code>Listener</code> will have its notification callback called
     * while in this method - so a client should always check if its
     * notification method was called immediately after calling this
     * method.
     * <p>
     * This method can be used if, for example, an algorithm has 
     * different approaches that result in different memory 
     * usage profiles; one, large memory but fast and
     * a second which is low-memory but slow. The algorithm starts
     * with the large memory approach, receives a low memory
     * notification, switches to the low memory approach and changes
     * when it should be notified for this new approach. The first
     * approach need to be notified at a lower percentage because it
     * uses lots of memory, possibly quickly; while the second
     * approach, possibly a file based algorithm, has smaller memory
     * requirements and uses memory less quickly thus one can
     * live with a higher notification threshold percentage.
     * 
     * @param listener 
     * @param percentage 
     */
    void updateListenerThreshold(Listener listener, int percentage);

    /** 
     * Remove a <code>Listener</code> from the <code>MemoryMonitor</code>.
     * Returns <code>true</code> if listener was removed and
     * <code>false</code> otherwise.
     * 
     * @param listener the listener to be removed
     * @return <code>true</code> if listener was removed.
     */
    boolean removeListener(Listener listener);

}
