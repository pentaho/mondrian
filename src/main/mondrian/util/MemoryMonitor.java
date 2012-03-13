/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2007-2007 Pentaho and others
// All Rights Reserved.
*/
package mondrian.util;

/**
 * API for Mondrian's memory monitors.
 *
 * <p>For Java4, the available monitors
 * do nothing since there is no reliable way of detecting that
 * memory is running low using such a JVM (you are welcome to
 * try to create one, but I believe you will fail - some such candidates
 * only make it more likely that an OutOfMemory condition will occur).
 *
 * <p>For Java5 one
 * can optionally enable a monitor which is based upon the Java5
 * memory classes locate in java.lang.management.
 *
 * <p>A client must implement the <code>MemoryMonitor.Listener</code> interface
 * and register with the <code>MemoryMonitor</code>.
 *
 * <p>The <code>MemoryMonitor</code> supports having multiple
 * <code>Listener</code> clients. The clients can have the same
 * threshold percentage or different values. The threshold percentage value
 * is used by the <code>MemoryMonitor</code> to determine when to
 * notify a client. It is the percentage of the total memory:
 * <blockquote>
 * <code>
 * 100 * free-memory / total-memory (0 &le; free-memory &le; total-memory).
 * </code>
 * </blockquote>
 *
 * @author Richard M. Emberson
 * @since Feb 01 2007
 */
public interface MemoryMonitor {

    /**
     * Adds a <code>Listener</code> to the <code>MemoryMonitor</code> with
     * a given threshold percentage.
     *
     * <p>If the threshold is below the Java5 memory managment system's
     * threshold, then the Listener is notified from within this
     * method.
     *
     * @param listener the <code>Listener</code> to be added.
     * @param thresholdPercentage the threshold percentage for this
     *   <code>Listener</code>.
     * @return <code>true</code> if the <code>Listener</code> was
     *   added and <code>false</code> otherwise.
     */
    boolean addListener(Listener listener, int thresholdPercentage);

    /**
     * Adds a <code>Listener</code> using the default threshold percentage.
     *
     * <p>If the threshold is below the Java5 memory managment system's
     * threshold, then the Listener is notified from within this
     * method.
     *
     * @param listener the <code>Listener</code> to be added.
     * @return <code>true</code> if the <code>Listener</code> was
     * added and <code>false</code> otherwise.
     */
    boolean addListener(final Listener listener);

    /**
     * Changes the threshold percentage of a given <code>Listener</code>.
     *
     * <p>If the new value is below the system's current value, then the
     * <code>Listener</code> will have its notification callback called
     * while in this method - so a client should always check if its
     * notification method was called immediately after calling this
     * method.
     *
     * <p>This method can be used if, for example, an algorithm has
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
     * @param listener the <code>Listener</code> being updated.
     * @param percentage new percentage threshold.
     */
    void updateListenerThreshold(Listener listener, int percentage);

    /**
     * Removes a <code>Listener</code> from the <code>MemoryMonitor</code>.
     * Returns <code>true</code> if listener was removed and
     * <code>false</code> otherwise.
     *
     * @param listener the listener to be removed
     * @return <code>true</code> if listener was removed.
     */
    boolean removeListener(Listener listener);

    /**
     * Clear out all <code>Listener</code>s and turnoff JVM
     * memory notification.
     */
    void removeAllListener();

    /**
     * Returns the maximum memory usage.
     *
     * @return the maximum memory usage.
     */
    long getMaxMemory();

    /**
     * Returns the current memory used.
     *
     * @return the current memory used.
     */
    long getUsedMemory();


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
     * This is an interface that a <code>MemoryMonitor</code> may optionally
     * implement. These methods give the tester access to some of the
     * internal, white-box data.
     * <p>
     * During testing Mondrian has a default
     * <code>MemoryMonitor</code> which might be replaced with a test
     * <code>MemoryMonitor</code>s using the <code>ThreadLocal</code>
     * mechanism. After the test using the test
     * <code>MemoryMonitor</code> finishes, a call to the
     * <code>resetFromTest</code> method allows
     * the default <code>MemoryMonitor</code> reset itself.
     * This is hook that should only be called as part of testing.
     */
    interface Test {

        /**
         * This should only be called when one is switching from a
         * test <code>MemoryMonitor</code> back to the default system
         * <code>MemoryMonitor</code>. In particular, look at
         * the <code>MemoryMonitorFactory</code>'s
         * <code>clearThreadLocalClassName()</code> method for its
         * usage.
         */
        void resetFromTest();
    }
}

// End MemoryMonitor.java
