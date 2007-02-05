/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2007-2007 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
*/
package mondrian.util;

import mondrian.olap.MondrianProperties;
import org.apache.log4j.Logger;
import java.util.LinkedList;
import java.util.ListIterator;

/** 
 *  Abstract implementation of <code>MemoryMonitor</code>.
 * 
 * @author <a>Richard M. Emberson</a>
 * @since Feb 03 2007
 * @version $Id$
 */
public abstract class AbstractMemoryMonitor implements MemoryMonitor {
    
    /** 
     * Basically, 100 percent. 
     */
    private static final int MAX_THRESHOLD = 100;

    /** 
     * Class used to associate <code>Listener</code> and threshold. 
     */
    static class Entry {
        final Listener listener;
        long threshold;
        Entry(final Listener listener, final long threshold) {
            this.listener = listener;
            this.threshold = threshold;
        }
        public boolean equals(final Object other) {
            return (other instanceof Entry) &&
                   (listener == ((Entry) other).listener);
        }
        public int hashCode() {
            return listener.hashCode();
        }
    }

    /** 
     * <code>LinkedList</code> of <code>Entry</code> objects. A
     * <code>LinkedList</code> was used for quick insertion and
     * removal.
     */
    private final LinkedList<Entry> listeners;
    
    /** 
     * The current low threshold level. This is the lowest level of any 
     * of the registered <code>Listener</code>s.
     */
    private long lowThreshold;

    /** 
     * Constructor of this base class.
     */
    protected AbstractMemoryMonitor() {
        listeners = new LinkedList<Entry>();
    }

    /** 
     * Returns the <code>Logger</code>.
     * 
     * @return the <code>Logger</code>.
     */
    protected abstract Logger getLogger();

    /** 
     * Returns the current lowest threshold of all registered
     * <code>Listener</code>s. 
     * 
     * @return the lowest threshold.
     */
    protected long getLowThreshold() {
        return lowThreshold;
    }

    /** 
     * Returns the default memory notification percentage.
     *
     * <p>This is the value of the Mondrian
     * {@link MondrianProperties#MemoryMonitorThreshold} property.
     * 
     * @return the default threshold percentage.
     */
    public int getDefaultThresholdPercentage() {
        return MondrianProperties.instance().MemoryMonitorThreshold.get();
    }

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
    public boolean addListener(final Listener listener) {
        return addListener(listener, getDefaultThresholdPercentage());
    }

    /** 
     * Adds a <code>Listener</code> with the given threshold percentage.
     *
     * <p>If the threshold is below the Java5 memory managment system's
     * threshold, then the Listener is notified from within this
     * method.
     * 
     * @param listener the <code>Listener</code> to be added.
     * @param percentage the threshold percentage for this 
     * <code>Listener</code>.
     * @return <code>true</code> if the <code>Listener</code> was 
     * added and <code>false</code> otherwise.
     */
    public boolean addListener(Listener listener, int percentage) {
        getLogger().info("addListener enter");
        try {
            // Should this listener being added be immediately 
            // notified that memory is short.
            boolean notifyNow = (usagePercentage() >= percentage);

            final long newThreshold = convertPercentageToThreshold(percentage);
            Entry e = new Entry(listener, newThreshold);

            synchronized (listeners) {
                long prevLowThreshold = generateLowThreshold();

                // Add the new listener to its proper place in the
                // list of listeners based upon threshold value.
                final ListIterator<Entry> iter = listeners.listIterator();
                while (iter.hasNext()) {
                    Entry ee = iter.next();
                    if (newThreshold <= ee.threshold) {
                        iter.add(e);
                        e = null;
                        break;
                    }
                }
                // If not null, then it has not been added yet,
                // either its the first one or its the biggest.
                if (e != null) {
                    listeners.addLast(e);
                }

                // If the new threshold is less than the previous
                // lowest threshold, then notify the Java5 system
                // that we are interested in being notified for this
                // lower value.
                lowThreshold = generateLowThreshold();
                if (lowThreshold < prevLowThreshold) {
                    notifyNewLowThreshold(lowThreshold);
                }
            }
            if (notifyNow) {
                listener.memoryUsageNotification(getUsed(), getMax());
            }
            return true;

        } finally {
            getLogger().info("addListener exit");
        }
    }

    /** 
     * Updates the percentage threshold of a given <code>Listener</code>.
     *
     * <p>If the threshold is below the Java5 memory managment system's
     * threshold, then the Listener is notified from within this
     * method.
     * 
     * @param listener the <code>Listener</code> being updated.
     * @param percentage new percentage threshold.
     */
    public void updateListenerThreshold(Listener listener, int percentage) {
        getLogger().info("updateListenerThreshold enter");
        try {
            // Should this listener being added be immediately 
            // notified that memory is short.
            boolean notifyNow = (usagePercentage() >= percentage);

            final long newThreshold = convertPercentageToThreshold(percentage);

            synchronized (listeners) {
                long prevLowThreshold = generateLowThreshold();

                Entry e = null;
                // Remove the listener from the list of listeners.
                ListIterator<Entry> iter = listeners.listIterator();
                while (iter.hasNext()) {
                    e = iter.next();
                    if (e.listener == listener) {
                        iter.remove();
                        break;
                    } else {
                        e = null;
                    }
                }
                // If 'e' is not null, then the listener was found.
                if (e != null) {
                    e.threshold = newThreshold;

                    // Add the listener.
                    iter = listeners.listIterator();
                    while (iter.hasNext()) {
                        Entry ee = iter.next();
                        if (newThreshold <= ee.threshold) {
                            iter.add(e);
                            break;
                        }
                    }
                    lowThreshold = generateLowThreshold();
                    if (lowThreshold != prevLowThreshold) {
                        notifyNewLowThreshold(lowThreshold);
                    }
                }
            }

            if (notifyNow) {
                listener.memoryUsageNotification(getUsed(), getMax());
            }

        } finally {
            getLogger().info("updateListenerThreshold exit");
        }
    }
    
    /** 
     * Removes a <code>Listener</code>.
     * 
     * @param listener the <code>Listener</code> to be removed.
     * @return <code>true</code> if <code>Listener</code> was remove and 
     * <code>false</code> otherwise.
     */
    public boolean removeListener(Listener listener) {
        getLogger().info("removeListener enter");
        try {
            boolean result = false;
            synchronized (listeners) {
                long prevLowThreshold = generateLowThreshold();

                final ListIterator<Entry> iter = listeners.listIterator();
                while (iter.hasNext()) {
                    Entry ee = iter.next();
                    if (listener == ee.listener) {
                        iter.remove();
                        result = true;
                        break;
                    }
                }

                // If there is a new low threshold, tell Java5
                lowThreshold = generateLowThreshold();
                if (lowThreshold > prevLowThreshold) {
                    notifyNewLowThreshold(lowThreshold);
                }
            
            }
            return result;
        } finally {
            getLogger().info("removeListener exit");
        }
    }

    /** 
     * This is not part of the <code>MemoryMonitor</code> interface. 
     * Clients that "know" that they have an instance of the
     * <code>AbstractMemoryMonitor</code> can cast and call this if required.
     * It is a back door to clear out out Listeners and turnoff
     * JVN memory notification.
     */
    public void removeAllListener() {
        getLogger().info("removeAllListener enter");
        try {
            notifyNewLowThreshold(getMax());
            listeners.clear();
        } finally {
            getLogger().info("removeAllListener exit");
        }
    }

    /** 
     * Returns the lowest threshold from the list of <code>Listener</code>s.
     * If there are no <code>Listener</code>s, then return the maximum
     * memory usage.
     * 
     * @return the lowest threshold or maximum memory usage.
     */
    protected long generateLowThreshold() {
        return listeners.isEmpty() 
               ? getMax()
               : listeners.get(0).threshold;
    }


    /** 
     * Notifies all <code>Listener</code>s that memory is running short.
     * 
     * @param usedMemory the current memory used.
     * @param maxMemory the maximum memory.
     */
    protected void notifyListeners(final long usedMemory,
                                   final long maxMemory) {
        synchronized (listeners) {
            for (Entry e : listeners) {
                if (usedMemory >= e.threshold) {
                    e.listener.memoryUsageNotification(usedMemory,
                                                       maxMemory);
                }
            }
        }
    }

    /** 
     * Derived classes implement this method if they wish to be notified
     * when there is a new lowest threshold.
     * 
     * @param newLowThreshold the new low threshold.
     */
    protected void notifyNewLowThreshold(final long newLowThreshold) {
        // empty
    }

    /** 
     * Returns the maximum memory usage.
     * 
     * @return the maximum memory usage.
     */
    protected abstract long getMax();

    /** 
     * Returns the current memory used.
     * 
     * @return the current memory used.
     */
    protected abstract long getUsed();

    /** 
     * Converts a percentage threshold to its corresponding memory value,
     * ( percentage * maximum-memory / 100 ).
     * 
     * @param percentage the threshold.
     * @return the memory value.
     */
    protected long convertPercentageToThreshold(final int percentage) {
        if (percentage < 0 || percentage > MAX_THRESHOLD) {
            throw new IllegalArgumentException(
                    "Percentage not in range: " +percentage);
        }

        long maxMemory = getMax();
        long l = (maxMemory * percentage) / MAX_THRESHOLD;
        return l;
    }
    
    /** 
     * Converts a memory value to its percentage.
     * 
     * @param threshold the memory value.
     * @return the percentage.
     */
    protected int convertThresholdToPercentage(final long threshold) {
        long maxMemory = getMax();
        int i = (int)( (MAX_THRESHOLD * threshold) / maxMemory);
        return i;
    }
    
    /** 
     * Returns how much memory is currently being used as a percentage.
     * 
     * @return currently used memory as a percentage. 
     */
    protected int usagePercentage() {
        return convertThresholdToPercentage(getUsed());
    }
}

// End AbstractMemoryMonitor.java
