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
 * The <code>FauxMemoryMonitor</code> implements the <code>MemoryMonitor</code>
 * interface but does nothing: all methods are empty.
 *
 * @author <a>Richard M. Emberson</a>
 * @since Feb 03 2007
 */
public class FauxMemoryMonitor implements MemoryMonitor {
    FauxMemoryMonitor() {
    }

    public boolean addListener(Listener listener, int thresholdPercentage) {
        return true;
    }

    public boolean addListener(final Listener listener) {
        return true;
    }

    public void updateListenerThreshold(Listener listener, int percentage) {
        // empty
    }

    public boolean removeListener(Listener listener) {
        return true;
    }
    public void removeAllListener() {
        // empty
    }
    public long getMaxMemory() {
        return Runtime.getRuntime().maxMemory();
    }
    public long getUsedMemory() {
        return Runtime.getRuntime().freeMemory();
    }
}

// End FauxMemoryMonitor.java
