/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2006 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/

package mondrian.util;

import mondrian.test.*;
import mondrian.olap.Connection;
import mondrian.olap.Result;
import mondrian.olap.Util;
import mondrian.olap.MemoryLimitExceededException;

import junit.framework.TestCase;

/**
 * Test case for {@link ObjectPool}.
 *
 * @version $Id$
 * @author Richard Emberson
 */
public class MemoryMonitorTest extends FoodMartTestCase {
    public MemoryMonitorTest() {
        super();
    }
    public MemoryMonitorTest(String name) {
        super(name);
    }

    
    public void testZeroUsage() throws Exception {
        if (Util.PreJdk15) {
            return;
        }
        class Listener implements MemoryMonitor.Listener {
            boolean wasNotified = false;
            Listener() {
            }
            public void memoryUsageNotification(long used, long max) {
                wasNotified = true;
            }
        }
        Listener listener = new Listener();
        MemoryMonitor mm = MemoryMonitorFactory.instance().getObject();
        try {
            // We use a percentage of '0' because we know that value is
            // less than or equal to the lowest JVM memory usage.
            mm.addListener(listener, 0);
            if (! listener.wasNotified) {
	        fail("Listener callback not called");
            }
        } finally {
            mm.removeListener(listener);
        }
    }
    public void testDeltaUsage() throws Exception {
        if (Util.PreJdk15) {
            return;
        }
        class Listener implements MemoryMonitor.Listener {
            boolean wasNotified = false;
            Listener() {
            }
            public void memoryUsageNotification(long used, long max) {
                wasNotified = true;
            }
        }
        Listener listener = new Listener();
        MemoryMonitor mm = MemoryMonitorFactory.instance().getObject();
        if (! (mm instanceof AbstractMemoryMonitor)) {
            // this test requires that we can access some of the
            // AbstractMemoryMonitor methods
            return;
        }
        AbstractMemoryMonitor amm = (AbstractMemoryMonitor) mm;
        // we will set a percentage slightly above the current
        // used level, and then allocate some objects that will
        // force a notification.
        long maxMemory = amm.getMax();
        long usedMemory = amm.getUsed();
        int currentPercentage = amm.usagePercentage();
        int delta = (int) (maxMemory - usedMemory)/10;
        int percentage = amm.convertThresholdToPercentage(delta);
        try {
            byte[][] bytes = new byte[10][];
            mm.addListener(listener, percentage + currentPercentage);
            for (int i = 0; i < bytes.length; i++) {
                bytes[i] = new byte[delta];
                if (listener.wasNotified) {
                    bytes = null;
                    break;
                }
            }
            if (! listener.wasNotified) {
	        fail("Listener callback not called");
            }
        } finally {
            mm.removeListener(listener);
        }
    }
    public void testUpdatePercent() throws Exception {
        if (Util.PreJdk15) {
            return;
        }
        class Listener implements MemoryMonitor.Listener {
            boolean wasNotified = false;
            Listener() {
            }
            public void memoryUsageNotification(long used, long max) {
                wasNotified = true;
            }
        }
        Listener listener = new Listener();
        MemoryMonitor mm = MemoryMonitorFactory.instance().getObject();
        if (! (mm instanceof AbstractMemoryMonitor)) {
            // this test requires that we can access some of the
            // AbstractMemoryMonitor methods
            return;
        }
        AbstractMemoryMonitor amm = (AbstractMemoryMonitor) mm;
        // we will set a percentage well above the current
        // used level, and then allocate an object, and then
        // update percentage to below new usage level.
        long maxMemory = amm.getMax();
        long usedMemory = amm.getUsed();
        int currentPercentage = amm.usagePercentage();
        int delta = (int) (maxMemory - usedMemory)/10;
        int percentage = amm.convertThresholdToPercentage(delta);
        try {
            mm.addListener(listener, 2 * percentage + currentPercentage);
            byte[] bytes = new byte[(int) (1.5 * delta)];
            if (listener.wasNotified) {
	        fail("Listener callback was called");
            }
            mm.updateListenerThreshold(listener, 
                        percentage + currentPercentage);
            if (! listener.wasNotified) {
	        fail("Listener callback was not called");
            }

        } finally {
            mm.removeListener(listener);
        }
    }
    public void testQuery() throws Exception {
        if (Util.PreJdk15) {
            return;
        }
        class Listener implements MemoryMonitor.Listener {
            boolean wasNotified = false;
            Listener() {
            }
            public void memoryUsageNotification(long used, long max) {
                wasNotified = true;
            }
        }
        Listener listener = new Listener();
        MemoryMonitor mm = MemoryMonitorFactory.instance().getObject();
        final String queryString = 
            "select \n" +
            "{ \n" +
            "[Measures].[Unit Sales], \n" +
            "[Measures].[Store Cost], \n" +
            "[Measures].[Store Sales], \n" +
            "[Measures].[Sales Count] \n" +
            "} \n" +
            "ON COLUMNS, \n" +
            "Crossjoin( \n" +
            "Descendants([Store].[All Stores]), \n" +
            "Descendants([Product].[All Products]) \n" +
            ") \n" +
            "ON ROWS \n" +
            "from [Sales]";
        if (! (mm instanceof AbstractMemoryMonitor)) {
            // this test requires that we can access some of the
            // AbstractMemoryMonitor methods
            return;
        }
        AbstractMemoryMonitor amm = (AbstractMemoryMonitor) mm;

        // may have to be run twice just so GC can occur.
        // previous test may have some garbage that can be collected.
        for (int i = 0; i < 2; i++) {
            long maxMemory = amm.getMax();
            long usedMemory = amm.getUsed();
            long delta = (maxMemory - usedMemory);
            long smallMemory = 32000000;

            // allocate an array big enough so that only about 32m are left
            byte[] bytes = null;
            if (delta > smallMemory) {
                bytes = new byte[(int) (delta-smallMemory)];
            }

            try {
                // use default percentage (90)
                mm.addListener(listener);
                Connection conn = getConnection();
                Result result = execute(conn, queryString);

                if (listener.wasNotified) {
                    break;
                }

                if (i != 0) {
                    fail("Memory Notification Exception did not occur");
                }
            } catch (MemoryLimitExceededException ex) {
                if (! listener.wasNotified) {
	            fail("Listener callback not called");
                }
                // pass
                break;
            } finally {
                mm.removeListener(listener);
            }
        }
    }

}

// End MemoryMonitorTest.java
