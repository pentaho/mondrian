/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2012-2012 Pentaho and others
// All Rights Reserved.
*/
package mondrian.util;

import mondrian.olap.Util;

import java.lang.ref.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * An expiring reference is a subclass of {@link SoftReference}
 * which pins the reference in memory until a certain timeout
 * is reached. After that, the reference is free to be garbage
 * collected if needed.
 *
 * <p>The timeout value must be provided as a String representing
 * both the time value and the time unit. For example, 1 second is
 * represented as "1s". Valid time units are [d, h, m, s, ms],
 * representing respectively days, hours, minutes, seconds and
 * milliseconds.
 */
public class ExpiringReference<T> extends SoftReference<T> {
    T hardRef;
    long expiry = Long.MIN_VALUE;

    /**
     * A Timer object to execute what we need to do.
     */
    private static final Timer timer =
        new Timer(
            "mondrian.util.ExpiringReference$timer",
            true);

    /**
     * Creates an expiring reference.
     * @param ref The referent.
     * @param timeout The timeout to enforce, in minutes.
     * If timeout is equal or less than 0, this means a hard reference.
     */
    public ExpiringReference(T ref, String timeout) {
        super(ref);
        setTimer(ref, timeout);
    }

    private synchronized void setTimer(T referent, String timeoutString) {
        Pair<Long, TimeUnit> pair =
            Util.parseInterval(timeoutString, null);
        final long timeout = pair.right.toMillis(pair.left);

        if (timeout == Long.MIN_VALUE
            && expiry != Long.MIN_VALUE)
        {
            // Reference was accessed through get().
            // Don't reset the expiry if it is active.
            return;
        }

        if (timeout == 0) {
            // Permanent ref mode.
            expiry = Long.MAX_VALUE;
            // Set the reference
            this.hardRef = referent;
            return;
        }

        if (timeout > 0) {
            // A timeout must be enforced.
            long newExpiry =
                System.currentTimeMillis() + timeout;

            if (newExpiry > expiry) {
                expiry = newExpiry;
            }

            // Set the reference
            this.hardRef = referent;

            // Schedule for cleanup.
            timer.schedule(
                getTask(),
                timeout + 10);

            // Notice the 1001 value above. This to make sure that when
            // the timer fires, the cleanup takes place. Else, undefined
            // behavior happens.
            return;
        }

        // Timeout is < 0. Act as a regular soft ref.
        this.hardRef = null;
    }

    TimerTask getTask() {
        return new TimerTask() {
            public void run() {
                if (expiry <= System.currentTimeMillis()) {
                    hardRef = null;
                }
            }
        };
    }

    public synchronized T get() {
        return get(Long.MIN_VALUE + "ms");
    }

    public synchronized T get(String timeout) {
        final T weakRef = super.get();

        if (weakRef != null) {
            // This object is still alive but was cleaned.
            // set a new TimerTask.
            setTimer(weakRef, timeout);
        }

        return weakRef;
    }
}
// End ExpiringReference.java
