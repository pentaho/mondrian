/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2011-2012 Pentaho
// All Rights Reserved.
*/
package mondrian.util;

import org.apache.log4j.Logger;

import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Implementation of {@link java.util.concurrent.Future} that completes
 * when a thread writes a value (or an exception) into a slot.
 */
public class SlotFuture<V> implements Future<V> {
    private V value;
    private Throwable throwable;
    private boolean done;
    private boolean cancelled;
    private final CountDownLatch dataGate = new CountDownLatch(1);
    private final ReentrantReadWriteLock stateLock =
        new ReentrantReadWriteLock();
    private static final Logger LOG = Logger.getLogger(SlotFuture.class);

    /**
     * Creates a SlotFuture.
     */
    public SlotFuture() {
    }

    public boolean cancel(boolean mayInterruptIfRunning) {
        stateLock.writeLock().lock();
        try {
            if (!done) {
                cancelled = true;
                done = true;
                dataGate.countDown();
                return true;
            } else {
                return false;
            }
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    public boolean isCancelled() {
        stateLock.readLock().lock();
        try {
            return cancelled;
        } finally {
            stateLock.readLock().unlock();
        }
    }

    public boolean isDone() {
        stateLock.readLock().lock();
        try {
            return done || cancelled || throwable != null;
        } finally {
            stateLock.readLock().unlock();
        }
    }

    public V get() throws ExecutionException, InterruptedException {
        // Wait for a put, fail or cancel
        dataGate.await();

        // Now a put, fail or cancel has occurred, state does not change; we
        // don't need even a read lock.
        if (throwable != null) {
            throw new ExecutionException(throwable);
        }
        return value;
    }

    public V get(long timeout, TimeUnit unit)
        throws ExecutionException, InterruptedException, TimeoutException
    {
        // Wait for a put, fail or cancel
        if (!dataGate.await(timeout, unit)) {
            throw new TimeoutException();
        }

        // Now a put, fail or cancel has occurred, state does not change; we
        // don't need even a read lock.
        if (throwable != null) {
            throw new ExecutionException(throwable);
        }
        return value;
    }

    /**
     * Writes a value into the slot, indicating that the task has completed
     * successfully.
     *
     * @param value Value to yield as the result of the computation
     *
     * @throws IllegalArgumentException if put, fail or cancel has already
     *     been invoked on this future
     */
    public void put(V value) {
        stateLock.writeLock().lock(); // need exclusive write access to state
        try {
            if (done) {
                final String message =
                    "Future is already done (cancelled=" + cancelled
                    + ", value=" + this.value
                    + ", throwable=" + throwable + ")";
                LOG.error(message);
                throw new IllegalArgumentException(
                    message);
            }
            this.value = value;
            this.done = true;
        } finally {
            stateLock.writeLock().unlock();
        }
        dataGate.countDown();
    }

    /**
     * Writes a throwable into the slot, indicating that the task has failed.
     *
     * @param throwable Exception that aborted the computation
     *
     * @throws IllegalArgumentException if put, fail or cancel has already
     *     been invoked on this future
     */
    public void fail(Throwable throwable) {
        stateLock.writeLock().lock(); // need exclusive write access to state
        try {
            if (done) {
                throw new IllegalArgumentException(
                    "Future is already done (cancelled=" + cancelled
                    + ", value=" + value
                    + ", throwable=" + this.throwable + ")");
            }
            this.throwable = throwable;
            this.done = true;
        } finally {
            stateLock.writeLock().unlock();
        }
        dataGate.countDown();
    }
}

// End SlotFuture.java
