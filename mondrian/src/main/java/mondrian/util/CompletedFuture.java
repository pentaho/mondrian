/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.util;

import java.util.concurrent.*;

/**
 * Implementation of {@link Future} that has already completed.
 */
public class CompletedFuture<V> implements Future<V> {
    private final V value;
    private final ExecutionException exception;

    /**
     * Creates a CompletedFuture.
     *
     * <p>If {@code throwable} is not null, the computation is deemed to have
     * failed. The exception will be thrown (wrapped in a
     * {@link ExecutionException}) when {@link #get} or
     * {@link #get(long, java.util.concurrent.TimeUnit)} are invoked.</p>
     *
     * <p>If exception is null, the computation is deemed to have succeeded.
     * In this case, a null value in {@code value} just means that the
     * computation yielded a null result.</p>
     *
     * @param value Value (may be null)
     * @param exception Exception that occurred while computing result
     */
    public CompletedFuture(V value, ExecutionException exception) {
        if (value != null && exception != null) {
            throw new IllegalArgumentException(
                "Value and exception must not both be specified");
        }
        this.value = value;
        this.exception = exception;
    }

    /**
     * Creates a completed future indicating success.
     *
     * @param t Result of computation
     * @return Completed future that will yield the result
     */
    public static <T> CompletedFuture<T> success(T t) {
        return new CompletedFuture<T>(t, null);
    }

    /**
     * Creates a completed future indicating failure.
     *
     * @param e Exception
     * @return Completed future that will throw
     */
    public static <T> CompletedFuture<T> fail(Throwable e) {
        return new CompletedFuture<T>(null, new ExecutionException(e));
    }

    public boolean cancel(boolean mayInterruptIfRunning) {
        // could not be cancelled, because already completed
        return false;
    }

    public boolean isCancelled() {
        // completed before could be cancelled
        return false;
    }

    public boolean isDone() {
        return true;
    }

    public V get() throws ExecutionException {
        if (exception != null) {
            throw exception;
        }
        return value;
    }

    public V get(long timeout, TimeUnit unit) throws ExecutionException {
        return get();
    }
}

// End CompletedFuture.java
