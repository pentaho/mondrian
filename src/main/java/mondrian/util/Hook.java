/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2013-2013 Julian Hyde
// All Rights Reserved.
*/
package mondrian.util;

import mondrian.olap.Util;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Collection of hooks that can be set by observers and are executed at various
 * parts of the query preparation process.
 *
 * <p>For testing and debugging rather than for end-users.</p>
 */
public enum Hook {
    /** Called when a DataSource is encountered. */
    DATA_SOURCE;

    private final List<Util.Function1<?, ?>> handlers =
        new CopyOnWriteArrayList<Util.Function1<?, ?>>();

    /** Adds a handler for this Hook.
     *
     * <p>Returns a {@link Hook.Closeable} so that you can use the following
     * try-finally pattern to prevent leaks:</p>
     *
     * <blockquote><pre>
     *     final Hook.Closeable closeable = Hook.FOO.add(HANDLER);
     *     try {
     *         ...
     *     } finally {
     *         closeable.close();
     *     }</pre>
     * </blockquote>
     */
    public <T, R> Closeable add(final Util.Function1<T, R> handler) {
        handlers.add(handler);
        return new Closeable() {
            public void close() {
                remove(handler);
            }
        };
    }

    /** Removes a handler from this Hook. */
    private boolean remove(Util.Function1 handler) {
        return handlers.remove(handler);
    }

    /** Runs all handlers registered for this Hook, with the given argument. */
    public void run(Object arg) {
        for (Util.Function1 handler : handlers) {
            //noinspection unchecked
            handler.apply(arg);
        }
    }

    /** Removes a Hook after use.
     *
     * <p>Note: Although it would be convenient, this interface cannot extend
     * {@code AutoCloseable} while we maintain compatibility with
     * JDK 1.6.</p>
     */
    public interface Closeable /*extends AutoCloseable*/ {
        void close(); // override, removing "throws"
    }
}

// End Hook.java
