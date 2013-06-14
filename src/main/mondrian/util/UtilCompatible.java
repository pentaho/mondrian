/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2007-2013 Pentaho
// All Rights Reserved.
*/
package mondrian.util;

import mondrian.olap.Util;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.Statement;
import java.util.*;

/**
 * Interface containing methods which are implemented differently in different
 * versions of the JDK.
 *
 * <p>The methods should not be called directly, only via the corresponding
 * static methods in {@link mondrian.olap.Util}, namely:<ul>
 * <li>{@link mondrian.olap.Util#makeBigDecimalFromDouble(double)}</li>
 * <li>{@link mondrian.olap.Util#quotePattern(String)}</li>
 * </ul></p>
 *
 * <p>This interface could in principle be extended to allow native
 * implementations of methods, or to serve as a factory for entire classes
 * which have different implementations in different environments.</p>
 *
 * @author jhyde
 * @since Feb 5, 2007
 */
public interface UtilCompatible {
    BigDecimal makeBigDecimalFromDouble(double d);

    String quotePattern(String s);

    <T> T getAnnotation(
        Method method, String annotationClassName, T defaultValue);

    String generateUuidString();

    /**
     * Cancels and closes a SQL Statement object. If errors are encountered,
     * they should be logged under {@link Util}.
     * @param stmt The statement to close.
     */
    void cancelStatement(Statement stmt);

    /**
     * Compiles a script to yield a Java interface.
     *
     * @param iface Interface script should implement
     * @param script Script code
     * @param engineName Name of engine (e.g. "JavaScript")
     * @param <T> Interface
     * @return Object that implements given interface
     */
    <T> T compileScript(
        Class<T> iface,
        String script,
        String engineName);

    /**
     * Removes a thread local from the current thread.
     *
     * <p>From JDK 1.5 onwards, calls {@link ThreadLocal#remove()}; before
     * that, no-ops.</p>
     *
     * @param threadLocal Thread local
     * @param <T> Type
     */
    <T> void threadLocalRemove(ThreadLocal<T> threadLocal);

    /**
     * Creates a hash set that, like {@link java.util.IdentityHashMap},
     * compares keys using identity.
     *
     * @param <T> Element type
     * @return Set
     */
    <T> Set<T> newIdentityHashSet();

    /**
     * As {@link java.util.Arrays#binarySearch(Object[], int, int, Object)}, but
     * available pre-JDK 1.6.
     */
    <T extends Comparable<T>> int binarySearch(T[] ts, int start, int end, T t);

    /**
     * Creates an object from which to get information about system memory
     * use. From JDK 1.5 onwards, uses
     * {@link java.lang.management.MemoryPoolMXBean}.
     *
     * @return Memory info
     */
    Util.MemoryInfo getMemoryInfo();

    /**
     * Equivalent to {@link Timer#Timer(String, boolean)}.
     * (Introduced in JDK 1.5.)
     *
     * @param name the name of the associated thread
     * @param isDaemon true if the associated thread should run as a daemon
     * @return timer
     */
    Timer newTimer(String name, boolean isDaemon);

    /**
     * Parses a locale from a BCP47 language tag (e.g. "en-US").
     *
     * <p>In JDK 1.6 and earlier, returns null.</p>
     *
     * @param localeString Language tag
     * @return Locale, or null
     */
    Locale localeForLanguageTag(String localeString);
}

// End UtilCompatible.java
