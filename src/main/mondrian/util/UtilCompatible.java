/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2007-2011 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.util;

import mondrian.olap.Util;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.Statement;

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
 * @version $Id$
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
    void cancelAndCloseStatement(Statement stmt);

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

    <T> void threadLocalRemove(ThreadLocal<T> threadLocal);
}

// End UtilCompatible.java
