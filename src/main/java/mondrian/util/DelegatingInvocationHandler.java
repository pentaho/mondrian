/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2007-2007 Pentaho
// All Rights Reserved.
*/
package mondrian.util;

import java.lang.reflect.*;

/**
 * A class derived from <code>DelegatingInvocationHandler</code> handles a
 * method call by looking for a method in itself with identical parameters. If
 * no such method is found, it forwards the call to a fallback object, which
 * must implement all of the interfaces which this proxy implements.
 *
 * <p>It is useful in creating a wrapper class around an interface which may
 * change over time.</p>
 *
 * <p>Example:
 *
 * <blockquote>
 * <pre>import java.sql.Connection;
 * Connection connection = ...;
 * Connection tracingConnection = (Connection) Proxy.newProxyInstance(
 *     null,
 *     new Class[] {Connection.class},
 *     new DelegatingInvocationHandler() {
 *         protected Object getTarget() {
 *             return connection;
 *         }
 *         Statement createStatement() {
 *             System.out.println("statement created");
 *             return connection.createStatement();
 *         }
 *     });</pre>
 * </blockquote>
 * </p>
 *
 * @author jhyde
 */
public abstract class DelegatingInvocationHandler
    implements InvocationHandler
{
    public Object invoke(
        Object proxy,
        Method method,
        Object [] args)
        throws Throwable
    {
        Class clazz = getClass();
        Method matchingMethod;
        try {
            matchingMethod =
                clazz.getMethod(
                    method.getName(),
                    method.getParameterTypes());
        } catch (NoSuchMethodException e) {
            matchingMethod = null;
        } catch (SecurityException e) {
            matchingMethod = null;
        }
        try {
            if (matchingMethod != null) {
                // Invoke the method in the derived class.
                return matchingMethod.invoke(this, args);
            }
            final Object target = getTarget();
            if (target == null) {
                throw new UnsupportedOperationException(
                    "method: " + method);
            }
            return method.invoke(
                target,
                args);
        } catch (InvocationTargetException e) {
            throw e.getTargetException();
        }
    }

    /**
     * Returns the object to forward method calls to, should the derived class
     * not implement the method. Generally, this object will be a member of the
     * derived class, supplied as a parameter to its constructor.
     *
     * <p>The default implementation returns null, which will cause the
     * {@link #invoke(Object, java.lang.reflect.Method, Object[])} method
     * to throw an {@link UnsupportedOperationException} if the derived class
     * does not have the required method.
     *
     * @return object to forward method calls to
     */
    protected Object getTarget() {
        return null;
    }
}

// End DelegatingInvocationHandler.java
