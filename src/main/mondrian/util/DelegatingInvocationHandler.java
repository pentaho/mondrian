/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2002-2002 Kana Software, Inc.
// Copyright (C) 2002-2005 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Dec 23, 2002
*/
package mondrian.util;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * A class derived from <code>DelegatingInvocationHandler</code> handles a
 * method call by looking for a method in itself with identical parameters. If
 * no such method is found, it forwards the call to a fallback object, which
 * must implement all of the interfaces which this proxy implements.
 *
 * <p> It is useful in creating a wrapper class around an interface which may
 * change over time.
 *
 * @author jhyde
 * @since 21 March, 2002
 * @version $Id$
 */
public abstract class DelegatingInvocationHandler implements InvocationHandler {
    private Object fallback;
    protected DelegatingInvocationHandler(Object fallback) {
        this.fallback = fallback;
    }
    public Object invoke(Object proxy, Method method, Object[] args)
            throws Throwable {
        Class clazz = getClass();
        Method matchingMethod;
        try {
            matchingMethod = clazz.getMethod(
                    method.getName(), method.getParameterTypes());
        } catch (NoSuchMethodException e) {
            matchingMethod = null;
        } catch (SecurityException e) {
            matchingMethod = null;
        }
        try {
            if (matchingMethod != null) {
                // Invoke the method in the derived class.
                return matchingMethod.invoke(this, args);
            } else {
                // Invoke the method on the proxy.
                return method.invoke(fallback, args);
            }
        } catch (InvocationTargetException e) {
            throw e.getTargetException();
        }
    }
}

// End DelegatingInvocationHandler.java
