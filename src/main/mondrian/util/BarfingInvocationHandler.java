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
import java.lang.reflect.Method;

/**
 * A class derived from <code>BarfingInvocationHandler</code> handles a
 * method call by looking for a method in itself with identical parameters. If
 * no such method is found, it throws {@link UnsupportedOperationException}.
 *
 * <p> It is useful when you are prototyping code. You can rapidly create a
 * prototype class which implements the important methods in an interface, then
 * implement other methods as they are called.
 *
 * @see DelegatingInvocationHandler
 * @author jhyde
 * @since Dec 23, 2002
 * @version $Id$
 */
public class BarfingInvocationHandler implements InvocationHandler {
    protected BarfingInvocationHandler() {
    }
    public Object invoke(Object proxy, Method method, Object[] args)
            throws Throwable {
        Class clazz = getClass();
        Method matchingMethod = null;
        try {
            matchingMethod = clazz.getMethod(
                    method.getName(), method.getParameterTypes());
        } catch (NoSuchMethodException e) {
            throw noMethod(method);
        } catch (SecurityException e) {
            throw noMethod(method);
        }
        if (matchingMethod.getReturnType() != method.getReturnType()) {
            throw noMethod(method);
        }
        // Invoke the method in the derived class.
        return matchingMethod.invoke(this, args);
    }

    /**
     * Called when this class (or its derived class) does not have the
     * required method from the interface.
     */
    protected UnsupportedOperationException noMethod(Method method) {
        StringBuffer buf = new StringBuffer();
        final Class[] parameterTypes = method.getParameterTypes();
        for (int i = 0; i < parameterTypes.length; i++) {
            if (i > 0) {
                buf.append(",");
            }
            buf.append(parameterTypes[i].getName());
        }
        String signature = method.getReturnType().getName() + " " +
                method.getDeclaringClass().getName() + "." +
                method.getName() + "(" + buf.toString() + ")";
        return new UnsupportedOperationException(signature);
    }
}

// End BarfingInvocationHandler.java
