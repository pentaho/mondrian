/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2002-2005 Julian Hyde
// Copyright (C) 2005-2009 Pentaho and others
// All Rights Reserved.
*/

package mondrian.olap.fun;

import mondrian.olap.*;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * Resolver which uses reflection to instantiate a {@link FunDef}.
 * This reduces the amount of anonymous classes.
 *
 * @author jhyde
 * @since Mar 23, 2006
 */
public class ReflectiveMultiResolver extends MultiResolver {
    private final Constructor constructor;
    private final String[] reservedWords;

    public ReflectiveMultiResolver(
        String name,
        String signature,
        String description,
        String[] signatures,
        Class clazz)
    {
        this(name, signature, description, signatures, clazz, null);
    }

    public ReflectiveMultiResolver(
        String name,
        String signature,
        String description,
        String[] signatures,
        Class clazz,
        String[] reservedWords)
    {
        super(name, signature, description, signatures);
        try {
            this.constructor = clazz.getConstructor(new Class[] {FunDef.class});
        } catch (NoSuchMethodException e) {
            throw Util.newInternal(
                e, "Error while registering resolver class " + clazz);
        }
        this.reservedWords = reservedWords;
    }

    protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
        try {
            return (FunDef) constructor.newInstance(new Object[] {dummyFunDef});
        } catch (InstantiationException e) {
            throw Util.newInternal(
                e, "Error while instantiating FunDef '" + getSignature() + "'");
        } catch (IllegalAccessException e) {
            throw Util.newInternal(
                e, "Error while instantiating FunDef '" + getSignature() + "'");
        } catch (InvocationTargetException e) {
            throw Util.newInternal(
                e, "Error while instantiating FunDef '" + getSignature() + "'");
        }
    }

    public String[] getReservedWords() {
        if (reservedWords != null) {
            return reservedWords;
        }
        return super.getReservedWords();
    }
}

// End ReflectiveMultiResolver.java
