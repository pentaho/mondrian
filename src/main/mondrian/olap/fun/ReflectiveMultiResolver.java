/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2002-2002 Kana Software, Inc.
// Copyright (C) 2002-2006 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.fun;

import mondrian.olap.Exp;
import mondrian.olap.FunDef;
import mondrian.olap.Util;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * Resolver which uses reflection to instantiate a {@link FunDef}.
 * This reduces the amount of anonymous classes.
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 23, 2006
 */
public class ReflectiveMultiResolver extends MultiResolver {
    private final Constructor constructor;
    private final String[] reservedWords;

    public ReflectiveMultiResolver(
            String name, String signature, String description,
            String[] signatures, Class clazz) {
        this(name, signature, description, signatures, clazz, null);
    }

    public ReflectiveMultiResolver(
            String name, String signature, String description,
            String[] signatures, Class clazz,
            String[] reservedWords) {
        super(name, signature, description, signatures);
        try {
            this.constructor = clazz.getConstructor(new Class[] {FunDef.class});
        } catch (NoSuchMethodException e) {
            throw Util.newInternal(e, "Error while registering resolver class " + clazz);
        }
        this.reservedWords = reservedWords;
    }

    protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
        try {
            return (FunDef) constructor.newInstance(new Object[] {dummyFunDef});
        } catch (InstantiationException e) {
            throw Util.newInternal(e, "Error while instantiating FunDef '" + getSignature() + "'");
        } catch (IllegalAccessException e) {
            throw Util.newInternal(e, "Error while instantiating FunDef '" + getSignature() + "'");
        } catch (InvocationTargetException e) {
            throw Util.newInternal(e, "Error while instantiating FunDef '" + getSignature() + "'");
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
