/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/

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
