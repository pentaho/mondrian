/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2002-2005 Julian Hyde
// Copyright (C) 2005-2011 Pentaho and others
// All Rights Reserved.
*/

package mondrian.olap.fun;

import mondrian.olap.*;

import java.util.List;

/**
 * A <code>SimpleResolver</code> resolves a single, non-overloaded function.
 *
 * @author jhyde
 * @since 3 March, 2002
 */
class SimpleResolver implements Resolver {
    private  final FunDef funDef;

    SimpleResolver(FunDef funDef) {
        this.funDef = funDef;
    }

    public FunDef getFunDef() {
        return funDef;
    }

    public String getName() {
        return funDef.getName();
    }

    public String getDescription() {
        return funDef.getDescription();
    }

    public String getSignature() {
        return funDef.getSignature();
    }

    public Syntax getSyntax() {
        return funDef.getSyntax();
    }

    public String[] getReservedWords() {
        return FunUtil.emptyStringArray;
    }

    public FunDef resolve(
        Exp[] args,
        Validator validator,
        List<Conversion> conversions)
    {
        int[] parameterTypes = funDef.getParameterCategories();
        if (parameterTypes.length != args.length) {
            return null;
        }
        for (int i = 0; i < args.length; i++) {
            if (!validator.canConvert(
                    i, args[i], parameterTypes[i], conversions))
            {
                return null;
            }
        }
        return funDef;
    }

    public boolean requiresExpression(int k) {
        int[] parameterTypes = funDef.getParameterCategories();
        return (k >= parameterTypes.length)
            || (parameterTypes[k] != Category.Set);
    }
}

// End SimpleResolver.java
