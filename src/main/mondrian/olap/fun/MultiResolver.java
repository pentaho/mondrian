/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2002-2005 Kana Software, Inc. and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Feb 12, 2003
*/
package mondrian.olap.fun;

import mondrian.olap.*;

/**
 * A <code>MultiResolver</code> considers several overloadings of the same
 * function. If one of these overloadings matches the actual arguments, it
 * calls the factory method {@link #createFunDef}.
 *
 * @author jhyde
 * @since Feb 12, 2003
 * @version $Id$
 */
abstract class MultiResolver extends FunUtil implements Resolver {
    private final String name;
    private final String signature;
    private final String description;
    private final String[] signatures;
    private final Syntax syntax;

    MultiResolver(
            String name,
            String signature,
            String description,
            String[] signatures) {
        this.name = name;
        this.signature = signature;
        this.description = description;
        this.signatures = signatures;
        Util.assertTrue(signatures.length > 0);
        this.syntax = decodeSyntacticType(signatures[0]);
        for (int i = 1; i < signatures.length; i++) {
            Util.assertTrue(decodeSyntacticType(signatures[i]) == syntax);
        }
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public String getSignature() {
        return signature;
    }

    public Syntax getSyntax() {
        return syntax;
    }

    public String[] getReservedWords() {
        return emptyStringArray;
    }

    public String[] getSignatures() {
        return signatures;
    }

    public FunDef resolve(
            Exp[] args, Validator validator, int[] conversionCount) {
outer:
        for (int j = 0; j < signatures.length; j++) {
            int[] parameterTypes = decodeParameterCategories(signatures[j]);
            if (parameterTypes.length != args.length) {
                continue;
            }
            for (int i = 0; i < args.length; i++) {
                if (!validator.canConvert(
                        args[i], parameterTypes[i], conversionCount)) {
                    continue outer;
                }
            }
            final String signature = signatures[j];
            int returnType = decodeReturnCategory(signature);
            FunDef dummy = new FunDefBase(this, returnType, parameterTypes) {};
            return createFunDef(args, dummy);
        }
        return null;
    }

    public boolean requiresExpression(int k) {
        for (int j = 0; j < signatures.length; j++) {
            int[] parameterTypes = decodeParameterCategories(signatures[j]);
            if ((k < parameterTypes.length) &&
                    parameterTypes[k] == Category.Set) {
                return false;
            }
        }
        return true;
    }

    protected abstract FunDef createFunDef(Exp[] args, FunDef dummyFunDef);
}

// End MultiResolver.java
