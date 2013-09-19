/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2002-2005 Julian Hyde and others
// Copyright (C) 2005-2011 Pentaho and others
// All Rights Reserved.
*/

package mondrian.olap.fun;

import mondrian.olap.*;

import java.util.List;

/**
 * A <code>MultiResolver</code> considers several overloadings of the same
 * function. If one of these overloadings matches the actual arguments, it
 * calls the factory method {@link #createFunDef}.
 *
 * @author jhyde
 * @since Feb 12, 2003
 */
public abstract class MultiResolver extends FunUtil implements Resolver {
    private final String name;
    private final String signature;
    private final String description;
    private final String[] signatures;
    private final Syntax syntax;

    /**
     * Creates a <code>MultiResolver</code>.
     *
     * @param name Name of function or operator
     * @param signature Signature of function or operator
     * @param description Description of function or operator
     * @param signatures Array of possible signatures, each of which is an
     *     encoding of the syntactic type, return type, and parameter
     *     types of this operator. The "Members" operator has a syntactic
     *     type "pxd" which means "an operator with
     *     {@link Syntax#Property property} syntax (p) which returns a set
     *     (x) and takes a dimension (d) as its argument".
     *     See {@link FunUtil#decodeSyntacticType(String)},
     *     {@link FunUtil#decodeReturnCategory(String)},
     *     {@link FunUtil#decodeParameterCategories(String)}.
     */
    protected MultiResolver(
        String name,
        String signature,
        String description,
        String[] signatures)
    {
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

    public FunDef getFunDef() {
        return null;
    }

    public FunDef resolve(
        Exp[] args,
        Validator validator,
        List<Conversion> conversions)
    {
outer:
        for (String signature : signatures) {
            int[] parameterTypes = decodeParameterCategories(signature);
            if (parameterTypes.length != args.length) {
                continue;
            }
            conversions.clear();
            for (int i = 0; i < args.length; i++) {
                if (!validator.canConvert(
                        i, args[i], parameterTypes[i], conversions))
                {
                    continue outer;
                }
            }
            int returnType = decodeReturnCategory(signature);
            FunDef dummy = createDummyFunDef(this, returnType, args);
            return createFunDef(args, dummy);
        }
        return null;
    }

    public boolean requiresExpression(int k) {
        for (String signature : signatures) {
            int[] parameterTypes = decodeParameterCategories(signature);
            if ((k < parameterTypes.length)
                && parameterTypes[k] == Category.Set)
            {
                return false;
            }
        }
        return true;
    }

    protected abstract FunDef createFunDef(Exp[] args, FunDef dummyFunDef);
}

// End MultiResolver.java
