/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2005-2005 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.fun;

import mondrian.olap.*;
import mondrian.olap.type.*;
import mondrian.olap.type.DimensionType;
import mondrian.olap.type.LevelType;
import mondrian.spi.UserDefinedFunction;

/**
 * Resolver for user-defined functions.
 */
public class UdfResolver implements Resolver {
    private final UserDefinedFunction udf;
    private static final String[] emptyStringArray = new String[0];

    public UdfResolver(UserDefinedFunction udf) {
        this.udf = udf;
    }
    public String getName() {
        return udf.getName();
    }

    public String getDescription() {
        return udf.getDescription();
    }

    public Syntax getSyntax() {
        return udf.getSyntax();
    }

    public FunDef resolve(
            Exp[] args, Validator validator, int[] conversionCount) {
        final Type[] parameterTypes = udf.getParameterTypes();
        if (args.length != parameterTypes.length) {
            return null;
        }
        int[] parameterCategories = new int[parameterTypes.length];
        Type[] argTypes = new Type[parameterTypes.length];
        for (int i = 0; i < parameterTypes.length; i++) {
            Type parameterType = parameterTypes[i];
            final Exp arg = args[i];
            final Type argType = argTypes[i] = arg.getTypeX();
            if (parameterType.equals(argType)) {
                continue;
            }
            final int parameterCategory = typeToCategory(parameterType);
            if (!validator.canConvert(
                    arg, parameterCategory, conversionCount)) {
                return null;
            }
            parameterCategories[i] = parameterCategory;
        }
        final Type returnType = udf.getReturnType(argTypes);
        final int returnCategory = typeToCategory(returnType);
        return new UdfFunDef(returnCategory, parameterCategories);
    }

    private static int typeToCategory(Type type) {
        if (type instanceof NumericType) {
            return Category.Numeric;
        } else if (type instanceof BooleanType) {
            return Category.Logical;
        } else if (type instanceof DimensionType) {
            return Category.Dimension;
        } else if (type instanceof HierarchyType) {
            return Category.Hierarchy;
        } else if (type instanceof MemberType) {
            return Category.Member;
        } else if (type instanceof LevelType) {
            return Category.Level;
        } else if (type instanceof ScalarType) {
            return Category.Value;
        } else if (type instanceof SetType) {
            return Category.Set;
        } else if (type instanceof StringType) {
            return Category.String;
        } else if (type instanceof SymbolType) {
            return Category.Symbol;
        } else if (type instanceof TupleType) {
            return Category.Tuple;
        } else {
            throw Util.newInternal("Unknown type " + type);
        }
    }

    public boolean requiresExpression(int k) {
        return false;
    }

    public String[] getReservedWords() {
        final String[] reservedWords = udf.getReservedWords();
        return reservedWords == null ? emptyStringArray : reservedWords;
    }

    /**
     * Adapter which converts a {@link UserDefinedFunction} into a
     * {@link FunDef}.
     */
    private class UdfFunDef extends FunDefBase {
        public UdfFunDef(int returnCategory, int[] parameterCategories) {
            super(UdfResolver.this, returnCategory, parameterCategories);
        }

        // implement FunDef
        public Object evaluate(Evaluator evaluator, Exp[] args) {
            return udf.execute(evaluator, args);
        }

        // Be conservative. Assume that this function depends on everything.
        // This will effectively disable caching.
        public boolean callDependsOn(FunCall call, Dimension dimension) {
            return true;
        }
    }
}

// End UdfResolver.java
