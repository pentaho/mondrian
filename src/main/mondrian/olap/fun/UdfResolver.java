/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2005-2006 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.fun;

import mondrian.olap.*;
import mondrian.olap.type.*;
import mondrian.spi.UserDefinedFunction;
import mondrian.calc.*;
import mondrian.calc.impl.GenericCalc;
import mondrian.mdx.ResolvedFunCall;

/**
 * Resolver for user-defined functions.
 *
 * @author jhyde
 * @since 2.0
 * @version $Id$
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

    public String getSignature() {
        Type[] parameterTypes = udf.getParameterTypes();
        int[] parameterCategories = new int[parameterTypes.length];
        for (int i = 0; i < parameterCategories.length; i++) {
            parameterCategories[i] = TypeUtil.typeToCategory(parameterTypes[i]);
        }
        Type returnType = udf.getReturnType(parameterTypes);
        int returnCategory = TypeUtil.typeToCategory(returnType);
        return getSyntax().getSignature(
            getName(),
            returnCategory,
            parameterCategories);
    }

    public Syntax getSyntax() {
        return udf.getSyntax();
    }

    public FunDef getFunDef() {
        Type[] parameterTypes = udf.getParameterTypes();
        int[] parameterCategories = new int[parameterTypes.length];
        for (int i = 0; i < parameterCategories.length; i++) {
            parameterCategories[i] = TypeUtil.typeToCategory(parameterTypes[i]);
        }
        Type returnType = udf.getReturnType(parameterTypes);
        int returnCategory = TypeUtil.typeToCategory(returnType);
        return new UdfFunDef(returnCategory, parameterCategories);
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
            final Type argType = argTypes[i] = arg.getType();
            if (parameterType.equals(argType)) {
                continue;
            }
            final int parameterCategory = TypeUtil.typeToCategory(parameterType);
            if (!validator.canConvert(
                    arg, parameterCategory, conversionCount)) {
                return null;
            }
            parameterCategories[i] = parameterCategory;
        }
        final Type returnType = udf.getReturnType(argTypes);
        final int returnCategory = TypeUtil.typeToCategory(returnType);
        return new UdfFunDef(returnCategory, parameterCategories);
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

        public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
            final Exp[] args = call.getArgs();
            Calc[] calcs = new Calc[args.length];
            UserDefinedFunction.Argument[] expCalcs =
                    new UserDefinedFunction.Argument[args.length];
            for (int i = 0; i < args.length; i++) {
                Exp arg = args[i];
                final Calc calc = compiler.compile(arg);
                final Calc scalarCalc = compiler.compileScalar(arg, true);
                expCalcs[i] = new CalcExp(calc, scalarCalc);
            }
            return new CalcImpl(call, calcs, udf, expCalcs);
        }
    }

    /**
     * Expression which evaluates a user-defined function.
     */
    private static class CalcImpl extends GenericCalc {
        private final Calc[] calcs;
        private final UserDefinedFunction udf;
        private final UserDefinedFunction.Argument[] args;

        public CalcImpl(
                ResolvedFunCall call,
                Calc[] calcs,
                UserDefinedFunction udf,
                UserDefinedFunction.Argument[] args) {
            super(call);
            this.calcs = calcs;
            this.udf = udf;
            this.args = args;
        }

        public Calc[] getCalcs() {
            return calcs;
        }

        public Object evaluate(Evaluator evaluator) {
            return udf.execute(evaluator, args);
        }

        public boolean dependsOn(Dimension dimension) {
            // Be pessimistic. This effectively disables expression caching.
            return true;
        }
    }

    /**
     * Wrapper around a {@link Calc} to make it appear as an {@link Exp}.
     * Only the {@link #evaluate(mondrian.olap.Evaluator)}
     * and {@link #evaluateScalar(mondrian.olap.Evaluator)} methods are
     * supported.
     */
    private static class CalcExp implements UserDefinedFunction.Argument {
        private final Calc calc;
        private final Calc scalarCalc;

        public CalcExp(Calc calc, Calc scalarCalc) {
            this.calc = calc;
            this.scalarCalc = scalarCalc;
        }

        public Type getType() {
            return calc.getType();
        }

        public Object evaluate(Evaluator evaluator) {
            return calc.evaluate(evaluator);
        }

        public Object evaluateScalar(Evaluator evaluator) {
            return scalarCalc.evaluate(evaluator);
        }
    }

}

// End UdfResolver.java
