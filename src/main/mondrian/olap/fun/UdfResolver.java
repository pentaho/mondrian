/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2005-2005 Julian Hyde
// Copyright (C) 2005-2012 Pentaho
// All Rights Reserved.
*/

package mondrian.olap.fun;

import mondrian.calc.*;
import mondrian.calc.impl.*;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;
import mondrian.olap.type.*;
import mondrian.spi.UserDefinedFunction;

import java.util.*;

/**
 * Resolver for user-defined functions.
 *
 * @author jhyde
 * @since 2.0
 */
public class UdfResolver implements Resolver {
    private final UdfFactory factory;
    private final UserDefinedFunction udf;

    private static final String[] emptyStringArray = new String[0];

    public UdfResolver(UdfFactory factory) {
        this.factory = factory;
        this.udf = factory.create();
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
        return new UdfFunDef(parameterCategories, returnType);
    }

    public FunDef resolve(
        Exp[] args,
        Validator validator,
        List<Conversion> conversions)
    {
        final Type[] parameterTypes = udf.getParameterTypes();
        if (args.length != parameterTypes.length) {
            return null;
        }
        int[] parameterCategories = new int[parameterTypes.length];
        Type[] castArgTypes = new Type[parameterTypes.length];
        for (int i = 0; i < parameterTypes.length; i++) {
            Type parameterType = parameterTypes[i];
            final Exp arg = args[i];
            final Type argType = arg.getType();
            final int parameterCategory =
                TypeUtil.typeToCategory(parameterType);
            if (!validator.canConvert(
                    i, arg, parameterCategory, conversions))
            {
                return null;
            }
            parameterCategories[i] = parameterCategory;
            if (!parameterType.equals(argType)) {
                castArgTypes[i] =
                    FunDefBase.castType(argType, parameterCategory);
            }
        }
        final Type returnType = udf.getReturnType(castArgTypes);
        return new UdfFunDef(parameterCategories, returnType);
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
        private Type returnType;

        public UdfFunDef(int[] parameterCategories, Type returnType) {
            super(
                UdfResolver.this,
                TypeUtil.typeToCategory(returnType),
                parameterCategories);
            this.returnType = returnType;
        }

        public Type getResultType(Validator validator, Exp[] args) {
            return returnType;
        }

        public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
            final Exp[] args = call.getArgs();
            Calc[] calcs = new Calc[args.length];
            UserDefinedFunction.Argument[] expCalcs =
                new UserDefinedFunction.Argument[args.length];
            for (int i = 0; i < args.length; i++) {
                Exp arg = args[i];
                final Calc calc = calcs[i] = compiler.compileAs(
                    arg,
                    castType(arg.getType(), parameterCategories[i]),
                    ResultStyle.ANY_LIST);
                calcs[i] = calc;
                final Calc scalarCalc = compiler.compileScalar(arg, true);
                final ListCalc listCalc;
                final IterCalc iterCalc;
                if (arg.getType() instanceof SetType) {
                    listCalc = compiler.compileList(arg, true);
                    iterCalc = compiler.compileIter(arg);
                } else {
                    listCalc = null;
                    iterCalc = null;
                }
                expCalcs[i] = new CalcExp(calc, scalarCalc, listCalc, iterCalc);
            }

            // Create a new instance of the UDF, because some UDFs use member
            // variables as state.
            UserDefinedFunction udf2 = factory.create();
            if (call.getType() instanceof SetType) {
                return new ListCalcImpl(call, calcs, udf2, expCalcs);
            } else {
                return new ScalarCalcImpl(call, calcs, udf2, expCalcs);
            }
        }
    }

    /**
     * Expression that evaluates a scalar user-defined function.
     */
    private static class ScalarCalcImpl extends GenericCalc {
        private final Calc[] calcs;
        private final UserDefinedFunction udf;
        private final UserDefinedFunction.Argument[] args;

        public ScalarCalcImpl(
            ResolvedFunCall call,
            Calc[] calcs,
            UserDefinedFunction udf,
            UserDefinedFunction.Argument[] args)
        {
            super(call);
            this.calcs = calcs;
            this.udf = udf;
            this.args = args;
        }

        public Calc[] getCalcs() {
            return calcs;
        }

        public Object evaluate(Evaluator evaluator) {
            try {
                return udf.execute(evaluator, args);
            } catch (Exception e) {
                return FunUtil.newEvalException(
                    "Exception while executing function " + udf.getName(),
                    e);
            }
        }

        public boolean dependsOn(Hierarchy hierarchy) {
            // Be pessimistic. This effectively disables expression caching.
            return true;
        }
    }

    /**
     * Expression that evaluates a list user-defined function.
     */
    private static class ListCalcImpl extends AbstractListCalc {
        private final UserDefinedFunction udf;
        private final UserDefinedFunction.Argument[] args;

        public ListCalcImpl(
            ResolvedFunCall call,
            Calc[] calcs,
            UserDefinedFunction udf,
            UserDefinedFunction.Argument[] args)
        {
            super(call, calcs);
            this.udf = udf;
            this.args = args;
        }

        public TupleList evaluateList(Evaluator evaluator) {
            final List list = (List) udf.execute(evaluator, args);

            // If arity is 1, assume they have returned a list of members.
            // For other arity, assume a list of member arrays.
            if (getType().getArity() == 1) {
                //noinspection unchecked
                return new UnaryTupleList((List<Member>) list);
            } else {
                // Use an adapter to make a list of member arrays look like
                // a list of members laid end-to-end.
                final int arity = getType().getArity();
                //noinspection unchecked
                final List<Member[]> memberArrayList = (List<Member[]>) list;
                return new ListTupleList(
                    arity,
                    new AbstractList<Member>() {
                        @Override
                        public Member get(int index) {
                            return memberArrayList.get(index / arity)
                                [index % arity];
                        }

                        @Override
                        public int size() {
                            return memberArrayList.size() * arity;
                        }
                    }
                );
            }
        }

        @Override
        public boolean dependsOn(Hierarchy hierarchy) {
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
        private final IterCalc iterCalc;
        private final ListCalc listCalc;

        /**
         * Creates a CalcExp.
         *
         * @param calc Compiled expression
         * @param scalarCalc Compiled expression that evaluates to a scalar
         * @param listCalc Compiled expression that evaluates an MDX set to
         *     a java list
         * @param iterCalc Compiled expression that evaluates an MDX set to
         */
        public CalcExp(
            Calc calc,
            Calc scalarCalc,
            ListCalc listCalc,
            IterCalc iterCalc)
        {
            this.calc = calc;
            this.scalarCalc = scalarCalc;
            this.listCalc = listCalc;
            this.iterCalc = iterCalc;
        }

        public Type getType() {
            return calc.getType();
        }

        public Object evaluate(Evaluator evaluator) {
            return adapt(calc.evaluate(evaluator));
        }

        public Object evaluateScalar(Evaluator evaluator) {
            return scalarCalc.evaluate(evaluator);
        }

        public List evaluateList(Evaluator eval) {
            if (listCalc == null) {
                throw new RuntimeException("Expression is not a set");
            }
            return adaptList(listCalc.evaluateList(eval));
        }

        public Iterable evaluateIterable(Evaluator eval) {
            if (iterCalc == null) {
                throw new RuntimeException("Expression is not a set");
            }
            return adaptIterable(iterCalc.evaluateIterable(eval));
        }

        /**
         * Adapts the output of {@link TupleList} and {@link TupleIterable}
         * calculator expressions to the old style, that returned either members
         * or arrays of members.
         *
         * @param o Output of calc
         * @return Output in new format (lists and iterables over lists of
         *    members)
         */
        private Object adapt(Object o) {
            if (o instanceof TupleIterable) {
                return adaptIterable((TupleIterable) o);
            }
            return o;
        }

        private List adaptList(final TupleList tupleList) {
            // List is required to be mutable -- so make a copy.
            if (tupleList.getArity() == 1) {
                return new ArrayList<Member>(tupleList.slice(0));
            } else {
                return new ArrayList<Member[]>(
                    TupleCollections.asMemberArrayList(tupleList));
            }
        }

        private Iterable adaptIterable(final TupleIterable tupleIterable) {
            if (tupleIterable instanceof TupleList) {
                return adaptList((TupleList) tupleIterable);
            }
            if (tupleIterable.getArity() == 1) {
                return tupleIterable.slice(0);
            } else {
                return TupleCollections.asMemberArrayIterable(tupleIterable);
            }
        }
    }

    /**
     * Factory for {@link UserDefinedFunction}.
     *
     * <p>This factory is required because a user-defined function is allowed
     * to store state in itself. Therefore it is unsanitary to use the same
     * UDF in the function table for validation and runtime. In the function
     * table there is a factory. We use one instance of instance of the UDF to
     * validate, and create another for the runtime plan.</p>
     */
    public interface UdfFactory {
        /**
         * Creates a UDF.
         *
         * @return UDF
         */
        UserDefinedFunction create();
    }

    /**
     * Implementation of {@link UdfFactory} that instantiates a given class
     * using a public default constructor.
     */
    public static class ClassUdfFactory implements UdfResolver.UdfFactory {
        private final Class<? extends UserDefinedFunction> clazz;
        private final String name;

        /**
         * Creates a ClassUdfFactory.
         *
         * @param clazz Class to instantiate
         * @param name Name
         */
        public ClassUdfFactory(
            Class<? extends UserDefinedFunction> clazz,
            String name)
        {
            this.clazz = clazz;
            this.name = name;
            assert clazz != null;
        }

        public UserDefinedFunction create() {
            return Util.createUdf(clazz, name);
        }
    }
}

// End UdfResolver.java
