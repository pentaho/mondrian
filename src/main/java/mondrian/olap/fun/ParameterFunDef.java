/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2003-2005 Julian Hyde
// Copyright (C) 2005-2009 Pentaho
// All Rights Reserved.
*/
package mondrian.olap.fun;

import mondrian.mdx.*;
import mondrian.olap.*;
import mondrian.olap.type.*;

import java.util.ArrayList;
import java.util.List;

/**
 * A <code>ParameterFunDef</code> is a pseudo-function describing calls to
 * <code>Parameter</code> and <code>ParamRef</code> functions. It exists only
 * fleetingly, and is then converted into a {@link mondrian.olap.Parameter}.
 * For internal use only.
 *
 * @author jhyde
 * @since Feb 14, 2003
 */
public class ParameterFunDef extends FunDefBase {
    public final String parameterName;
    private final Type type;
    public final Exp exp;
    public final String parameterDescription;

    ParameterFunDef(
        FunDef funDef,
        String parameterName,
        Type type,
        int returnCategory,
        Exp exp,
        String description)
    {
        super(
            funDef.getName(),
            funDef.getSignature(),
            funDef.getDescription(),
            funDef.getSyntax(),
            returnCategory,
            funDef.getParameterCategories());
        assertPrecondition(
            getName().equals("Parameter")
            || getName().equals("ParamRef"));
        this.parameterName = parameterName;
        this.type = type;
        this.exp = exp;
        this.parameterDescription = description;
    }

    public Exp createCall(Validator validator, Exp[] args) {
        Parameter parameter = validator.createOrLookupParam(
            this.getName().equals("Parameter"),
            parameterName, type, exp, parameterDescription);
        return new ParameterExpr(parameter);
    }

    public Type getResultType(Validator validator, Exp[] args) {
        return type;
    }

    private static boolean isConstant(Exp typeArg) {
        if (typeArg instanceof LevelExpr) {
            // e.g. "[Time].[Quarter]"
            return true;
        }
        if (typeArg instanceof HierarchyExpr) {
            // e.g. "[Time].[By Week]"
            return true;
        }
        if (typeArg instanceof DimensionExpr) {
            // e.g. "[Time]"
            return true;
        }
        if (typeArg instanceof FunCall) {
            // e.g. "[Time].CurrentMember.Hierarchy". They probably wrote
            // "[Time]", and the automatic type conversion did the rest.
            FunCall hierarchyCall = (FunCall) typeArg;
            if (hierarchyCall.getFunName().equals("Hierarchy")
                && hierarchyCall.getArgCount() > 0
                && hierarchyCall.getArg(0) instanceof FunCall)
            {
                FunCall currentMemberCall = (FunCall) hierarchyCall.getArg(0);
                if (currentMemberCall.getFunName().equals("CurrentMember")
                    && currentMemberCall.getArgCount() > 0
                    && currentMemberCall.getArg(0) instanceof DimensionExpr)
                {
                    return true;
                }
            }
        }
        return false;
    }

    public static String getParameterName(Exp[] args) {
        if (args[0] instanceof Literal
            && args[0].getCategory() == Category.String)
        {
            return (String) ((Literal) args[0]).getValue();
        } else {
            throw Util.newInternal("Parameter name must be a string constant");
        }
    }

    /**
     * Returns an approximate type for a parameter, based upon the 1'th
     * argument. Does not use the default value expression, so this method
     * can safely be used before the expression has been validated.
     */
    public static Type getParameterType(Exp[] args) {
        if (args[1] instanceof Id) {
            Id id = (Id) args[1];
            String[] names = id.toStringArray();
            if (names.length == 1) {
                final String name = names[0];
                if (name.equals("NUMERIC")) {
                    return new NumericType();
                }
                if (name.equals("STRING")) {
                    return new StringType();
                }
            }
        } else if (args[1] instanceof Literal) {
            final Literal literal = (Literal) args[1];
            if (literal.getValue().equals("NUMERIC")) {
                return new NumericType();
            } else if (literal.getValue().equals("STRING")) {
                return new StringType();
            }
        } else if (args[1] instanceof MemberExpr) {
            return new MemberType(null, null, null, null);
        }
        return new StringType();
    }

    /**
     * Resolves calls to the <code>Parameter</code> MDX function.
     */
    public static class ParameterResolver extends MultiResolver {
        private static final String[] SIGNATURES = {
            // Parameter(string const, symbol, string[, string const]): string
            "fS#yS#", "fS#yS",
            // Parameter(string const, symbol, numeric[, string const]): numeric
            "fn#yn#", "fn#yn",
            // Parameter(string const, hierarchy constant, member[, string
            // const[, symbol]]): member
            "fm#hm#", "fm#hm",
            // Parameter(string const, hierarchy constant, set[, string
            // const]): set
            "fx#hx#", "fx#hx",
        };

        public ParameterResolver() {
            super(
                "Parameter",
                "Parameter(<Name>, <Type>, <DefaultValue>, <Description>, <Set>)",
                "Returns default value of parameter.",
                SIGNATURES);
        }

        public String[] getReservedWords() {
            return new String[]{"NUMERIC", "STRING"};
        }

        protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
            String parameterName = getParameterName(args);
            Exp typeArg = args[1];
            int category;
            Type type = typeArg.getType();
            switch (typeArg.getCategory()) {
            case Category.Dimension:
            case Category.Hierarchy:
            case Category.Level:
                Dimension dimension = type.getDimension();
                if (!isConstant(typeArg)) {
                    throw newEvalException(
                        dummyFunDef,
                        "Invalid parameter '" + parameterName
                        + "'. Type must be a NUMERIC, STRING, or a dimension, "
                        + "hierarchy or level");
                }
                if (dimension == null) {
                    throw newEvalException(
                        dummyFunDef,
                        "Invalid dimension for parameter '"
                        + parameterName + "'");
                }
                type =
                    new MemberType(
                        type.getDimension(),
                        type.getHierarchy(),
                        type.getLevel(),
                        null);
                category = Category.Member;
                break;

            case Category.Symbol:
                String s = (String) ((Literal) typeArg).getValue();
                if (s.equalsIgnoreCase("NUMERIC")) {
                    category = Category.Numeric;
                    type = new NumericType();
                    break;
                } else if (s.equalsIgnoreCase("STRING")) {
                    category = Category.String;
                    type = new StringType();
                    break;
                }
                // fall through and throw error
            default:
                // Error is internal because the function call has already been
                // type-checked.
                throw newEvalException(
                    dummyFunDef,
                    "Invalid type for parameter '" + parameterName
                    + "'; expecting NUMERIC, STRING or a hierarchy");
            }

            // Default value
            Exp exp = args[2];
            Validator validator =
                createSimpleValidator(BuiltinFunTable.instance());
            final List<Conversion> conversionList = new ArrayList<Conversion>();
            String typeName = Category.instance.getName(category).toUpperCase();
            if (!validator.canConvert(2, exp, category, conversionList)) {
                throw newEvalException(
                    dummyFunDef,
                    "Default value of parameter '" + parameterName
                    + "' is inconsistent with its type, " + typeName);
            }
            if (exp.getCategory() == Category.Set
                && category == Category.Member)
            {
                // Default value is a set; take this an indication that
                // the type is 'set of <member type>'.
                type = new SetType(type);
            }
            if (category == Category.Member) {
                Type expType = exp.getType();
                if (expType instanceof SetType) {
                    expType = ((SetType) expType).getElementType();
                }
                if (distinctFrom(type.getDimension(), expType.getDimension())
                    || distinctFrom(type.getHierarchy(), expType.getHierarchy())
                    || distinctFrom(type.getLevel(), expType.getLevel()))
                {
                    throw newEvalException(
                        dummyFunDef,
                        "Default value of parameter '" + parameterName
                        + "' is not consistent with the parameter type '"
                        + type);
                }
            }

            String parameterDescription = null;
            if (args.length > 3) {
                if (args[3] instanceof Literal
                    && args[3].getCategory() == Category.String)
                {
                    parameterDescription =
                        (String) ((Literal) args[3]).getValue();
                } else {
                    throw newEvalException(
                        dummyFunDef,
                        "Description of parameter '" + parameterName
                        + "' must be a string constant");
                }
            }

            return new ParameterFunDef(
                dummyFunDef, parameterName, type, category,
                exp, parameterDescription);
        }

        private static <T> boolean distinctFrom(T t1, T t2) {
            return t1 != null
               && t2 != null
               && !t1.equals(t2);
        }
    }

    /**
     * Resolves calls to the <code>ParamRef</code> MDX function.
     */
    public static class ParamRefResolver extends MultiResolver {
        public ParamRefResolver() {
            super(
                "ParamRef",
                "ParamRef(<Name>)",
                "Returns the current value of this parameter. If it is null, returns the default value.",
                new String[]{"fv#"});
        }

        protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
            String parameterName = getParameterName(args);
            return new ParameterFunDef(
                dummyFunDef, parameterName, null, Category.Unknown, null,
                null);
        }
    }
}

// End ParameterFunDef.java
