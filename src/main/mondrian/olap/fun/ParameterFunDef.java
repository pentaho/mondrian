/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2003-2005 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Feb 14, 2003
*/
package mondrian.olap.fun;

import mondrian.olap.*;
import mondrian.olap.type.*;
import mondrian.mdx.HierarchyExpr;
import mondrian.mdx.DimensionExpr;

/**
 * A <code>ParameterFunDef</code> is a pseudo-function describing calls to
 * <code>Parameter</code> and <code>ParamRef</code> functions. It exists only
 * fleetingly, and is then converted into a {@link mondrian.olap.Parameter}.
 * For internal use only.
 *
 * @author jhyde
 * @version $Id$
 * @since Feb 14, 2003
 */
public class ParameterFunDef extends FunDefBase {
    public final String parameterName;
    private final Hierarchy hierarchy;
    public final Exp exp;
    public final String parameterDescription;

    ParameterFunDef(FunDef funDef,
            String parameterName,
            Hierarchy hierarchy,
            int returnType,
            Exp exp,
            String description) {
        super(funDef.getName(),
                funDef.getSignature(),
                funDef.getDescription(),
                funDef.getSyntax(),
                returnType,
                funDef.getParameterCategories());
        assertPrecondition(getName().equals("Parameter") ||
                getName().equals("ParamRef"));
        this.parameterName = parameterName;
        this.hierarchy = hierarchy;
        this.exp = exp;
        this.parameterDescription = description;
    }

    public Exp createCall(Validator validator, Exp[] args) {
        Parameter param = validator.createOrLookupParam(this, args);
        return validator.validate(param);
    }

    public Type getResultType(Validator validator, Exp[] args) {
        switch (returnCategory) {
        case Category.String:
            return new StringType();
        case Category.Numeric:
            return new NumericType();
        case Category.Numeric | Category.Integer:
            return new DecimalType(Integer.MAX_VALUE, 0);
        case Category.Member:
            return MemberType.forHierarchy(hierarchy);
        default:
            throw Category.instance.badValue(returnCategory);
        }
    }

    private static boolean isConstantHierarchy(Exp typeArg) {
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
            if (hierarchyCall.getFunDef().getName().equals("Hierarchy") &&
                    hierarchyCall.getArgCount() > 0 &&
                    hierarchyCall.getArg(0) instanceof FunCall) {
                FunCall currentMemberCall = (FunCall) hierarchyCall.getArg(0);
                if (currentMemberCall.getFunDef().getName().equals("CurrentMember") &&
                        currentMemberCall.getArgCount() > 0 &&
                        currentMemberCall.getArg(0) instanceof DimensionExpr) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Resolves calls to the <code>Parameter</code> MDX function.
     */
    public static class ParameterResolver extends MultiResolver {
        private static final String[] SIGNATURES = new String[]{
                        "fS#yS#", "fS#yS", // Parameter(string const, symbol, string[, string const]): string
                        "fn#yn#", "fn#yn", // Parameter(string const, symbol, numeric[, string const]): numeric
                        "fm#hm#", "fm#hm", // Parameter(string const, hierarchy constant, member[, string const]): member
                    };

        public ParameterResolver() {
            super("Parameter",
                    "Parameter(<Name>, <Type>, <DefaultValue>, <Description>)",
                    "Returns default value of parameter.", SIGNATURES);
        }

        public String[] getReservedWords() {
            return new String[]{"NUMERIC", "STRING"};
        }

        protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
            String parameterName;
            if (args[0] instanceof Literal &&
                    args[0].getCategory() == Category.String) {
                parameterName = (String) ((Literal) args[0]).getValue();
            } else {
                throw newEvalException(dummyFunDef, "Parameter name must be a string constant");
            }
            Exp typeArg = args[1];
            Hierarchy hierarchy;
            int type;
            switch (typeArg.getCategory()) {
            case Category.Hierarchy:
            case Category.Dimension:
                hierarchy = typeArg.getType().getHierarchy();
                if (hierarchy == null || !isConstantHierarchy(typeArg)) {
                    throw newEvalException(dummyFunDef, "Invalid hierarchy for parameter '" + parameterName + "'");
                }
                type = Category.Member;
                break;
            case Category.Symbol:
                hierarchy = null;
                String s = (String) ((Literal) typeArg).getValue();
                if (s.equalsIgnoreCase("NUMERIC")) {
                    type = Category.Numeric;
                    break;
                } else if (s.equalsIgnoreCase("STRING")) {
                    type = Category.String;
                    break;
                }
                // fall through and throw error
            default:
                // Error is internal because the function call has already been
                // type-checked.
                throw newEvalException(dummyFunDef,
                        "Invalid type for parameter '" + parameterName + "'; expecting NUMERIC, STRING or a hierarchy");
            }
            Exp exp = args[2];
            if (exp.getCategory() != type) {
                String typeName = Category.instance.getName(type).toUpperCase();
                throw newEvalException(dummyFunDef, "Default value of parameter '" + parameterName + "' is inconsistent with its type, " + typeName);
            }
            if (type == Category.Member) {
                Hierarchy expHierarchy = exp.getType().getHierarchy();
                if (expHierarchy != hierarchy) {
                    throw newEvalException(dummyFunDef, "Default value of parameter '" + parameterName + "' must belong to the hierarchy " + hierarchy);
                }
            }
            String parameterDescription = null;
            if (args.length > 3) {
                if (args[3] instanceof Literal &&
                        args[3].getCategory() == Category.String) {
                    parameterDescription = (String) ((Literal) args[3]).getValue();
                } else {
                    throw newEvalException(dummyFunDef, "Description of parameter '" + parameterName + "' must be a string constant");
                }
            }

            return new ParameterFunDef(dummyFunDef, parameterName, hierarchy, type, exp, parameterDescription);
        }
    }

    /**
     * Resolves calls to the <code>ParamRef</code> MDX function.
     */
    public static class ParamRefResolver extends MultiResolver {
        public ParamRefResolver() {
            super("ParamRef", "ParamRef(<Name>)", "Returns the current value of this parameter. If it is null, returns the default value.", new String[]{"fv#"});
        }

        protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
            String parameterName;
            if (args[0] instanceof Literal &&
                    args[0].getCategory() == Category.String) {
                parameterName = (String) ((Literal) args[0]).getValue();
            } else {
                throw newEvalException(dummyFunDef, "Parameter name must be a string constant");
            }
            return new ParameterFunDef(dummyFunDef, parameterName, null, Category.Unknown, null, null);
        }
    }
}

// End ParameterFunDef.java
