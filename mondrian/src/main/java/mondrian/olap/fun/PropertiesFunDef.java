/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2004-2005 Julian Hyde
// Copyright (C) 2005-2018 Hitachi Vantara and others
// All Rights Reserved.
*/

package mondrian.olap.fun;

import mondrian.calc.*;
import mondrian.calc.impl.GenericCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;

import java.util.List;

/**
 * Definition of the <code>Properties</code> MDX function.
 *
 * @author jhyde
 * @since Mar 23, 2006
 */
class PropertiesFunDef extends FunDefBase {
    static final ResolverImpl Resolver = new ResolverImpl();

    public PropertiesFunDef(
        String name,
        String signature,
        String description,
        Syntax syntax,
        int returnType,
        int[] parameterTypes)
    {
        super(name, signature, description, syntax, returnType, parameterTypes);
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final MemberCalc memberCalc = compiler.compileMember(call.getArg(0));
        final StringCalc stringCalc = compiler.compileString(call.getArg(1));
        return new GenericCalc(call) {
            public Object evaluate(Evaluator evaluator) {
                return properties(
                    memberCalc.evaluateMember(evaluator),
                        stringCalc.evaluateString(evaluator));
            }

            public Calc[] getCalcs() {
                return new Calc[] {memberCalc, stringCalc};
            }
        };
    }

    static Object properties(Member member, String s) {
        boolean matchCase = MondrianProperties.instance().CaseSensitive.get();
        Object o = member.getPropertyValue(s, matchCase);
        if (o == null) {
            if (!Util.isValidProperty(s, member.getLevel())) {
                throw new MondrianEvaluationException(
                    "Property '" + s
                    + "' is not valid for member '" + member + "'");
            }
        }
        return o;
    }

    /**
     * Resolves calls to the <code>PROPERTIES</code> MDX function.
     */
    private static class ResolverImpl extends ResolverBase {
        private static final int[] PARAMETER_TYPES = {
            Category.Member, Category.String
        };

        private ResolverImpl() {
            super(
                "Properties",
                "<Member>.Properties(<String Expression>)",
                "Returns the value of a member property.",
                Syntax.Method);
        }

        private boolean matches(
            Exp[] args,
            int[] parameterTypes,
            Validator validator,
            List<Conversion> conversions)
        {
            if (parameterTypes.length != args.length) {
                return false;
            }
            for (int i = 0; i < args.length; i++) {
                if (!validator.canConvert(
                        i, args[i], parameterTypes[i], conversions))
                {
                    return false;
                }
            }
            return true;
        }

        public FunDef resolve(
            Exp[] args,
            Validator validator,
            List<Conversion> conversions)
        {
            if (!matches(args, PARAMETER_TYPES, validator, conversions)) {
                return null;
            }
            int returnType = deducePropertyCategory(args[0], args[1]);
            return new PropertiesFunDef(
                getName(), getSignature(), getDescription(), getSyntax(),
                returnType, PARAMETER_TYPES);
        }

        /**
         * Deduces the category of a property. This is possible only if the
         * name is a string literal, and the member's hierarchy is unambigous.
         * If the type cannot be deduced, returns {@link Category#Value}.
         *
         * @param memberExp Expression for the member
         * @param propertyNameExp Expression for the name of the property
         * @return Category of the property
         */
        private int deducePropertyCategory(
            Exp memberExp,
            Exp propertyNameExp)
        {
            if (!(propertyNameExp instanceof Literal)) {
                return Category.Value;
            }
            String propertyName =
                (String) ((Literal) propertyNameExp).getValue();
            Hierarchy hierarchy = memberExp.getType().getHierarchy();
            if (hierarchy == null) {
                return Category.Value;
            }
            Level[] levels = hierarchy.getLevels();
            Property property = lookupProperty(
                levels[levels.length - 1], propertyName);
            if (property == null) {
                // we'll likely get a runtime error
                return Category.Value;
            } else {
                switch (property.getType()) {
                case TYPE_BOOLEAN:
                    return Category.Logical;
                case TYPE_NUMERIC:
                case TYPE_INTEGER:
                case TYPE_LONG:
                    return Category.Numeric;
                case TYPE_STRING:
                    return Category.String;
                case TYPE_DATE:
                case TYPE_TIME:
                case TYPE_TIMESTAMP:
                    return Category.DateTime;
                default:
                    throw Util.badValue(property.getType());
                }
            }
        }

        public boolean requiresExpression(int k) {
            return true;
        }
    }
}

// End PropertiesFunDef.java
