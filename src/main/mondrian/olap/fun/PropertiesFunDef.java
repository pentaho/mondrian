/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2005-2005 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.fun;

import mondrian.olap.*;
import mondrian.calc.*;
import mondrian.calc.impl.GenericCalc;
import mondrian.mdx.ResolvedFunCall;

/**
 * Defines the <code>PROPERTIES</code> MDX function.
 */
class PropertiesFunDef extends FunDefBase {
    public PropertiesFunDef(
            String name,
            String signature,
            String description,
            Syntax syntax,
            int returnType,
            int[] parameterTypes) {
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
        Object o = member.getPropertyValue(s);
        if (o == null) {
            if (!Util.isValidProperty(member, s)) {
                throw new MondrianEvaluationException(
                        "Property '" + s +
                        "' is not valid for member '" + member + "'");
            }
        }
        return o;
    }

    /**
     * Resolves calls to the <code>PROPERTIES</code> MDX function.
     */
    static class Resolver extends ResolverBase {
        Resolver() {
            super("Properties",
                  "<Member>.Properties(<String Expression>)",
                  "Returns the value of a member property.",
                  Syntax.Method);
        }

        public FunDef resolve(
                Exp[] args, Validator validator, int[] conversionCount) {
            final int[] argTypes = new int[]{Category.Member, Category.String};
            if ((args.length != 2) ||
                    (args[0].getCategory() != Category.Member) ||
                    (args[1].getCategory() != Category.String)) {
                return null;
            }
            int returnType;
            if (args[1] instanceof Literal) {
                String propertyName = (String) ((Literal) args[1]).getValue();
                Hierarchy hierarchy = args[0].getType().getHierarchy();
                Level[] levels = hierarchy.getLevels();
                Property property = lookupProperty(
                        levels[levels.length - 1], propertyName);
                if (property == null) {
                    // we'll likely get a runtime error
                    returnType = Category.Value;
                } else {
                    switch (property.getType()) {
                    case Property.TYPE_BOOLEAN:
                        returnType = Category.Logical;
                        break;
                    case Property.TYPE_NUMERIC:
                        returnType = Category.Numeric;
                        break;
                    case Property.TYPE_STRING:
                        returnType = Category.String;
                        break;
                    default:
                        throw Util.newInternal("Unknown property type "
                            + property.getType());
                    }
                }
            } else {
                returnType = Category.Value;
            }
            return new PropertiesFunDef(getName(),
                                        getSignature(),
                                        getDescription(),
                                        getSyntax(),
                                        returnType,
                                        argTypes);
        }

        public boolean requiresExpression(int k) {
            return true;
        }
    }
}

// End PropertiesFunDef.java

