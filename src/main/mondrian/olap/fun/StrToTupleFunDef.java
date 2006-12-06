/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2006 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.fun;

import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;
import mondrian.calc.StringCalc;
import mondrian.calc.impl.AbstractMemberCalc;
import mondrian.calc.impl.AbstractTupleCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.mdx.DimensionExpr;
import mondrian.mdx.HierarchyExpr;
import mondrian.olap.*;
import mondrian.olap.type.Type;
import mondrian.olap.type.TupleType;
import mondrian.olap.type.MemberType;
import mondrian.olap.type.StringType;
import mondrian.resource.MondrianResource;

import java.util.ArrayList;
import java.util.List;

/**
 * Definition of the <code>StrToTuple</code> MDX function.
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 23, 2006
 */
class StrToTupleFunDef extends FunDefBase {
    static final ResolverImpl Resolver = new ResolverImpl();

    private StrToTupleFunDef(int[] parameterTypes) {
        super("StrToTuple",
            "StrToTuple(<String Expression>)",
            "Constructs a tuple from a string.",
            Syntax.Function, Category.Tuple, parameterTypes);
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final StringCalc stringCalc = compiler.compileString(call.getArg(0));
        Type elementType = call.getType();
        if (elementType instanceof MemberType) {
            final Hierarchy hierarchy = elementType.getHierarchy();
            return new AbstractMemberCalc(call, new Calc[] {stringCalc}) {
                public Member evaluateMember(Evaluator evaluator) {
                    String string = stringCalc.evaluateString(evaluator);
                    return parseMember(evaluator, string, hierarchy);
                }
            };
        } else {
            TupleType tupleType = (TupleType) elementType;
            final Hierarchy[] hierarchies =
                new Hierarchy[tupleType.elementTypes.length];
            for (int i = 0; i < tupleType.elementTypes.length; i++) {
                hierarchies[i] = tupleType.elementTypes[i].getHierarchy();
            }
            return new AbstractTupleCalc(call, new Calc[] {stringCalc}) {
                public Member[] evaluateTuple(Evaluator evaluator) {
                    String string = stringCalc.evaluateString(evaluator);
                    return parseTuple(evaluator, string, hierarchies);
                }
            };
        }
    }

    /**
     * Parses a tuple, such as "([Gender].[M], [Marital Status].[S])".
     *
     * @param evaluator Evaluator, provides a {@link mondrian.olap.SchemaReader}
     *   and {@link Cube}
     * @param string String to parse
     * @param hierarchies Hierarchies of the members
     * @return Tuple represented as array of members
     */
    private Member[] parseTuple(
        Evaluator evaluator, String string, Hierarchy[] hierarchies)
    {
        final Member[] members = new Member[hierarchies.length];
        int i = StrToSetFunDef.parseTuple(
            evaluator, string, 0, members, hierarchies);
        // todo: check for garbage at end of string
        return members;
    }

    private Member parseMember(
        Evaluator evaluator, String string, Hierarchy hierarchy) {
        Member[] members = {null};
        int i = StrToSetFunDef.parseMember(
            evaluator, string, 0, members, new Hierarchy[] {hierarchy}, 0);
        // todo: check for garbage at end of string
        return members[0];
    }

    public Exp createCall(Validator validator, Exp[] args) {
        final int argCount = args.length;
        if (argCount <= 1) {
            throw MondrianResource.instance().MdxFuncArgumentsNum.ex(getName());
        }
        for (int i = 1; i < argCount; i++) {
            final Exp arg = args[i];
            if (arg instanceof DimensionExpr) {
                // if arg is a dimension, switch to dimension's default
                // hierarchy
                DimensionExpr dimensionExpr = (DimensionExpr) arg;
                Dimension dimension = dimensionExpr.getDimension();
                args[i] = new HierarchyExpr(dimension.getHierarchy());
            } else if (arg instanceof Hierarchy) {
                // nothing
            } else {
                throw MondrianResource.instance().MdxFuncNotHier.ex(
                    i + 1, getName());
            }
        }
        return super.createCall(validator, args);
    }

    public Type getResultType(Validator validator, Exp[] args) {
        switch (args.length) {
        case 1:
            // This is a call to the standard version of StrToTuple,
            // which doesn't give us any hints about type.
            return new TupleType(null);

        case 2:
            final Type argType = args[1].getType();
            return new MemberType(
                argType.getDimension(),
                argType.getHierarchy(),
                argType.getLevel(),
                null);

        default: {
            // This is a call to Mondrian's extended version of
            // StrToTuple, of the form
            //   StrToTuple(s, <Hier1>, ... , <HierN>)
            //
            // The result is a tuple
            //  (<Hier1>, ... ,  <HierN>)
            final List<Type> list = new ArrayList<Type>();
            for (int i = 1; i < args.length; i++) {
                Exp arg = args[i];
                final Type type = arg.getType();
                list.add(type);
            }
            final Type[] types = list.toArray(new Type[list.size()]);
            return new TupleType(types);
        }
        }
    }

    private static class ResolverImpl extends ResolverBase {
        ResolverImpl() {
            super("StrToTuple",
                    "StrToTuple(<String Expression>)",
                    "Constructs a tuple from a string.",
                    Syntax.Function);
        }

        public FunDef resolve(
                Exp[] args, Validator validator, int[] conversionCount) {
            if (args.length < 1) {
                return null;
            }
            Type type = args[0].getType();
            if (!(type instanceof StringType)) {
                return null;
            }
            for (int i = 1; i < args.length; i++) {
                Exp exp = args[i];
                if (!(exp instanceof DimensionExpr)) {
                    return null;
                }
            }
            int[] argTypes = new int[args.length];
            argTypes[0] = Category.String;
            for (int i = 1; i < argTypes.length; i++) {
                argTypes[i] = Category.Hierarchy;
            }
            return new StrToTupleFunDef(argTypes);
        }
    }
}

// End StrToTupleFunDef.java
