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

import mondrian.olap.*;
import mondrian.olap.type.Type;
import mondrian.olap.type.SetType;
import mondrian.olap.type.TupleType;
import mondrian.olap.type.StringType;
import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;
import mondrian.mdx.ResolvedFunCall;
import mondrian.mdx.DimensionExpr;
import mondrian.mdx.HierarchyExpr;
import mondrian.resource.MondrianResource;

import java.util.ArrayList;
import java.util.List;

/**
 * Definition of the <code>StrToSet</code> MDX builtin function.
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 23, 2006
 */
class StrToSetFunDef extends FunDefBase {
    static final ResolverImpl Resolver = new ResolverImpl();

    public StrToSetFunDef(int[] parameterTypes) {
        super("StrToSet", "<Set> StrToSet(<String>[, <Dimension>...])",
                "Constructs a set from a string expression.",
                Syntax.Function, Category.Set, parameterTypes);
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        throw new UnsupportedOperationException();
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
            } else if (arg instanceof HierarchyExpr) {
                // nothing
            } else {
                throw MondrianResource.instance().MdxFuncNotHier.ex(
                    i + 1, getName());
            }
        }
        return super.createCall(validator, args);
    }

    public Type getResultType(Validator validator, Exp[] args) {
        if (args.length == 1) {
            // This is a call to the standard version of StrToSet,
            // which doesn't give us any hints about type.
            return new SetType(null);
        } else {
            // This is a call to Mondrian's extended version of
            // StrToSet, of the form
            //   StrToSet(s, <Hier1>, ... , <HierN>)
            //
            // The result is a set of tuples
            //  (<Hier1>, ... ,  <HierN>)
            final List<Type> list = new ArrayList<Type>();
            for (int i = 1; i < args.length; i++) {
                Exp arg = args[i];
                final Type type = arg.getType();
                list.add(type);
            }
            final Type[] types = list.toArray(new Type[list.size()]);
            return new SetType(new TupleType(types));
        }
    }

    private static class ResolverImpl extends ResolverBase {
        ResolverImpl() {
            super(
                    "StrToSet",
                    "StrToSet(<String Expression>)",
                    "Constructs a set from a string expression.",
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
            return new StrToSetFunDef(argTypes);
        }
    }
}

// End StrToSetFunDef.java
