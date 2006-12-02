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
import mondrian.mdx.ResolvedFunCall;
import mondrian.mdx.DimensionExpr;
import mondrian.mdx.HierarchyExpr;
import mondrian.olap.*;
import mondrian.olap.type.Type;
import mondrian.olap.type.TupleType;
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
    static final StrToTupleFunDef instance = new StrToTupleFunDef();

    private StrToTupleFunDef() {
        super("StrToTuple",
                "StrToTuple(<String Expression>)",
                "Constructs a tuple from a string.",
                "ftS");
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        throw Util.needToImplement(this);
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
        if (args.length == 1) {
            // This is a call to the standard version of StrToTuple,
            // which doesn't give us any hints about type.
            return new TupleType(null);
        } else {
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

// End StrToTupleFunDef.java
