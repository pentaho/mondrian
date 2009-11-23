/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2009-2009 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.fun;

import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;
import mondrian.calc.impl.AbstractMemberIterCalc;
import mondrian.calc.impl.AbstractTupleIterCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.mdx.NamedSetExpr;
import mondrian.olap.*;
import mondrian.olap.type.SetType;

import java.util.List;


/**
 * Definition of the <code>AS</code> MDX operator.
 *
 * <p>Using <code>AS</code>, you can define an alias for an MDX expression
 * anywhere it appears in a query, and use that alias as you would a calculated
 * yet.
 *
 * @author jhyde
 * @version $Id$
 * @since Oct 7, 2009
 */
class AsFunDef extends FunDefBase {
    public static final Resolver RESOLVER = new ResolverImpl();
    private final Query.ScopedNamedSet scopedNamedSet;

    /**
     * Creates an AsFunDef.
     *
     * @param scopedNamedSet Named set definition
     */
    private AsFunDef(Query.ScopedNamedSet scopedNamedSet) {
        super(
            "AS",
            "<Expression> AS <Name>",
            "Assigns an alias to an expression",
            "ixxn");
        this.scopedNamedSet = scopedNamedSet;
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        // Argument 0, the definition of the set, has been resolved since the
        // scoped named set was created. Implicit conversions, like converting
        // a member to a set, have been performed. Use the new expression.
        scopedNamedSet.setExp(call.getArg(0));

        if (((SetType) call.getType()).getArity() == 1) {
            return new AbstractMemberIterCalc(call, new Calc[0]) {
                public Iterable<Member> evaluateMemberIterable(
                    Evaluator evaluator)
                {
                    final Evaluator.NamedSetEvaluator namedSetEvaluator =
                        evaluator.getNamedSetEvaluator(scopedNamedSet, false);
                    return namedSetEvaluator.evaluateMemberIterable();
                }
            };
        } else {
            return new AbstractTupleIterCalc(call, new Calc[0]) {
                public Iterable<Member []> evaluateTupleIterable(
                    Evaluator evaluator)
                {
                    final Evaluator.NamedSetEvaluator namedSetEvaluator =
                        evaluator.getNamedSetEvaluator(scopedNamedSet, false);
                    return namedSetEvaluator.evaluateTupleIterable();
                }
            };
        }
    }

    private static class ResolverImpl extends ResolverBase {
        public ResolverImpl() {
            super("AS", null, null, Syntax.Infix);
        }

        public FunDef resolve(
            Exp[] args,
            Validator validator,
            List<Conversion> conversions)
        {
            final Exp exp = args[0];
            if (!validator.canConvert(
                0, args[0], Category.Set, conversions))
            {
                return null;
            }

            // By the time resolve is called, the id argument has already been
            // resolved... to a named set, namely itself. That's not pretty.
            // We'd rather it stayed as an id, and we'd rather that a named set
            // was not visible in the scope that defines it. But we can work
            // with this.
            final String name =
                ((NamedSetExpr) args[1]).getNamedSet().getName();

            final Query.ScopedNamedSet scopedNamedSet =
                (Query.ScopedNamedSet) ((NamedSetExpr) args[1]).getNamedSet();
//                validator.getQuery().createScopedNamedSet(
//                    name, (QueryPart) exp, exp);
            return new AsFunDef(scopedNamedSet);
        }
    }
}

// End AsFunDef.java
