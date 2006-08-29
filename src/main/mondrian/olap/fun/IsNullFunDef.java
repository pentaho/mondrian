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
import mondrian.calc.MemberCalc;
import mondrian.calc.impl.AbstractBooleanCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.Evaluator;
import mondrian.olap.FunDef;
import mondrian.olap.Member;
import mondrian.olap.Literal;

/**
 * Definition of the <code>IS NULL</code> MDX function.
 *
 * @author medstat
 * @version $Id$
 * @since Aug 21, 2006
 */
class IsNullFunDef extends FunDefBase {
    /**
     * Resolves calls to this operation. Operator is applied to 2 args, the
     * second of which is always the NULL literal, because the NULL literal is
     * the only expression whose type is <NullType> (for now, at least).
     */
    static final ReflectiveMultiResolver Resolver = new ReflectiveMultiResolver(
            "IS", // unparsing 2nd arg will provide the 'NULL'.
            "<Expression> IS NULL",
            "Returns whether an object is null",
            new String[]{"ibmU", "iblU", "ibhU", "ibdU"},
            IsNullFunDef.class);

    public IsNullFunDef(FunDef dummyFunDef) {
        super(dummyFunDef);
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        assert call.getArgCount() == 2;
        assert call.getArg(1) == Literal.nullValue;
        final MemberCalc memberCalc = compiler.compileMember(call.getArg(0));
        return new AbstractBooleanCalc(call, new Calc[]{memberCalc}) {
            public boolean evaluateBoolean(Evaluator evaluator) {
                Member member = memberCalc.evaluateMember(evaluator);
                return member.isNull();
            }
        };
    }
}

// End IsNullFunDef.java
