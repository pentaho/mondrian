/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/


package mondrian.olap.fun;

import mondrian.calc.*;
import mondrian.calc.impl.AbstractLevelCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;
import mondrian.olap.type.LevelType;
import mondrian.olap.type.Type;

/**
 * Definition of the <code>&lt;Member&gt;.Level</code> MDX builtin function.
 *
 * @author jhyde
 * @since Mar 23, 2006
 */
public class MemberLevelFunDef extends FunDefBase {
    static final MemberLevelFunDef instance = new MemberLevelFunDef();

    private MemberLevelFunDef() {
        super("Level", "Returns a member's level.", "plm");
    }

    public Type getResultType(Validator validator, Exp[] args) {
        final Type argType = args[0].getType();
        return LevelType.forType(argType);
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final MemberCalc memberCalc =
                compiler.compileMember(call.getArg(0));
        return new CalcImpl(call, memberCalc);
    }

    public static class CalcImpl extends AbstractLevelCalc {
        private final MemberCalc memberCalc;

        public CalcImpl(Exp exp, MemberCalc memberCalc) {
            super(exp, new Calc[] {memberCalc});
            this.memberCalc = memberCalc;
        }

        public Level evaluateLevel(Evaluator evaluator) {
            Member member = memberCalc.evaluateMember(evaluator);
            return member.getLevel();
        }
    }
}

// End MemberLevelFunDef.java
