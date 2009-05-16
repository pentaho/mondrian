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
import mondrian.calc.LevelCalc;
import mondrian.calc.impl.AbstractMemberListCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.Member;
import mondrian.olap.Evaluator;
import mondrian.olap.Level;

import java.util.List;

/**
 * Definition of the <code>&lt;Level&gt;.Members</code> MDX function.
 *
 * @author jhyde
 * @version $Id$
 * @since Jan 17, 2009
 */
public class LevelMembersFunDef extends FunDefBase {
    public static final LevelMembersFunDef INSTANCE = new LevelMembersFunDef();

    private LevelMembersFunDef() {
        super("Members", "Returns the set of members in a level.", "pxl");
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final LevelCalc levelCalc =
            compiler.compileLevel(call.getArg(0));
        return new AbstractMemberListCalc(call, new Calc[] {levelCalc}) {
            public List<Member> evaluateMemberList(Evaluator evaluator) {
                Level level = levelCalc.evaluateLevel(evaluator);
                return levelMembers(level, evaluator, false);
            }
        };
    }
}

// End LevelMembersFunDef.java
