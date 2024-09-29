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
import mondrian.calc.impl.AbstractListCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.Evaluator;
import mondrian.olap.Level;

/**
 * Definition of the <code>&lt;Level&gt;.Members</code> MDX function.
 *
 * @author jhyde
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
        return new AbstractListCalc(call, new Calc[] {levelCalc}) {
            public TupleList evaluateList(Evaluator evaluator) {
                Level level = levelCalc.evaluateLevel(evaluator);
                return levelMembers(level, evaluator, false);
            }
        };
    }
}

// End LevelMembersFunDef.java
