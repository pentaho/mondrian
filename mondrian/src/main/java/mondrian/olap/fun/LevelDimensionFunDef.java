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
import mondrian.calc.impl.AbstractDimensionCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;

/**
 * Definition of the <code>&lt;Level&gt;.Dimension</code>
 * MDX builtin function.
 *
 * @author jhyde
 * @since Jul 20, 2009
 */
class LevelDimensionFunDef extends FunDefBase {
    public static final FunDefBase INSTANCE = new LevelDimensionFunDef();

    public LevelDimensionFunDef() {
        super(
            "Dimension",
            "Returns the dimension that contains a specified level.", "pdl");
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler)
    {
        final LevelCalc levelCalc =
            compiler.compileLevel(call.getArg(0));
        return new AbstractDimensionCalc(call, new Calc[] {levelCalc}) {
            public Dimension evaluateDimension(Evaluator evaluator) {
                Level level =  levelCalc.evaluateLevel(evaluator);
                return level.getDimension();
            }
        };
    }
}

// End LevelDimensionFunDef.java
