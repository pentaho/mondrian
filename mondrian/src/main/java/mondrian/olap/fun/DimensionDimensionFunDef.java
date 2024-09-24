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

import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;
import mondrian.calc.impl.ConstantCalc;
import mondrian.mdx.DimensionExpr;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.Dimension;
import mondrian.olap.type.DimensionType;

/**
 * Definition of the <code>&lt;Dimension&gt;.Dimension</code>
 * MDX builtin function.
 *
 * @author jhyde
 * @since Jul 20, 2009
 */
class DimensionDimensionFunDef extends FunDefBase {
    public static final FunDefBase INSTANCE = new DimensionDimensionFunDef();

    private DimensionDimensionFunDef() {
        super(
            "Dimension",
            "Returns the dimension that contains a specified hierarchy.",
            "pdd");
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler)
    {
        Dimension dimension =
            ((DimensionExpr) call.getArg(0)).getDimension();
        return new ConstantCalc(
            DimensionType.forDimension(dimension),
            dimension);
    }
}

// End DimensionDimensionFunDef.java
