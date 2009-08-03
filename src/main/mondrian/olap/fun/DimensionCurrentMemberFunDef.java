/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2007 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.fun;

import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;
import mondrian.calc.DimensionCalc;
import mondrian.calc.impl.AbstractExpCompiler;
import mondrian.mdx.ResolvedFunCall;

/**
 * Definition of the <code>&lt;Dimension&gt;.CurrentMember</code> MDX builtin
 * function.
 *
 * <p>Syntax:
 * <blockquote><code>&lt;Dimension&gt;.CurrentMember</code></blockquote>
 *
 * <p>XXX TODO: obsolete
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 23, 2006
 */
public class DimensionCurrentMemberFunDef extends FunDefBase {
    static final DimensionCurrentMemberFunDef instance =
            new DimensionCurrentMemberFunDef();

    private DimensionCurrentMemberFunDef() {
        super("CurrentMember",
                "Returns the current member along a dimension during an iteration.",
                "pmd");
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final DimensionCalc dimensionCalc =
                compiler.compileDimension(call.getArg(0));
        return new AbstractExpCompiler.DimensionCurrentMemberCalc(call, dimensionCalc);
    }
}

// End DimensionCurrentMemberFunDef.java
