/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.olap.fun;

import mondrian.calc.*;
import mondrian.calc.impl.AbstractDimensionCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;

/**
 * Definition of the <code>&lt;Hierarchy&gt;.Dimension</code> MDX
 * builtin function.
 *
 * @author jhyde
 * @since Mar 23, 2006
 */
public class HierarchyDimensionFunDef extends FunDefBase {
    static final HierarchyDimensionFunDef instance =
        new HierarchyDimensionFunDef();

    private HierarchyDimensionFunDef() {
        super(
            "Dimension",
            "Returns the dimension that contains a specified hierarchy.",
            "pdh");
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final HierarchyCalc hierarchyCalc =
                compiler.compileHierarchy(call.getArg(0));
        return new CalcImpl(call, hierarchyCalc);
    }

    public static class CalcImpl extends AbstractDimensionCalc {
        private final HierarchyCalc hierarchyCalc;

        public CalcImpl(Exp exp, HierarchyCalc hierarchyCalc) {
            super(exp, new Calc[] {hierarchyCalc});
            this.hierarchyCalc = hierarchyCalc;
        }

        public Dimension evaluateDimension(Evaluator evaluator) {
            Hierarchy hierarchy =
                    hierarchyCalc.evaluateHierarchy(evaluator);
            return hierarchy.getDimension();
        }
    }
}

// End HierarchyDimensionFunDef.java
