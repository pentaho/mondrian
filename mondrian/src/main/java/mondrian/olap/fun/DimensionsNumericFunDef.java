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
import mondrian.calc.impl.AbstractHierarchyCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;
import mondrian.olap.type.HierarchyType;
import mondrian.olap.type.Type;
import mondrian.rolap.RolapCube;
import mondrian.rolap.RolapHierarchy;

import java.util.List;

/**
 * Definition of the <code>Dimensions(&lt;Numeric Expression&gt;)</code>
 * MDX builtin function.
 *
 * <p>NOTE: Actually returns a hierarchy. This is consistent with Analysis
 * Services.
 *
 * @author jhyde
 * @since Jul 20, 2009
 */
class DimensionsNumericFunDef extends FunDefBase {
    public static final FunDefBase INSTANCE = new DimensionsNumericFunDef();

    private DimensionsNumericFunDef() {
        super(
            "Dimensions",
            "Returns the hierarchy whose zero-based position within the cube "
            + "is specified by a numeric expression.",
            "fhn");
    }

    public Type getResultType(Validator validator, Exp[] args) {
        return HierarchyType.Unknown;
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler)
    {
        final IntegerCalc integerCalc =
            compiler.compileInteger(call.getArg(0));
        return new AbstractHierarchyCalc(call, new Calc[] {integerCalc})
        {
            public Hierarchy evaluateHierarchy(Evaluator evaluator) {
                int n = integerCalc.evaluateInteger(evaluator);
                return nthHierarchy(evaluator, n);
            }
        };
    }

    RolapHierarchy nthHierarchy(Evaluator evaluator, int n) {
        RolapCube cube = (RolapCube) evaluator.getCube();
        List<RolapHierarchy> hierarchies = cube.getHierarchies();
        if (n >= hierarchies.size() || n < 0) {
            throw newEvalException(
                this, "Index '" + n + "' out of bounds");
        }
        return hierarchies.get(n);
    }
}

// End DimensionsNumericFunDef.java
