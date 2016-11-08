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

/**
 * Definition of the <code>Dimensions(&lt;String Expression&gt;)</code>
 * MDX builtin function.
 *
 * <p>NOTE: Actually returns a hierarchy. This is consistent with Analysis
 * Services.
 *
 * @author jhyde
 * @since Jul 20, 2009
 */
class DimensionsStringFunDef extends FunDefBase {
    public static final FunDefBase INSTANCE = new DimensionsStringFunDef();

    private DimensionsStringFunDef() {
        super(
            "Dimensions",
            "Returns the hierarchy whose name is specified by a string.",
            "fhS");
    }

    public Type getResultType(Validator validator, Exp[] args) {
        return HierarchyType.Unknown;
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler)
    {
        final StringCalc stringCalc =
            compiler.compileString(call.getArg(0));
        return new AbstractHierarchyCalc(call, new Calc[] {stringCalc})
        {
            public Hierarchy evaluateHierarchy(Evaluator evaluator) {
                String dimensionName =
                    stringCalc.evaluateString(evaluator);
                return findHierarchy(dimensionName, evaluator);
            }
        };
    }

    /**
     * Looks up a hierarchy in the current cube with a given name.
     *
     * @param name Hierarchy name
     * @param evaluator Evaluator
     * @return Hierarchy
     */
    Hierarchy findHierarchy(String name, Evaluator evaluator) {
        if (name.indexOf("[") == -1) {
            name = Util.quoteMdxIdentifier(name);
        }
        OlapElement o = evaluator.getSchemaReader().lookupCompound(
            evaluator.getCube(),
            parseIdentifier(name),
            false,
            Category.Hierarchy);
        if (o instanceof Hierarchy) {
            return (Hierarchy) o;
        } else if (o == null) {
            throw newEvalException(
                this, "Hierarchy '" + name + "' not found");
        } else {
            throw newEvalException(
                this, "Hierarchy(" + name + ") found " + o);
        }
    }
}

// End DimensionsStringFunDef.java
