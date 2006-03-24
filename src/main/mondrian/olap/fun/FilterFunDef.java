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
import mondrian.calc.ListCalc;
import mondrian.calc.BooleanCalc;
import mondrian.calc.impl.AbstractListCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.type.SetType;
import mondrian.olap.type.MemberType;
import mondrian.olap.*;

import java.util.List;
import java.util.ArrayList;

/**
 * Definition of the <code>Filter</code> MDX function.
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 23, 2006
 */
class FilterFunDef extends FunDefBase {
    static final FilterFunDef instance = new FilterFunDef();

    private FilterFunDef() {
        super(
                "Filter",
                "Filter(<Set>, <Search Condition>)",
                "Returns the set resulting from filtering a set based on a search condition.",
                "fxxb");
    }

    public Calc compileCall(final ResolvedFunCall call, ExpCompiler compiler) {
        final ListCalc listCalc = compiler.compileList(call.getArg(0));
        final BooleanCalc calc = compiler.compileBoolean(call.getArg(1));
        if (((SetType) listCalc.getType()).getElementType() instanceof MemberType) {
            return new AbstractListCalc(call, new Calc[] {listCalc, calc}) {
                public List evaluateList(Evaluator evaluator) {
                    // Use a native evaluator, if more efficient.
                    // TODO: Figure this out at compile time.
                    SchemaReader schemaReader = evaluator.getSchemaReader();
                    NativeEvaluator nativeEvaluator =
                            schemaReader.getNativeSetEvaluator(
                                    call.getFunDef(), call.getArgs(), evaluator, this);
                    if (nativeEvaluator != null) {
                        return (List) nativeEvaluator.execute();
                    }

                    List members = listCalc.evaluateList(evaluator);
                    List result = new ArrayList();
                    Evaluator evaluator2 = evaluator.push();
                    for (int i = 0, count = members.size(); i < count; i++) {
                        Member member = (Member) members.get(i);
                        evaluator2.setContext(member);
                        if (calc.evaluateBoolean(evaluator2)) {
                            result.add(member);
                        }
                    }
                    return result;
                }

                public boolean dependsOn(Dimension dimension) {
                    return anyDependsButFirst(getCalcs(), dimension);
                }
            };
        } else {
            return new AbstractListCalc(call, new Calc[] {listCalc, calc}) {
                public List evaluateList(Evaluator evaluator) {
                    // Use a native evaluator, if more efficient.
                    // TODO: Figure this out at compile time.
                    SchemaReader schemaReader = evaluator.getSchemaReader();
                    NativeEvaluator nativeEvaluator =
                            schemaReader.getNativeSetEvaluator(
                                    call.getFunDef(), call.getArgs(), evaluator, this);
                    if (nativeEvaluator != null) {
                        return (List) nativeEvaluator.execute();
                    }

                    List tupleList = listCalc.evaluateList(evaluator);
                    List result = new ArrayList();
                    Evaluator evaluator2 = evaluator.push();
                    for (int i = 0, count = tupleList.size(); i < count; i++) {
                        Member[] members = (Member []) tupleList.get(i);
                        evaluator2.setContext(members);
                        if (calc.evaluateBoolean(evaluator2)) {
                            result.add(members);
                        }
                    }
                    return result;
                }

                public boolean dependsOn(Dimension dimension) {
                    return anyDependsButFirst(getCalcs(), dimension);
                }
            };
        }
    }
}

// End FilterFunDef.java
