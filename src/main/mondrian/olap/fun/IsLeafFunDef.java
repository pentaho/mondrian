/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2014-2014 Pentaho
// All Rights Reserved.
*/
package mondrian.olap.fun;

import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;
import mondrian.calc.MemberCalc;
import mondrian.calc.impl.AbstractBooleanCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.Evaluator;
import mondrian.olap.FunDef;
import mondrian.olap.Level;
import mondrian.olap.Member;
import mondrian.resource.MondrianResource;
import mondrian.rolap.RolapLevel;
import mondrian.rolap.RolapMember;

/**
 * Definition of the <code>IsLeaf</code> MDX function.
 *
 * <p>Syntax:
 * <blockquote><code>IsLeaf(Member Expression)
 * </code></blockquote>
 */
public class IsLeafFunDef extends FunDefBase {

    static final ReflectiveMultiResolver Resolver =
            new ReflectiveMultiResolver(
                "IsLeaf",
                "IsLeaf(<Member Expression>)",
                "Returns whether a specified member is a leaf member.",
                new String[] { "fbm"},
                IsLeafFunDef.class);


    // A leaf member is a member of a hierarchy that has no children.
    public IsLeafFunDef(FunDef dummyFunDef) {
        super(dummyFunDef);
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final MemberCalc memberCalc =
                compiler.compileMember(call.getArg(0));

        return new AbstractBooleanCalc(call, new Calc[] {memberCalc}) {
            public boolean evaluateBoolean(Evaluator evaluator) {
                RolapMember rolapMember =
                    (RolapMember) memberCalc.evaluateMember(evaluator);
                if (rolapMember == null) {
                    throw newEvalException(
                        MondrianResource.instance().NullValue.ex());
                }

                RolapLevel rolapLevel = rolapMember.getLevel();

                // 1) First check if this is a parent-child hierarchy
                // 2) ragged hierarchy
                // 3) balanced hierarchy
                if (rolapLevel.isParentChild()) {
                    return rolapMember.isParentChildLeaf();
                }

                //Ragged: check if is a ragged hierarchy
                if (rolapLevel.getHierarchy().isRagged()) {
                    if (evaluator.getSchemaReader().getRole().getAccessDetails(
                            rolapMember.getHierarchy())
                            .getBottomLevelDepth() == rolapLevel.getDepth())
                    {
                        return true;
                    }

                    return rolapLevel.getDimension()
                            .getSchema().getSchemaReader()
                            .getMemberChildren(rolapMember).size() == 0;
                }

                // Balanced: check if this level is the last in the array
                 if (rolapLevel.equals(rolapMember.getHierarchy()
                         .getLevels()[rolapMember.getHierarchy()
                         .getLevels().length - 1]))
                 {
                     return true;
                 }

                if (evaluator.getSchemaReader().getRole().getAccessDetails(
                        rolapMember.getHierarchy())
                        .getBottomLevelDepth()  == rolapLevel.getDepth())
                {
                    return true;
                }
                return false;
            }
        };
    }
}

// End IsLeafFunDef.java