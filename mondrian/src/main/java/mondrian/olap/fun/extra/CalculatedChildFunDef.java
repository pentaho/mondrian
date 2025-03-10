/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package mondrian.olap.fun.extra;

import mondrian.calc.*;
import mondrian.calc.impl.AbstractMemberCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;
import mondrian.olap.fun.FunDefBase;

import java.util.List;

/**
 * Definition of the <code>CalculatedChild</code> MDX function.
 *
 * <p>Syntax:
 * <blockquote><code>&lt;Member&gt;
 * CalculatedChild(&lt;String&gt;)</code></blockquote>
 *
 * @author bchow
 * @since 2006/4/12
 */
public class CalculatedChildFunDef extends FunDefBase {
    public static final CalculatedChildFunDef instance =
        new CalculatedChildFunDef();

    CalculatedChildFunDef() {
        super(
            "CalculatedChild",
            "Returns an existing calculated child member with name <String> from the specified <Member>.",
            "mmmS");
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final MemberCalc memberCalc = compiler.compileMember(call.getArg(0));
        final StringCalc stringCalc = compiler.compileString(call.getArg(1));

        return new AbstractMemberCalc(
            call,
            new Calc[] {memberCalc, stringCalc})
        {
            public Member evaluateMember(Evaluator evaluator) {
                Member member = memberCalc.evaluateMember(evaluator);
                String name = stringCalc.evaluateString(evaluator);
                return getCalculatedChild(member, name, evaluator);
            }
        };
    }

    private Member getCalculatedChild(
        Member parent,
        String childName,
        Evaluator evaluator)
    {
        final SchemaReader schemaReader =
                evaluator.getQuery().getSchemaReader(true);
        Level childLevel = parent.getLevel().getChildLevel();
        if (childLevel == null) {
            return parent.getHierarchy().getNullMember();
        }
        List<Member> calcMemberList =
            schemaReader.getCalculatedMembers(childLevel);

        for (Member child : calcMemberList) {
            // the parent check is required in case there are parallel children
            // with the same names
            if (child.getParentMember().equals(parent)
                && child.getName().equals(childName))
            {
                return child;
            }
        }

        return parent.getHierarchy().getNullMember();
    }
}


// End CalculatedChildFunDef.java
