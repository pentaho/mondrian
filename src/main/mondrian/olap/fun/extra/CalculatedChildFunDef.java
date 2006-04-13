/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2006 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.fun.extra;

import mondrian.olap.*;
import mondrian.olap.fun.FunDefBase;
import mondrian.calc.*;
import mondrian.calc.impl.AbstractMemberCalc;
import mondrian.mdx.ResolvedFunCall;

import java.util.List;

/**
 * Definition of the
 * <code>&lt;Member&gt;.CalculatedChild(&lt;String&gt;)</code> MDX
 * function.
 *
 * @author bchow
 * @version $Id$
 * @since 2006/4/12
 */
public class CalculatedChildFunDef extends FunDefBase {
	static final CalculatedChildFunDef instance = new CalculatedChildFunDef();

    CalculatedChildFunDef() {
    	super("CalculatedChild", "<Member>.CalculatedChild(<String>)",
    			"Returns an existing calculated child member with name <String> from the specified <Member>.", "mmmS");
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
    	final MemberCalc memberCalc = compiler.compileMember(call.getArg(0));
    	final StringCalc stringCalc = compiler.compileString(call.getArg(1));

        return new AbstractMemberCalc(call,  new Calc[] {memberCalc, stringCalc}) {
	        public Member evaluateMember(Evaluator evaluator) {
	        	Member member = memberCalc.evaluateMember(evaluator);
	        	String name = stringCalc.evaluateString(evaluator);
	        	Member child = getCalculatedChild(member, name, evaluator);
	        	return child;
	        }
	     };
    }

    private Member getCalculatedChild(
            Member parent, String childName, Evaluator evaluator) {

        final SchemaReader schemaReader =
                evaluator.getQuery().getSchemaReader(true);
        Level childLevel = parent.getLevel().getChildLevel();
        if (childLevel == null) {
        	return parent.getHierarchy().getNullMember();
        }
        List calcMemberList = schemaReader.getCalculatedMembers(childLevel);

        for (int i = 0; i < calcMemberList.size(); i++) {
            Member child = (Member) calcMemberList.get(i);
        	// the parent check is required in case there are parallel children
        	// with the same names
        	if (child.getParentMember() == parent &&
                    child.getName().equals(childName)) {
        		return child;
            }
        }

        return parent.getHierarchy().getNullMember();
    }
}


// End CalculatedChildFunDef.java
