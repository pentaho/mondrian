/*
// $Id$
// (C) Copyright 2002 Kana Software, Inc.
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 3 March, 2002
*/
package mondrian.olap.fun;
import mondrian.olap.*;
import java.io.PrintWriter;

/**
 * <code>TupleFunDef</code> implements the '( ... )' operator which builds
 * tuples, as in <code>([Time].CurrentMember,
 * [Stores].[USA].[California])</code>.
 *
 * @author jhyde
 * @since 3 March, 2002
 * @version $Id$
 **/
class TupleFunDef extends FunDefBase
{
	int[] argTypes;
	TupleFunDef(int[] argTypes) {
		super(
			"()",
			"(<Member> [, <Member>]...)",
			"Parenthesis operator constructs a tuple.  If there is only one member, the expression is equivalent to the member expression.",
			FunDef.TypeParentheses,
			Exp.CatTuple,
			argTypes);
		this.argTypes = argTypes;
	}
	public int getReturnType() {
		return Exp.CatTuple;
	}
	public int[] getParameterTypes() {
		return argTypes;
	}
	public void unparse(Exp[] args, PrintWriter pw, ElementCallback callback) {
		ExpBase.unparseList(pw, args, "(", ", ", ")", callback);
	}
	public Hierarchy getHierarchy(Exp[] args) {
		// _Tuple(<Member1>[,<MemberI>]...), which is written
		// (<Member1>[,<MemberI>]...), has Hierarchy [Hie1] x ... x [HieN],
		// which we can't represent, so we return null.  But if there is only
		// one member, it merely represents a parenthesized expression, whose
		// Hierarchy is that of the member.
		return (args.length == 1) ?
			args[0].getHierarchy() :
			null;
	}
	public Object evaluate(Evaluator evaluator, Exp[] args) {
		Member[] members = new Member[args.length];
		for (int i = 0; i < args.length; i++) {
			members[i] = getMemberArg(evaluator, args, i, true);
		}
		return members;
	}
}

// End TupleFunDef.java
