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
 * <code>RangeFunDef</code> implements the ':' operator.
 *
 * @author jhyde
 * @since 3 March, 2002
 * @version $Id$
 **/
class RangeFunDef extends FunDefBase {
	RangeFunDef() {
		super(
			":",
			"{<Member> : <Member>}",
			"Infix colon operator returns the set of members between a given pair of members.",
			FunDef.TypeInfix,
			Exp.CatSet,
			new int[] {Exp.CatMember, Exp.CatMember});
	}
	public void unparse(Exp[] args, PrintWriter pw, ElementCallback callback) {
		ExpBase.unparseList(pw, args, "{", " : ", "}", callback);
	}
	public int getReturnType() {
		return Exp.CatSet;
	}
	public int[] getParameterTypes() {
		return new int[] {Exp.CatMember, Exp.CatMember};
	}
}

// End RangeFunDef.java
