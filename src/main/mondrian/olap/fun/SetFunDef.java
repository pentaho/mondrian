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
import java.util.Vector;

/**
 * <code>SetFunDef</code> implements the 'set' function (whose syntax is the
 * brace operator, <code>{ ... }</code>).
 *
 * @author jhyde
 * @since 3 March, 2002
 * @version $Id$
 **/
class SetFunDef extends FunDefBase {
	SetFunDef(Resolver resolver, int syntacticType, int[] argTypes) {
		super(resolver, syntacticType, Exp.CatSet, argTypes);
	}

	public void unparse(Exp[] args, PrintWriter pw, ElementCallback callback) {
		ExpBase.unparseList(pw, args, "{", ", ", "}", callback);
	}
	public Hierarchy getHierarchy(Exp[] args) {
		// All of the members in {<Member1>[,<MemberI>]...} must have the same
		// Hierarchy.  But if there are no members, we can't derive a
		// Hierarchy, so we return null.
		return (args.length == 0) ?
			null :
			args[0].getHierarchy();
	}
	public Object evaluate(Evaluator evaluator, Exp[] args) {
		Vector vector = null;
		for (int i = 0; i < args.length; i++) {
			ExpBase arg = (ExpBase) args[i];
			Object o;
			if (arg instanceof Member) {
				o = arg;
			} else {
				Member[] members = arg.isConstantTuple();
				if (members != null) {
					o = members;
				} else {
					o = arg.evaluate(evaluator);
				}
			}
			if (o instanceof Vector) {
				Vector vector2 = (Vector) o;
				if (vector == null) {
					vector = vector2;
				} else {
					for (int j = 0, count = vector2.size(); j <
							 count; j++) {
						Object o2 = vector2.elementAt(j);
						if (o2 instanceof Member &&
								((Member) o2).isNull()) {
							continue;
						}
						vector.addElement(o2);
					}
				}
			} else {
				if (o instanceof Member &&
						((Member) o).isNull()) {
					continue;
				}
				if (vector == null) {
					vector = new Vector();
				}
				vector.addElement(o);
			}
		}
		return vector;
	}
}

// End SetFunDef.java
