/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 27 July, 2001
*/

package mondrian.olap;

/**
 * An <code>Evaluator</code> holds the context necessary to evaluate an
 * expression.
 *
 * @author jhyde
 * @since 27 July, 2001
 * @version $Id$
 **/
public interface Evaluator {
	/** Returns the current cube. */
	Cube getCube();
	/** Creates a new evaluator with the same state. */
	Evaluator push(Member[] members);
	/** Restores previous evaluator. */
	Evaluator pop();
	/** Makes <code>member</code> the current member of its dimension. Returns
	 * the previous context. */
	Member setContext(Member member);
	void setContext(Member[] members);
	Member getContext(Dimension dimension);
	Object evaluateCurrent();
	Object xx(Literal literal);
	Object xx(FunCall funCall);
	Object xx(Id id);
	Object xx(OlapElement mdxElement);
	Object xx(Parameter parameter);
	String format(Object o);
};

// End Evaluator.java
