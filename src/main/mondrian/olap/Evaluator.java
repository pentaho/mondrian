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
	/** Creates a new evaluator with the same state.
	* Equivalent to {@link #push(Member[]) push(new Member[0])}. **/
	Evaluator push();
	/** Creates a new evaluator with the same state except for one member.
	 * Equivalent to {@link #push(Member[]) push(new Member[]
	 * &#124;member&#125;)}. **/
	Evaluator push(Member member);
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
	/**
	 * Returns number of ancestor evaluators. Used to check for infinite
	 * loops.
	 *
	 * @post return getParent() == null ? 0 : getParent().getDepth() + 1
	 */
	int getDepth();
	/**
	 * Returns parent evaluator.
	 */
	Evaluator getParent();
	/**
	 * Retrieves the value of property <code>name</code>. If more than one
	 * member in the current context defines that property, the one with the
	 * highest solve order has precedence.
	 */
	Object getProperty(String name);
}

// End Evaluator.java
