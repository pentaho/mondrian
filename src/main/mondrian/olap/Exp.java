/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 1999-2003 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 20 January, 1999
*/

package mondrian.olap;

import java.io.PrintWriter;

/**
 * An <code>Exp</code> is an MDX expression.
 **/
public interface Exp {
	Object clone();
	/**
	 * Returns the {@link Category} of the expression.
	 * @post Category.instance().isValid(return)
	 **/
	int getType();

	boolean isSet();
	boolean isMember();
	boolean isElement();
	boolean isEmptySet();
	/**
	 * Returns the dimension of a this expression, or null if no dimension is
	 * defined. Applicable only to set expressions.
	 *
	 * <p>Example 1:
	 * <blockquote><pre>
	 * [Sales].children
	 * </pre></blockquote>
	 * has dimension <code>[Sales]</code>.</p>
	 *
	 * <p>Example 2:
	 * <blockquote><pre>
	 * order(except([Promotion Media].[Media Type].members,
	 *              {[Promotion Media].[Media Type].[No Media]}),
	 *       [Measures].[Unit Sales], DESC)
	 * </pre></blockquote>
	 * has dimension [Promotion Media].</p>
	 *
	 * <p>Example 3:
	 * <blockquote><pre>
	 * CrossJoin([Product].[Product Department].members,
	 *           [Gender].members)
	 * </pre></blockquote>
	 * has no dimension (well, actually it is [Product] x [Gender], but we
	 * can't represent that, so we return null);</p>
	 **/
	Dimension getDimension();
	Hierarchy getHierarchy();
	void unparse(PrintWriter pw);
	Exp resolve(Resolver resolver);
	boolean usesDimension(Dimension dimension);
	/**
	 * Adds 'exp' as the right child of the CrossJoin whose left child has
	 * 'iPosition' hierarchies (hence 'iPosition' - 1 CrossJoins) under it.  If
	 * added successfully, returns -1, else returns the number of hierarchies
	 * under this node.
	 **/
	int addAtPosition(Exp e, int iPosition);
	Object evaluate(Evaluator evaluator);
	Object evaluateScalar(Evaluator evaluator);

    /**
     * Provides context necessary to resolve identifiers to objects, function
     * calls to specific functions.
     *
     * <p>An expression calls {@link #resolveChild} on each of its children,
     * which in turn calls {@link Exp#resolve}.
     */
    interface Resolver {
        Query getQuery();
        Exp resolveChild(Exp exp);
        Parameter resolveChild(Parameter parameter);
        void resolveChild(MemberProperty memberProperty);
        void resolveChild(QueryAxis axis);
        void resolveChild(Formula formula);
        boolean requiresExpression();
        FunTable getFunTable();
        /**
         * Creates or retrieves the parameter corresponding to a "Parameter" or
         * "ParamRef" function call.
         */
        Parameter createOrLookupParam(FunCall call);
    }
}

// End Exp.java
