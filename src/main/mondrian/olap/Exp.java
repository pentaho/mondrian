/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 1999-2002 Kana Software, Inc. and others.
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
	 * Returns the type of the expression.  Allowable values are {@link
	 * #CatUnknown}, {@link #CatArray}, {@link #CatDimension}, {@link
	 * #CatHierarchy}, {@link #CatLevel}, {@link #CatLogical}, {@link
	 * #CatMember}, {@link #CatNumeric}, {@link #CatSet}, {@link #CatString},
	 * {@link #CatTuple}, {@link #CatSymbol}, {@link #CatParameter}, {@link
	 * #CatCube}, {@link #CatValue}.
	 **/
	int getType();
	final int CatUnknown   = 0;
	final int CatArray     = 1;
	final int CatDimension = 2;
	final int CatHierarchy = 3;
	final int CatLevel     = 4;
	final int CatLogical   = 5;
	final int CatMember    = 6;
	final int CatNumeric   = 7;
	final int CatSet       = 8;
	final int CatString    = 9;
	final int CatTuple     = 10;
	final int CatSymbol    = 11;
	final int CatParameter = 12;
	final int CatCube      = 13;
	/** Any expression yielding a string or numeric value. **/
	final int CatValue     = 14;
	/** Expression which is to be evaluated later. **/
	final int CatExpression = 0;
	/** Flag which indicates that expression must be constant. **/
	final int CatConstant = 64;
	/** Mask to remove flags. **/
	final int CatMask = 31;
	final EnumeratedValues catEnum = new EnumeratedValues(new String[] {
		"unknown", "array", "dimension", "hierarchy", "level", "logical",
		"member", "numeric", "set", "string", "tuple", "symbol", "parameter",
		"cube", "value"});

	boolean isSet();
	boolean isMember();
	boolean isElement();
	boolean isParameter();
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
	void unparse(PrintWriter pw, ElementCallback callback);
	Exp resolve(Query q);
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
}

// End Exp.java
