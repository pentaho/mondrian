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
import java.util.*;
import java.io.*;

/**
 * Skeleton implementation of {@link Exp} interface.
 **/
public abstract class ExpBase
	extends QueryPart
	implements Exp
{
	public abstract Object clone();

	static Exp[] cloneArray(Exp[] a)
	{
		Exp[] a2 = new Exp[a.length];
		for (int i = 0; i < a.length; i++)
			a2[i] = (Exp) a[i].clone();
		return a2;
	}

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
	public Dimension getDimension()
	{
		Hierarchy mdxHierarchy = getHierarchy();
		if (mdxHierarchy != null) {
			return mdxHierarchy.getDimension();
		}
		return null;
	}

	public Hierarchy getHierarchy()
	{
		return null;
	}

	public final boolean isSet()
	{
		int cat = getType();
		return cat == CatSet || cat == CatTuple;
	}

	public final boolean isMember()
	{
		return getType() == CatMember;
	}

	public final boolean isElement()
	{
		int category = getType();
		return isMember() ||
			category == CatHierarchy ||
			category == CatLevel ||
			category == CatDimension;
	}

	public final boolean isParameter()
	{
		return getType() == CatParameter;
	}

	public final boolean isEmptySet()
	{
		if (this instanceof FunCall) {
			FunCall f = (FunCall) this;
			return f.getSyntacticType() == FunDef.TypeBraces &&
				f.args.length == 0;
		} else {
			return false;
		}
	}

	/**
	 * Returns an array of {@link Member}s if this is a member or a tuple,
	 * null otherwise.
	 **/
	public final Member[] isConstantTuple()
	{
		if (this instanceof Member) {
			return new Member[] {(Member) this};
		}
		if (!(this instanceof FunCall)) {
			return null;
		}
		FunCall f = (FunCall) this;
		if (!f.isCallToTuple()) {
			return null;
		}
		for (int i = 0; i < f.args.length; i++) {
			if (!(f.args[i] instanceof Member)) {
				return null;
			}
		}
		Member[] members = new Member[f.args.length];
		System.arraycopy(f.args, 0, members, 0, f.args.length);
		return members;
	}

	protected static boolean arrayUsesDimension(Exp[] exps, Dimension dim)
	{
		for (int i = 0; i < exps.length; i++)
			if (exps[i].usesDimension(dim))
				return true;
		return false;
	}

	static Exp[] makeArray(Exp x)
	{
		Exp[] array = new Exp[x == null ? 0 : ((ExpBase) x).getChainLength()];
		for (int i = 0; x != null; x = (Exp) ((ExpBase) x).next) {
			array[i++] = x;
		}
		return array;
	}

	public int addAtPosition(Exp e, int iPosition)
	{
		// Since this method has not been overridden for this type of
		// expression, we presume that the expression has a dimensionality of
		// 1.  We therefore return 1 to indicate that we could not add the
		// expression, and that this expression has a dimensionality of 1.
		return 1;
	}
	public Object evaluate(Evaluator evaluator)
	{
		throw new Error("unsupported");
	}
	public Object evaluateScalar(Evaluator evaluator)
	{
		Object o = evaluate(evaluator);
		if (o instanceof Member) {
			evaluator.setContext((Member) o);
			return evaluator.evaluateCurrent();
		} else if (o instanceof Member[]) {
			evaluator.setContext((Member[]) o);
			return evaluator.evaluateCurrent();
		} else {
			return o;
		}
	}

	public static String getSignature(
			String name, int syntacticType, int returnType, int[] argTypes) {
		String prefix = "";
		switch (syntacticType) {
		case FunDef.TypeInfix:
			// e.g. "<Numeric Expression> / <Numeric Expression>"
			return getTypeDescription(argTypes[0]) + " " + name + " " +
				getTypeDescription(argTypes[1]);
		case FunDef.TypePrefix:
			// e.g. "- <Numeric Expression>"
			return name + " " + getTypeDescription(argTypes[0]);
		case FunDef.TypeProperty:
			// e.g. "<Set>.Current"
			return getTypeDescription(argTypes[0]) + "." + name;
		case FunDef.TypeFunction:
			// e.g. "StripCalculatedMembers(<Set>)"
			return (returnType == CatUnknown ? "" :
					getTypeDescription(returnType) + " ") +
				name + "(" + getTypeDescriptionCommaList(argTypes, 0) +
				")";
		case FunDef.TypeMethod:
			// e.g. "<Member>.Lead(<Numeric Expression>)"
			return (returnType == CatUnknown ? "" :
					getTypeDescription(returnType) + " ") +
				name + "(" + getTypeDescriptionCommaList(argTypes, 1) +
				")";
		case FunDef.TypeBraces:
			return "{" + getTypeDescriptionCommaList(argTypes, 0) + "}";
		case FunDef.TypeParentheses:
			return "(" + getTypeDescriptionCommaList(argTypes, 0) + ")";
		case FunDef.TypeCase:
			String s = getTypeDescription(argTypes[0]);
			if (argTypes[0] == CatLogical) {
				return "CASE WHEN " + s + " THEN <Expression> ... END";
			} else {
				return "CASE " + s + " WHEN " + s + " THEN <Expression> ... END";
			}
		default:
			throw Util.newInternal("unknown syntactic type " + syntacticType);
		}
	}

	private static String getTypeDescription(int type) {
		switch (type) {
		case CatUnknown:
			return "<Unknown>";
		case CatArray:
			return "<Array>";
		case CatDimension:
			return "<Dimension>";
		case CatHierarchy:
			return "<Hierarchy>";
		case CatLevel:
			return "<Level>";
		case CatLogical:
			return "<Logical Expression>";
		case CatMember:
			return "<Member>";
		case CatNumeric:
		case CatNumeric | CatExpression:
			return "<Numeric Expression>";
		case CatSet:
			return "<Set>";
		case CatString:
		case CatString | CatExpression:
			return "<String Expression>";
		case CatTuple:
			return "<Tuple>";
		case CatSymbol:
			return "<Symbol>";
		case CatParameter:
			return "<Parameter>";
		case CatCube:
			return "<Cube>";
		case CatValue:
			return "<Value>";
        default:
            throw Util.newInternal("unknown expression type " + type);
		}
	}

	private static String getTypeDescriptionCommaList(int[] types, int start)
	{
		String s = "";
		for (int i = start; i < types.length; i++) {
			if (i > start) {
				s += ", ";
			}
			s += getTypeDescription(types[i]);
		}
		return s;
	}

	public static void unparseList(
		PrintWriter pw, Exp[] exps, String start, String mid, String end,
		ElementCallback callback)
	{
		pw.print(start);
		for (int i = 0; i < exps.length; i++) {
			if (i > 0) {
				pw.print(mid);
			}
			exps[i].unparse(pw, callback);
		}
		pw.print(end);
	}

	public static int[] getTypes(Exp[] exps) {
		int[] types = new int[exps.length];
		for (int i = 0; i < exps.length; i++) {
			types[i] = exps[i].getType();
		}
		return types;
	}
}


// End Exp.java
