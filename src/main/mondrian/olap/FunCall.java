/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 1998-2003 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 20 January, 1999
*/

package mondrian.olap;
import java.io.PrintWriter;

/**
 * A <code>FunCall</code> is a function applied to a list of operands.
 **/
public class FunCall extends ExpBase
{
	/** Name of the function. **/
	private String fun;
	/** The arguments to the function call.  Note that for methods, 0-th arg is
	 * 'this'. **/
	public Exp[] args;
	/** Definition, set after resolve. **/
	private FunDef funDef;

	/** As {@link FunDef#getSyntax}. **/
	private Syntax syntax;

	public FunCall(String fun, Exp[] args) {
		this(fun, Syntax.Function, args);
	}

	public FunCall(String fun, Syntax syntax, Exp[] args)
	{
		this.fun = fun;
		this.args = args;
		this.syntax = syntax;
		if (syntax == Syntax.Braces) {
			Util.assertTrue(fun.equals("{}"));
        } else if (syntax == Syntax.Parentheses) {
			Util.assertTrue(fun.equals("()"));
        } else if (syntax == Syntax.Internal) {
			Util.assertTrue(fun.startsWith("$"));
        } else {
			Util.assertTrue(!fun.startsWith("$") &&
						!fun.equals("{}") &&
						!fun.equals("()"));
		}
	}

    /** @deprecated use #FunCall(String,Syntax,Exp[]) */
    public FunCall(String fun, Exp[] args, int syntax) {
        this(fun, Syntax.get(syntax), args);
    }

	public Object clone() {
		return new FunCall(fun, syntax, ExpBase.cloneArray(args));
	}
	public String getFunName() {
		return fun;
	}
	public Syntax getSyntax() {
		return syntax;
	}
	public final boolean isCallTo(String funName) {
		return fun.equalsIgnoreCase(funName);
	}
	public boolean isCallToTuple() {
		return getSyntax() == Syntax.Parentheses;
	}
	public boolean isCallToCrossJoin() {
		return fun.equalsIgnoreCase("CROSSJOIN") || fun.equals("*");
	}
	public boolean isCallToParameter() {
		return fun.equalsIgnoreCase("Parameter") ||
			fun.equalsIgnoreCase("ParamRef");
	}
	public boolean isCallToFilter() {
		return fun.equalsIgnoreCase("FILTER");
	}
	public Object[] getChildren() {
		return args;
	}
	public void replaceChild(int i, QueryPart with) {
		args[i] = (Exp) with;
	}

	public void removeChild(int iPosition) {
		Exp newArgs[] = new Exp[args.length - 1];
		int j = 0;
		for (int i = 0; i < args.length; i++) {
			if (i == iPosition) {
				++i;
			}
			if (j != newArgs.length) {
				// this condition helps for removing last element
				newArgs[j] = args[i];
			}
			++j;
		}
		args = newArgs;
	}

	public boolean usesDimension(Dimension dimension) {
		return ExpBase.arrayUsesDimension(args, dimension);
	}

    public FunDef getFunDef() {
		return funDef;
	}

	public final int getType() {
		return funDef.getReturnType();
	}

	public Hierarchy getHierarchy() {
		return funDef.getHierarchy(args);
	}

	public Exp resolve(Resolver resolver) {
		for (int i = 0; i < args.length; i++) {
			args[i] = resolver.resolveChild(args[i]);
		}
        funDef = resolver.getFunTable().getDef(this, resolver);
		if (this.isCallToParameter()) {
			Parameter param = resolver.createOrLookupParam(this);
			return resolver.resolveChild(param);
		} else if (this.isCallTo("StrToTuple") ||
				   this.isCallTo("StrToSet")) {
			if (args.length <= 1) {
				throw Util.getRes().newMdxFuncArgumentsNum(fun);
			}
			for (int j = 1; j < args.length; j++) {
				if (args[j] instanceof Dimension) {
					// if arg is a dimension, switch to dimension's default
					// hierarchy
					args[j] = args[j].getHierarchy();
				} else if (args[j] instanceof Hierarchy) {
					// nothing
				} else {
					throw Util.getRes().newMdxFuncNotHier(new Integer(j+1), fun);
				}
			}
		}
        int[] types = funDef.getParameterTypes();
        Util.assertTrue(types.length == args.length);
        for (int i = 0; i < args.length; i++) {
            Exp arg = args[i];
            args[i] = FunTable.instance().convert(arg, types[i], resolver);
        }
        return this;
	}

	public void unparse(PrintWriter pw)
	{
		funDef.unparse(args, pw);
	}

	/**
	 * See {@link ExpBase#addAtPosition} for description (although this
	 * refinement does most of the work).
	 **/
	public int addAtPosition(Exp e, int iPosition)
	{
		if (isCallToCrossJoin()) {
			Exp left = args[0], right = args[1];
			int nLeft = left.addAtPosition(e, iPosition),
				nRight;
			if (nLeft == -1) {
				return -1; // added successfully
			} else if (nLeft == iPosition) {
				// This node has 'iPosition' hierarchies in its left tree.
				// Convert
				//   CrossJoin(ltree, rtree)
				// into
				//   CrossJoin(CrossJoin(ltree, e), rtree)
				// so that 'e' is the 'iPosition'th hierarchy from the left.
				args[0] = new FunCall(
						"CrossJoin", Syntax.Function, new Exp[] {left, e});
				return -1; // added successfully
			} else {
				Util.assertTrue(
					nLeft < iPosition,
					"left tree had enough dimensions, yet still failed to " +
					"place expression");
				nRight = right.addAtPosition(e, iPosition - nLeft);
				if (nRight == -1)
					return -1; // added successfully
				else
					return nLeft + nRight; // not added
			}
		} else if( isCallToTuple() ){
			// For all functions besides CrossJoin, the dimensionality is
			// determined by the first argument alone.  (For example,
			// 'Union(CrossJoin(a, b), CrossJoin(c, d)' has a dimensionality of
			// 2.)
			Exp newArgs[] = new Exp[ args.length + 1 ];
			if( iPosition == 0 ){
				// the expression has to go first
				newArgs[0] = e;
				for( int i = 0; i < args.length; i++ ){
					newArgs[i+1] = args[i];
				}
			} else if( iPosition < 0 || iPosition >= args.length ){
				//the expression has to go last
				for( int i = 0; i < args.length; i++ ){
					newArgs[i] = args[i];
				}
				newArgs[args.length] = e;
			} else {
				//the expression has to go in the middle
				int i = 0;
				for( i = 0; i < iPosition; i++ ){
					newArgs[i] = args[i];
				}
				newArgs[iPosition] = e;
				for( i =(iPosition + 1); i < newArgs.length; i++ ){
					newArgs[i] = args[i-1];
				}
			}
			args = newArgs;
			return -1;
		} else {
			if (getSyntax() == Syntax.Braces &&
				args[0] instanceof FunCall &&
				((FunCall)args[0]).isCallToTuple()){
				// DO not add to the tuple, return -1 to create new CrossJoin
				return 1;
			}
			return args[0].addAtPosition( e, iPosition );
		}
	}

	// implement Exp
	public Object evaluate(Evaluator evaluator) {
		return evaluator.xx(this);
	}

	/**
	 * delegates to funDef
	 */
	public boolean dependsOn(Dimension dimension) {
		return funDef.dependsOn(args, dimension);
	}

}

// End FunCall.java
