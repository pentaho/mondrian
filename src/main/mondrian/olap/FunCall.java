/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 1998-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 20 January, 1999
*/

package mondrian.olap;
import mondrian.olap.type.*;

import java.io.PrintWriter;

/**
 * A <code>FunCall</code> is a function applied to a list of operands.
 **/
public class FunCall extends ExpBase {
    /** Name of the function. **/
    private final String fun;

    /**
     * The arguments to the function call.  Note that for methods, 0-th arg is
     * 'this'.
     *
     */
    private Exp[] args;

    /** Definition, set after resolve. **/
    private FunDef funDef;

    /** As {@link FunDef#getSyntax}. **/
    private final Syntax syntax;

    /**
     * The type of the return value. Set during {@link #accept(Validator)}.
     */
    private Type type;

    public FunCall(String fun, Exp[] args) {
        this(fun, Syntax.Function, args);
    }

    public FunCall(String fun, Syntax syntax, Exp[] args) {
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

    public Object clone() {
        return new FunCall(fun, syntax, ExpBase.cloneArray(args));
    }

    /**
     * Returns the function name.
     *
     * @return function name.
     */
    public String getFunName() {
        return fun;
    }

    /**
     * Returns the syntax of this function call.
     *
     * @return the Syntax.
     */
    public Syntax getSyntax() {
        return syntax;
    }

    /**
     * Returns the Exp argument at the specified index.
     *
     * @param      index   the index of the Exp.
     * @return     the Exp at the specified index of this array of Exp.
     *             The first Exp is at index <code>0</code>.
     * @see #getArgs()
     */
    public Exp getArg(int index) {
        return args[index];
    }

    /**
     * Returns the internal array of Exp arguments.
     *
     * <p>Note: this does NOT do a copy.
     *
     * @return the array of expressions
     */
    public Exp[] getArgs() {
        return args;
    }

    /**
     * Returns the number of arguments.
     *
     * @return number of arguments.
     * @see #getArgs()
     */
    public final int getArgCount() {
        return args.length;
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

    public FunDef getFunDef() {
        return funDef;
    }

    public final int getCategory() {
        return funDef.getReturnCategory();
    }

    public final Type getTypeX() {
        return type;
    }

    public Exp accept(Validator validator) {
        final FunTable funTable = validator.getFunTable();
        for (int i = 0; i < args.length; i++) {
            args[i] = validator.validate(args[i]);
        }
        funDef = funTable.getDef(this, validator);
        return funDef.validateCall(validator, this);
    }

    public void unparse(PrintWriter pw) {
        funDef.unparse(args, pw);
    }

    /**
     * See {@link ExpBase#addAtPosition} for description (although this
     * refinement does most of the work).
     **/
    public int addAtPosition(Exp e, int iPosition) {
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
                return (nRight == -1)
                    ? -1 // added successfully
                    : nLeft + nRight; // not added
            }
        } else if (isCallToTuple()) {
            // For all functions besides CrossJoin, the dimensionality is
            // determined by the first argument alone.  (For example,
            // 'Union(CrossJoin(a, b), CrossJoin(c, d)' has a dimensionality of
            // 2.)
            Exp newArgs[] = new Exp[args.length + 1];
            if (iPosition == 0) {
                // the expression has to go first
                newArgs[0] = e;
                for (int i = 0; i < args.length; i++) {
                    newArgs[i + 1] = args[i];
                }
            } else if (iPosition < 0 || iPosition >= args.length) {
                //the expression has to go last
                for (int i = 0; i < args.length; i++) {
                    newArgs[i] = args[i];
                }
                newArgs[args.length] = e;
            } else {
                //the expression has to go in the middle
                for (int i = 0; i < iPosition; i++) {
                    newArgs[i] = args[i];
                }
                newArgs[iPosition] = e;
                for (int i = iPosition + 1; i < newArgs.length; i++) {
                    newArgs[i] = args[i - 1];
                }
            }
            args = newArgs;
            return -1;
        } else {
            if ((getSyntax() == Syntax.Braces) &&
                (args[0] instanceof FunCall) &&
                ((FunCall)args[0]).isCallToTuple()) {
                // DO not add to the tuple, return -1 to create new CrossJoin
                return 1;
            }
            return args[0].addAtPosition(e, iPosition);
        }
    }

    public Object evaluate(Evaluator evaluator) {
        return evaluator.xx(this);
    }

    public boolean dependsOn(Dimension dimension) {
        // delegate to FunDef
        return funDef.dependsOn(args, dimension);
    }

    /**
     * Sets the return type of this call.
     */
    public void setType(Type type) {
        this.type = type;
    }
}

// End FunCall.java
