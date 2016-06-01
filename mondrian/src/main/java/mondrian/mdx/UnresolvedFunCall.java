/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.mdx;

import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;
import mondrian.olap.*;
import mondrian.olap.fun.FunUtil;
import mondrian.olap.type.Type;

import java.io.PrintWriter;

/**
 * An expression consisting of a named function or operator
 * applied to a set of arguments. The syntax determines whether this is
 * called infix, with function call syntax, and so forth.
 *
 * @author jhyde
 * @since Sep 28, 2005
 */
public class UnresolvedFunCall extends ExpBase implements FunCall {
    private final String name;
    private final Syntax syntax;
    private final Exp[] args;

    /**
     * Creates a function call with {@link Syntax#Function} syntax.
     */
    public UnresolvedFunCall(String name, Exp[] args) {
        this(name, Syntax.Function, args);
    }

    /**
     * Creates a function call.
     */
    public UnresolvedFunCall(String name, Syntax syntax, Exp[] args) {
        assert name != null;
        assert syntax != null;
        assert args != null;
        this.name = name;
        this.syntax = syntax;
        this.args = args;
        switch (syntax) {
        case Braces:
            Util.assertTrue(name.equals("{}"));
            break;
        case Parentheses:
            Util.assertTrue(name.equals("()"));
            break;
        case Internal:
            Util.assertTrue(name.startsWith("$"));
            break;
        case Empty:
            Util.assertTrue(name.equals(""));
            break;
        default:
            Util.assertTrue(
                !name.startsWith("$")
                && !name.equals("{}")
                && !name.equals("()"));
            break;
        }
    }

    @SuppressWarnings({"CloneDoesntCallSuperClone"})
    public UnresolvedFunCall clone() {
        return new UnresolvedFunCall(name, syntax, ExpBase.cloneArray(args));
    }

    public int getCategory() {
        throw new UnsupportedOperationException();
    }

    public Type getType() {
        throw new UnsupportedOperationException();
    }

    public void unparse(PrintWriter pw) {
        syntax.unparse(name, args, pw);
    }

    public Object accept(MdxVisitor visitor) {
        final Object o = visitor.visit(this);
        if (visitor.shouldVisitChildren()) {
            // visit the call's arguments
            for (Exp arg : args) {
                arg.accept(visitor);
            }
        }
        return o;
    }

    public Exp accept(Validator validator) {
        Exp[] newArgs = new Exp[args.length];
        FunDef funDef =
            FunUtil.resolveFunArgs(
                validator, null, args, newArgs, name, syntax);
        return funDef.createCall(validator, newArgs);
    }

    public Calc accept(ExpCompiler compiler) {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns the function name.
     *
     * @return function name
     */
    public String getFunName() {
        return name;
    }

    /**
     * Returns the syntax of this function call.
     *
     * @return the syntax of the call
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

    public Object[] getChildren() {
        return args;
    }
}

// End UnresolvedFunCall.java
