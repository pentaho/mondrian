/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 1998-2002 Kana Software, Inc.
// Copyright (C) 2001-2007 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/

package mondrian.mdx;
import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;
import mondrian.olap.fun.*;
import mondrian.olap.type.Type;
import mondrian.olap.*;

import java.io.PrintWriter;

/**
 * A <code>ResolvedFunCall</code> is a function applied to a list of operands,
 * which has been validated and resolved to a
 * {@link FunDef function definition}.
 *
 * @author jhyde
 * @version $Id$
 * @since Jan 6, 2006
 */
public final class ResolvedFunCall extends ExpBase implements FunCall {

    /**
     * The arguments to the function call.  Note that for methods, 0-th arg is
     * 'this'.
     */
    private final Exp[] args;

    /**
     * Return type of this function call.
     */
    private final Type returnType;

    /**
     * Function definition.
     */
    private final FunDef funDef;

    /**
     * Creates a function call.
     *
     * @param funDef Function definition
     * @param args Arguments
     * @param returnType Return type
     */
    public ResolvedFunCall(FunDef funDef, Exp[] args, Type returnType) {
        assert funDef != null;
        assert args != null;
        assert returnType != null;
        this.funDef = funDef;
        this.args = args;
        this.returnType = returnType;
    }

    public String toString() {
        return Util.unparse(this);
    }

    @SuppressWarnings({"CloneDoesntCallSuperClone"})
    public ResolvedFunCall clone() {
        return new ResolvedFunCall(
            funDef, ExpBase.cloneArray(args), returnType);
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

    public String getFunName() {
        return funDef.getName();
    }

    public Syntax getSyntax() {
        return funDef.getSyntax();
    }

    public Object[] getChildren() {
        return args;
    }

    /**
     * Returns the definition of the function which is being called.
     *
     * @return function definition
     */
    public FunDef getFunDef() {
        return funDef;
    }

    public final int getCategory() {
        return funDef.getReturnCategory();
    }

    public final Type getType() {
        return returnType;
    }

    public Exp accept(Validator validator) {
        // even though the function has already been validated, we need
        // to walk through the arguments to determine which measures are
        // referenced
        Exp[] newArgs = new Exp[args.length];
        FunUtil.resolveFunArgs(
            validator, funDef, args, newArgs, getFunName(), getSyntax());

        return this;
    }

    public void unparse(PrintWriter pw) {
        funDef.unparse(args, pw);
    }

    public Calc accept(ExpCompiler compiler) {
        return funDef.compileCall(this, compiler);
    }

    public Object accept(MdxVisitor visitor) {
        final Object o = visitor.visit(this);
        // visit the call's arguments
        for (Exp arg : args) {
            arg.accept(visitor);
        }
        return o;
    }
}

// End ResolvedFunCall.java
