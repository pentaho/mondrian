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
import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;
import mondrian.olap.type.Type;

import java.io.PrintWriter;

/**
 * A <code>FunCall</code> is a function applied to a list of operands.
 **/
public class FunCall extends ExpBase {

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
    public FunCall(FunDef funDef, Exp[] args, Type returnType) {
        assert funDef != null;
        assert args != null;
        assert returnType != null;
        this.funDef = funDef;
        this.args = args;
        this.returnType = returnType;
    }

    public String toString() {
        return toMdx();
    }

    public Object clone() {
        return new FunCall(funDef, ExpBase.cloneArray(args), returnType);
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

    public void replaceChild(int i, QueryPart with) {
        args[i] = (Exp) with;
    }

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
        return this; // already validated
    }

    public void unparse(PrintWriter pw) {
        funDef.unparse(args, pw);
    }

    public Object evaluate(Evaluator evaluator) {
        return evaluator.visit(this);
    }

    public Calc accept(ExpCompiler compiler) {
        return funDef.compileCall(this, compiler);
    }

}

// End FunCall.java
