/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2006 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.calc.impl;

import mondrian.olap.*;
import mondrian.calc.*;

/**
 * Abstract implementation of {@link mondrian.calc.ExpCompiler}
 *
 * @author jhyde
 * @version $Id$
 * @since Jan 2, 2006
 */
public class DelegatingExpCompiler implements ExpCompiler {
    private final ExpCompiler parent;

    protected DelegatingExpCompiler(ExpCompiler parent) {
        this.parent = parent;
    }

    /**
     * Hook for post-processing.
     */
    protected Calc afterCompile(Exp exp, Calc calc) {
        return calc;
    }

    public Evaluator getEvaluator() {
        return parent.getEvaluator();
    }

    public Validator getValidator() {
        return parent.getValidator();
    }

    public Calc compile(Exp exp) {
        return parent.compile(exp);
    }

    public MemberCalc compileMember(Exp exp) {
        MemberCalc calc = parent.compileMember(exp);
        return (MemberCalc) afterCompile(exp, calc);
    }

    public LevelCalc compileLevel(Exp exp) {
        final LevelCalc calc = parent.compileLevel(exp);
        return (LevelCalc) afterCompile(exp, calc);
    }

    public DimensionCalc compileDimension(Exp exp) {
        final DimensionCalc calc = parent.compileDimension(exp);
        return (DimensionCalc) afterCompile(exp, calc);
    }

    public HierarchyCalc compileHierarchy(Exp exp) {
        final HierarchyCalc calc = parent.compileHierarchy(exp);
        return (HierarchyCalc) afterCompile(exp, calc);
    }

    public IntegerCalc compileInteger(Exp exp) {
        final IntegerCalc calc = parent.compileInteger(exp);
        return (IntegerCalc) afterCompile(exp, calc);
    }

    public StringCalc compileString(Exp exp) {
        final StringCalc calc = parent.compileString(exp);
        return (StringCalc) afterCompile(exp, calc);
    }

    public ListCalc compileList(Exp exp) {
        final ListCalc calc = parent.compileList(exp);
        return (ListCalc) afterCompile(exp, calc);
    }

    public BooleanCalc compileBoolean(Exp exp) {
        final BooleanCalc calc = parent.compileBoolean(exp);
        return (BooleanCalc) afterCompile(exp, calc);
    }

    public DoubleCalc compileDouble(Exp exp) {
        final DoubleCalc calc = parent.compileDouble(exp);
        return (DoubleCalc) afterCompile(exp, calc);
    }

    public TupleCalc compileTuple(Exp exp) {
        final TupleCalc calc = parent.compileTuple(exp);
        return (TupleCalc) afterCompile(exp, calc);
    }

    public Calc compileScalar(Exp exp, boolean scalar) {
        final Calc calc = parent.compileScalar(exp, scalar);
        return afterCompile(exp, calc);
    }
}

// End DelegatingExpCompiler.java
