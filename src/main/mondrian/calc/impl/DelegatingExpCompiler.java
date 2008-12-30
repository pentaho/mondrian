/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2008 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.calc.impl;

import mondrian.olap.*;
import mondrian.olap.type.Type;
import mondrian.calc.*;

import java.util.List;

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
     *
     * @param exp Expression to compile
     * @param calc Calculator created by compiler
     * @param mutable Whether the result is mutuable
     * @return Calculator after post-processing
     */
    protected Calc afterCompile(Exp exp, Calc calc, boolean mutable) {
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

    public Calc compileAs(
        Exp exp,
        Type resultType,
        List<ResultStyle> preferredResultTypes)
    {
        return parent.compileAs(exp, resultType, preferredResultTypes);
    }

    public MemberCalc compileMember(Exp exp) {
        MemberCalc calc = parent.compileMember(exp);
        return (MemberCalc) afterCompile(exp, calc, false);
    }

    public LevelCalc compileLevel(Exp exp) {
        final LevelCalc calc = parent.compileLevel(exp);
        return (LevelCalc) afterCompile(exp, calc, false);
    }

    public DimensionCalc compileDimension(Exp exp) {
        final DimensionCalc calc = parent.compileDimension(exp);
        return (DimensionCalc) afterCompile(exp, calc, false);
    }

    public HierarchyCalc compileHierarchy(Exp exp) {
        final HierarchyCalc calc = parent.compileHierarchy(exp);
        return (HierarchyCalc) afterCompile(exp, calc, false);
    }

    public IntegerCalc compileInteger(Exp exp) {
        final IntegerCalc calc = parent.compileInteger(exp);
        return (IntegerCalc) afterCompile(exp, calc, false);
    }

    public StringCalc compileString(Exp exp) {
        final StringCalc calc = parent.compileString(exp);
        return (StringCalc) afterCompile(exp, calc, false);
    }

    public DateTimeCalc compileDateTime(Exp exp) {
        final DateTimeCalc calc = parent.compileDateTime(exp);
        return (DateTimeCalc) afterCompile(exp, calc, false);
    }

    public ListCalc compileList(Exp exp) {
        return compileList(exp, false);
    }

    public ListCalc compileList(Exp exp, boolean mutable) {
        final ListCalc calc = parent.compileList(exp, mutable);
        return (ListCalc) afterCompile(exp, calc, mutable);
    }

    public IterCalc compileIter(Exp exp) {
        final IterCalc calc = parent.compileIter(exp);
        return (IterCalc) afterCompile(exp, calc, false);
    }

    public BooleanCalc compileBoolean(Exp exp) {
        final BooleanCalc calc = parent.compileBoolean(exp);
        return (BooleanCalc) afterCompile(exp, calc, false);
    }

    public DoubleCalc compileDouble(Exp exp) {
        final DoubleCalc calc = parent.compileDouble(exp);
        return (DoubleCalc) afterCompile(exp, calc, false);
    }

    public TupleCalc compileTuple(Exp exp) {
        final TupleCalc calc = parent.compileTuple(exp);
        return (TupleCalc) afterCompile(exp, calc, false);
    }

    public Calc compileScalar(Exp exp, boolean scalar) {
        final Calc calc = parent.compileScalar(exp, scalar);
        return afterCompile(exp, calc, false);
    }

    public ParameterSlot registerParameter(Parameter parameter) {
        return parent.registerParameter(parameter);
    }

    public List<ResultStyle> getAcceptableResultStyles() {
        return parent.getAcceptableResultStyles();
    }
}

// End DelegatingExpCompiler.java
