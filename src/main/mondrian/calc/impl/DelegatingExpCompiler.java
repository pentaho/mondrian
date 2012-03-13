/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2006-2011 Pentaho
// All Rights Reserved.
*/
package mondrian.calc.impl;

import mondrian.calc.*;
import mondrian.mdx.MdxVisitor;
import mondrian.olap.*;
import mondrian.olap.type.Type;

import java.io.PrintWriter;
import java.util.List;

/**
 * Abstract implementation of {@link mondrian.calc.ExpCompiler}
 *
 * @author jhyde
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
        final Calc calc = parent.compile(wrap(exp));
        return afterCompile(exp, calc, false);
    }

    public Calc compileAs(
        Exp exp,
        Type resultType,
        List<ResultStyle> preferredResultTypes)
    {
        return parent.compileAs(wrap(exp), resultType, preferredResultTypes);
    }

    public MemberCalc compileMember(Exp exp) {
        MemberCalc calc = parent.compileMember(wrap(exp));
        return (MemberCalc) afterCompile(exp, calc, false);
    }

    public LevelCalc compileLevel(Exp exp) {
        final LevelCalc calc = parent.compileLevel(wrap(exp));
        return (LevelCalc) afterCompile(exp, calc, false);
    }

    public DimensionCalc compileDimension(Exp exp) {
        final DimensionCalc calc = parent.compileDimension(wrap(exp));
        return (DimensionCalc) afterCompile(exp, calc, false);
    }

    public HierarchyCalc compileHierarchy(Exp exp) {
        final HierarchyCalc calc = parent.compileHierarchy(wrap(exp));
        return (HierarchyCalc) afterCompile(exp, calc, false);
    }

    public IntegerCalc compileInteger(Exp exp) {
        final IntegerCalc calc = parent.compileInteger(wrap(exp));
        return (IntegerCalc) afterCompile(exp, calc, false);
    }

    public StringCalc compileString(Exp exp) {
        final StringCalc calc = parent.compileString(wrap(exp));
        return (StringCalc) afterCompile(exp, calc, false);
    }

    public DateTimeCalc compileDateTime(Exp exp) {
        final DateTimeCalc calc = parent.compileDateTime(wrap(exp));
        return (DateTimeCalc) afterCompile(exp, calc, false);
    }

    public final ListCalc compileList(Exp exp) {
        return compileList(exp, false);
    }

    public ListCalc compileList(Exp exp, boolean mutable) {
        final ListCalc calc = parent.compileList(wrap(exp), mutable);
        return (ListCalc) afterCompile(exp, calc, mutable);
    }

    public IterCalc compileIter(Exp exp) {
        final IterCalc calc = parent.compileIter(wrap(exp));
        return (IterCalc) afterCompile(exp, calc, false);
    }

    public BooleanCalc compileBoolean(Exp exp) {
        final BooleanCalc calc = parent.compileBoolean(wrap(exp));
        return (BooleanCalc) afterCompile(exp, calc, false);
    }

    public DoubleCalc compileDouble(Exp exp) {
        final DoubleCalc calc = parent.compileDouble(wrap(exp));
        return (DoubleCalc) afterCompile(exp, calc, false);
    }

    public TupleCalc compileTuple(Exp exp) {
        final TupleCalc calc = parent.compileTuple(wrap(exp));
        return (TupleCalc) afterCompile(exp, calc, false);
    }

    public Calc compileScalar(Exp exp, boolean scalar) {
        final Calc calc = parent.compileScalar(wrap(exp), scalar);
        return afterCompile(exp, calc, false);
    }

    public ParameterSlot registerParameter(Parameter parameter) {
        return parent.registerParameter(parameter);
    }

    public List<ResultStyle> getAcceptableResultStyles() {
        return parent.getAcceptableResultStyles();
    }

    /**
     * Wrapping an expression ensures that when it is visited, it calls
     * back to this compiler rather than our parent (wrapped) compiler.
     *
     * <p>All methods that pass an expression to the delegate compiler should
     * wrap expressions in this way. Hopefully the delegate compiler doesn't
     * use {@code instanceof}; it should be using the visitor pattern instead.
     *
     * <p>If we didn't do this, the decorator would get forgotten at the first
     * level of recursion. It's not pretty, and I thought about other ways
     * of combining Visitor + Decorator. For instance, I tried replacing
     * {@link #afterCompile(mondrian.olap.Exp, mondrian.calc.Calc, boolean)}
     * with a callback (Strategy), but the exit points in ExpCompiler not
     * clear because there are so many methods.
     *
     * @param e Expression to be wrapped
     * @return wrapper expression
     */
    private Exp wrap(Exp e) {
        return new WrapExp(e, this);
    }

    /**
     * See {@link mondrian.calc.impl.DelegatingExpCompiler#wrap}.
     */
    private static class WrapExp implements Exp {
        private final Exp e;
        private final ExpCompiler wrappingCompiler;

        WrapExp(
            Exp e,
            ExpCompiler wrappingCompiler)
        {
            this.e = e;
            this.wrappingCompiler = wrappingCompiler;
        }

        public Exp clone() {
            throw new UnsupportedOperationException();
        }

        public int getCategory() {
            return e.getCategory();
        }

        public Type getType() {
            return e.getType();
        }

        public void unparse(PrintWriter pw) {
            e.unparse(pw);
        }

        public Exp accept(Validator validator) {
            return e.accept(validator);
        }

        public Calc accept(ExpCompiler compiler) {
            return e.accept(wrappingCompiler);
        }

        public Object accept(MdxVisitor visitor) {
            return e.accept(visitor);
        }
    }
}

// End DelegatingExpCompiler.java
