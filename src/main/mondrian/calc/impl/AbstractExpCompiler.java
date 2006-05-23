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
import mondrian.olap.fun.*;
import mondrian.olap.type.*;
import mondrian.olap.type.DimensionType;
import mondrian.olap.type.LevelType;
import mondrian.calc.*;

/**
 * Abstract implementation of the {@link mondrian.calc.ExpCompiler} interface.
 *
 * @author jhyde
 * @version $Id$
 * @since Sep 29, 2005
 */
public class AbstractExpCompiler implements ExpCompiler {
    private final Evaluator evaluator;
    private final Validator validator;

    public AbstractExpCompiler(Evaluator evaluator, Validator validator) {
        this.evaluator = evaluator;
        this.validator = validator;
    }

    public Evaluator getEvaluator() {
        return evaluator;
    }

    public Validator getValidator() {
        return validator;
    }

    public Calc compile(Exp exp) {
        return exp.accept(this);
    }

    public MemberCalc compileMember(Exp exp) {
        final Type type = exp.getType();
        if (type instanceof DimensionType) {
            final DimensionCalc dimensionCalc = compileDimension(exp);
            return new DimensionCurrentMemberFunDef.CalcImpl(
                    new DummyExp(TypeUtil.toMemberType(type)), dimensionCalc);
        } else if (type instanceof HierarchyType) {
            final HierarchyCalc hierarchyCalc = compileHierarchy(exp);
            return new HierarchyCurrentMemberFunDef.CalcImpl(
                    new DummyExp(TypeUtil.toMemberType(type)), hierarchyCalc);
        }
        assert type instanceof MemberType;
        return (MemberCalc) compile(exp);
    }

    public LevelCalc compileLevel(Exp exp) {
        final Type type = exp.getType();
        if (type instanceof MemberType) {
            // <Member> --> <Member>.Level
            final MemberCalc memberCalc = compileMember(exp);
            return new MemberLevelFunDef.CalcImpl(
                    new DummyExp(LevelType.forType(type)),
                    memberCalc);
        }
        assert type instanceof LevelType;
        return (LevelCalc) compile(exp);
    }

    public DimensionCalc compileDimension(Exp exp) {
        final Type type = exp.getType();
        if (type instanceof HierarchyType) {
            final HierarchyCalc hierarchyCalc = compileHierarchy(exp);
            return new HierarchyDimensionFunDef.CalcImpl(
                    exp, hierarchyCalc);
        }
        assert type instanceof DimensionType : type;
        return (DimensionCalc) compile(exp);
    }

    public HierarchyCalc compileHierarchy(Exp exp) {
        final Type type = exp.getType();
        if (type instanceof DimensionType ||
                type instanceof MemberType) {
            // <Dimension> --> <Dimension>.CurrentMember.Hierarchy
            final MemberCalc memberCalc = compileMember(exp);
            return new MemberHierarchyFunDef.CalcImpl(
                    new DummyExp(HierarchyType.forType(type)),
                    memberCalc);
        }
        if (type instanceof LevelType) {
            // <Level> --> <Level>.Hierarchy
            final LevelCalc levelCalc = compileLevel(exp);
            return new LevelHierarchyFunDef.CalcImpl(
                    new DummyExp(HierarchyType.forType(type)),
                    levelCalc);
        }
        assert type instanceof HierarchyType;
        return (HierarchyCalc) compile(exp);
    }

    public IntegerCalc compileInteger(Exp exp) {
        final Calc calc = compileScalar(exp, false);
        if (calc instanceof IntegerCalc) {
            return (IntegerCalc) calc;
        } else if (calc instanceof DoubleCalc) {
            final DoubleCalc doubleCalc = (DoubleCalc) calc;
            return new AbstractIntegerCalc(exp, new Calc[] {doubleCalc}) {
                public int evaluateInteger(Evaluator evaluator) {
                    return (int) doubleCalc.evaluateDouble(evaluator);
                }
            };
        } else {
            return (IntegerCalc) calc;
        }
    }

    public StringCalc compileString(Exp exp) {
        return (StringCalc) compile(exp);
    }

    public ListCalc compileList(Exp exp) {
        return (ListCalc) compile(exp);
    }

    public BooleanCalc compileBoolean(Exp exp) {
        return (BooleanCalc) compileScalar(exp, false);
    }

    public DoubleCalc compileDouble(Exp exp) {
        return (DoubleCalc) compileScalar(exp, false);
    }

    public TupleCalc compileTuple(Exp exp) {
        return (TupleCalc) compile(exp);
    }

    public Calc compileScalar(Exp exp, boolean convert) {
        final Type type = exp.getType();
        if (type instanceof MemberType) {
            MemberType memberType = (MemberType) type;
            MemberCalc calc = compileMember(exp);
            return new MemberValueCalc(
                    new DummyExp(memberType.getValueType()),
                    new MemberCalc[] {calc});
        } else if (type instanceof DimensionType) {
            final DimensionCalc dimensionCalc = compileDimension(exp);
            MemberType memberType = MemberType.forType(type);
            final MemberCalc dimensionCurrentMemberCalc =
                    new DimensionCurrentMemberFunDef.CalcImpl(
                            new DummyExp(memberType),
                            dimensionCalc);
            return new MemberValueCalc(
                    new DummyExp(memberType.getValueType()),
                    new MemberCalc[] {dimensionCurrentMemberCalc});
        } else if (type instanceof HierarchyType) {
            HierarchyType hierarchyType = (HierarchyType) type;
            MemberType memberType =
                    MemberType.forHierarchy(hierarchyType.getHierarchy());
            final HierarchyCalc hierarchyCalc = compileHierarchy(exp);
            final MemberCalc hierarchyCurrentMemberCalc =
                    new HierarchyCurrentMemberFunDef.CalcImpl(
                            new DummyExp(type), hierarchyCalc);
            return new MemberValueCalc(
                    new DummyExp(memberType.getValueType()),
                    new MemberCalc[] {hierarchyCurrentMemberCalc});
        } else if (type instanceof TupleType) {
            TupleType tupleType = (TupleType) type;
            TupleCalc tupleCalc = compileTuple(exp);
            final TupleValueCalc scalarCalc = new TupleValueCalc(
                    new DummyExp(tupleType.getValueType()), tupleCalc);
            return scalarCalc.optimize();
        } else if (type instanceof ScalarType) {
            if (convert) {
                if (type instanceof BooleanType) {
                    return compileBoolean(exp);
                } else if (type instanceof NumericType) {
                    return compileDouble(exp);
                } else if (type instanceof StringType) {
                    return compileString(exp);
                } else {
                    return compile(exp);
                }
            } else {
                return compile(exp);
            }
        } else {
            return compile(exp);
        }
    }
}

// End AbtractExpCompiler.java
