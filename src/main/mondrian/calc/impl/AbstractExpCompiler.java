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
import mondrian.olap.fun.*;
import mondrian.olap.type.*;
import mondrian.olap.type.DimensionType;
import mondrian.olap.type.LevelType;
import mondrian.resource.MondrianResource;
import mondrian.calc.*;

import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;

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
    private final Map<Parameter, ParameterSlotImpl> parameterSlots =
        new HashMap<Parameter, ParameterSlotImpl>();
    private List<ResultStyle> resultStyles;

    /**
     * Creates an AbstractExpCompiler
     *
     * @param evaluator Evaluator
     * @param validator Validator
     */
    public AbstractExpCompiler(Evaluator evaluator, Validator validator) {
        this(evaluator, validator, ResultStyle.ANY_LIST);
    }

    /**
     * Creates an AbstractExpCompiler which is constrained to produce one of
     * a set of result styles.
     *
     * @param evaluator Evaluator
     * @param validator Validator
     * @param resultStyles List of result styles, preferred first, must not be
     */
    public AbstractExpCompiler(
        Evaluator evaluator,
        Validator validator,
        List<ResultStyle> resultStyles)
    {
        this.evaluator = evaluator;
        this.validator = validator;
        this.resultStyles = (resultStyles == null)
            ? ResultStyle.ANY_LIST : resultStyles;
    }

    public Evaluator getEvaluator() {
        return evaluator;
    }

    public Validator getValidator() {
        return validator;
    }

    /**
     * {@inheritDoc}
     *
     * Uses the current ResultStyle to compile the expression.
     */
    public Calc compile(Exp exp) {
        return exp.accept(this);
    }

    /**
     * {@inheritDoc}
     *
     * Uses a new ResultStyle to compile the expression.
     */
    public Calc compileAs(
        Exp exp,
        Type resultType,
        List<ResultStyle> preferredResultTypes)
    {
        assert preferredResultTypes != null;
        if (Util.Retrowoven) {
            // Copy and replace ITERABLE
            // A number of functions declare that they can accept
            // ITERABLEs so here is where that those are converted to innocent
            // LISTs for jdk1.4 and other retrowoven code.
            List<ResultStyle> tmp =
                new ArrayList<ResultStyle>(preferredResultTypes.size());
            for (ResultStyle preferredResultType : preferredResultTypes) {
                tmp.add(
                    (preferredResultType == ResultStyle.ITERABLE)
                        ? ResultStyle.LIST
                        : preferredResultType);
            }
            preferredResultTypes = tmp;
        }
        List<ResultStyle> save = this.resultStyles;
        try {
            this.resultStyles = preferredResultTypes;
            if (resultType != null && resultType != exp.getType()) {
                if (resultType instanceof MemberType) {
                    return compileMember(exp);
                } else if (resultType instanceof LevelType) {
                    return compileLevel(exp);
                } else if (resultType instanceof HierarchyType) {
                    return compileHierarchy(exp);
                } else if (resultType instanceof DimensionType) {
                    return compileDimension(exp);
                }
            }
            return compile(exp);
        } finally {
            this.resultStyles = save;
        }
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
        } else if (type instanceof NullType) {
            throw MondrianResource.instance().NullNotSupported.ex();
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
                new DummyExp(new DimensionType(type.getDimension())),
                    hierarchyCalc);
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
        final Type type = calc.getType();
        if (type instanceof DecimalType
            && ((DecimalType) type).getScale() == 0) {
            return (IntegerCalc) calc;
        } else if (type instanceof NumericType) {
            if (calc instanceof ConstantCalc) {
                ConstantCalc constantCalc = (ConstantCalc) calc;
                return new ConstantCalc(
                    new DecimalType(Integer.MAX_VALUE, 0),
                    constantCalc.evaluateInteger(null));
            } else if (calc instanceof DoubleCalc) {
                final DoubleCalc doubleCalc = (DoubleCalc) calc;
                return new AbstractIntegerCalc(exp, new Calc[] {doubleCalc}) {
                    public int evaluateInteger(Evaluator evaluator) {
                        return (int) doubleCalc.evaluateDouble(evaluator);
                    }
                };
            }
        }
        return (IntegerCalc) calc;
    }

    public StringCalc compileString(Exp exp) {
        return (StringCalc) compile(exp);
    }

    public DateTimeCalc compileDateTime(Exp exp) {
        return (DateTimeCalc) compile(exp);
    }

    public ListCalc compileList(Exp exp) {
        return compileList(exp, false);
    }

    public ListCalc compileList(Exp exp, boolean mutable) {
        final List<ResultStyle> resultStyleList;
        if (mutable) {
            resultStyleList = ResultStyle.MUTABLELIST_ONLY;
        } else {
            resultStyleList = ResultStyle.LIST_ONLY;
        }
        Calc calc = compileAs(exp, null, resultStyleList);
        if (calc instanceof ListCalc) {
            return (ListCalc) calc;
        }
        if (calc == null) {
            calc = compileAs(exp, null, ResultStyle.ITERABLE_ANY);
            assert calc != null;
        }
        if (calc instanceof IterCalc) {
            if (((SetType) calc.getType()).getArity() == 1) {
                return toList((MemberIterCalc) calc);
            } else {
                return toList((TupleIterCalc) calc);
            }
        } else {
            // A set can only be implemented as a list or an iterable.
            throw Util.newInternal("Cannot convert calc to list: " + calc);
        }
    }

    /**
     * Converts an iterable to a list.
     *
     * @param calc Calc
     * @return List calculation.
     */
    private MemberListCalc toList(MemberIterCalc calc) {
        return new IterableMemberListCalc(calc);
    }

    /**
     * Converts an iterable to a list.
     *
     * @param calc Calc
     * @return List calculation.
     */
    private TupleListCalc toList(TupleIterCalc calc) {
        return new IterableTupleListCalc(calc);
    }

    public IterCalc compileIter(Exp exp) {
        Calc calc = compileAs(exp, null, ResultStyle.ITERABLE_ONLY);
        if (calc == null) {
            calc = compileAs(exp, null, ResultStyle.ANY_ONLY);
            assert calc != null;
        }
        if (calc instanceof IterCalc) {
            return (IterCalc) calc;
        } else {
            if (((SetType) calc.getType()).getArity() == 1) {
                return toIter((MemberListCalc) calc);
            } else {
                return toIter((TupleListCalc) calc);
            }
        }
    }

    private MemberIterCalc toIter(final MemberListCalc memberListCalc) {
        return new MemberListIterCalc(memberListCalc);
    }

    private TupleIterCalc toIter(final TupleListCalc tupleListCalc) {
        return new TupleListIterCalc(tupleListCalc);
    }

    public BooleanCalc compileBoolean(Exp exp) {
        final Calc calc = compileScalar(exp, false);
        if (calc instanceof BooleanCalc) {
            if (calc instanceof ConstantCalc) {
                final Object o = calc.evaluate(null);
                if (!(o instanceof Boolean)) {
                    return ConstantCalc.constantBoolean(
                        CastFunDef.toBoolean(o, new BooleanType()));
                }
            }
            return (BooleanCalc) calc;
        } else if (calc instanceof DoubleCalc) {
            final DoubleCalc doubleCalc = (DoubleCalc) calc;
            return new AbstractBooleanCalc(exp, new Calc[] {doubleCalc}) {
                public boolean evaluateBoolean(Evaluator evaluator) {
                    return doubleCalc.evaluateDouble(evaluator) != 0;
                }
            };
        } else if (calc instanceof IntegerCalc) {
            final IntegerCalc integerCalc = (IntegerCalc) calc;
            return new AbstractBooleanCalc(exp, new Calc[] {integerCalc}) {
                public boolean evaluateBoolean(Evaluator evaluator) {
                    return integerCalc.evaluateInteger(evaluator) != 0;
                }
            };
        } else {
            return (BooleanCalc) calc;
        }
    }

    public DoubleCalc compileDouble(Exp exp) {
        final DoubleCalc calc = (DoubleCalc) compileScalar(exp, false);
        if (calc instanceof ConstantCalc
            && !(calc.evaluate(null) instanceof Double)) {
            return ConstantCalc.constantDouble(
                calc.evaluateDouble(null));
        }
        return calc;
    }

    public TupleCalc compileTuple(Exp exp) {
        return (TupleCalc) compile(exp);
    }

    public Calc compileScalar(Exp exp, boolean specific) {
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
                            new DummyExp(memberType), hierarchyCalc);
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
            if (specific) {
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

    public ParameterSlot registerParameter(Parameter parameter) {
        ParameterSlot slot = parameterSlots.get(parameter);
        if (slot != null) {
            return slot;
        }
        int index = parameterSlots.size();
        ParameterSlotImpl slot2 = new ParameterSlotImpl(parameter, index);
        parameterSlots.put(parameter, slot2);
        slot2.value = parameter.getValue();

        // Compile the expression only AFTER the parameter has been
        // registered with a slot. Otherwise a cycle is possible.
        Calc calc = parameter.getDefaultExp().accept(this);
        slot2.setDefaultValueCalc(calc);
        return slot2;
    }

    public List<ResultStyle> getAcceptableResultStyles() {
        return resultStyles;
    }

    /**
     * Implementation of {@link ParameterSlot}.
     */
    private static class ParameterSlotImpl implements ParameterSlot {
        private final Parameter parameter;
        private final int index;
        private Calc defaultValueCalc;
        private Object value;
        private Object cachedDefaultValue;

        /**
         * Creates a ParameterSlotImpl.
         *
         * @param parameter Parameter
         * @param index Unique index of the slot
         */
        public ParameterSlotImpl(
            Parameter parameter, int index)
        {
            this.parameter = parameter;
            this.index = index;
        }

        public int getIndex() {
            return index;
        }

        public Calc getDefaultValueCalc() {
            return defaultValueCalc;
        }

        public Parameter getParameter() {
            return parameter;
        }

        /**
         * Sets a compiled expression to compute the default value of the
         * parameter.
         *
         * @param calc Compiled expression to compute default value of
         * parameter
         *
         * @see #getDefaultValueCalc()
         */
        private void setDefaultValueCalc(Calc calc) {
            this.defaultValueCalc = calc;
        }

        public void setParameterValue(Object value) {
            this.value = value;
        }

        public Object getParameterValue() {
            return value;
        }

        public void setCachedDefaultValue(Object value) {
            this.cachedDefaultValue = value;
        }

        public Object getCachedDefaultValue() {
            return cachedDefaultValue;
        }
    }

    /**
     * Adapter that converts a member list calc into a member iter calc.
     */
    private static class MemberListIterCalc extends AbstractMemberIterCalc {
        private final MemberListCalc memberListCalc;

        public MemberListIterCalc(MemberListCalc memberListCalc) {
            super(
                new DummyExp(memberListCalc.getType()),
                new Calc[]{memberListCalc});
            this.memberListCalc = memberListCalc;
        }

        public Iterable<Member> evaluateMemberIterable(Evaluator evaluator) {
            return memberListCalc.evaluateMemberList(evaluator);
        }
    }

    /**
     * Adapter that converts a tuple list calc into a tuple iter calc.
     */
    private static class TupleListIterCalc extends AbstractTupleIterCalc {
        private final TupleListCalc tupleListCalc;

        public TupleListIterCalc(TupleListCalc tupleListCalc) {
            super(
                new DummyExp(tupleListCalc.getType()),
                new Calc[]{tupleListCalc});
            this.tupleListCalc = tupleListCalc;
        }

        public Iterable<Member[]> evaluateTupleIterable(Evaluator evaluator) {
            return tupleListCalc.evaluateTupleList(evaluator);
        }
    }
}

// End AbstractExpCompiler.java
