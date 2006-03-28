/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2002-2002 Kana Software, Inc.
// Copyright (C) 2002-2005 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 26 February, 2002
*/
package mondrian.olap.fun;

import mondrian.calc.*;
import mondrian.calc.impl.*;
import mondrian.mdx.DimensionExpr;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;
import mondrian.olap.fun.extra.NthQuartileFunDef;
import mondrian.olap.type.DimensionType;
import mondrian.olap.type.LevelType;
import mondrian.olap.type.Type;
import mondrian.olap.type.TypeUtil;
import org.eigenbase.xom.XOMUtil;

import java.io.PrintWriter;
import java.util.*;

/**
 * <code>BuiltinFunTable</code> contains a list of all built-in MDX functions.
 *
 * <p>Note: Boolean expressions return {@link Boolean#TRUE},
 * {@link Boolean#FALSE} or null. null is returned if the expression can not be
 * evaluated because some values have not been loaded from database yet.</p>
 *
 * @author jhyde
 * @since 26 February, 2002
 * @version $Id$
 */
public class BuiltinFunTable extends FunTableImpl {

    /** the singleton */
    private static BuiltinFunTable instance;

    /**
     * Creates a function table containing all of the builtin MDX functions.
     * This method should only be called from {@link BuiltinFunTable#instance}.
     */
    protected BuiltinFunTable() {
        super();
    }

    protected void defineFunctions() {
        defineReserved("NULL");

        // first char: p=Property, m=Method, i=Infix, P=Prefix
        // 2nd:

        // ARRAY FUNCTIONS
        if (false) define(new FunDefBase(
                "SetToArray",
                "SetToArray(<Set>[, <Set>]...[, <Numeric Expression>])",
                "Converts one or more sets to an array for use in a user-defined function.",
                "fa*") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                throw new UnsupportedOperationException();
            }
        });

        //
        // DIMENSION FUNCTIONS
        define(HierarchyDimensionFunDef.instance);

        define(new FunDefBase(
                "Dimension",
                "<Dimension>.Dimension",
                "Returns the dimension that contains a specified hierarchy.",
                "pdd") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                Dimension dimension =
                        ((DimensionExpr) call.getArg(0)).getDimension();
                return new ConstantCalc(
                        DimensionType.forDimension(dimension),
                        dimension);
            }

        });

        define(new FunDefBase(
                "Dimension",
                "<Level>.Dimension",
                "Returns the dimension that contains a specified level.",
                "pdl") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final LevelCalc levelCalc =
                        compiler.compileLevel(call.getArg(0));
                return new AbstractDimensionCalc(call, new Calc[] {levelCalc}) {
                    public Dimension evaluateDimension(Evaluator evaluator) {
                        Level level =  levelCalc.evaluateLevel(evaluator);
                        return level.getDimension();
                    }
                };
            }
        });

        define(new FunDefBase(
                "Dimension",
                "<Member>.Dimension",
                "Returns the dimension that contains a specified member.",
                "pdm") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final MemberCalc memberCalc =
                        compiler.compileMember(call.getArg(0));
                return new AbstractDimensionCalc(call, new Calc[] {memberCalc}) {
                    public Dimension evaluateDimension(Evaluator evaluator) {
                        Member member = memberCalc.evaluateMember(evaluator);
                        return member.getDimension();
                    }
                };
            }
        });

        define(new FunDefBase(
                "Dimensions",
                "Dimensions(<Numeric Expression>)",
                "Returns the dimension whose zero-based position within the cube is specified by a numeric expression.",
                "fdn") {
            public Type getResultType(Validator validator, Exp[] args) {
                return DimensionType.Unknown;
            }

            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final IntegerCalc integerCalc =
                        compiler.compileInteger(call.getArg(0));
                return new AbstractDimensionCalc(call, new Calc[] {integerCalc}) {
                    public Dimension evaluateDimension(Evaluator evaluator) {
                        int n = integerCalc.evaluateInteger(evaluator);
                        return nthDimension(evaluator, n);
                    }
                };
            }

            Dimension nthDimension(Evaluator evaluator, int n) {
                Cube cube = evaluator.getCube();
                Dimension[] dimensions = cube.getDimensions();
                if (n >= dimensions.length || n < 0) {
                    throw newEvalException(
                            this, "Index '" + n + "' out of bounds");
                }
                return dimensions[n];
            }
        });
        define(new FunDefBase(
                "Dimensions",
                "Dimensions(<String Expression>)",
                "Returns the dimension whose name is specified by a string.",
                "fdS") {
            public Type getResultType(Validator validator, Exp[] args) {
                return DimensionType.Unknown;
            }

            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final StringCalc stringCalc =
                        compiler.compileString(call.getArg(0));
                return new AbstractDimensionCalc(call, new Calc[] {stringCalc}) {
                    public Dimension evaluateDimension(Evaluator evaluator) {
                        String dimensionName =
                                stringCalc.evaluateString(evaluator);
                        return findDimension(dimensionName, evaluator);
                    }
                };
            }

            Dimension findDimension(String s, Evaluator evaluator) {
                if (s.indexOf("[") == -1) {
                    s = Util.quoteMdxIdentifier(s);
                }
                OlapElement o = evaluator.getSchemaReader().lookupCompound(
                        evaluator.getCube(),
                        explode(s),
                        false,
                        Category.Dimension);
                if (o instanceof Dimension) {
                    return (Dimension) o;
                } else if (o == null) {
                    throw newEvalException(this, "Dimension '" + s + "' not found");
                } else {
                    throw newEvalException(this, "Dimensions(" + s + ") found " + o);
                }
            }
        });

        //
        // HIERARCHY FUNCTIONS
        define(LevelHierarchyFunDef.instance);
        define(MemberHierarchyFunDef.instance);

        //
        // LEVEL FUNCTIONS
        define(MemberLevelFunDef.instance);

        define(new FunDefBase(
                "Levels",
                "<Hierarchy>.Levels(<Numeric Expression>)",
                "Returns the level whose position in a hierarchy is specified by a numeric expression.",
                "mlhn") {
            public Type getResultType(Validator validator, Exp[] args) {
                final Type argType = args[0].getType();
                return LevelType.forType(argType);
            }

            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final HierarchyCalc hierarchyCalc =
                        compiler.compileHierarchy(call.getArg(0));
                final IntegerCalc ordinalCalc =
                        compiler.compileInteger(call.getArg(1));
                return new AbstractLevelCalc(call, new Calc[] {hierarchyCalc, ordinalCalc}) {
                    public Level evaluateLevel(Evaluator evaluator) {
                        Hierarchy hierarchy =
                                hierarchyCalc.evaluateHierarchy(evaluator);
                        int ordinal = ordinalCalc.evaluateInteger(evaluator);
                        return nthLevel(hierarchy, ordinal);
                    }
                };
            }

            Level nthLevel(Hierarchy hierarchy, int n) {
                Level[] levels = hierarchy.getLevels();

                if (n >= levels.length || n < 0) {
                    throw newEvalException(
                            this, "Index '" + n + "' out of bounds");
                }
                return levels[n];
            }
        });

        define(new FunDefBase(
                "Levels",
                "Levels(<String Expression>)",
                "Returns the level whose name is specified by a string expression.",
                "flS") {
            public Type getResultType(Validator validator, Exp[] args) {
                final Type argType = args[0].getType();
                return LevelType.forType(argType);
            }
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final StringCalc stringCalc =
                        compiler.compileString(call.getArg(0));
                return new AbstractLevelCalc(call, new Calc[] {stringCalc}) {
                    public Level evaluateLevel(Evaluator evaluator) {
                        String levelName =
                                stringCalc.evaluateString(evaluator);
                        return findLevel(evaluator, levelName);
                    }
                };
            }

            Level findLevel(Evaluator evaluator, String s) {
                Cube cube = evaluator.getCube();
                OlapElement o = (s.startsWith("[")) ?
                        evaluator.getSchemaReader().lookupCompound(
                                cube,
                                explode(s),
                                false,
                                Category.Level) :
                        // lookupCompound barfs if "s" doesn't have matching
                        // brackets, so don't even try
                        null;

                if (o instanceof Level) {
                    return (Level) o;
                } else if (o == null) {
                    throw newEvalException(this, "Level '" + s + "' not found");
                } else {
                    throw newEvalException(this, "Levels('" + s + "') found " + o);
                }
            }
        });

        //
        // LOGICAL FUNCTIONS
        define(IsEmptyFunDef.Resolver);

        define(new FunDefBase(
                "IS NULL",
                "<Member> IS NULL",
                "Returns whether a member is null.",
                "pbm") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final MemberCalc memberCalc =
                        compiler.compileMember(call.getArg(0));
                return new AbstractBooleanCalc(call, new Calc[] {memberCalc}) {
                    public boolean evaluateBoolean(Evaluator evaluator) {
                        Member member = memberCalc.evaluateMember(evaluator);
                        return member.isNull();
                    }
                };
            }

        });

        define(IsFunDef.Resolver);

        //
        // MEMBER FUNCTIONS
        define(AncestorFunDef.Resolver);

        define(new FunDefBase(
                "Cousin",
                "Cousin(<member>, <ancestor member>)",
                "Returns the member with the same relative position under <ancestor member> as the member specified.",
                "fmmm") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final MemberCalc memberCalc =
                        compiler.compileMember(call.getArg(0));
                final MemberCalc ancestorMemberCalc =
                        compiler.compileMember(call.getArg(1));
                return new AbstractMemberCalc(call, new Calc[] {memberCalc, ancestorMemberCalc}) {
                    public Member evaluateMember(Evaluator evaluator) {
                        Member member = memberCalc.evaluateMember(evaluator);
                        Member ancestorMember = ancestorMemberCalc.evaluateMember(evaluator);
                        return cousin(
                                evaluator.getSchemaReader(),
                                member,
                                ancestorMember);
                    }
                };
            }

        });

        define(DimensionCurrentMemberFunDef.instance);

        define(HierarchyCurrentMemberFunDef.instance);

        define(new FunDefBase(
                "DataMember",
                "<Member>.DataMember",
                "Returns the system-generated data member that is associated with a nonleaf member of a dimension.",
                "pmm") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final MemberCalc memberCalc =
                        compiler.compileMember(call.getArg(0));
                return new AbstractMemberCalc(call, new Calc[] {memberCalc}) {
                    public Member evaluateMember(Evaluator evaluator) {
                        Member member = memberCalc.evaluateMember(evaluator);
                        return member.getDataMember();
                    }
                };
            }

        });

        define(new FunDefBase(
                "DefaultMember",
                "<Dimension>.DefaultMember",
                "Returns the default member of a dimension.",
                "pmd") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final DimensionCalc dimensionCalc =
                        compiler.compileDimension(call.getArg(0));
                return new AbstractMemberCalc(call, new Calc[] {dimensionCalc}) {
                    public Member evaluateMember(Evaluator evaluator) {
                        Dimension dimension =
                                dimensionCalc.evaluateDimension(evaluator);
                        return evaluator.getSchemaReader()
                                .getHierarchyDefaultMember(
                                        dimension.getHierarchies()[0]);
                    }
                };
            }
        });

        define(new FunDefBase(
                "DefaultMember",
                "<Hierarchy>.DefaultMember",
                "Returns the default member of a hierarchy.",
                "pmh") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final HierarchyCalc hierarchyCalc =
                        compiler.compileHierarchy(call.getArg(0));
                return new AbstractMemberCalc(call, new Calc[] {hierarchyCalc}) {
                    public Member evaluateMember(Evaluator evaluator) {
                        Hierarchy hierarchy =
                                hierarchyCalc.evaluateHierarchy(evaluator);
                        return evaluator.getSchemaReader()
                                .getHierarchyDefaultMember(hierarchy);
                    }
                };
            }
        });

        define(new FunDefBase(
                "FirstChild",
                "<Member>.FirstChild",
                "Returns the first child of a member.",
                "pmm") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final MemberCalc memberCalc =
                        compiler.compileMember(call.getArg(0));
                return new AbstractMemberCalc(call, new Calc[] {memberCalc}) {
                    public Member evaluateMember(Evaluator evaluator) {
                        Member member = memberCalc.evaluateMember(evaluator);
                        return firstChild(evaluator, member);
                    }
                };
            }

            Member firstChild(Evaluator evaluator, Member member) {
                Member[] children = evaluator.getSchemaReader()
                        .getMemberChildren(member);
                return (children.length == 0)
                        ? member.getHierarchy().getNullMember()
                        : children[0];
            }
        });

        define(new FunDefBase(
                "FirstSibling",
                "<Member>.FirstSibling",
                "Returns the first child of the parent of a member.",
                "pmm") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final MemberCalc memberCalc =
                        compiler.compileMember(call.getArg(0));
                return new AbstractMemberCalc(call, new Calc[] {memberCalc}) {
                    public Member evaluateMember(Evaluator evaluator) {
                        Member member = memberCalc.evaluateMember(evaluator);
                        return firstSibling(member, evaluator);
                    }
                };
            }

            Member firstSibling(Member member, Evaluator evaluator) {
                Member parent = member.getParentMember();
                Member[] children;
                if (parent == null) {
                    if (member.isNull()) {
                        return member;
                    }
                    children = evaluator.getSchemaReader().getHierarchyRootMembers(member.getHierarchy());
                } else {
                    children = evaluator.getSchemaReader().getMemberChildren(parent);
                }
                return children[0];
            }
        });

        define(LeadLagFunDef.LagResolver);

        define(new FunDefBase(
                "LastChild",
                "<Member>.LastChild",
                "Returns the last child of a member.",
                "pmm") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final MemberCalc memberCalc =
                        compiler.compileMember(call.getArg(0));
                return new AbstractMemberCalc(call, new Calc[] {memberCalc}) {
                    public Member evaluateMember(Evaluator evaluator) {
                        Member member = memberCalc.evaluateMember(evaluator);
                        return lastChild(evaluator, member);
                    }
                };
            }

            Member lastChild(Evaluator evaluator, Member member) {
                Member[] children =
                        evaluator.getSchemaReader().getMemberChildren(member);
                return (children.length == 0)
                        ? member.getHierarchy().getNullMember()
                        : children[children.length - 1];
            }
        });

        define(new FunDefBase(
                "LastSibling",
                "<Member>.LastSibling",
                "Returns the last child of the parent of a member.",
                "pmm") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final MemberCalc memberCalc =
                        compiler.compileMember(call.getArg(0));
                return new AbstractMemberCalc(call, new Calc[] {memberCalc}) {
                    public Member evaluateMember(Evaluator evaluator) {
                        Member member = memberCalc.evaluateMember(evaluator);
                        return firstSibling(member, evaluator);
                    }
                };
            }

            Member firstSibling(Member member, Evaluator evaluator) {
                Member parent = member.getParentMember();
                Member[] children;
                final SchemaReader schemaReader = evaluator.getSchemaReader();
                if (parent == null) {
                    if (member.isNull()) {
                        return member;
                    }
                    children = schemaReader.getHierarchyRootMembers(
                            member.getHierarchy());
                } else {
                    children = schemaReader.getMemberChildren(parent);
                }
                return children[children.length - 1];
            }
        });

        define(LeadLagFunDef.LeadResolver);

        define(new FunDefBase(
                "Members",
                "Members(<String Expression>)",
                "Returns the member whose name is specified by a string expression.",
                "fmS") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                throw new UnsupportedOperationException();
            }
        });

        define(new FunDefBase(
                "NextMember",
                "<Member>.NextMember",
                "Returns the next member in the level that contains a specified member.",
                "pmm") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final MemberCalc memberCalc =
                        compiler.compileMember(call.getArg(0));
                return new AbstractMemberCalc(call, new Calc[] {memberCalc}) {
                    public Member evaluateMember(Evaluator evaluator) {
                        Member member = memberCalc.evaluateMember(evaluator);
                        return evaluator.getSchemaReader().getLeadMember(member, +1);
                    }
                };
            }

        });

        define(OpeningClosingPeriodFunDef.OpeningPeriodResolver);
        define(OpeningClosingPeriodFunDef.ClosingPeriodResolver);

        define(ParallelPeriodFunDef.Resolver);


        define(new FunDefBase(
                "Parent",
                "<Member>.Parent",
                "Returns the parent of a member.",
                "pmm") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final MemberCalc memberCalc =
                        compiler.compileMember(call.getArg(0));
                return new AbstractMemberCalc(call, new Calc[] {memberCalc}) {
                    public Member evaluateMember(Evaluator evaluator) {
                        Member member = memberCalc.evaluateMember(evaluator);
                        return memberParent(evaluator, member);
                    }
                };
            }

            Member memberParent(Evaluator evaluator, Member member) {
                Member parent = evaluator.getSchemaReader().getMemberParent(member);
                if (parent == null) {
                    parent = member.getHierarchy().getNullMember();
                }
                return parent;
            }

        });

        define(new FunDefBase(
                "PrevMember",
                "<Member>.PrevMember",
                "Returns the previous member in the level that contains a specified member.",
                "pmm") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final MemberCalc memberCalc =
                        compiler.compileMember(call.getArg(0));
                return new AbstractMemberCalc(call, new Calc[] {memberCalc}) {
                    public Member evaluateMember(Evaluator evaluator) {
                        Member member = memberCalc.evaluateMember(evaluator);
                        return evaluator.getSchemaReader().getLeadMember(member, -1);
                    }
                };
            }
        });

        define(new FunDefBase(
                "StrToMember",
                "StrToMember(<String Expression>)",
                "Returns a member from a unique name String in MDX format.",
                "fmS") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final StringCalc memberNameCalc =
                        compiler.compileString(call.getArg(0));
                return new AbstractMemberCalc(call, new Calc[] {memberNameCalc}) {
                    public Member evaluateMember(Evaluator evaluator) {
                        String memberName =
                                memberNameCalc.evaluateString(evaluator);
                        return strToMember(evaluator, memberName);
                    }
                };
            }

            Member strToMember(Evaluator evaluator, String memberName) {
                Cube cube = evaluator.getCube();
                SchemaReader schemaReader = evaluator.getSchemaReader();
                String[] uniqueNameParts = Util.explode(memberName);
                Member member = (Member) schemaReader.lookupCompound(cube,
                        uniqueNameParts, true, Category.Member);
                // Member member = schemaReader.getMemberByUniqueName(uniqueNameParts, false);
                return member;
            }
        });

        if (false) define(new FunDefBase(
                "ValidMeasure",
                "ValidMeasure(<Tuple>)",
                "Returns a valid measure in a virtual cube by forcing inapplicable dimensions to their top level.",
                "fm*") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                throw new UnsupportedOperationException();
            }
        });
        //
        // NUMERIC FUNCTIONS
        define(AggregateFunDef.resolver);

        // Obsolete??
        define(new MultiResolver(
                "$AggregateChildren",
                "$AggregateChildren(<Hierarchy>)",
                "Equivalent to 'Aggregate(<Hierarchy>.CurrentMember.Children); for internal use.",
                new String[] {"Inh"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new FunDefBase(dummyFunDef) {
                    public void unparse(Exp[] args, PrintWriter pw) {
                        pw.print(getName());
                        pw.print("(");
                        args[0].unparse(pw);
                        pw.print(")");
                    }

                    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                        final HierarchyCalc hierarchyCalc =
                                compiler.compileHierarchy(call.getArg(0));
                        final Calc valueCalc = new ValueCalc(call);
                        return new AbstractCalc(call) {
                            public Object evaluate(Evaluator evaluator) {
                                Hierarchy hierarchy =
                                        hierarchyCalc.evaluateHierarchy(evaluator);
                                return aggregateChildren(evaluator, hierarchy, valueCalc);
                            }

                            public Calc[] getCalcs() {
                                return new Calc[] {hierarchyCalc, valueCalc};
                            }
                        };
                    }

                    Object aggregateChildren(
                            Evaluator evaluator, Hierarchy hierarchy, final Calc valueFunCall) {
                        Member member = evaluator.getParent().getContext(hierarchy.getDimension());
                        List members =
                                (List) member.getPropertyValue(
                                        Property.CONTRIBUTING_CHILDREN.name);
                        Aggregator aggregator =
                                (Aggregator) evaluator.getProperty(
                                        Property.AGGREGATION_TYPE.name, null);
                        if (aggregator == null) {
                            throw FunUtil.newEvalException(null, "Could not find an aggregator in the current evaluation context");
                        }
                        Aggregator rollup = aggregator.getRollup();
                        if (rollup == null) {
                            throw FunUtil.newEvalException(null, "Don't know how to rollup aggregator '" + aggregator + "'");
                        }
                        return rollup.aggregate(evaluator.push(), members, valueFunCall);
                    }
                };
            }
        });

        define(AvgFunDef.Resolver);

        define(CorrelationFunDef.Resolver);

        define(CountFunDef.Resolver);

        define(new FunDefBase(
                "Count",
                "<Set>.Count",
                "Returns the number of tuples in a set including empty cells.",
                "pnx") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final ListCalc memberListCalc =
                        compiler.compileList(call.getArg(0));
                return new AbstractIntegerCalc(call, new Calc[] {memberListCalc}) {
                    public int evaluateInteger(Evaluator evaluator) {
                        List memberList =
                                memberListCalc.evaluateList(evaluator);
                        return count(evaluator, memberList, true);
                    }
                };
            }
        });

        define(CovarianceFunDef.CovarianceResolver);
        define(CovarianceFunDef.CovarianceNResolver);

        define(new FunDefBase(
                "IIf",
                "IIf(<Logical Expression>, <String Expression1>, <String Expression2>)",
                "Returns one of two string values determined by a logical test.",
                "fSbSS") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final BooleanCalc booleanCalc =
                        compiler.compileBoolean(call.getArg(0));
                final StringCalc calc1 = compiler.compileString(call.getArg(1));
                final StringCalc calc2 = compiler.compileString(call.getArg(2));
                return new AbstractStringCalc(call, new Calc[] {booleanCalc, calc1, calc2}) {
                    public String evaluateString(Evaluator evaluator) {
                        final boolean b =
                                booleanCalc.evaluateBoolean(evaluator);
                        StringCalc calc = b ? calc1 : calc2;
                        return calc.evaluateString(evaluator);
                    }
                };
            }
        });

        define(new FunDefBase(
                "IIf",
                "IIf(<Logical Expression>, <Numeric Expression1>, <Numeric Expression2>)",
                "Returns one of two numeric values determined by a logical test.",
                "fnbnn") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final BooleanCalc booleanCalc =
                        compiler.compileBoolean(call.getArg(0));
                final Calc calc1 = compiler.compileScalar(call.getArg(1), true);
                final Calc calc2 = compiler.compileScalar(call.getArg(2), true);
                return new GenericCalc(call) {
                    public Object evaluate(Evaluator evaluator) {
                        final boolean b =
                                booleanCalc.evaluateBoolean(evaluator);
                        Calc calc = b ? calc1 : calc2;
                        return calc.evaluate(evaluator);
                    }

                    public Calc[] getCalcs() {
                        return new Calc[] {booleanCalc, calc1, calc2};
                    }
                };
            }

        });

        define(LinReg.InterceptResolver);
        define(LinReg.PointResolver);
        define(LinReg.R2Resolver);
        define(LinReg.SlopeResolver);
        define(LinReg.VarianceResolver);

        define(MinMaxFunDef.MaxResolver);

        define(MedianFunDef.Resolver);

        define(MinMaxFunDef.MinResolver);

        define(new FunDefBase(
                "Ordinal",
                "<Level>.Ordinal",
                "Returns the zero-based ordinal value associated with a level.",
                "pnl") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final LevelCalc levelCalc =
                        compiler.compileLevel(call.getArg(0));
                return new AbstractIntegerCalc(call, new Calc[] {levelCalc}) {
                    public int evaluateInteger(Evaluator evaluator) {
                        final Level level = levelCalc.evaluateLevel(evaluator);
                        return level.getDepth();
                    }
                };
            }

        });

        define(RankFunDef.Resolver);

        define(CacheFunDef.Resolver);

        define(StdevFunDef.StdevResolver);
        define(StdevFunDef.StddevResolver);

        define(StdevPFunDef.StdevpResolver);
        define(StdevPFunDef.StddevpResolver);

        define(SumFunDef.Resolver);

        define(new FunDefBase(
                "Value",
                "<Measure>.Value",
                "Returns the value of a measure.",
                "pnm") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final MemberCalc memberCalc =
                        compiler.compileMember(call.getArg(0));
                return new AbstractCalc(call) {
                    public Object evaluate(Evaluator evaluator) {
                        Member member = memberCalc.evaluateMember(evaluator);
                        Member old = evaluator.setContext(member);
                        Object value = evaluator.evaluateCurrent();
                        evaluator.setContext(old);
                        return value;
                    }

                    public boolean dependsOn(Dimension dimension) {
                        if (super.dependsOn(dimension)) {
                            return true;
                        }
                        if (memberCalc.getType().usesDimension(dimension, true) ) {
                            return false;
                        }
                        return true;
                    }
                    public Calc[] getCalcs() {
                        return new Calc[] {memberCalc};
                    }
                };
            }

        });

        define(VarFunDef.VarResolver);
        define(VarFunDef.VarianceResolver);

        define(VarPFunDef.VariancePResolver);
        define(VarPFunDef.VarPResolver);

        //
        // SET FUNCTIONS

        define(AddCalculatedMembersFunDef.instance);

        define(new FunDefBase(
                "Ascendants",
                "Ascendants(<Member>)",
                "Returns the set of the ascendants of a specified member.",
                "fxm") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final MemberCalc memberCalc =
                        compiler.compileMember(call.getArg(0));
                return new AbstractListCalc(call, new Calc[] {memberCalc}) {
                    public List evaluateList(Evaluator evaluator) {
                        Member member = memberCalc.evaluateMember(evaluator);
                        return ascendants(member);
                    }
                };
            }

            List ascendants(Member member) {
                if (member.isNull()) {
                    return Collections.EMPTY_LIST;
                }
                Member[] members = member.getAncestorMembers();
                final List result = new ArrayList(members.length + 1);
                result.add(member);
                XOMUtil.addAll(result, members);
                return result;
            }
        });

        define(TopBottomCountFunDef.BottomCountResolver);
        define(TopBottomPercentSumFunDef.BottomPercentResolver);
        define(TopBottomPercentSumFunDef.BottomSumResolver);
        define(TopBottomCountFunDef.TopCountResolver);
        define(TopBottomPercentSumFunDef.TopPercentResolver);
        define(TopBottomPercentSumFunDef.TopSumResolver);

        define(new FunDefBase(
                "Children",
                "<Member>.Children",
                "Returns the children of a member.",
                "pxm") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final MemberCalc memberCalc =
                        compiler.compileMember(call.getArg(0));
                return new AbstractListCalc(call, new Calc[] {memberCalc}) {
                    public List evaluateList(Evaluator evaluator) {
                        Member member = memberCalc.evaluateMember(evaluator);
                        Member[] children = getNonEmptyMemberChildren(evaluator, member);
                        return Arrays.asList(children);
                    }
                };
            }

        });

        define(CrossJoinFunDef.Resolver);

        define(NonEmptyCrossJoinFunDef.Resolver);

        define(CrossJoinFunDef.StarResolver);

        define(DescendantsFunDef.Resolver);

        define(new FunDefBase(
                "Distinct",
                "Distinct(<Set>)",
                "Eliminates duplicate tuples from a set.",
                "fxx") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final ListCalc listCalc =
                        compiler.compileList(call.getArg(0));
                return new AbstractListCalc(call, new Calc[] {listCalc}) {
                    public List evaluateList(Evaluator evaluator) {
                        List list = listCalc.evaluateList(evaluator);
                        return distinct(list);
                    }
                };
            }

            List distinct(List list) {
                HashSet hashSet = new HashSet(list.size());
                Iterator iter = list.iterator();
                List result = new ArrayList();

                while (iter.hasNext()) {
                    Object element = iter.next();
                    MemberHelper lookupObj = new MemberHelper(element);

                    if (hashSet.add(lookupObj)) {
                        result.add(element);
                    }
                }
                return result;
            }
        });

        define(DrilldownLevelFunDef.Resolver);

        if (false) define(new FunDefBase(
                "DrilldownLevelBottom",
                "DrilldownLevelBottom(<Set>, <Count>[, [<Level>][, <Numeric Expression>]])",
                "Drills down the bottom N members of a set, at a specified level, to one level below.",
                "fx*") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                throw new UnsupportedOperationException();
            }
        });

        if (false) define(new FunDefBase(
                "DrilldownLevelTop",
                "DrilldownLevelTop(<Set>, <Count>[, [<Level>][, <Numeric Expression>]])",
                "Drills down the top N members of a set, at a specified level, to one level below.",
                "fx*") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                throw new UnsupportedOperationException();
            }
        });

        define(DrilldownMemberFunDef.Resolver);

        if (false) define(new FunDefBase(
                "DrilldownMemberBottom",
                "DrilldownMemberBottom(<Set1>, <Set2>, <Count>[, [<Numeric Expression>][, RECURSIVE]])",
                "Like DrilldownMember except that it includes only the bottom N children.",
                "fx*") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                throw new UnsupportedOperationException();
            }
        });

        if (false) define(new FunDefBase(
                "DrilldownMemberTop",
                "DrilldownMemberTop(<Set1>, <Set2>, <Count>[, [<Numeric Expression>][, RECURSIVE]])",
                "Like DrilldownMember except that it includes only the top N children.",
                "fx*") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                throw new UnsupportedOperationException();
            }
        });

        if (false) define(new FunDefBase(
                "DrillupLevel",
                "DrillupLevel(<Set>[, <Level>])",
                "Drills up the members of a set that are below a specified level.",
                "fx*") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                throw new UnsupportedOperationException();
            }
        });

        if (false) define(new FunDefBase(
                "DrillupMember",
                "DrillupMember(<Set1>, <Set2>)",
                "Drills up the members in a set that are present in a second specified set.",
                "fx*") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                throw new UnsupportedOperationException();
            }
        });

        define(ExceptFunDef.Resolver);

        if (false) define(new FunDefBase(
                "Extract",
                "Extract(<Set>, <Dimension>[, <Dimension>...])",
                "Returns a set of tuples from extracted dimension elements. The opposite of Crossjoin.",
                "fx*") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                throw new UnsupportedOperationException();
            }
        });

        define(FilterFunDef.instance);

        define(GenerateFunDef.Resolver);
        define(HeadTailFunDef.HeadResolver);

        define(HierarchizeFunDef.Resolver);

        define(IntersectFunDef.resolver);
        define(LastPeriodsFunDef.Resolver);

        define(new FunDefBase(
                "Members",
                "<Dimension>.Members",
                "Returns the set of members in a dimension.",
                "pxd") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final DimensionCalc dimensionCalc =
                        compiler.compileDimension(call.getArg(0));
                return new AbstractListCalc(call, new Calc[] {dimensionCalc}) {
                    public List evaluateList(Evaluator evaluator) {
                        Dimension dimension =
                                dimensionCalc.evaluateDimension(evaluator);
                        return dimensionMembers(dimension, evaluator, false);
                    }
                };
            }

        });

        define(new FunDefBase(
                "AllMembers",
                "<Dimension>.AllMembers",
                "Returns a set that contains all members, including calculated members, of the specified dimension.",
                "pxd") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final DimensionCalc dimensionCalc =
                        compiler.compileDimension(call.getArg(0));
                return new AbstractListCalc(call, new Calc[] {dimensionCalc}) {
                    public List evaluateList(Evaluator evaluator) {
                        Dimension dimension =
                                dimensionCalc.evaluateDimension(evaluator);
                        return dimensionMembers(dimension, evaluator, true);
                    }
                };
            }

        });

        define(new FunDefBase(
                "Members",
                "<Hierarchy>.Members",
                "Returns the set of members in a hierarchy.",
                "pxh") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final HierarchyCalc hierarchyCalc =
                        compiler.compileHierarchy(call.getArg(0));
                return new AbstractListCalc(call, new Calc[] {hierarchyCalc}) {
                    public List evaluateList(Evaluator evaluator) {
                        Hierarchy hierarchy =
                                hierarchyCalc.evaluateHierarchy(evaluator);
                        return hierarchyMembers(hierarchy, evaluator, false);
                    }
                };
            }

        });

        define(new FunDefBase(
                "AllMembers",
                "<Hierarchy>.AllMembers",
                "Returns a set that contains all members, including calculated members, of the specified hierarchy.",
                "pxh") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final HierarchyCalc hierarchyCalc =
                        compiler.compileHierarchy(call.getArg(0));
                return new AbstractListCalc(call, new Calc[] {hierarchyCalc}) {
                    public List evaluateList(Evaluator evaluator) {
                        Hierarchy hierarchy =
                                hierarchyCalc.evaluateHierarchy(evaluator);
                        return hierarchyMembers(hierarchy, evaluator, true);
                    }
                };
            }
        });

        define(new FunDefBase(
                "Members",
                "<Level>.Members",
                "Returns the set of members in a level.",
                "pxl") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final LevelCalc levelCalc =
                        compiler.compileLevel(call.getArg(0));
                return new AbstractListCalc(call, new Calc[] {levelCalc}) {
                    public List evaluateList(Evaluator evaluator) {
                        Level level = levelCalc.evaluateLevel(evaluator);
                        return levelMembers(level, evaluator, false);
                    }
                };
            }
        });

        define(new FunDefBase(
                "AllMembers",
                "<Level>.AllMembers",
                "Returns a set that contains all members, including calculated members, of the specified level.",
                "pxl") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final LevelCalc levelCalc =
                        compiler.compileLevel(call.getArg(0));
                return new AbstractListCalc(call, new Calc[] {levelCalc}) {
                    public List evaluateList(Evaluator evaluator) {
                        Level level = levelCalc.evaluateLevel(evaluator);
                        return levelMembers(level, evaluator, true);
                    }
                };
            }
        });

        define(XtdFunDef.MtdResolver);
        define(OrderFunDef.Resolver);
        define(PeriodsToDateFunDef.Resolver);
        define(XtdFunDef.QtdResolver);

        define(new FunDefBase(
                "StripCalculatedMembers",
                "StripCalculatedMembers(<Set>)",
                "Removes calculated members from a set.",
                "fxx") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final ListCalc listCalc =
                        compiler.compileList(call.getArg(0));
                return new AbstractListCalc(call, new Calc[] {listCalc}) {
                    public List evaluateList(Evaluator evaluator) {
                        final List list = listCalc.evaluateList(evaluator);
                        if (list != null) {
                            removeCalculatedMembers(list);
                        }
                        return list;
                    }
                };
            }

        });

        define(new FunDefBase(
                "Siblings",
                "<Member>.Siblings",
                "Returns the siblings of a specified member, including the member itself.",
                "pxm") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final MemberCalc memberCalc =
                        compiler.compileMember(call.getArg(0));
                return new AbstractListCalc(call, new Calc[] {memberCalc}) {
                    public List evaluateList(Evaluator evaluator) {
                        final Member member =
                                memberCalc.evaluateMember(evaluator);
                        return memberSiblings(member, evaluator);
                    }
                };
            }

            List memberSiblings(Member member, Evaluator evaluator) {
                if (member.isNull()) {
                    // the null member has no siblings -- not even itself
                    return Collections.EMPTY_LIST;
                }
                Member parent = member.getParentMember();
                final SchemaReader schemaReader = evaluator.getSchemaReader();
                Member[] siblings = (parent == null)
                    ? schemaReader.getHierarchyRootMembers(member.getHierarchy())
                    : schemaReader.getMemberChildren(parent);

                return Arrays.asList(siblings);
            }
        });

        define(StrToSetFunDef.Resolver);
        define(SubsetFunDef.Resolver);
        define(HeadTailFunDef.TailResolver);
        define(ToggleDrillStateFunDef.Resolver);
        define(UnionFunDef.Resolver);
        define(VisualTotalsFunDef.Resolver);
        define(XtdFunDef.WtdResolver);
        define(XtdFunDef.YtdResolver);
        define(RangeFunDef.instance); // "<member> : <member>" operator
        define(SetFunDef.Resolver); // "{ <member> [,...] }" operator

        //
        // STRING FUNCTIONS
        define(FormatFunDef.Resolver);

        define(new FunDefBase(
                "Caption",
                "<Dimension>.Caption",
                "Returns the caption of a dimension.",
                "pSd") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final DimensionCalc dimensionCalc =
                        compiler.compileDimension(call.getArg(0));
                return new AbstractStringCalc(call, new Calc[] {dimensionCalc}) {
                    public String evaluateString(Evaluator evaluator) {
                        final Dimension dimension =
                                dimensionCalc.evaluateDimension(evaluator);
                        return dimension.getCaption();
                    }
                };
            }
        });

        define(new FunDefBase(
                "Caption",
                "<Hierarchy>.Caption",
                "Returns the caption of a hierarchy.",
                "pSh") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final HierarchyCalc hierarchyCalc =
                        compiler.compileHierarchy(call.getArg(0));
                return new AbstractStringCalc(call, new Calc[] {hierarchyCalc}) {
                    public String evaluateString(Evaluator evaluator) {
                        final Hierarchy hierarchy =
                                hierarchyCalc.evaluateHierarchy(evaluator);
                        return hierarchy.getCaption();
                    }
                };
            }

        });

        define(new FunDefBase(
                "Caption",
                "<Level>.Caption",
                "Returns the caption of a level.",
                "pSl") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final LevelCalc levelCalc =
                        compiler.compileLevel(call.getArg(0));
                return new AbstractStringCalc(call, new Calc[] {levelCalc}) {
                    public String evaluateString(Evaluator evaluator) {
                        final Level level = levelCalc.evaluateLevel(evaluator);
                        return level.getCaption();
                    }
                };
            }
        });

        define(new FunDefBase(
                "Caption",
                "<Member>.Caption",
                "Returns the caption of a member.",
                "pSm") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final MemberCalc memberCalc =
                        compiler.compileMember(call.getArg(0));
                return new AbstractStringCalc(call, new Calc[] {memberCalc}) {
                    public String evaluateString(Evaluator evaluator) {
                        final Member member =
                                memberCalc.evaluateMember(evaluator);
                        return member.getCaption();
                    }
                };
            }
        });

        define(new FunDefBase(
                "Name",
                "<Dimension>.Name",
                "Returns the name of a dimension.",
                "pSd") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final DimensionCalc dimensionCalc =
                        compiler.compileDimension(call.getArg(0));
                return new AbstractStringCalc(call, new Calc[] {dimensionCalc}) {
                    public String evaluateString(Evaluator evaluator) {
                        final Dimension dimension =
                                dimensionCalc.evaluateDimension(evaluator);
                        return dimension.getName();
                    }
                };
            }
        });

        define(new FunDefBase(
                "Name",
                "<Hierarchy>.Name",
                "Returns the name of a hierarchy.",
                "pSh") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final HierarchyCalc hierarchyCalc =
                        compiler.compileHierarchy(call.getArg(0));
                return new AbstractStringCalc(call, new Calc[] {hierarchyCalc}) {
                    public String evaluateString(Evaluator evaluator) {
                        final Hierarchy hierarchy =
                                hierarchyCalc.evaluateHierarchy(evaluator);
                        return hierarchy.getName();
                    }
                };
            }
        });

        define(new FunDefBase(
                "Name",
                "<Level>.Name",
                "Returns the name of a level.",
                "pSl") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final LevelCalc levelCalc =
                        compiler.compileLevel(call.getArg(0));
                return new AbstractStringCalc(call, new Calc[] {levelCalc}) {
                    public String evaluateString(Evaluator evaluator) {
                        final Level level = levelCalc.evaluateLevel(evaluator);
                        return level.getName();
                    }
                };
            }
        });

        define(new FunDefBase(
                "Name",
                "<Member>.Name",
                "Returns the name of a member.",
                "pSm") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final MemberCalc memberCalc =
                        compiler.compileMember(call.getArg(0));
                return new AbstractStringCalc(call, new Calc[] {memberCalc}) {
                    public String evaluateString(Evaluator evaluator) {
                        final Member member =
                                memberCalc.evaluateMember(evaluator);
                        return member.getName();
                    }
                };
            }
        });

        define(new FunDefBase(
                "SetToStr",
                "SetToStr(<Set>)",
                "Constructs a string from a set.",
                "fSx") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final ListCalc listCalc =
                        compiler.compileList(call.getArg(0));
                return new AbstractStringCalc(call, new Calc[] {listCalc}) {
                    public String evaluateString(Evaluator evaluator) {
                        final List list = listCalc.evaluateList(evaluator);
                        return strToSet(list);
                    }
                };
            }

            String strToSet(List list) {
                StringBuffer buf = new StringBuffer();
                buf.append("{");
                for (int i = 0; i < list.size(); i++) {
                    if (i > 0) {
                        buf.append(", ");
                    }
                    final Object o = list.get(i);
                    appendMemberOrTuple(buf, o);
                }
                buf.append("}");
                return buf.toString();
            }
        });

        define(new FunDefBase(
                "TupleToStr",
                "TupleToStr(<Tuple>)",
                "Constructs a string from a tuple.",
                "fSt") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                if (TypeUtil.couldBeMember(call.getArg(0).getType())) {
                    final MemberCalc memberCalc =
                            compiler.compileMember(call.getArg(0));
                    return new AbstractStringCalc(call, new Calc[] {memberCalc}) {
                        public String evaluateString(Evaluator evaluator) {
                            final Member member =
                                    memberCalc.evaluateMember(evaluator);
                            if (member.isNull()) {
                                return "";
                            }
                            StringBuffer buf = new StringBuffer();
                            appendMember(buf, member);
                            return buf.toString();
                        }
                    };
                } else {
                    final TupleCalc tupleCalc =
                            compiler.compileTuple(call.getArg(0));
                    return new AbstractStringCalc(call, new Calc[] {tupleCalc}) {
                        public String evaluateString(Evaluator evaluator) {
                            final Member[] members =
                                    tupleCalc.evaluateTuple(evaluator);
                            if (members == null) {
                                return "";
                            }
                            StringBuffer buf = new StringBuffer();
                            appendTuple(buf, members);
                            return buf.toString();
                        }
                    };
                }
            }

        });

        define(new FunDefBase(
                "UniqueName",
                "<Dimension>.UniqueName",
                "Returns the unique name of a dimension.",
                "pSd") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final DimensionCalc dimensionCalc =
                        compiler.compileDimension(call.getArg(0));
                return new AbstractStringCalc(call, new Calc[] {dimensionCalc}) {
                    public String evaluateString(Evaluator evaluator) {
                        final Dimension dimension =
                                dimensionCalc.evaluateDimension(evaluator);
                        return dimension.getUniqueName();
                    }
                };
            }
        });

        define(new FunDefBase(
                "UniqueName",
                "<Hierarchy>.UniqueName",
                "Returns the unique name of a hierarchy.",
                "pSh") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final HierarchyCalc hierarchyCalc =
                        compiler.compileHierarchy(call.getArg(0));
                return new AbstractStringCalc(call, new Calc[] {hierarchyCalc}) {
                    public String evaluateString(Evaluator evaluator) {
                        final Hierarchy hierarchy =
                                hierarchyCalc.evaluateHierarchy(evaluator);
                        return hierarchy.getUniqueName();
                    }
                };
            }
        });

        define(new FunDefBase(
                "UniqueName",
                "<Level>.UniqueName",
                "Returns the unique name of a level.",
                "pSl") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final LevelCalc levelCalc =
                        compiler.compileLevel(call.getArg(0));
                return new AbstractStringCalc(call, new Calc[] {levelCalc}) {
                    public String evaluateString(Evaluator evaluator) {
                        final Level level = levelCalc.evaluateLevel(evaluator);
                        return level.getUniqueName();
                    }
                };
            }
        });

        define(new FunDefBase(
                "UniqueName",
                "<Member>.UniqueName",
                "Returns the unique name of a member.",
                "pSm") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final MemberCalc memberCalc =
                        compiler.compileMember(call.getArg(0));
                return new AbstractStringCalc(call, new Calc[] {memberCalc}) {
                    public String evaluateString(Evaluator evaluator) {
                        final Member member =
                                memberCalc.evaluateMember(evaluator);
                        return member.getUniqueName();
                    }
                };
            }
        });

        //
        // TUPLE FUNCTIONS
        if (false) define(new FunDefBase(
                "Current",
                "<Set>.Current",
                "Returns the current tuple from a set during an iteration.",
                "ptx") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                throw new UnsupportedOperationException();
            }
        });

        // we do not support the <String expression> arguments
        if (false) define(new FunDefBase(
                "Item",
                "<Set>.Item(<String Expression>[, <String Expression>...] | <Index>)",
                "Returns a tuple from a set.",
                "mx*") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                throw new UnsupportedOperationException();
            }
        });

        define(SetItemFunDef.intResolver);
        define(SetItemFunDef.stringResolver);
        define(TupleItemFunDef.instance);
        define(StrToTupleFunDef.instance);

        // special resolver for "()"
        define(TupleFunDef.Resolver);

        //
        // GENERIC VALUE FUNCTIONS
        define(CoalesceEmptyFunDef.Resolver);
        define(CaseTestFunDef.Resolver);
        define(CaseMatchFunDef.Resolver);
        define(PropertiesFunDef.Resolver);

        //
        // PARAMETER FUNCTIONS
        define(new ParameterFunDef.ParameterResolver());
        define(new ParameterFunDef.ParamRefResolver());

        //
        // OPERATORS
        define(new FunDefBase(
                "+",
                "<Numeric Expression> + <Numeric Expression>",
                "Adds two numbers.",
                "innn") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final DoubleCalc calc0 = compiler.compileDouble(call.getArg(0));
                final DoubleCalc calc1 = compiler.compileDouble(call.getArg(1));
                return new AbstractDoubleCalc(call, new Calc[] {calc0, calc1}) {
                    public double evaluateDouble(Evaluator evaluator) {
                        final double v0 = calc0.evaluateDouble(evaluator);
                        final double v1 = calc1.evaluateDouble(evaluator);
                        if (v0 == DoubleNull) {
                            if (v1 == DoubleNull) {
                                return DoubleNull;
                            } else {
                                return v1;
                            }
                        } else {
                            if (v1 == DoubleNull) {
                                return v0;
                            } else {
                                return v0 + v1;
                            }
                        }
                    }
                };
            }

        });

        define(new FunDefBase(
                "-",
                "<Numeric Expression> - <Numeric Expression>",
                "Subtracts two numbers.",
                "innn") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final DoubleCalc calc0 = compiler.compileDouble(call.getArg(0));
                final DoubleCalc calc1 = compiler.compileDouble(call.getArg(1));
                return new AbstractDoubleCalc(call, new Calc[] {calc0, calc1}) {
                    public double evaluateDouble(Evaluator evaluator) {
                        final double v0 = calc0.evaluateDouble(evaluator);
                        final double v1 = calc1.evaluateDouble(evaluator);
                        if (v0 == DoubleNull) {
                            if (v1 == DoubleNull) {
                                return DoubleNull;
                            } else {
                                return - v1;
                            }
                        } else {
                            if (v1 == DoubleNull) {
                                return v0;
                            } else {
                                return v0 - v1;
                            }
                        }
                    }
                };
            }
        });

        define(new FunDefBase(
                "*",
                "<Numeric Expression> * <Numeric Expression>",
                "Multiplies two numbers.",
                "innn") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final DoubleCalc calc0 = compiler.compileDouble(call.getArg(0));
                final DoubleCalc calc1 = compiler.compileDouble(call.getArg(1));
                return new AbstractDoubleCalc(call, new Calc[] {calc0, calc1}) {
                    public double evaluateDouble(Evaluator evaluator) {
                        final double v0 = calc0.evaluateDouble(evaluator);
                        final double v1 = calc1.evaluateDouble(evaluator);
                        // Multiply and divide return null if EITHER arg is null.
                        if (v0 == DoubleNull || v1 == DoubleNull) {
                            return DoubleNull;
                        } else {
                            return v0 * v1;
                        }
                    }
                };
            }
        });

        define(new FunDefBase(
                "/",
                "<Numeric Expression> / <Numeric Expression>",
                "Divides two numbers.",
                "innn") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final DoubleCalc calc0 = compiler.compileDouble(call.getArg(0));
                final DoubleCalc calc1 = compiler.compileDouble(call.getArg(1));
                return new AbstractDoubleCalc(call, new Calc[] {calc0, calc1}) {
                    public double evaluateDouble(Evaluator evaluator) {
                        final double v0 = calc0.evaluateDouble(evaluator);
                        final double v1 = calc1.evaluateDouble(evaluator);
                        // Multiply and divide return null if EITHER arg is null.
                        if (v0 == DoubleNull || v1 == DoubleNull) {
                            return DoubleNull;
                        } else {
                            return v0 / v1;
                        }
                    }
                };
            }

            // todo: use this, via reflection
            public double evaluate(double d1, double d2) {
                return d1 / d2;
            }
        });

        define(new FunDefBase(
                "-",
                "- <Numeric Expression>",
                "Returns the negative of a number.",
                "Pnn") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final DoubleCalc calc = compiler.compileDouble(call.getArg(0));
                return new AbstractDoubleCalc(call, new Calc[] {calc}) {
                    public double evaluateDouble(Evaluator evaluator) {
                        final double v = calc.evaluateDouble(evaluator);
                        if (v == DoubleNull) {
                            return DoubleNull;
                        } else {
                            return - v;
                        }
                    }
                };
            }
        });

        define(new FunDefBase(
                "||",
                "<String Expression> || <String Expression>",
                "Concatenates two strings.",
                "iSSS") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final StringCalc calc0 = compiler.compileString(call.getArg(0));
                final StringCalc calc1 = compiler.compileString(call.getArg(1));
                return new AbstractStringCalc(call, new Calc[] {calc0, calc1}) {
                    public String evaluateString(Evaluator evaluator) {
                        final String s0 = calc0.evaluateString(evaluator);
                        final String s1 = calc1.evaluateString(evaluator);
                        return s0 + s1;
                    }
                };
            }

        });

        define(new FunDefBase(
                "AND",
                "<Logical Expression> AND <Logical Expression>",
                "Returns the conjunction of two conditions.",
                "ibbb") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final BooleanCalc calc0 = compiler.compileBoolean(call.getArg(0));
                final BooleanCalc calc1 = compiler.compileBoolean(call.getArg(1));
                return new AbstractBooleanCalc(call, new Calc[] {calc0, calc1}) {
                    public boolean evaluateBoolean(Evaluator evaluator) {
                        if (!calc0.evaluateBoolean(evaluator)) {
                            return false;
                        }
                        return calc1.evaluateBoolean(evaluator);
                    }
                };
            }
        });

        define(new FunDefBase(
                "OR",
                "<Logical Expression> OR <Logical Expression>",
                "Returns the disjunction of two conditions.",
                "ibbb") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final BooleanCalc calc0 = compiler.compileBoolean(call.getArg(0));
                final BooleanCalc calc1 = compiler.compileBoolean(call.getArg(1));
                return new AbstractBooleanCalc(call, new Calc[] {calc0, calc1}) {
                    public boolean evaluateBoolean(Evaluator evaluator) {
                        if (calc0.evaluateBoolean(evaluator)) {
                            return true;
                        }
                        return calc1.evaluateBoolean(evaluator);
                    }
                };
            }
        });

        define(new FunDefBase(
                "XOR",
                "<Logical Expression> XOR <Logical Expression>",
                "Returns whether two conditions are mutually exclusive.",
                "ibbb") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final BooleanCalc calc0 = compiler.compileBoolean(call.getArg(0));
                final BooleanCalc calc1 = compiler.compileBoolean(call.getArg(1));
                return new AbstractBooleanCalc(call, new Calc[] {calc0, calc1}) {
                    public boolean evaluateBoolean(Evaluator evaluator) {
                        final boolean b0 = calc0.evaluateBoolean(evaluator);
                        final boolean b1 = calc1.evaluateBoolean(evaluator);
                        return b0 != b1;
                    }
                };
            }
        });

        define(new FunDefBase(
                "NOT",
                "NOT <Logical Expression>",
                "Returns the negation of a condition.",
                "Pbb") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final BooleanCalc calc = compiler.compileBoolean(call.getArg(0));
                return new AbstractBooleanCalc(call, new Calc[] {calc}) {
                    public boolean evaluateBoolean(Evaluator evaluator) {
                        return !calc.evaluateBoolean(evaluator);
                    }
                };
            }
        });

        define(new FunDefBase(
                "=",
                "<String Expression> = <String Expression>",
                "Returns whether two expressions are equal.",
                "ibSS") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final StringCalc calc0 = compiler.compileString(call.getArg(0));
                final StringCalc calc1 = compiler.compileString(call.getArg(1));
                return new AbstractBooleanCalc(call, new Calc[] {calc0, calc1}) {
                    public boolean evaluateBoolean(Evaluator evaluator) {
                        final String b0 = calc0.evaluateString(evaluator);
                        final String b1 = calc1.evaluateString(evaluator);
                        if (b0 == null || b1 == null) {
                            return BooleanNull;
                        }
                        return b0.equals(b1);
                    }
                };
            }
        });

        define(new FunDefBase(
                "=",
                "<Numeric Expression> = <Numeric Expression>",
                "Returns whether two expressions are equal.",
                "ibnn") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final DoubleCalc calc0 = compiler.compileDouble(call.getArg(0));
                final DoubleCalc calc1 = compiler.compileDouble(call.getArg(1));
                return new AbstractBooleanCalc(call, new Calc[] {calc0, calc1}) {
                    public boolean evaluateBoolean(Evaluator evaluator) {
                        final double v0 = calc0.evaluateDouble(evaluator);
                        final double v1 = calc1.evaluateDouble(evaluator);
                        if (v0 == Double.NaN || v1 == Double.NaN || v0 == DoubleNull || v1 == DoubleNull) {
                            return BooleanNull;
                        }
                        return v0 == v1;
                    }
                };
            }
        });

        define(new FunDefBase(
                "<>",
                "<String Expression> <> <String Expression>",
                "Returns whether two expressions are not equal.",
                "ibSS") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final StringCalc calc0 = compiler.compileString(call.getArg(0));
                final StringCalc calc1 = compiler.compileString(call.getArg(1));
                return new AbstractBooleanCalc(call, new Calc[] {calc0, calc1}) {
                    public boolean evaluateBoolean(Evaluator evaluator) {
                        final String b0 = calc0.evaluateString(evaluator);
                        final String b1 = calc1.evaluateString(evaluator);
                        if (b0 == null || b1 == null) {
                            return BooleanNull;
                        }
                        return !b0.equals(b1);
                    }
                };
            }
        });

        define(new FunDefBase(
                "<>",
                "<Numeric Expression> <> <Numeric Expression>",
                "Returns whether two expressions are not equal.",
                "ibnn") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final DoubleCalc calc0 = compiler.compileDouble(call.getArg(0));
                final DoubleCalc calc1 = compiler.compileDouble(call.getArg(1));
                return new AbstractBooleanCalc(call, new Calc[] {calc0, calc1}) {
                    public boolean evaluateBoolean(Evaluator evaluator) {
                        final double v0 = calc0.evaluateDouble(evaluator);
                        final double v1 = calc1.evaluateDouble(evaluator);
                        if (v0 == Double.NaN || v1 == Double.NaN || v0 == DoubleNull || v1 == DoubleNull) {
                            return BooleanNull;
                        }
                        return v0 != v1;
                    }
                };
            }
        });

        define(new FunDefBase(
                "<",
                "<Numeric Expression> < <Numeric Expression>",
                "Returns whether an expression is less than another.",
                "ibnn") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final DoubleCalc calc0 = compiler.compileDouble(call.getArg(0));
                final DoubleCalc calc1 = compiler.compileDouble(call.getArg(1));
                return new AbstractBooleanCalc(call, new Calc[] {calc0, calc1}) {
                    public boolean evaluateBoolean(Evaluator evaluator) {
                        final double v0 = calc0.evaluateDouble(evaluator);
                        final double v1 = calc1.evaluateDouble(evaluator);
                        if (v0 == Double.NaN || v1 == Double.NaN || v0 == DoubleNull || v1 == DoubleNull) {
                            return BooleanNull;
                        }
                        return v0 < v1;
                    }
                };
            }
        });

        define(new FunDefBase(
                "<",
                "<String Expression> < <String Expression>",
                "Returns whether an expression is less than another.",
                "ibSS") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final StringCalc calc0 = compiler.compileString(call.getArg(0));
                final StringCalc calc1 = compiler.compileString(call.getArg(1));
                return new AbstractBooleanCalc(call, new Calc[] {calc0, calc1}) {
                    public boolean evaluateBoolean(Evaluator evaluator) {
                        final String b0 = calc0.evaluateString(evaluator);
                        final String b1 = calc1.evaluateString(evaluator);
                        if (b0 == null || b1 == null) {
                            return BooleanNull;
                        }
                        return b0.compareTo(b1) < 0;
                    }
                };
            }
        });

        define(new FunDefBase(
                "<=",
                "<Numeric Expression> <= <Numeric Expression>",
                "Returns whether an expression is less than or equal to another.",
                "ibnn") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final DoubleCalc calc0 = compiler.compileDouble(call.getArg(0));
                final DoubleCalc calc1 = compiler.compileDouble(call.getArg(1));
                return new AbstractBooleanCalc(call, new Calc[] {calc0, calc1}) {
                    public boolean evaluateBoolean(Evaluator evaluator) {
                        final double v0 = calc0.evaluateDouble(evaluator);
                        final double v1 = calc1.evaluateDouble(evaluator);
                        if (v0 == Double.NaN || v1 == Double.NaN || v0 == DoubleNull || v1 == DoubleNull) {
                            return BooleanNull;
                        }
                        return v0 <= v1;
                    }
                };
            }
        });

        define(new FunDefBase(
                "<=",
                "<String Expression> <= <String Expression>",
                "Returns whether an expression is less than or equal to another.",
                "ibSS") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final StringCalc calc0 = compiler.compileString(call.getArg(0));
                final StringCalc calc1 = compiler.compileString(call.getArg(1));
                return new AbstractBooleanCalc(call, new Calc[] {calc0, calc1}) {
                    public boolean evaluateBoolean(Evaluator evaluator) {
                        final String b0 = calc0.evaluateString(evaluator);
                        final String b1 = calc1.evaluateString(evaluator);
                        if (b0 == null || b1 == null) {
                            return BooleanNull;
                        }
                        return b0.compareTo(b1) <= 0;
                    }
                };
            }
        });

        define(new FunDefBase(
                ">",
                "<Numeric Expression> > <Numeric Expression>",
                "Returns whether an expression is greater than another.",
                "ibnn") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final DoubleCalc calc0 = compiler.compileDouble(call.getArg(0));
                final DoubleCalc calc1 = compiler.compileDouble(call.getArg(1));
                return new AbstractBooleanCalc(call, new Calc[] {calc0, calc1}) {
                    public boolean evaluateBoolean(Evaluator evaluator) {
                        final double v0 = calc0.evaluateDouble(evaluator);
                        final double v1 = calc1.evaluateDouble(evaluator);
                        if (v0 == Double.NaN || v1 == Double.NaN || v0 == DoubleNull || v1 == DoubleNull) {
                            return BooleanNull;
                        }
                        return v0 > v1;
                    }
                };
            }
        });

        define(new FunDefBase(
                ">",
                "<String Expression> > <String Expression>",
                "Returns whether an expression is greater than another.",
                "ibSS") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final StringCalc calc0 = compiler.compileString(call.getArg(0));
                final StringCalc calc1 = compiler.compileString(call.getArg(1));
                return new AbstractBooleanCalc(call, new Calc[] {calc0, calc1}) {
                    public boolean evaluateBoolean(Evaluator evaluator) {
                        final String b0 = calc0.evaluateString(evaluator);
                        final String b1 = calc1.evaluateString(evaluator);
                        if (b0 == null || b1 == null) {
                            return BooleanNull;
                        }
                        return b0.compareTo(b1) > 0;
                    }
                };
            }
        });

        define(new FunDefBase(
                ">=",
                "<Numeric Expression> >= <Numeric Expression>",
                "Returns whether an expression is greater than or equal to another.",
                "ibnn") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final DoubleCalc calc0 = compiler.compileDouble(call.getArg(0));
                final DoubleCalc calc1 = compiler.compileDouble(call.getArg(1));
                return new AbstractBooleanCalc(call, new Calc[] {calc0, calc1}) {
                    public boolean evaluateBoolean(Evaluator evaluator) {
                        final double v0 = calc0.evaluateDouble(evaluator);
                        final double v1 = calc1.evaluateDouble(evaluator);
                        if (v0 == Double.NaN || v1 == Double.NaN || v0 == DoubleNull || v1 == DoubleNull) {
                            return BooleanNull;
                        }
                        return v0 >= v1;
                    }
                };
            }
        });

        define(new FunDefBase(
                ">=",
                "<String Expression> >= <String Expression>",
                "Returns whether an expression is greater than or equal to another.",
                "ibSS") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final StringCalc calc0 = compiler.compileString(call.getArg(0));
                final StringCalc calc1 = compiler.compileString(call.getArg(1));
                return new AbstractBooleanCalc(call, new Calc[] {calc0, calc1}) {
                    public boolean evaluateBoolean(Evaluator evaluator) {
                        final String b0 = calc0.evaluateString(evaluator);
                        final String b1 = calc1.evaluateString(evaluator);
                        if (b0 == null || b1 == null) {
                            return BooleanNull;
                        }
                        return b0.compareTo(b1) >= 0;
                    }
                };
            }
        });

        // NON-STANDARD FUNCTIONS

        define(NthQuartileFunDef.FirstQResolver);

        define(NthQuartileFunDef.ThirdQResolver);
    }


    static void appendMemberOrTuple(
            StringBuffer buf,
            Object memberOrTuple) {
        if (memberOrTuple instanceof Member) {
            Member member = (Member) memberOrTuple;
            appendMember(buf, member);
        } else {
            Member[] members = (Member[]) memberOrTuple;
            appendTuple(buf, members);
        }
    }

    private static void appendMember(StringBuffer buf, Member member) {
        buf.append(member.getUniqueName());
    }

    private static void appendTuple(StringBuffer buf, Member[] members) {
        buf.append("(");
        for (int j = 0; j < members.length; j++) {
            if (j > 0) {
                buf.append(", ");
            }
            Member member = members[j];
            appendMember(buf, member);
        }
        buf.append(")");
    }

    /**
     * Returns a read-only version of the name-to-resolvers map. Used by the
     * testing framework.
     */
    protected static Map getNameToResolversMap() {
        return Collections.unmodifiableMap(((BuiltinFunTable)instance()).mapNameToResolvers);
    }

    /** Returns (creating if necessary) the singleton. */
    public static BuiltinFunTable instance() {
        if (instance == null) {
            instance = new BuiltinFunTable();
            instance.init();
        }
        return instance;
    }

    protected Member[] getNonEmptyMemberChildren(Evaluator evaluator, Member member) {
        SchemaReader sr = evaluator.getSchemaReader();
        if (evaluator.isNonEmpty()) {
            return sr.getMemberChildren(member, evaluator);
        }
        return sr.getMemberChildren(member);
    }

    /**
     * Returns members of a level which are not empty (according to the
     * criteria expressed by the evaluator). Calculated members are included.
     */
    protected static Member[] getNonEmptyLevelMembers(
            Evaluator evaluator,
            Level level) {
        SchemaReader sr = evaluator.getSchemaReader();
        if (evaluator.isNonEmpty()) {
            final Member[] members = sr.getLevelMembers(level, evaluator);
            return Util.addLevelCalculatedMembers(sr, level, members);
        }
        return sr.getLevelMembers(level, true);
    }

    static List levelMembers(
            Level level,
            Evaluator evaluator,
            final boolean includeCalcMembers) {
        Member[] members = getNonEmptyLevelMembers(evaluator, level);
        ArrayList memberList = new ArrayList(Arrays.asList(members));
        if (!includeCalcMembers) {
            FunUtil.removeCalculatedMembers(memberList);
        }
        FunUtil.hierarchize(memberList, false);
        return memberList;
    }

    static List hierarchyMembers(
            Hierarchy hierarchy,
            Evaluator evaluator,
            final boolean includeCalcMembers) {
        List memberList = FunUtil.addMembers(evaluator.getSchemaReader(),
                new ArrayList(), hierarchy);
        if (!includeCalcMembers && memberList != null) {
            FunUtil.removeCalculatedMembers(memberList);
        }
        FunUtil.hierarchize(memberList, false);
        return memberList;
    }

    static List dimensionMembers(
            Dimension dimension,
            Evaluator evaluator,
            final boolean includeCalcMembers) {
        Hierarchy hierarchy = dimension.getHierarchy();
        List memberList = FunUtil.addMembers(evaluator.getSchemaReader(),
                new ArrayList(), hierarchy);
        if (!includeCalcMembers && memberList != null) {
            FunUtil.removeCalculatedMembers(memberList);
        }
        if (!dimension.isMeasures()) {
            // leave measures in their natural order (calculated members last)
            FunUtil.hierarchize(memberList, false);
        }
        return memberList;
    }
}

// End BuiltinFunTable.java
