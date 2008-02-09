/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2002-2002 Kana Software, Inc.
// Copyright (C) 2002-2008 Julian Hyde and others
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
import mondrian.olap.Member;
import mondrian.olap.fun.extra.NthQuartileFunDef;
import mondrian.olap.fun.extra.CalculatedChildFunDef;
import mondrian.olap.fun.vba.Vba;
import mondrian.olap.fun.vba.Excel;
import mondrian.olap.type.DimensionType;
import mondrian.olap.type.LevelType;
import mondrian.olap.type.Type;
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

        // Empty expression
        define(
            new FunDefBase(
                "",
                "",
                "Dummy function representing the empty expression",
                Syntax.Empty,
                Category.Empty,
                new int[0]) {});

        // first char: p=Property, m=Method, i=Infix, P=Prefix
        // 2nd:

        // ARRAY FUNCTIONS

        // "SetToArray(<Set>[, <Set>]...[, <Numeric Expression>])"
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

        // "<Dimension>.Dimension"
        define(new FunDefBase(
                "Dimension",
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

        // "<Level>.Dimension"
        define(new FunDefBase(
                "Dimension",
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

        // "<Member>.Dimension"
        define(new FunDefBase(
                "Dimension",
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

        // "Dimensions(<Numeric Expression>)"
        define(new FunDefBase(
                "Dimensions",
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

        // "Dimensions(<String Expression>)"
        define(new FunDefBase(
                "Dimensions",
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
                        parseIdentifier(s),
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

        // "<Hierarchy>.Levels(<Numeric Expression>)"
        define(new FunDefBase(
                "Levels",
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

        // "<Hierarchy>.Levels(<String Expression>)"
        define(new FunDefBase(
                "Levels",
                "Returns the level whose name is specified by a string expression.",
                "mlhS") {
            public Type getResultType(Validator validator, Exp[] args) {
                final Type argType = args[0].getType();
                return LevelType.forType(argType);
            }

            public Calc compileCall(
                final ResolvedFunCall call, ExpCompiler compiler)
            {
                final HierarchyCalc hierarchyCalc =
                    compiler.compileHierarchy(call.getArg(0));
                final StringCalc nameCalc =
                    compiler.compileString(call.getArg(1));
                return new AbstractLevelCalc(
                    call, new Calc[] {hierarchyCalc, nameCalc}) {
                    public Level evaluateLevel(Evaluator evaluator) {
                        Hierarchy hierarchy =
                            hierarchyCalc.evaluateHierarchy(evaluator);
                        String name = nameCalc.evaluateString(evaluator);
                        for (Level level : hierarchy.getLevels()) {
                            if (level.getName().equals(name)) {
                                return level;
                            }
                        }
                        throw newEvalException(
                            call.getFunDef(),
                            "Level '" + name + "' not found in hierarchy '"
                                + hierarchy + "'");
                    }
                };
            }
        });

        // "Levels(<String Expression>)"
        define(new FunDefBase(
                "Levels",
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
                                parseIdentifier(s),
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
        define(IsEmptyFunDef.FunctionResolver);
        define(IsEmptyFunDef.PostfixResolver);
        define(IsNullFunDef.Resolver);
        define(IsFunDef.Resolver);

        //
        // MEMBER FUNCTIONS
        define(AncestorFunDef.Resolver);

        define(new FunDefBase(
                "Cousin",
                "<Member> Cousin(<Member>, <Ancestor Member>)",
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

        // "<Member>.DataMember"
        define(new FunDefBase(
                "DataMember",
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

        // "<Dimension>.DefaultMember"
        define(new FunDefBase(
                "DefaultMember",
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

        // "<Hierarchy>.DefaultMember"
        define(new FunDefBase(
                "DefaultMember",
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

        // "<Member>.FirstChild"
        define(new FunDefBase(
                "FirstChild",
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

        // <Member>.FirstSibling
        define(new FunDefBase(
                "FirstSibling",
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
                return children[0];
            }
        });

        define(LeadLagFunDef.LagResolver);

        // <Member>.LastChild
        define(new FunDefBase(
                "LastChild",
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

        // <Member>.LastSibling
        define(new FunDefBase(
                "LastSibling",
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

        // Members(<String Expression>)
        define(new FunDefBase(
                "Members",
                "Returns the member whose name is specified by a string expression.",
                "fmS") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                throw new UnsupportedOperationException();
            }
        });

        // <Member>.NextMember
        define(new FunDefBase(
                "NextMember",
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

        // <Member>.Parent
        define(new FunDefBase(
                "Parent",
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

        // <Member>.PrevMember
        define(new FunDefBase(
                "PrevMember",
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

        // StrToMember(<String Expression>)
        define(new FunDefBase(
                "StrToMember",
                "Returns a member from a unique name String in MDX format.",
                "fmS") {
            public Calc compileCall(ResolvedFunCall call,ExpCompiler compiler) {
                final StringCalc memberNameCalc =
                        compiler.compileString(call.getArg(0));
                return new AbstractMemberCalc(call,new Calc[]{memberNameCalc}) {
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
                List<Id.Segment> uniqueNameParts =
                    Util.parseIdentifier(memberName);
                return (Member) schemaReader.lookupCompound(
                    cube, uniqueNameParts, true, Category.Member);
            }
        });

        define(ValidMeasureFunDef.instance);

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
                        return new GenericCalc(call) {
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

        // <Set>.Count
        define(new FunDefBase(
                "Count",
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

        define(IifFunDef.STRING_INSTANCE);
        define(IifFunDef.NUMERIC_INSTANCE);
        define(IifFunDef.BOOLEAN_INSTANCE);
        define(IifFunDef.MEMBER_INSTANCE);
        define(IifFunDef.LEVEL_INSTANCE);
        define(IifFunDef.HIERARCHY_INSTANCE);
        define(IifFunDef.DIMENSION_INSTANCE);
        define(IifFunDef.SET_INSTANCE);

        // InStr(<String Expression>, <String Expression>)
        define(new FunDefBase(
                "InStr",
                "Returns the position of the first occurrence of one string within another." +
                    " Implements very basic form of InStr",
                "fnSS") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final StringCalc stringCalc1 = compiler.compileString(call.getArg(0));
                final StringCalc stringCalc2 = compiler.compileString(call.getArg(1));
                return new AbstractIntegerCalc(call, new Calc[] {stringCalc1, stringCalc2}) {
                    public int evaluateInteger(Evaluator evaluator) {
                        String value = stringCalc1.evaluateString(evaluator);
                        String pattern = stringCalc2.evaluateString(evaluator);
                        return value.indexOf(pattern) + 1;
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
        define(MinMaxFunDef.MinResolver);

        define(MedianFunDef.Resolver);
        define(PercentileFunDef.Resolver);

        // <Level>.Ordinal
        define(new FunDefBase(
                "Ordinal",
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

        // <Measure>.Value
        define(new FunDefBase(
                "Value",
                "Returns the value of a measure.",
                "pnm") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final MemberCalc memberCalc =
                        compiler.compileMember(call.getArg(0));
                return new GenericCalc(call) {
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

        define(AddCalculatedMembersFunDef.resolver);

        // Ascendants(<Member>)
        define(new FunDefBase(
                "Ascendants",
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

            List<Member> ascendants(Member member) {
                if (member.isNull()) {
                    return Collections.emptyList();
                }
                Member[] members = member.getAncestorMembers();
                final List<Member> result =
                    new ArrayList<Member>(members.length + 1);
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

        // <Member>.Children
        define(new FunDefBase(
                "Children",
                "Returns the children of a member.",
                "pxm") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final MemberCalc memberCalc =
                        compiler.compileMember(call.getArg(0));
                return new AbstractListCalc(call, new Calc[] {memberCalc}, false) {
                    public List evaluateList(Evaluator evaluator) {
                        // Return the list of children. The list is immutable,
                        // hence 'false' above.
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
        define(DistinctFunDef.instance);
        define(DrilldownLevelFunDef.Resolver);

        define(DrilldownLevelTopBottomFunDef.DrilldownLevelTopResolver);
        define(DrilldownLevelTopBottomFunDef.DrilldownLevelBottomResolver);
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
        define(ExtractFunDef.Resolver);
        define(FilterFunDef.instance);

        define(GenerateFunDef.ListResolver);
        define(GenerateFunDef.StringResolver);
        define(HeadTailFunDef.HeadResolver);

        define(HierarchizeFunDef.Resolver);

        define(IntersectFunDef.resolver);
        define(LastPeriodsFunDef.Resolver);

        // <Dimension>.Members
        define(new FunDefBase(
                "Members",
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

        // <Dimension>.AllMembers
        define(new FunDefBase(
                "AllMembers",
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

        // <Hierarchy>.Members
        define(new FunDefBase(
                "Members",
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

        // <Hierarchy>.AllMembers
        define(new FunDefBase(
                "AllMembers",
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

        // <Level>.Members
        define(new FunDefBase(
                "Members",
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

        // <Level>.AllMembers
        define(new FunDefBase(
                "AllMembers",
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

        // StripCalculatedMembers(<Set>)
        define(new FunDefBase(
                "StripCalculatedMembers",
                "Removes calculated members from a set.",
                "fxx") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final ListCalc listCalc =
                        compiler.compileList(call.getArg(0));
                return new AbstractListCalc(call, new Calc[] {listCalc}) {
                    public List evaluateList(Evaluator evaluator) {
                        final List<Member> list = listCalc.evaluateList(evaluator);
                        removeCalculatedMembers(list);
                        return list;
                    }
                };
            }

        });

        // <Member>.Siblings
        define(new FunDefBase(
                "Siblings",
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
                Member[] siblings;
                if (parent == null) {
                    siblings =
                        schemaReader.getHierarchyRootMembers(
                            member.getHierarchy());
                } else {
                    siblings = schemaReader.getMemberChildren(parent);
                }
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

        // <Dimension>.Caption
        define(new FunDefBase(
                "Caption",
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

        // <Hierarchy>.Caption
        define(new FunDefBase(
                "Caption",
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

        // <Level>.Caption
        define(new FunDefBase(
                "Caption",
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

        // <Member>.Caption
        define(new FunDefBase(
                "Caption",
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

        // <Dimension>.Name
        define(new FunDefBase(
                "Name",
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

        // <Hierarchy>.Name
        define(new FunDefBase(
                "Name",
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

        // <Level>.Name
        define(new FunDefBase(
                "Name",
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

        // <Member>.Name
        define(new FunDefBase(
                "Name",
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

        define(SetToStrFunDef.instance);

        define(TupleToStrFunDef.instance);

        // <Dimension>.UniqueName
        define(new FunDefBase(
                "UniqueName",
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

        // <Hierarchy>.UniqueName
        define(new FunDefBase(
                "UniqueName",
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

        // <Level>.UniqueName
        define(new FunDefBase(
                "UniqueName",
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

        // <Member>.UniqueName
        define(new FunDefBase(
                "UniqueName",
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

        // <Set>.Current
        if (false) define(new FunDefBase(
                "Current",
                "Returns the current tuple from a set during an iteration.",
                "ptx") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                throw new UnsupportedOperationException();
            }
        });

        define(SetItemFunDef.intResolver);
        define(SetItemFunDef.stringResolver);
        define(TupleItemFunDef.instance);
        define(StrToTupleFunDef.Resolver);

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

        // <Numeric Expression> + <Numeric Expression>
        define(new FunDefBase(
                "+",
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

        // <Numeric Expression> - <Numeric Expression>
        define(new FunDefBase(
                "-",
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

        // <Numeric Expression> * <Numeric Expression>
        define(new FunDefBase(
                "*",
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

        // <Numeric Expression> / <Numeric Expression>
        define(new FunDefBase(
                "/",
                "Divides two numbers.",
                "innn") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final DoubleCalc calc0 = compiler.compileDouble(call.getArg(0));
                final DoubleCalc calc1 = compiler.compileDouble(call.getArg(1));
                final boolean isNullDenominatorProducesNull =
                    MondrianProperties.instance().
                    NullDenominatorProducesNull.get();

                // If the mondrian property
                //   mondrian.olap.NullOrZeroDenominatorProducesNull 
                // is false(default), Null in denominator with numeric numerator
                // returns infinity. This is consistent with MSAS.
                //
                // If this property is true, Null or zero in denominator returns
                // Null. This is only used by certain applications and does not
                // conform to MSAS behavior.
                if (isNullDenominatorProducesNull != true) {
                    return new AbstractDoubleCalc(call, new Calc[] {calc0, calc1}) {
                        public double evaluateDouble(Evaluator evaluator) {
                            final double v0 = calc0.evaluateDouble(evaluator);
                            final double v1 = calc1.evaluateDouble(evaluator);
                            // Null in numerator always returns DoubleNull.
                            //
                            if (v0 == DoubleNull) {
                                return DoubleNull;
                            } else if (v1 == DoubleNull) {
                                // Null only in denominator returns Infinity.
                                return Double.POSITIVE_INFINITY;
                            } else {
                                return v0 / v1;
                            }
                        }
                    };
                } else {
                    return new AbstractDoubleCalc(call, new Calc[] {calc0, calc1}) {
                        public double evaluateDouble(Evaluator evaluator) {
                            final double v0 = calc0.evaluateDouble(evaluator);
                            final double v1 = calc1.evaluateDouble(evaluator);
                            // Null in numerator or denominator returns DoubleNull.
                            //
                            if (v0 == DoubleNull || v1 == DoubleNull) {
                                return DoubleNull;
                            } else {
                                return v0 / v1;
                            }
                        }
                    };
                }
            }
        });

        // - <Numeric Expression>
        define(new FunDefBase(
                "-",
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

        // <String Expression> || <String Expression>
        define(new FunDefBase(
                "||",
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

        // <Logical Expression> AND <Logical Expression>
        define(new FunDefBase(
                "AND",
                "Returns the conjunction of two conditions.",
                "ibbb") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final BooleanCalc calc0 = compiler.compileBoolean(call.getArg(0));
                final BooleanCalc calc1 = compiler.compileBoolean(call.getArg(1));
                return new AbstractBooleanCalc(call, new Calc[] {calc0, calc1}) {
                    public boolean evaluateBoolean(Evaluator evaluator) {
                        boolean b0 = calc0.evaluateBoolean(evaluator);
                        // don't short-circuit evaluation if we're evaluating
                        // the axes; that way, we can combine all measures
                        // referenced in the AND expression in a single query
                        if (!evaluator.isEvalAxes() && !b0) {
                            return false;
                        }
                        boolean b1 = calc1.evaluateBoolean(evaluator);
                        return b0 && b1;
                    }
                };
            }
        });

        // <Logical Expression> OR <Logical Expression>
        define(new FunDefBase(
                "OR",
                "Returns the disjunction of two conditions.",
                "ibbb") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final BooleanCalc calc0 = compiler.compileBoolean(call.getArg(0));
                final BooleanCalc calc1 = compiler.compileBoolean(call.getArg(1));
                return new AbstractBooleanCalc(call, new Calc[] {calc0, calc1}) {
                    public boolean evaluateBoolean(Evaluator evaluator) {
                        boolean b0 = calc0.evaluateBoolean(evaluator);
                        // don't short-circuit evaluation if we're evaluating
                        // the axes; that way, we can combine all measures
                        // referenced in the OR expression in a single query
                        if (!evaluator.isEvalAxes() && b0) {
                            return true;
                        }
                        boolean b1 = calc1.evaluateBoolean(evaluator);
                        return b0 || b1;
                    }
                };
            }
        });

        // <Logical Expression> XOR <Logical Expression>
        define(new FunDefBase(
                "XOR",
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

        // NOT <Logical Expression>
        define(new FunDefBase(
                "NOT",
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

        // <String Expression> = <String Expression>
        define(new FunDefBase(
                "=",
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

        // <Numeric Expression> = <Numeric Expression>
        define(new FunDefBase(
                "=",
                "Returns whether two expressions are equal.",
                "ibnn") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final DoubleCalc calc0 = compiler.compileDouble(call.getArg(0));
                final DoubleCalc calc1 = compiler.compileDouble(call.getArg(1));
                return new AbstractBooleanCalc(call, new Calc[] {calc0, calc1}) {
                    public boolean evaluateBoolean(Evaluator evaluator) {
                        final double v0 = calc0.evaluateDouble(evaluator);
                        final double v1 = calc1.evaluateDouble(evaluator);
                        if (Double.isNaN(v0) || Double.isNaN(v1) || v0 == DoubleNull || v1 == DoubleNull) {
                            return BooleanNull;
                        }
                        return v0 == v1;
                    }
                };
            }
        });

        // <String Expression> <> <String Expression>
        define(new FunDefBase(
                "<>",
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

        // <Numeric Expression> <> <Numeric Expression>
        define(new FunDefBase(
                "<>",
                "Returns whether two expressions are not equal.",
                "ibnn") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final DoubleCalc calc0 = compiler.compileDouble(call.getArg(0));
                final DoubleCalc calc1 = compiler.compileDouble(call.getArg(1));
                return new AbstractBooleanCalc(call, new Calc[] {calc0, calc1}) {
                    public boolean evaluateBoolean(Evaluator evaluator) {
                        final double v0 = calc0.evaluateDouble(evaluator);
                        final double v1 = calc1.evaluateDouble(evaluator);
                        if (Double.isNaN(v0) || Double.isNaN(v1) || v0 == DoubleNull || v1 == DoubleNull) {
                            return BooleanNull;
                        }
                        return v0 != v1;
                    }
                };
            }
        });

        // <Numeric Expression> < <Numeric Expression>
        define(new FunDefBase(
                "<",
                "Returns whether an expression is less than another.",
                "ibnn") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final DoubleCalc calc0 = compiler.compileDouble(call.getArg(0));
                final DoubleCalc calc1 = compiler.compileDouble(call.getArg(1));
                return new AbstractBooleanCalc(call, new Calc[] {calc0, calc1}) {
                    public boolean evaluateBoolean(Evaluator evaluator) {
                        final double v0 = calc0.evaluateDouble(evaluator);
                        final double v1 = calc1.evaluateDouble(evaluator);
                        if (Double.isNaN(v0) || Double.isNaN(v1) || v0 == DoubleNull || v1 == DoubleNull) {
                            return BooleanNull;
                        }
                        return v0 < v1;
                    }
                };
            }
        });

        // <String Expression> < <String Expression>
        define(new FunDefBase(
                "<",
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

        // <Numeric Expression> <= <Numeric Expression>
        define(new FunDefBase(
                "<=",
                "Returns whether an expression is less than or equal to another.",
                "ibnn") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final DoubleCalc calc0 = compiler.compileDouble(call.getArg(0));
                final DoubleCalc calc1 = compiler.compileDouble(call.getArg(1));
                return new AbstractBooleanCalc(call, new Calc[] {calc0, calc1}) {
                    public boolean evaluateBoolean(Evaluator evaluator) {
                        final double v0 = calc0.evaluateDouble(evaluator);
                        final double v1 = calc1.evaluateDouble(evaluator);
                        if (Double.isNaN(v0) || Double.isNaN(v1) || v0 == DoubleNull || v1 == DoubleNull) {
                            return BooleanNull;
                        }
                        return v0 <= v1;
                    }
                };
            }
        });

        // <String Expression> <= <String Expression>
        define(new FunDefBase(
                "<=",
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

        // <Numeric Expression> > <Numeric Expression>
        define(new FunDefBase(
                ">",
                "Returns whether an expression is greater than another.",
                "ibnn") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final DoubleCalc calc0 = compiler.compileDouble(call.getArg(0));
                final DoubleCalc calc1 = compiler.compileDouble(call.getArg(1));
                return new AbstractBooleanCalc(call, new Calc[] {calc0, calc1}) {
                    public boolean evaluateBoolean(Evaluator evaluator) {
                        final double v0 = calc0.evaluateDouble(evaluator);
                        final double v1 = calc1.evaluateDouble(evaluator);
                        if (Double.isNaN(v0) || Double.isNaN(v1) || v0 == DoubleNull || v1 == DoubleNull) {
                            return BooleanNull;
                        }
                        return v0 > v1;
                    }
                };
            }
        });

        // <String Expression> > <String Expression>
        define(new FunDefBase(
                ">",
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

        // <Numeric Expression> >= <Numeric Expression>
        define(new FunDefBase(
                ">=",
                "Returns whether an expression is greater than or equal to another.",
                "ibnn") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final DoubleCalc calc0 = compiler.compileDouble(call.getArg(0));
                final DoubleCalc calc1 = compiler.compileDouble(call.getArg(1));
                return new AbstractBooleanCalc(call, new Calc[] {calc0, calc1}) {
                    public boolean evaluateBoolean(Evaluator evaluator) {
                        final double v0 = calc0.evaluateDouble(evaluator);
                        final double v1 = calc1.evaluateDouble(evaluator);
                        if (Double.isNaN(v0) || Double.isNaN(v1) || v0 == DoubleNull || v1 == DoubleNull) {
                            return BooleanNull;
                        }
                        return v0 >= v1;
                    }
                };
            }
        });

        // <String Expression> >= <String Expression>
        define(new FunDefBase(
                ">=",
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

        define(CalculatedChildFunDef.instance);

        define(CastFunDef.Resolver);

        // UCase(<String Expression>)
        define(new FunDefBase(
                "UCase",
                "Returns a string that has been converted to uppercase",
                "fSS") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final Locale locale = compiler.getEvaluator().getConnectionLocale();
                final StringCalc stringCalc = compiler.compileString(call.getArg(0));
                return new AbstractStringCalc(call, new Calc[]{stringCalc}) {
                    public String evaluateString(Evaluator evaluator) {
                        String value = stringCalc.evaluateString(evaluator);
                        return value.toUpperCase(locale);
                    }
                };
            }

        });

        // Len(<String Expression>)
        define(new FunDefBase(
                "Len",
                "Returns the number of characters in a string",
                "fnS") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final StringCalc stringCalc = compiler.compileString(call.getArg(0));
                return new AbstractIntegerCalc(call, new Calc[] {stringCalc}) {
                    public int evaluateInteger(Evaluator evaluator) {
                        String value = stringCalc.evaluateString(evaluator);
                        return value.length();
                    }
                };
            }

        });

        // Define VBA functions.
        for (FunDef funDef : JavaFunDef.scan(Vba.class)) {
            define(funDef);
        }

        // Define Excel functions.
        for (FunDef funDef : JavaFunDef.scan(Excel.class)) {
            define(funDef);
        }
    }

    /**
     * Returns the singleton, creating if necessary.
     *
     * @return the singleton
     */
    public static BuiltinFunTable instance() {
        if (instance == null) {
            instance = new BuiltinFunTable();
            instance.init();
        }
        return instance;
    }
}

// End BuiltinFunTable.java
