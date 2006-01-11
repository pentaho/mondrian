/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2002-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 26 February, 2002
*/
package mondrian.olap.fun;

import mondrian.calc.*;
import mondrian.calc.impl.*;
import mondrian.mdx.*;
import mondrian.olap.*;
import mondrian.olap.type.*;
import mondrian.olap.type.LevelType;
import mondrian.olap.type.DimensionType;
import mondrian.resource.MondrianResource;
import mondrian.util.Format;

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

    /** the singleton **/
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
        define(new HierarchyDimensionFunDef());

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
                if ((n > dimensions.length) || (n < 1)) {
                    throw newEvalException(
                            this, "Index '" + n + "' out of bounds");
                }
                return dimensions[n - 1];
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
        define(new LevelHierarchyFunDef());
        define(new MemberHierarchyFunDef());

        //
        // LEVEL FUNCTIONS
        define(new MemberLevelFunDef());

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

                if ((n >= levels.length) || (n < 0)) {
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
        define(new MultiResolver(
                "IsEmpty",
                "IsEmpty(<Value Expression>)",
                "Determines if an expression evaluates to the empty cell value.",
                new String[] {"fbS", "fbn"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new FunDefBase(dummyFunDef) {
                    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                        final Calc calc = compiler.compileScalar(call.getArg(0), true);
                        return new AbstractBooleanCalc(call, new Calc[] {calc}) {
                            public boolean evaluateBoolean(Evaluator evaluator) {
                                Object o = calc.evaluate(evaluator);
                                return o == null;
                            }
                        };
                    }

                };
            }
        });

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

        define(new MultiResolver(
                "IS",
                "<Expression> IS <Expression>",
                "Returns whether two objects are the same (idempotent)",
                new String[] {"ibmm", "ibll", "ibhh", "ibdd"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new FunDefBase(dummyFunDef) {
                    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                        final Calc calc0 = compiler.compile(call.getArg(0));
                        final Calc calc1 = compiler.compile(call.getArg(1));
                        return new AbstractBooleanCalc(call, new Calc[] {calc0, calc1}) {
                            public boolean evaluateBoolean(Evaluator evaluator) {
                                Object o0 = calc0.evaluate(evaluator);
                                Object o1 = calc1.evaluate(evaluator);
                                return o0 == o1;
                            }
                        };
                    }

                };
            }
        });

        //
        // MEMBER FUNCTIONS
        define(new MultiResolver(
                "Ancestor",
                "Ancestor(<Member>, {<Level>|<Numeric Expression>})",
                "Returns the ancestor of a member at a specified level.",
                new String[] {"fmml", "fmmn"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new FunDefBase(dummyFunDef) {
                    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                        final MemberCalc memberCalc =
                                compiler.compileMember(call.getArg(0));
                        final Type type1 = call.getArg(1).getType();
                        if (type1 instanceof mondrian.olap.type.LevelType) {
                            final LevelCalc levelCalc =
                                    compiler.compileLevel(call.getArg(1));
                            return new AbstractMemberCalc(call, new Calc[] {levelCalc}) {
                                public Member evaluateMember(Evaluator evaluator) {
                                    Level level = levelCalc.evaluateLevel(evaluator);
                                    Member member = memberCalc.evaluateMember(evaluator);
                                    int distance = member.getLevel().getDepth() - level.getDepth();
                                    return ancestor(evaluator, member, distance, level);
                                }
                            };
                        } else {
                            final IntegerCalc distanceCalc =
                                    compiler.compileInteger(call.getArg(1));
                            return new AbstractMemberCalc(call, new Calc[] {distanceCalc}) {
                                public Member evaluateMember(Evaluator evaluator) {
                                    int distance = distanceCalc.evaluateInteger(evaluator);
                                    Member member = memberCalc.evaluateMember(evaluator);
                                    return ancestor(evaluator, member, distance, null);
                                }
                            };
                        }
                    }

                };
            }
        });

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

        define(new DimensionCurrentMemberFunDef());

        define(new HierarchyCurrentMemberFunDef());

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

        define(new MultiResolver(
                "Lag",
                "<Member>.Lag(<Numeric Expression>)",
                "Returns a member further along the specified member's dimension.",
                new String[]{"mmmn"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new FunDefBase(dummyFunDef) {
                    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                        final MemberCalc memberCalc =
                                compiler.compileMember(call.getArg(0));
                        final IntegerCalc integerCalc =
                                compiler.compileInteger(call.getArg(1));
                        return new AbstractMemberCalc(call, new Calc[] {memberCalc, integerCalc}) {
                            public Member evaluateMember(Evaluator evaluator) {
                                Member member = memberCalc.evaluateMember(evaluator);
                                int n = integerCalc.evaluateInteger(evaluator);
                                return evaluator.getSchemaReader().getLeadMember(member, -n);
                            }
                        };
                    }

                };
            }
        });

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

        define(new MultiResolver(
                "Lead",
                "<Member>.Lead(<Numeric Expression>)",
                "Returns a member further along the specified member's dimension.",
                new String[]{"mmmn"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new FunDefBase(dummyFunDef) {
                    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                        final MemberCalc memberCalc =
                                compiler.compileMember(call.getArg(0));
                        final IntegerCalc integerCalc =
                                compiler.compileInteger(call.getArg(1));
                        return new AbstractMemberCalc(call, new Calc[] {memberCalc, integerCalc}) {
                            public Member evaluateMember(Evaluator evaluator) {
                                Member member = memberCalc.evaluateMember(evaluator);
                                int n = integerCalc.evaluateInteger(evaluator);
                                return evaluator.getSchemaReader().getLeadMember(member, n);
                            }
                        };
                    }
                };
            }});

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

        define(OpeningClosingPeriodFunDef.createResolver(true));
        define(OpeningClosingPeriodFunDef.createResolver(false));

        define(new MultiResolver(
                "ParallelPeriod",
                "ParallelPeriod([<Level>[, <Numeric Expression>[, <Member>]]])",
                "Returns a member from a prior period in the same relative position as a specified member.",
                new String[] {"fm", "fml", "fmln", "fmlnm"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new FunDefBase(dummyFunDef) {
                    public Type getResultType(Validator validator, Exp[] args) {
                        if (args.length == 0) {
                            // With no args, the default implementation cannot
                            // guess the hierarchy, so we supply the Time
                            // dimension.
                            Hierarchy hierarchy = validator.getQuery()
                                    .getCube().getTimeDimension()
                                    .getHierarchy();
                            return MemberType.forHierarchy(hierarchy);
                        }
                        return super.getResultType(validator, args);
                    }

                    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                        // Member defaults to [Time].currentmember
                        Exp[] args = call.getArgs();
                        final MemberCalc memberCalc;
                        switch (args.length) {
                        case 3:
                            memberCalc = compiler.compileMember(args[2]);
                            break;
                        case 1:
                            final Dimension dimension =
                                    args[0].getType().getHierarchy().getDimension();
                            memberCalc = new DimensionCurrentMemberCalc(dimension);
                            break;
                        default:
                            final Dimension timeDimension =
                                    compiler.getEvaluator().getCube()
                                    .getTimeDimension();
                            memberCalc = new DimensionCurrentMemberCalc(
                                    timeDimension);
                            break;
                        }

                        // Numeric Expression defaults to 1.
                        final IntegerCalc lagValueCalc = (args.length >= 2) ?
                                compiler.compileInteger(args[1]) :
                                ConstantCalc.constantInteger(1);

                        // If level is not specified, we compute it from
                        // member at runtime.
                        final LevelCalc ancestorLevelCalc =
                                args.length >= 1 ?
                                compiler.compileLevel(args[0]) :
                                null;

                        return new AbstractMemberCalc(call, new Calc[] {memberCalc, lagValueCalc, ancestorLevelCalc}) {
                            public Member evaluateMember(Evaluator evaluator) {
                                Member member = memberCalc.evaluateMember(
                                        evaluator);
                                int lagValue = lagValueCalc.evaluateInteger(
                                        evaluator);
                                Level ancestorLevel;
                                if (ancestorLevelCalc != null) {
                                    ancestorLevel = ancestorLevelCalc
                                            .evaluateLevel(evaluator);
                                } else {
                                    Member parent = member.getParentMember();
                                    if (parent == null) {
                                        // This is a root member,
                                        // so there is no parallelperiod.
                                        return member.getHierarchy()
                                                .getNullMember();
                                    }
                                    ancestorLevel = parent.getLevel();
                                }
                                return parallelPeriod(member, ancestorLevel,
                                        evaluator, lagValue);
                            }
                        };
                    }

                    Member parallelPeriod(
                            Member member,
                            Level ancestorLevel,
                            Evaluator evaluator,
                            int lagValue) {
                        // Now do some error checking.
                        // The ancestorLevel and the member must be from the
                        // same hierarchy.
                        if (member.getHierarchy() != ancestorLevel.getHierarchy()) {
                            MondrianResource.instance().FunctionMbrAndLevelHierarchyMismatch.ex(
                                    "ParallelPeriod", ancestorLevel.getHierarchy().getUniqueName(),
                                    member.getHierarchy().getUniqueName()
                            );
                        }

                        int distance = member.getLevel().getDepth() -
                                ancestorLevel.getDepth();
                        Member ancestor = FunUtil.ancestor(
                                evaluator, member, distance, ancestorLevel);
                        Member inLaw = evaluator.getSchemaReader()
                                .getLeadMember(ancestor, -lagValue);
                        return FunUtil.cousin(
                                evaluator.getSchemaReader(), member, inLaw);
                    }
                };
            }
        });

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
        define(new AggregateFunDef.Resolver());

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

        define(new MultiResolver(
                "Avg",
                "Avg(<Set>[, <Numeric Expression>])",
                "Returns the average value of a numeric expression evaluated over a set.",
                new String[]{"fnx", "fnxn"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new AbstractAggregateFunDef(dummyFunDef) {
                    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                        final ListCalc listCalc =
                                compiler.compileList(call.getArg(0));
                        final Calc calc = call.getArgCount() > 1 ?
                                compiler.compileScalar(call.getArg(1), true) :
                                new ValueCalc(call);
                        return new AbstractCalc(call) {
                            public Object evaluate(Evaluator evaluator) {
                                List memberList = listCalc.evaluateList(evaluator);
                                return avg(evaluator.push(), memberList, calc);
                            }

                            public Calc[] getCalcs() {
                                return new Calc[] {listCalc, calc};
                            }

                            public boolean dependsOn(Dimension dimension) {
                                return anyDependsButFirst(getCalcs(), dimension);
                            }
                        };
                    }
                };
            }
        });

        define(new MultiResolver(
                "Correlation",
                "Correlation(<Set>, <Numeric Expression>[, <Numeric Expression>])",
                "Returns the correlation of two series evaluated over a set.",
                new String[]{"fnxn","fnxnn"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new AbstractAggregateFunDef(dummyFunDef) {
                    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                        final ListCalc listCalc =
                                compiler.compileList(call.getArg(0));
                        final Calc calc1 =
                                compiler.compileScalar(call.getArg(1), true);
                        final Calc calc2 = call.getArgCount() > 2 ?
                                compiler.compileScalar(call.getArg(2), true) :
                                new ValueCalc(call);
                        return new AbstractDoubleCalc(call, new Calc[] {listCalc, calc1, calc2}) {
                            public double evaluateDouble(Evaluator evaluator) {
                                List memberList = listCalc.evaluateList(evaluator);
                                return correlation(evaluator.push(),
                                        memberList, calc1, calc2);
                            }

                            public boolean dependsOn(Dimension dimension) {
                                return anyDependsButFirst(getCalcs(), dimension);
                            }
                        };
                    }
                };
            }
        });

        final String[] resWords = {"INCLUDEEMPTY", "EXCLUDEEMPTY"};
        define(new MultiResolver(
                "Count",
                "Count(<Set>[, EXCLUDEEMPTY | INCLUDEEMPTY])",
                "Returns the number of tuples in a set, empty cells included unless the optional EXCLUDEEMPTY flag is used.",
            new String[]{"fnx", "fnxy"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new AbstractAggregateFunDef(dummyFunDef) {
                    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                        final ListCalc memberListCalc =
                                compiler.compileList(call.getArg(0));
                        final boolean includeEmpty =
                                call.getArgCount() < 2 ||
                                ((Literal) call.getArg(1)).getValue().equals(
                                        "INCLUDEEMPTY");
                        return new AbstractIntegerCalc(
                                call, new Calc[] {memberListCalc}) {
                            public int evaluateInteger(Evaluator evaluator) {
                                List memberList =
                                        memberListCalc.evaluateList(evaluator);
                                return count(evaluator, memberList, includeEmpty);
                            }

                            public boolean dependsOn(Dimension dimension) {
                                // COUNT(<set>, INCLUDEEMPTY) is straightforward -- it
                                // depends only on the dimensions that <Set> depends
                                // on.
                                if (super.dependsOn(dimension)) {
                                    return true;
                                }
                                if (includeEmpty) {
                                    return false;
                                }
                                // COUNT(<set>, EXCLUDEEMPTY) depends only on the
                                // dimensions that <Set> depends on, plus all
                                // dimensions not masked by the set.
                                if (memberListCalc.getType().usesDimension(dimension, true) ) {
                                    return false;
                                }
                                return true;
                            }
                        };
                    }

                };
            }
            public String[] getReservedWords() {
                return resWords;
            }
        });

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

        define(new MultiResolver(
                "Covariance",
                "Covariance(<Set>, <Numeric Expression>[, <Numeric Expression>])",
                "Returns the covariance of two series evaluated over a set (biased).",
                new String[]{"fnxn","fnxnn"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new FunDefBase(dummyFunDef) {

                    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                        final ListCalc listCalc =
                                compiler.compileList(call.getArg(0));
                        final Calc calc1 =
                                compiler.compileScalar(call.getArg(1), true);
                        final Calc calc2 = call.getArgCount() > 2 ?
                                compiler.compileScalar(call.getArg(2), true) :
                                new ValueCalc(call);
                        return new AbstractCalc(call) {
                            public Object evaluate(Evaluator evaluator) {
                                List memberList = listCalc.evaluateList(evaluator);
                                return covariance(evaluator.push(), memberList,
                                        calc1, calc2, true);
                            }

                            public Calc[] getCalcs() {
                                return new Calc[] {listCalc, calc1, calc2};
                            }
                        };
                    }
                };
            }
        });

        define(new MultiResolver(
                "CovarianceN",
                "CovarianceN(<Set>, <Numeric Expression>[, <Numeric Expression>])",
                "Returns the covariance of two series evaluated over a set (unbiased).",
                new String[]{"fnxn","fnxnn"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new AbstractAggregateFunDef(dummyFunDef) {
                    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                        final ListCalc listCalc =
                                compiler.compileList(call.getArg(0));
                        final Calc calc1 =
                                compiler.compileScalar(call.getArg(1), true);
                        final Calc calc2 = call.getArgCount() > 2 ?
                                compiler.compileScalar(call.getArg(2), true) :
                                new ValueCalc(call);
                        return new AbstractCalc(call) {
                            public Object evaluate(Evaluator evaluator) {
                                List memberList = listCalc.evaluateList(evaluator);
                                return covariance(evaluator.push(), memberList,
                                        calc1, calc2, false);
                            }

                            public Calc[] getCalcs() {
                                return new Calc[] {listCalc, calc1, calc2};
                            }

                            public boolean dependsOn(Dimension dimension) {
                                return anyDependsButFirst(getCalcs(), dimension);
                            }
                        };
                    }
                };
            }
        });

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

        define(new LinReg.InterceptFunDef.Resolver());

        define(new LinReg.PointFunDef.Resolver());

        define(new LinReg.R2FunDef.Resolver());

        define(new LinReg.SlopeFunDef.Resolver());

        define(new LinReg.VarianceFunDef.Resolver());

        define(new MultiResolver(
                "Max",
                "Max(<Set>[, <Numeric Expression>])",
                "Returns the maximum value of a numeric expression evaluated over a set.",
                new String[]{"fnx", "fnxn"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new AbstractAggregateFunDef(dummyFunDef) {
                    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                        final ListCalc listCalc =
                                compiler.compileList(call.getArg(0));
                        final Calc calc = call.getArgCount() > 1 ?
                                compiler.compileScalar(call.getArg(1), true) :
                                new ValueCalc(call);
                        return new AbstractCalc(call) {
                            public Object evaluate(Evaluator evaluator) {
                                List memberList = listCalc.evaluateList(evaluator);
                                return max(evaluator.push(), memberList, calc);
                            }

                            public Calc[] getCalcs() {
                                return new Calc[] {listCalc, calc};
                            }

                            public boolean dependsOn(Dimension dimension) {
                                return anyDependsButFirst(getCalcs(), dimension);
                            }
                        };
                    }
                };
            }
        });

        define(new MultiResolver(
                "Median",
                "Median(<Set>[, <Numeric Expression>])",
                "Returns the median value of a numeric expression evaluated over a set.",
                new String[]{"fnx", "fnxn"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new AbstractAggregateFunDef(dummyFunDef) {
                    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                        final ListCalc listCalc =
                                compiler.compileList(call.getArg(0));
                        final Calc calc = call.getArgCount() > 1 ?
                                compiler.compileScalar(call.getArg(1), true) :
                                new ValueCalc(call);
                        return new AbstractCalc(call) {
                            public Object evaluate(Evaluator evaluator) {
                                List memberList = listCalc.evaluateList(evaluator);
                                return median(evaluator.push(), memberList, calc);
                            }

                            public Calc[] getCalcs() {
                                return new Calc[] {listCalc, calc};
                            }

                            public boolean dependsOn(Dimension dimension) {
                                return anyDependsButFirst(getCalcs(), dimension);
                            }
                        };
                    }
                };
            }
        });

        define(new MultiResolver(
                "Min",
                "Min(<Set>[, <Numeric Expression>])",
                "Returns the minimum value of a numeric expression evaluated over a set.",
                new String[]{"fnx", "fnxn"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new AbstractAggregateFunDef(dummyFunDef) {
                    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                        final ListCalc listCalc =
                                compiler.compileList(call.getArg(0));
                        final Calc calc = call.getArgCount() > 1 ?
                                compiler.compileScalar(call.getArg(1), true) :
                                new ValueCalc(call);
                        return new AbstractCalc(call) {
                            public Object evaluate(Evaluator evaluator) {
                                List memberList = listCalc.evaluateList(evaluator);
                                return min(evaluator.push(), memberList, calc);
                            }

                            public Calc[] getCalcs() {
                                return new Calc[] {listCalc, calc};
                            }

                            public boolean dependsOn(Dimension dimension) {
                                return anyDependsButFirst(getCalcs(), dimension);
                            }
                        };
                    }
                };
            }
        });

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

        define(RankFunDef.createResolver());

        define(new CacheFunDef.CacheFunResolver());

        final MultiResolver stdevResolver = new MultiResolver(
                        "Stdev",
                        "Stdev(<Set>[, <Numeric Expression>])",
                        "Returns the standard deviation of a numeric expression evaluated over a set (unbiased).",
                        new String[]{"fnx", "fnxn"}) {
                    protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                        return new AbstractAggregateFunDef(dummyFunDef) {
                            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                                final ListCalc listCalc =
                                        compiler.compileList(call.getArg(0));
                                final Calc calc = call.getArgCount() > 1 ?
                                        compiler.compileScalar(call.getArg(1), true) :
                                        new ValueCalc(call);
                                return new AbstractCalc(call) {
                                    public Object evaluate(Evaluator evaluator) {
                                        List memberList = listCalc.evaluateList(evaluator);
                                        return stdev(evaluator.push(), memberList, calc, false);
                                    }

                                    public Calc[] getCalcs() {
                                        return new Calc[] {listCalc, calc};
                                    }

                                    public boolean dependsOn(Dimension dimension) {
                                        return anyDependsButFirst(getCalcs(), dimension);
                                    }
                                };
                            }
                        };
                    }
                };

        define(stdevResolver);
        define(new MultiResolver(
                "Stddev",
                "Stddev(<Set>[, <Numeric Expression>])",
                "Alias for Stdev.",
                new String[]{"fnx", "fnxn"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return stdevResolver.createFunDef(args, dummyFunDef);
            }
        });

        final MultiResolver stdevpResolver = new MultiResolver(
                        "StdevP",
                        "StdevP(<Set>[, <Numeric Expression>])",
                        "Returns the standard deviation of a numeric expression evaluated over a set (biased).",
                        new String[]{"fnx", "fnxn"}) {
                    protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                        return new AbstractAggregateFunDef(dummyFunDef) {
                            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                                final ListCalc listCalc =
                                        compiler.compileList(call.getArg(0));
                                final Calc calc = call.getArgCount() > 1 ?
                                        compiler.compileScalar(call.getArg(1), true) :
                                        new ValueCalc(call);
                                return new AbstractCalc(call) {
                                    public Object evaluate(Evaluator evaluator) {
                                        List memberList = listCalc.evaluateList(evaluator);
                                        return stdev(evaluator.push(), memberList, calc, true);
                                    }

                                    public Calc[] getCalcs() {
                                        return new Calc[] {listCalc, calc};
                                    }

                                    public boolean dependsOn(Dimension dimension) {
                                        return anyDependsButFirst(getCalcs(), dimension);
                                    }
                                };
                            }
                        };
                    }
                };

        define(stdevpResolver);

        define(new MultiResolver(
                "StddevP",
                "StddevP(<Set>[, <Numeric Expression>])",
                "Alias for StdevP.",
                new String[]{"fnx", "fnxn"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return stdevpResolver.createFunDef(args, dummyFunDef);
            }
        });

        define(new MultiResolver(
                "Sum", "Sum(<Set>[, <Numeric Expression>])", "Returns the sum of a numeric expression evaluated over a set.",
                new String[]{"fnx", "fnxn"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new AbstractAggregateFunDef(dummyFunDef) {
                    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                        final ListCalc listCalc =
                                compiler.compileList(call.getArg(0));
                        final Calc calc = call.getArgCount() > 1 ?
                                compiler.compileScalar(call.getArg(1), true) :
                                new ValueCalc(call);
                        return new AbstractDoubleCalc(call, new Calc[] {listCalc, calc}) {
                            public double evaluateDouble(Evaluator evaluator) {
                                List memberList = listCalc.evaluateList(evaluator);
                                return sumDouble(evaluator.push(), memberList, calc);
                            }

                            public Calc[] getCalcs() {
                                return new Calc[] {listCalc, calc};
                            }

                            public boolean dependsOn(Dimension dimension) {
                                return anyDependsButFirst(getCalcs(), dimension);
                            }
                        };
                    }
                };
            }
        });

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

        final MultiResolver varResolver = new MultiResolver(
                        "Var",
                        "Var(<Set>[, <Numeric Expression>])",
                        "Returns the variance of a numeric expression evaluated over a set (unbiased).",
                        new String[]{"fnx", "fnxn"}) {
                    protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                        return new AbstractAggregateFunDef(dummyFunDef) {
                            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                                final ListCalc listCalc =
                                        compiler.compileList(call.getArg(0));
                                final Calc calc = call.getArgCount() > 1 ?
                                        compiler.compileScalar(call.getArg(1), true) :
                                        new ValueCalc(call);
                                return new AbstractCalc(call) {
                                    public Object evaluate(Evaluator evaluator) {
                                        List memberList = listCalc.evaluateList(evaluator);
                                        return var(evaluator.push(), memberList, calc, false);
                                    }

                                    public Calc[] getCalcs() {
                                        return new Calc[] {listCalc, calc};
                                    }

                                    public boolean dependsOn(Dimension dimension) {
                                        return anyDependsButFirst(getCalcs(), dimension);
                                    }
                                };
                            }
                        };
                    }
                };
        define(varResolver);

        define(new MultiResolver(
                "Variance", "Variance(<Set>[, <Numeric Expression>])",
                "Alias for Var.",
                new String[]{"fnx", "fnxn"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return varResolver.createFunDef(args, dummyFunDef);
            }
        });

        final MultiResolver variancepResolver = new MultiResolver(
                        "VarianceP",
                        "VarianceP(<Set>[, <Numeric Expression>])",
                        "Alias for VarP.",
                        new String[]{"fnx", "fnxn"}) {
                    protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                        return new AbstractAggregateFunDef(dummyFunDef) {
                            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                                final ListCalc listCalc =
                                        compiler.compileList(call.getArg(0));
                                final Calc calc = call.getArgCount() > 1 ?
                                        compiler.compileScalar(call.getArg(1), true) :
                                        new ValueCalc(call);
                                return new AbstractCalc(call) {
                                    public Object evaluate(Evaluator evaluator) {
                                        List memberList = listCalc.evaluateList(evaluator);
                                        return var(evaluator.push(), memberList, calc, true);
                                    }

                                    public Calc[] getCalcs() {
                                        return new Calc[] {listCalc, calc};
                                    }

                                    public boolean dependsOn(Dimension dimension) {
                                        return anyDependsButFirst(getCalcs(), dimension);
                                    }
                                };
                            }
                        };
                    }
                };
        define(variancepResolver);

        define(new MultiResolver(
                "VarP",
                "VarP(<Set>[, <Numeric Expression>])",
                "Returns the variance of a numeric expression evaluated over a set (biased).",
                new String[]{"fnx", "fnxn"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return variancepResolver.createFunDef(args, dummyFunDef);
            }
        });

        //
        // SET FUNCTIONS

        /*
         * AddCalculatedMembers adds calculated members that are siblings
         * of the members in the set. The set is limited to one dimension.
         */
        define(new FunDefBase(
                "AddCalculatedMembers",
                "AddCalculatedMembers(<Set>)",
                "Adds calculated members to a set.",
                "fxx") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final ListCalc listCalc = compiler.compileList(call.getArg(0));
                return new AbstractListCalc(call, new Calc[] {listCalc}) {
                    public List evaluateList(Evaluator evaluator) {
                        final List list = listCalc.evaluateList(evaluator);
                        return addCalculatedMembers(list, evaluator);
                    }
                };
            }

            private List addCalculatedMembers(List memberList, Evaluator evaluator) {
                // Determine unique levels in the set
                Map levelMap = new HashMap();
                Dimension dim = null;

                Iterator it = memberList.iterator();
                while (it.hasNext()) {
                    Object obj = it.next();
                    if (!(obj instanceof Member)) {
                        throw newEvalException(this, "Only single dimension members allowed in set for AddCalculatedMembers");
                    }
                    Member member = (Member) obj;
                    if (dim == null) {
                        dim = member.getDimension();
                    } else if (dim != member.getDimension()) {
                        throw newEvalException(this, "Only members from the same dimension are allowed in the AddCalculatedMembers set: "
                                + dim.toString() + " vs " + member.getDimension().toString());
                    }
                    if (!levelMap.containsKey(member.getLevel())) {
                        levelMap.put(member.getLevel(), null);
                    }
                }
                /*
                 * For each level, add the calculated members from both
                 * the schema and the query
                 */
                List workingList = new ArrayList(memberList);
                final SchemaReader schemaReader =
                        evaluator.getQuery().getSchemaReader(true);
                it = levelMap.keySet().iterator();
                while (it.hasNext()) {
                    Level level = (Level) it.next();
                    List calcMemberList =
                            schemaReader.getCalculatedMembers(level);
                    workingList.addAll(calcMemberList);
                }
                memberList = workingList;
                return memberList;
            }
        });

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

        define(new MultiResolver(
                "BottomCount",
                "BottomCount(<Set>, <Count>[, <Numeric Expression>])",
                "Returns a specified number of items from the bottom of a set, optionally ordering the set first.",
                new String[]{"fxxnn", "fxxn"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new FunDefBase(dummyFunDef) {
                    public Calc compileCall(final ResolvedFunCall call, ExpCompiler compiler) {
                        final ListCalc listCalc =
                                compiler.compileList(call.getArg(0));
                        final IntegerCalc integerCalc =
                                compiler.compileInteger(call.getArg(1));
                        final Calc orderCalc =
                                call.getArgCount() > 2 ?
                                compiler.compileScalar(call.getArg(2), true) :
                                null;
                        return new AbstractListCalc(call, new Calc[] {listCalc, integerCalc, orderCalc}) {
                            public List evaluateList(Evaluator evaluator) {
                                // Use a native evaluator, if more efficient.
                                // TODO: Figure this out at compile time.
                                SchemaReader schemaReader = evaluator.getSchemaReader();
                                NativeEvaluator nativeEvaluator = schemaReader.getNativeSetEvaluator(call.getFunDef(), evaluator, call.getArgs());
                                if (nativeEvaluator != null) {
                                    return (List) nativeEvaluator.execute();
                                }

                                List list = listCalc.evaluateList(evaluator);
                                int n = integerCalc.evaluateInteger(evaluator);
                                if (orderCalc != null) {
                                    boolean desc = false, brk = true;
                                    sort(evaluator.push(), list, orderCalc, desc, brk);
                                }
                                if (n < list.size()) {
                                    list = list.subList(0, n);
                                }
                                return list;
                            }
                        };
                    }
                };
            }
        });

        define(new MultiResolver(
                "BottomPercent",
                "BottomPercent(<Set>, <Percentage>, <Numeric Expression>)",
                "Sorts a set and returns the bottom N elements whose cumulative total is at least a specified percentage.",
                new String[]{"fxxnn"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new FunDefBase(dummyFunDef) {
                    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                        final ListCalc listCalc =
                                compiler.compileList(call.getArg(0));
                        final DoubleCalc doubleCalc =
                                compiler.compileDouble(call.getArg(1));
                        final Calc calc =
                                compiler.compileScalar(call.getArg(2), true);
                        return new AbstractListCalc(call, new Calc[] {listCalc, doubleCalc, calc}) {
                            public List evaluateList(Evaluator evaluator) {
                                List list = listCalc.evaluateList(evaluator);
                                double n = doubleCalc.evaluateDouble(evaluator);
                                return topOrBottom(evaluator.push(), list, calc, false, true, n);
                            }
                        };
                    }
                };
            }
        });

        define(new MultiResolver(
                "BottomSum",
                "BottomSum(<Set>, <Value>, <Numeric Expression>)",
                "Sorts a set and returns the bottom N elements whose cumulative total is at least a specified value.",
                new String[]{"fxxnn"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new FunDefBase(dummyFunDef) {
                    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                        final ListCalc listCalc =
                                compiler.compileList(call.getArg(0));
                        final DoubleCalc doubleCalc =
                                compiler.compileDouble(call.getArg(1));
                        final Calc calc =
                                compiler.compileScalar(call.getArg(2), true);
                        return new AbstractListCalc(call, new Calc[] {listCalc, doubleCalc, calc}) {
                            public List evaluateList(Evaluator evaluator) {
                                List list = listCalc.evaluateList(evaluator);
                                double n = doubleCalc.evaluateDouble(evaluator);
                                return topOrBottom(evaluator.push(), list, calc, false, false, n);
                            }
                        };
                    }
                };
            }
        });

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

        define(new MultiResolver(
                "Crossjoin",
                "Crossjoin(<Set1>, <Set2>)",
                "Returns the cross product of two sets.",
                new String[]{"fxxx"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new CrossJoinFunDef(dummyFunDef);
            }
        });

        define(new MultiResolver(
                "NonEmptyCrossJoin",
                "NonEmptyCrossJoin(<Set1>, <Set2>)",
                "Returns the cross product of two sets, excluding empty tuples and tuples without associated fact table data.",
                new String[]{"fxxx"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new NonEmptyCrossJoinFunDef(dummyFunDef);
            }
        });

        define(new MultiResolver(
                "*",
                "<Set1> * <Set2>",
                "Returns the cross product of two sets.",
                new String[]{"ixxx", "ixmx", "ixxm", "ixmm"}) {
            public FunDef resolve(
                    Exp[] args, Validator validator, int[] conversionCount) {
                // This function only applies in contexts which require a set.
                // Elsewhere, "*" is the multiplication operator.
                // This means that [Measures].[Unit Sales] * [Gender].[M] is
                // well-defined.
                if (validator.requiresExpression()) {
                    return null;
                }
                return super.resolve(args, validator, conversionCount);
            }

            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new CrossJoinFunDef(dummyFunDef);
            }
        });

        define(new DescendantsFunDef.Resolver());

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

        define(new MultiResolver(
                "DrilldownLevel",
                "DrilldownLevel(<Set>[, <Level>]) or DrilldownLevel(<Set>, , <Index>)",
                "Drills down the members of a set, at a specified level, to one level below. Alternatively, drills down on a specified dimension in the set.",
                new String[]{"fxx", "fxxl"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new FunDefBase(dummyFunDef) {
                    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                        final ListCalc listCalc =
                                compiler.compileList(call.getArg(0));
                        final LevelCalc levelCalc =
                                call.getArgCount() > 1 ?
                                compiler.compileLevel(call.getArg(1)) :
                                null;
                        return new AbstractListCalc(call, new Calc[] {listCalc, levelCalc}) {
                            public List evaluateList(Evaluator evaluator) {
                                List list = listCalc.evaluateList(evaluator);
                                if (list.size() == 0) {
                                    return list;
                                }
                                int searchDepth = -1;
                                if (levelCalc != null) {
                                    Level level = levelCalc.evaluateLevel(evaluator);
                                    searchDepth = level.getDepth();
                                }
                                return drill(searchDepth, list, evaluator);
                            }
                        };
                    }

                    List drill(int searchDepth, List list, Evaluator evaluator) {
                        if (searchDepth == -1) {
                            searchDepth = ((Member)list.get(0)).getLevel().getDepth();

                            for (int i = 1, m = list.size(); i < m; i++) {
                                Member member = (Member) list.get(i);
                                int memberDepth = member.getLevel().getDepth();

                                if (memberDepth > searchDepth) {
                                    searchDepth = memberDepth;
                                }
                            }
                        }

                        List drilledSet = new ArrayList();

                        for (int i = 0, m = list.size(); i < m; i++) {
                            Member member = (Member) list.get(i);
                            drilledSet.add(member);

                            Member nextMember = i == (m - 1) ?
                                    null :
                                    (Member) list.get(i + 1);

                            //
                            // This member is drilled if it's at the correct depth
                            // and if it isn't drilled yet. A member is considered
                            // to be "drilled" if it is immediately followed by
                            // at least one descendant
                            //
                            if (member.getLevel().getDepth() == searchDepth
                                    && !FunUtil.isAncestorOf(member, nextMember, true)) {
                                Member[] childMembers = evaluator.getSchemaReader().getMemberChildren(member);
                                for (int j = 0; j < childMembers.length; j++) {
                                    drilledSet.add(childMembers[j]);
                                }
                            }
                        }
                        return drilledSet;
                    }
                };
            }
        });

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

        define(new DrilldownMemberFunDef.Resolver());

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

        define(new MultiResolver(
                "Except",
                "Except(<Set1>, <Set2>[, ALL])",
                "Finds the difference between two sets, optionally retaining duplicates.",
                new String[]{"fxxx", "fxxxy"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new FunDefBase(dummyFunDef) {
                    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                        // todo: implement ALL
                        final ListCalc listCalc1 =
                                compiler.compileList(call.getArg(0));
                        final ListCalc listCalc2 =
                                compiler.compileList(call.getArg(1));
                        return new AbstractListCalc(call, new Calc[] {listCalc1, listCalc2}) {
                            public List evaluateList(Evaluator evaluator) {
                                List list1 = listCalc1.evaluateList(evaluator);
                                List list2 = listCalc2.evaluateList(evaluator);
                                return except(list1, list2);
                            }
                        };
                    }

                    List except(final List list0, final List list1) {
                        HashSet set = new HashSet(list1);
                        List result = new ArrayList();
                        for (int i = 0, count = list0.size(); i < count; i++) {
                            Object o = list0.get(i);
                            if (!set.contains(o)) {
                                result.add(o);
                            }
                        }
                        return result;
                    }
                };
            }
        });

        if (false) define(new FunDefBase(
                "Extract",
                "Extract(<Set>, <Dimension>[, <Dimension>...])",
                "Returns a set of tuples from extracted dimension elements. The opposite of Crossjoin.",
                "fx*") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                throw new UnsupportedOperationException();
            }
        });

        define(new FunDefBase(
                "Filter",
                "Filter(<Set>, <Search Condition>)",
                "Returns the set resulting from filtering a set based on a search condition.",
                "fxxb") {
            public Calc compileCall(final ResolvedFunCall call, ExpCompiler compiler) {
                final ListCalc listCalc = compiler.compileList(call.getArg(0));
                final BooleanCalc calc = compiler.compileBoolean(call.getArg(1));
                if (((SetType) listCalc.getType()).getElementType() instanceof MemberType) {
                    return new AbstractListCalc(call, new Calc[] {listCalc, calc}) {
                        public List evaluateList(Evaluator evaluator) {
                            // Use a native evaluator, if more efficient.
                            // TODO: Figure this out at compile time.
                            SchemaReader schemaReader = evaluator.getSchemaReader();
                            NativeEvaluator nativeEvaluator = schemaReader.getNativeSetEvaluator(call.getFunDef(), evaluator, call.getArgs());
                            if (nativeEvaluator != null) {
                                return (List) nativeEvaluator.execute();
                            }

                            List members = listCalc.evaluateList(evaluator);
                            List result = new ArrayList();
                            Evaluator evaluator2 = evaluator.push();
                            for (int i = 0, count = members.size(); i < count; i++) {
                                Member member = (Member) members.get(i);
                                evaluator2.setContext(member);
                                if (calc.evaluateBoolean(evaluator2)) {
                                    result.add(member);
                                }
                            }
                            return result;
                        }

                        public boolean dependsOn(Dimension dimension) {
                            return anyDependsButFirst(getCalcs(), dimension);
                        }
                    };
                } else {
                    return new AbstractListCalc(call, new Calc[] {listCalc, calc}) {
                        public List evaluateList(Evaluator evaluator) {
                            // Use a native evaluator, if more efficient.
                            // TODO: Figure this out at compile time.
                            SchemaReader schemaReader = evaluator.getSchemaReader();
                            NativeEvaluator nativeEvaluator = schemaReader.getNativeSetEvaluator(call.getFunDef(), evaluator, call.getArgs());
                            if (nativeEvaluator != null) {
                                return (List) nativeEvaluator.execute();
                            }

                            List tupleList = listCalc.evaluateList(evaluator);
                            List result = new ArrayList();
                            Evaluator evaluator2 = evaluator.push();
                            for (int i = 0, count = tupleList.size(); i < count; i++) {
                                Member[] members = (Member []) tupleList.get(i);
                                evaluator2.setContext(members);
                                if (calc.evaluateBoolean(evaluator2)) {
                                    result.add(members);
                                }
                            }
                            return result;
                        }

                        public boolean dependsOn(Dimension dimension) {
                            return anyDependsButFirst(getCalcs(), dimension);
                        }
                    };
                }
            }

        });

        define(new MultiResolver(
                "Generate",
                "Generate(<Set1>, <Set2>[, ALL])",
                "Applies a set to each member of another set and joins the resulting sets by union.",
                new String[] {"fxxx", "fxxxy"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                final boolean all = getLiteralArg(args, 2, "", new String[] {"ALL"}, dummyFunDef).equalsIgnoreCase("ALL");
                return new FunDefBase(dummyFunDef) {
                    public Type getResultType(Validator validator, Exp[] args) {
                        final Type type = args[1].getType();
                        final Type memberType = TypeUtil.toMemberOrTupleType(type);
                        return new SetType(memberType);
                    }

                    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                        final ListCalc listCalc1 =
                                compiler.compileList(call.getArg(0));
                        final ListCalc listCalc2 =
                                compiler.compileList(call.getArg(1));
                        return new AbstractListCalc(call, new Calc[] {listCalc1, listCalc2}) {
                            public List evaluateList(Evaluator evaluator) {
                                final List list1 = listCalc1.evaluateList(evaluator);
                                return generate(evaluator, list1, listCalc2, all);
                            }

                            public boolean dependsOn(Dimension dimension) {
                                return anyDependsButFirst(getCalcs(), dimension);
                            }
                        };
                    }

                    List generate(
                            Evaluator evaluator,
                            List members,
                            final ListCalc listCalc,
                            final boolean all) {
                        final Evaluator evaluator2 = evaluator.push();
                        List result = new ArrayList();
                        HashSet emitted = all ? null : new HashSet();
                        for (int i = 0; i < members.size(); i++) {
                            Object o = members.get(i);
                            if (o instanceof Member) {
                                evaluator2.setContext((Member) o);
                            } else {
                                evaluator2.setContext((Member[]) o);
                            }
                            final List result2 = listCalc.evaluateList(evaluator2);
                            if (all) {
                                result.addAll(result2);
                            } else {
                                for (int j = 0; j < result2.size(); j++) {
                                    Object row = result2.get(j);
                                    if (emitted.add(row)) {
                                        result.add(row);
                                    }
                                }
                            }
                        }
                        return result;
                    }
                };
            }
        });

        define(new MultiResolver(
                "Head",
                "Head(<Set>[, < Numeric Expression >])",
                "Returns the first specified number of elements in a set.",
                new String[] {"fxx", "fxxn"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new FunDefBase(dummyFunDef) {
                    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                        final ListCalc listCalc =
                                compiler.compileList(call.getArg(0));
                        final IntegerCalc integerCalc =
                                call.getArgCount() > 1 ?
                                compiler.compileInteger(call.getArg(1)) :
                                ConstantCalc.constantInteger(1);
                        return new AbstractListCalc(call, new Calc[] {listCalc, integerCalc}) {
                            public List evaluateList(Evaluator evaluator) {
                                List list = listCalc.evaluateList(evaluator);
                                int count = integerCalc.evaluateInteger(evaluator);
                                return head(count, list);
                            }
                        };
                    }

                    List head(final int count, List members) {
                        assert members != null;
                        if (count >= members.size()) {
                            return members;
                        }
                        if (count <= 0) {
                            return Collections.EMPTY_LIST;
                        }
                        return members.subList(0, count);
                    }
                };
            }
        });

        final String[] prePost = {"PRE","POST"};
        define(new MultiResolver(
                "Hierarchize", "Hierarchize(<Set>[, POST])", "Orders the members of a set in a hierarchy.",
                new String[] {"fxx", "fxxy"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                String order = getLiteralArg(args, 1, "PRE", prePost, dummyFunDef);
                final boolean post = order.equals("POST");
                return new FunDefBase(dummyFunDef) {
                    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                        final ListCalc listCalc =
                                compiler.compileList(call.getArg(0));
                        return new AbstractListCalc(call, new Calc[] {listCalc}) {
                            public List evaluateList(Evaluator evaluator) {
                                List list = listCalc.evaluateList(evaluator);
                                hierarchize(list, post);
                                return list;
                            }
                        };
                    }

                };
            }

            public String[] getReservedWords() {
                return prePost;
            }
        });

        define(new MultiResolver(
                "Intersect",
                "Intersect(<Set1>, <Set2>[, ALL])",
                "Returns the intersection of two input sets, optionally retaining duplicates.",
                new String[] {"fxxxy", "fxxx"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                final boolean all = getLiteralArg(args, 2, "", new String[] {"ALL"}, dummyFunDef).equalsIgnoreCase("ALL");
                return new IntersectFunDef(dummyFunDef, all);
            }
        });

        if (false) define(new FunDefBase(
                "LastPeriods",
                "LastPeriods(<Index>[, <Member>])",
                "Returns a set of members prior to and including a specified member.",
                "fx*") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                throw new UnsupportedOperationException();
            }
        });

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

        define(new XtdFunDef.Resolver(
                "Mtd",
                "Mtd([<Member>])",
                "A shortcut function for the PeriodsToDate function that specifies the level to be Month.",
                new String[]{"fx", "fxm"},
                mondrian.olap.LevelType.TimeMonths));

        define(new OrderFunDef.OrderResolver());

        define(new MultiResolver(
                "PeriodsToDate",
                "PeriodsToDate([<Level>[, <Member>]])",
                "Returns a set of periods (members) from a specified level starting with the first period and ending with a specified member.",
                new String[]{"fx", "fxl", "fxlm"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new FunDefBase(dummyFunDef) {
                    public Type getResultType(Validator validator, Exp[] args) {
                        if (args.length == 0) {
                            // With no args, the default implementation cannot
                            // guess the hierarchy.
                            Hierarchy hierarchy = validator.getQuery()
                                    .getCube().getTimeDimension()
                                    .getHierarchy();
                            return new SetType(
                                    MemberType.forHierarchy(hierarchy));
                        }
                        final Type type = args[0].getType();
                        if (type.getHierarchy().getDimension()
                                .getDimensionType() !=
                                mondrian.olap.DimensionType.TimeDimension) {
                            throw MondrianResource.instance().TimeArgNeeded.ex(getName());
                        }
                        return super.getResultType(validator, args);
                    }

                    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                        final LevelCalc levelCalc =
                                call.getArgCount() > 0 ?
                                compiler.compileLevel(call.getArg(0)) :
                                null;
                        final MemberCalc memberCalc =
                                call.getArgCount() > 1 ?
                                compiler.compileMember(call.getArg(1)) :
                                null;
                        final Dimension timeDimension = compiler
                                .getEvaluator().getCube().getTimeDimension();
                        return new AbstractListCalc(call, new Calc[] {levelCalc, memberCalc}) {
                            public List evaluateList(Evaluator evaluator) {
                                final Member member;
                                final Level level;
                                if (levelCalc == null) {
                                    member = evaluator.getContext(timeDimension);
                                    level = member.getLevel().getParentLevel();
                                } else {
                                    level = levelCalc.evaluateLevel(evaluator);
                                    if (memberCalc == null) {
                                        member = evaluator.getContext(
                                                level.getHierarchy().getDimension());
                                    } else {
                                        member = memberCalc.evaluateMember(evaluator);
                                    }
                                }
                                return periodsToDate(evaluator, level, member);
                            }

                            public boolean dependsOn(Dimension dimension) {
                                if (super.dependsOn(dimension)) {
                                    return true;
                                }
                                if (memberCalc != null) {
                                    return false;
                                } else if (levelCalc != null) {
                                    return levelCalc.getType().usesDimension(dimension, true) ;
                                } else {
                                    return dimension == timeDimension;
                                }
                            }
                        };
                    }

                };
            }
        });

        define(new XtdFunDef.Resolver(
                "Qtd",
                "Qtd([<Member>])",
                "A shortcut function for the PeriodsToDate function that specifies the level to be Quarter.",
                new String[]{"fx", "fxm"},
                mondrian.olap.LevelType.TimeQuarters));

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

        define(new StrToSetFunDef.Resolver());

        define(new MultiResolver(
                "Subset",
                "Subset(<Set>, <Start>[, <Count>])",
                "Returns a subset of elements from a set.",
                new String[] {"fxxn", "fxxnn"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new FunDefBase(dummyFunDef) {
                    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                        final ListCalc listCalc =
                                compiler.compileList(call.getArg(0));
                        final IntegerCalc startCalc =
                                compiler.compileInteger(call.getArg(1));
                        final IntegerCalc countCalc =
                                call.getArgCount() > 2 ?
                                compiler.compileInteger(call.getArg(2)) :
                                null;
                        return new AbstractListCalc(call, new Calc[] {listCalc, startCalc, countCalc}) {
                            public List evaluateList(Evaluator evaluator) {
                                final List list =
                                        listCalc.evaluateList(evaluator);
                                final int start =
                                        startCalc.evaluateInteger(evaluator);
                                int end;
                                if (countCalc != null) {
                                    final int count =
                                        countCalc.evaluateInteger(evaluator);
                                    end = start + count;
                                } else {
                                    end = list.size();
                                }
                                if (end > list.size()) {
                                    end = list.size();
                                }
                                if (start >= end || start < 0) {
                                    return Collections.EMPTY_LIST;
                                }
                                assert 0 <= start;
                                assert start < end;
                                assert end <= list.size();
                                if (start == 0 && end == list.size()) {
                                    return list;
                                } else {
                                    return list.subList(start, end);
                                }
                            }
                        };
                    }

                };
            }
        });

        define(new MultiResolver(
                "Tail",
                "Tail(<Set>[, <Count>])",
                "Returns a subset from the end of a set.",
                new String[] {"fxx", "fxxn"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new FunDefBase(dummyFunDef) {
                    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                        final ListCalc listCalc =
                                compiler.compileList(call.getArg(0));
                        final IntegerCalc integerCalc =
                                call.getArgCount() > 1 ?
                                compiler.compileInteger(call.getArg(1)) :
                                ConstantCalc.constantInteger(1);
                        return new AbstractListCalc(call, new Calc[] {listCalc, integerCalc}) {
                            public List evaluateList(Evaluator evaluator) {
                                List list = listCalc.evaluateList(evaluator);
                                int count = integerCalc.evaluateInteger(evaluator);
                                return tail(count, list);
                            }
                        };
                    }

                    List tail(final int count, List members) {
                        assert members != null;
                        if (count >= members.size()) {
                            return members;
                        }
                        if (count <= 0) {
                            return Collections.EMPTY_LIST;
                        }
                        return members.subList(members.size() - count,
                                members.size());
                    }
                };
            }
        });

        define(new MultiResolver(
                "ToggleDrillState",
                "ToggleDrillState(<Set1>, <Set2>[, RECURSIVE])",
                "Toggles the drill state of members. This function is a combination of DrillupMember and DrilldownMember.",
                new String[]{"fxxx", "fxxxy"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new FunDefBase(dummyFunDef) {
                    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                        if (call.getArgCount() > 2) {
                            throw MondrianResource.instance().ToggleDrillStateRecursiveNotSupported.ex();
                        }
                        final ListCalc listCalc0 =
                                compiler.compileList(call.getArg(0));
                        final ListCalc listCalc1 =
                                compiler.compileList(call.getArg(1));
                        return new AbstractListCalc(call, new Calc[] {listCalc0, listCalc1}) {
                            public List evaluateList(Evaluator evaluator) {
                                final List list0 = listCalc0.evaluateList(evaluator);
                                final List list1 = listCalc1.evaluateList(evaluator);
                                return toggleDrillState(evaluator, list0, list1);
                            }
                        };
                    }

                    List toggleDrillState(Evaluator evaluator, List v0, List list1) {
                        if (list1.isEmpty()) {
                            return v0;
                        }
                        if (v0.isEmpty()) {
                            return v0;
                        }
                        HashSet set = new HashSet();
                        set.addAll(list1);
                        HashSet set1 = set;
                        List result = new ArrayList();
                        int i = 0, n = v0.size();
                        while (i < n) {
                            Object o = v0.get(i++);
                            result.add(o);
                            Member m = null;
                            int k = -1;
                            if (o instanceof Member) {
                                if (!set1.contains(o)) {
                                    continue;
                                }
                                m = (Member) o;
                                k = -1;
                            } else {
                                Util.assertTrue(o instanceof Member[]);
                                Member[] members = (Member[]) o;
                                for (int j = 0; j < members.length; j++) {
                                    Member member = members[j];
                                    if (set1.contains(member)) {
                                        k = j;
                                        m = member;
                                        break;
                                    }
                                }
                                if (k == -1) {
                                    continue;
                                }
                            }
                            boolean isDrilledDown = false;
                            if (i < n) {
                                Object next = v0.get(i);
                                Member nextMember = (k < 0) ? (Member) next :
                                        ((Member[]) next)[k];
                                boolean strict = true;
                                if (FunUtil.isAncestorOf(m, nextMember, strict)) {
                                    isDrilledDown = true;
                                }
                            }
                            if (isDrilledDown) {
                                // skip descendants of this member
                                do {
                                    Object next = v0.get(i);
                                    Member nextMember = (k < 0) ? (Member) next :
                                            ((Member[]) next)[k];
                                    boolean strict = true;
                                    if (FunUtil.isAncestorOf(m, nextMember, strict)) {
                                        i++;
                                    } else {
                                        break;
                                    }
                                } while (i < n);
                            } else {
                                Member[] children = evaluator.getSchemaReader().getMemberChildren(m);
                                for (int j = 0; j < children.length; j++) {
                                    if (k < 0) {
                                        result.add(children[j]);
                                    } else {
                                        Member[] members = (Member[]) ((Member[]) o).clone();
                                        members[k] = children[j];
                                        result.add(members);
                                    }
                                }
                            }
                        }
                        return result;
                    }

                };
            }

            public String[] getReservedWords() {
                return new String[] {"RECURSIVE"};
            }
        });

        define(new MultiResolver(
                "TopCount",
                "TopCount(<Set>, <Count>[, <Numeric Expression>])",
                "Returns a specified number of items from the top of a set, optionally ordering the set first.",
                new String[]{"fxxnn", "fxxn"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new FunDefBase(dummyFunDef) {
                    public Calc compileCall(final ResolvedFunCall call, ExpCompiler compiler) {
                        final ListCalc listCalc =
                                compiler.compileList(call.getArg(0));
                        final IntegerCalc integerCalc =
                                compiler.compileInteger(call.getArg(1));
                        final Calc orderCalc =
                                call.getArgCount() > 2 ?
                                compiler.compileScalar(call.getArg(2), true) :
                                null;
                        return new AbstractListCalc(call, new Calc[] {listCalc, integerCalc, orderCalc}) {
                            public List evaluateList(Evaluator evaluator) {
                                // Use a native evaluator, if more efficient.
                                // TODO: Figure this out at compile time.
                                SchemaReader schemaReader = evaluator.getSchemaReader();
                                NativeEvaluator nativeEvaluator = schemaReader.getNativeSetEvaluator(call.getFunDef(), evaluator, call.getArgs());
                                if (nativeEvaluator != null) {
                                    return (List) nativeEvaluator.execute();
                                }

                                List list = listCalc.evaluateList(evaluator);
                                int n = integerCalc.evaluateInteger(evaluator);
                                if (orderCalc != null) {
                                    boolean desc = true, brk = true;
                                    sort(evaluator.push(), list, orderCalc, desc, brk);
                                }
                                if (n < list.size()) {
                                    list = list.subList(0, n);
                                }
                                return list;
                            }
                        };
                    }
                };
            }
        });

        define(new MultiResolver(
                "TopPercent",
                "TopPercent(<Set>, <Percentage>, <Numeric Expression>)",
                "Sorts a set and returns the top N elements whose cumulative total is at least a specified percentage.",
                new String[]{"fxxnn"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new FunDefBase(dummyFunDef) {
                    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                        final ListCalc listCalc =
                                compiler.compileList(call.getArg(0));
                        final DoubleCalc doubleCalc =
                                compiler.compileDouble(call.getArg(1));
                        final Calc calc =
                                compiler.compileScalar(call.getArg(2), true);
                        return new AbstractListCalc(call, new Calc[] {listCalc, doubleCalc, calc}) {
                            public List evaluateList(Evaluator evaluator) {
                                List list = listCalc.evaluateList(evaluator);
                                double n = doubleCalc.evaluateDouble(evaluator);
                                return topOrBottom(evaluator.push(), list, calc, true, true, n);
                            }
                        };
                    }
                };
            }
        });

        define(new MultiResolver(
                "TopSum",
                "TopSum(<Set>, <Value>, <Numeric Expression>)",
                "Sorts a set and returns the top N elements whose cumulative total is at least a specified value.",
                new String[]{"fxxnn"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new FunDefBase(dummyFunDef) {
                    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                        final ListCalc listCalc =
                                compiler.compileList(call.getArg(0));
                        final DoubleCalc doubleCalc =
                                compiler.compileDouble(call.getArg(1));
                        final Calc calc =
                                compiler.compileScalar(call.getArg(2), true);
                        return new AbstractListCalc(call, new Calc[] {listCalc, doubleCalc, calc}) {
                            public List evaluateList(Evaluator evaluator) {
                                List list = listCalc.evaluateList(evaluator);
                                double n = doubleCalc.evaluateDouble(evaluator);
                                return topOrBottom(evaluator.push(), list, calc, true, false, n);
                            }
                        };
                    }
                };
            }
        });

        final String[] allDistinct = new String[] {"ALL", "DISTINCT"};
        define(new MultiResolver(
                "Union",
                "Union(<Set1>, <Set2>[, ALL])",
                "Returns the union of two sets, optionally retaining duplicates.",
                new String[] {"fxxx", "fxxxy"}) {
            public String[] getReservedWords() {
                return allDistinct;
            }

            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                String allString = getLiteralArg(args, 2, "DISTINCT", allDistinct, dummyFunDef);
                final boolean all = allString.equalsIgnoreCase("ALL");
                checkCompatible(args[0], args[1], dummyFunDef);
                return new FunDefBase(dummyFunDef) {
                    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                        final ListCalc listCalc0 =
                                compiler.compileList(call.getArg(0));
                        final ListCalc listCalc1 =
                                compiler.compileList(call.getArg(1));
                        return new AbstractListCalc(call, new Calc[] {listCalc0, listCalc1}) {
                            public List evaluateList(Evaluator evaluator) {
                                List list0 = listCalc0.evaluateList(evaluator);
                                List list1 = listCalc1.evaluateList(evaluator);
                                return union(list0, list1, all);
                            }
                        };
                    }

                    List union(List list0, List list1, final boolean all) {
                        assert list0 != null;
                        assert list1 != null;
                        if (all) {
                            if (list0.isEmpty()) {
                                return list1;
                            }
                            list0.addAll(list1);
                            return list0;
                        } else {
                            Set added = new HashSet();
                            List result = new ArrayList();
                            FunUtil.addUnique(result, list0, added);
                            FunUtil.addUnique(result, list1, added);
                            return result;
                        }
                    }
                };
            }
        });

        if (false) define(new FunDefBase(
                "VisualTotals",
                "VisualTotals(<Set>, <Pattern>)",
                "Dynamically totals child members specified in a set using a pattern for the total label in the result set.",
                "fx*") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                throw new UnsupportedOperationException();
            }
        });

        define(new XtdFunDef.Resolver(
                "Wtd",
                "Wtd([<Member>])",
                "A shortcut function for the PeriodsToDate function that specifies the level to be Week.",
                new String[]{"fx", "fxm"},
                mondrian.olap.LevelType.TimeWeeks));

        define(new XtdFunDef.Resolver(
                "Ytd",
                "Ytd([<Member>])",
                "A shortcut function for the PeriodsToDate function that specifies the level to be Year.",
                new String[]{"fx", "fxm"},
                mondrian.olap.LevelType.TimeYears));

        define(new FunDefBase(
                ":",
                "<Member>:<Member>",
                "Infix colon operator returns the set of members between a given pair of members.",
                "ixmm") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final MemberCalc memberCalc0 =
                        compiler.compileMember(call.getArg(0));
                final MemberCalc memberCalc1 =
                        compiler.compileMember(call.getArg(1));
                return new AbstractListCalc(call, new Calc[] {memberCalc0, memberCalc1}) {
                    public List evaluateList(Evaluator evaluator) {
                        final Member member0 = memberCalc0.evaluateMember(evaluator);
                        final Member member1 = memberCalc1.evaluateMember(evaluator);
                        return colon(member0, member1, evaluator);
                    }
                };
            }

            List colon(
                    final Member member0, final Member member1, Evaluator evaluator) {
                if (member0.isNull() || member1.isNull()) {
                    return Collections.EMPTY_LIST;
                }
                if (member0.getLevel() != member1.getLevel()) {
                    throw newEvalException(this, "Members must belong to the same level");
                }
                return FunUtil.memberRange(evaluator, member0, member1);
            }
        });

        // special resolver for the "{...}" operator
        define(new ResolverBase(
                "{}",
                "{<Member> [, <Member>]...}",
                "Brace operator constructs a set.",
                Syntax.Braces) {
            public FunDef resolve(
                    Exp[] args, Validator validator, int[] conversionCount) {
                int[] parameterTypes = new int[args.length];
                for (int i = 0; i < args.length; i++) {
                    if (validator.canConvert(
                            args[i], Category.Member, conversionCount)) {
                        parameterTypes[i] = Category.Member;
                        continue;
                    }
                    if (validator.canConvert(
                            args[i], Category.Set, conversionCount)) {
                        parameterTypes[i] = Category.Set;
                        continue;
                    }
                    if (validator.canConvert(
                            args[i], Category.Tuple, conversionCount)) {
                        parameterTypes[i] = Category.Tuple;
                        continue;
                    }
                    return null;
                }
                return new SetFunDef(this, parameterTypes);
            }
        });

        //
        // STRING FUNCTIONS
        define(new MultiResolver(
                "Format",
                "Format(<Numeric Expression>, <String Expression>)",
                "Formats a number to string.",
                new String[] { "fSmS", "fSnS" }) {
            protected FunDef createFunDef(final Exp[] args,
                                          final FunDef dummyFunDef) {
                final Locale locale = Locale.getDefault(); // todo: use connection's locale
                if (args[1] instanceof Literal) {
                    // Constant string expression: optimize by compiling
                    // format string.
                    String formatString = (String) ((Literal) args[1]).getValue();
                    final Format format = new Format(formatString, locale);
                    return new FunDefBase(dummyFunDef) {
                        public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                            final Calc calc =
                                    compiler.compileScalar(call.getArg(0), true);
                            return new AbstractStringCalc(call, new Calc[] {calc}) {
                                public String evaluateString(Evaluator evaluator) {
                                    final Object o = calc.evaluate(evaluator);
                                    return format.format(o);
                                }
                            };
                        }

                    };
                } else {
                    // Variable string expression
                    return new FunDefBase(dummyFunDef) {
                        public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                            final Calc calc =
                                    compiler.compileScalar(call.getArg(0), true);
                            final StringCalc stringCalc =
                                    compiler.compileString(call.getArg(1));
                            return new AbstractStringCalc(call, new Calc[] {calc, stringCalc}) {
                                public String evaluateString(Evaluator evaluator) {
                                    final Object o = calc.evaluate(evaluator);
                                    final String formatString =
                                            stringCalc.evaluateString(evaluator);
                                    final Format format =
                                            new Format(formatString, locale);
                                    return format.format(o);
                                }
                            };
                        }

                    };
                }
            }
        });

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

        define(new FunDefBase(
                "Item",
                "<Set>.Item(<Index>)",
                "Returns a tuple from the set specified in <Set>. The tuple to be returned is specified by the zero-based position of the tuple in the set in <Index>.",
                "mmxn") {
            public Type getResultType(Validator validator, Exp[] args) {
                SetType setType = (SetType) args[0].getType();
                return setType.getElementType();
            }

            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final ListCalc listCalc =
                        compiler.compileList(call.getArg(0));
                final IntegerCalc indexCalc =
                        compiler.compileInteger(call.getArg(1));
                final Type elementType = ((SetType) listCalc.getType()).getElementType();
                if (elementType instanceof TupleType) {
                    final TupleType tupleType = (TupleType) elementType;
                    final Member[] nullTuple = makeNullTuple(tupleType);
                    return new AbstractTupleCalc(call, new Calc[] {listCalc, indexCalc}) {
                        public Member[] evaluateTuple(Evaluator evaluator) {
                            final List list = listCalc.evaluateList(evaluator);
                            assert list != null;
                            final int index = indexCalc.evaluateInteger(evaluator);
                            int listSize = list.size();
                            if (index >= listSize || index < 0) {
                                return nullTuple;
                            } else {
                                return (Member[]) list.get(index);
                            }
                        }
                    };
                } else {
                    final MemberType memberType = (MemberType) elementType;
                    final Member nullMember = makeNullMember(memberType);
                    return new AbstractMemberCalc(call, new Calc[] {listCalc, indexCalc}) {
                        public Member evaluateMember(Evaluator evaluator) {
                            final List list = listCalc.evaluateList(evaluator);
                            assert list != null;
                            final int index = indexCalc.evaluateInteger(evaluator);
                            int listSize = list.size();
                            if (index >= listSize || index < 0) {
                                return nullMember;
                            } else {
                                return (Member) list.get(index);
                            }
                        }
                    };
                }
            }

            Object makeNullMember(Evaluator evaluator, Exp[] args) {
                final Type elementType = ((SetType) args[0].getType()).getElementType();
                return makeNullMemberOrTuple(elementType);
            }

            Object makeNullMemberOrTuple(final Type elementType) {
                if (elementType instanceof MemberType) {
                    MemberType memberType = (MemberType) elementType;
                    return makeNullMember(memberType);
                } else if (elementType instanceof TupleType) {
                    return makeNullTuple((TupleType) elementType);
                } else {
                    throw Util.newInternal("bad type " + elementType);
                }
            }


        });

        define(new FunDefBase(
                "Item",
                "<Tuple>.Item(<Index>)",
                "Returns a member from the tuple specified in <Tuple>. The member to be returned is specified by the zero-based position of the member in the set in <Index>.",
                "mmtn") {
            public Type getResultType(Validator validator, Exp[] args) {
                // Suppose we are called as follows:
                //   ([Gender].CurrentMember, [Store].CurrentMember).Item(n)
                //
                // We know that our result is a member type, but we don't
                // know which dimension.
                return MemberType.Unknown;
            }

            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                final Type type = call.getArg(0).getType();
                if (type instanceof MemberType) {
                    final MemberCalc memberCalc =
                            compiler.compileMember(call.getArg(0));
                    final IntegerCalc indexCalc =
                            compiler.compileInteger(call.getArg(1));
                    return new AbstractMemberCalc(call, new Calc[] {memberCalc, indexCalc}) {
                        public Member evaluateMember(Evaluator evaluator) {
                            final Member member =
                                    memberCalc.evaluateMember(evaluator);
                            final int index =
                                    indexCalc.evaluateInteger(evaluator);
                            if (index != 0) {
                                return null;
                            }
                            return member;
                        }
                    };
                } else {
                    final TupleCalc tupleCalc =
                            compiler.compileTuple(call.getArg(0));
                    final IntegerCalc indexCalc =
                            compiler.compileInteger(call.getArg(1));
                    return new AbstractMemberCalc(call, new Calc[] {tupleCalc, indexCalc}) {
                        final Member[] nullTupleMembers =
                                makeNullTuple((TupleType) tupleCalc.getType());
                        public Member evaluateMember(Evaluator evaluator) {
                            final Member[] members =
                                    tupleCalc.evaluateTuple(evaluator);
                            assert members == null ||
                                    members.length == nullTupleMembers.length;
                            final int index = indexCalc.evaluateInteger(evaluator);
                            if (members == null) {
                                return nullTupleMembers[index];
                            }
                            if (index >= members.length || index < 0) {
                                return null;
                            }
                            return members[index];
                        }
                    };
                }
            }

        });

        define(new FunDefBase(
                "StrToTuple",
                "StrToTuple(<String Expression>)",
                "Constructs a tuple from a string.",
                "ftS") {
            public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                throw Util.needToImplement(this);
            }

            public Exp createCall(Validator validator, Exp[] args) {
                final int argCount = args.length;
                if (argCount <= 1) {
                    throw MondrianResource.instance().MdxFuncArgumentsNum.ex(getName());
                }
                for (int i = 1; i < argCount; i++) {
                    final Exp arg = args[i];
                    if (arg instanceof DimensionExpr) {
                        // if arg is a dimension, switch to dimension's default
                        // hierarchy
                        DimensionExpr dimensionExpr = (DimensionExpr) arg;
                        Dimension dimension = dimensionExpr.getDimension();
                        args[i] = new HierarchyExpr(dimension.getHierarchy());
                    } else if (arg instanceof Hierarchy) {
                        // nothing
                    } else {
                        throw MondrianResource.instance().MdxFuncNotHier.ex(
                                new Integer(i + 1), getName());
                    }
                }
                return super.createCall(validator, args);
            }

            public Type getResultType(Validator validator, Exp[] args) {
                if (args.length == 1) {
                    // This is a call to the standard version of StrToTuple,
                    // which doesn't give us any hints about type.
                    return new TupleType(null);
                } else {
                    // This is a call to Mondrian's extended version of
                    // StrToTuple, of the form
                    //   StrToTuple(s, <Hier1>, ... , <HierN>)
                    //
                    // The result is a tuple
                    //  (<Hier1>, ... ,  <HierN>)
                    final ArrayList list = new ArrayList();
                    for (int i = 1; i < args.length; i++) {
                        Exp arg = args[i];
                        final Type type = arg.getType();
                        list.add(type);
                    }
                    final Type[] types =
                            (Type[]) list.toArray(new Type[list.size()]);
                    return new TupleType(types);
                }
            }
        });

        // special resolver for "()"
        define(new ResolverBase(
            "()",
            null,
            null,
            Syntax.Parentheses) {
            public FunDef resolve(
                    Exp[] args, Validator validator, int[] conversionCount) {
                // Compare with TupleFunDef.getReturnCategory().  For example,
                //   ([Gender].members) is a set,
                //   ([Gender].[M]) is a member,
                //   (1 + 2) is a numeric,
                // but
                //   ([Gender].[M], [Marital Status].[S]) is a tuple.
                if (args.length == 1) {
                    return new ParenthesesFunDef(args[0].getCategory());
                } else {
                    final int[] argTypes = new int[args.length];
                    Arrays.fill(argTypes, Category.Member);
                    return (FunDef) new TupleFunDef(argTypes);
                }
            }
        });

        //
        // GENERIC VALUE FUNCTIONS
        define(new ResolverBase(
                "CoalesceEmpty",
                "CoalesceEmpty(<Value Expression>[, <Value Expression>]...)",
                "Coalesces an empty cell value to a different value. All of the expressions must be of the same type (number or string).",
                Syntax.Function) {
            public FunDef resolve(
                    Exp[] args, Validator validator, int[] conversionCount) {
                if (args.length < 1) {
                    return null;
                }
                final int[] types = {Category.Numeric, Category.String};
                final int[] argTypes = new int[args.length];
                for (int j = 0; j < types.length; j++) {
                    int type = types[j];
                    int matchingArgs = 0;
                    conversionCount[0] = 0;
                    for (int i = 0; i < args.length; i++) {
                        if (validator.canConvert(args[i], type, conversionCount)) {
                            matchingArgs++;
                        }
                        argTypes[i] = type;
                    }
                    if (matchingArgs == args.length) {
                        return new CoalesceEmptyFunDef(this, type, argTypes);
                    }
                }
                return null;
            }

            public boolean requiresExpression(int k) {
                return true;
            }
        });

        define(new ResolverBase(
                "_CaseTest",
                "Case When <Logical Expression> Then <Expression> [...] [Else <Expression>] End",
                "Evaluates various conditions, and returns the corresponding expression for the first which evaluates to true.",
                Syntax.Case) {
            public FunDef resolve(
                    Exp[] args, Validator validator, int[] conversionCount) {
                if (args.length < 1) {
                    return null;
                }
                int j = 0;
                int clauseCount = args.length / 2;
                int mismatchingArgs = 0;
                int returnType = args[1].getCategory();
                for (int i = 0; i < clauseCount; i++) {
                    if (!validator.canConvert(args[j++], Category.Logical, conversionCount)) {
                        mismatchingArgs++;
                    }
                    if (!validator.canConvert(args[j++], returnType, conversionCount)) {
                        mismatchingArgs++;
                    }
                }
                if (j < args.length) {
                    if (!validator.canConvert(args[j++], returnType, conversionCount)) {
                        mismatchingArgs++;
                    }
                }
                Util.assertTrue(j == args.length);
                if (mismatchingArgs != 0) {
                    return null;
                }
                return new FunDefBase(this, returnType, ExpBase.getTypes(args)) {
                    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                        final Exp[] args = call.getArgs();
                        final BooleanCalc[] conditionCalcs =
                                new BooleanCalc[args.length / 2];
                        final Calc[] exprCalcs =
                                new Calc[args.length / 2];
                        final List calcList = new ArrayList();
                        for (int i = 0, j = 0; i < exprCalcs.length; i++) {
                            conditionCalcs[i] =
                                    compiler.compileBoolean(args[j++]);
                            calcList.add(conditionCalcs[i]);
                            exprCalcs[i] =
                                    compiler.compileScalar(args[j++], true);
                            calcList.add(exprCalcs[i]);
                        }
                        final Calc defaultCalc =
                                args.length % 2 == 1 ?
                                compiler.compileScalar(args[args.length - 1], true) :
                                ConstantCalc.constantNull(call.getType());
                        calcList.add(defaultCalc);
                        final Calc[] calcs = (Calc[])
                                calcList.toArray(new Calc[calcList.size()]);

                        return new AbstractCalc(call) {
                            public Object evaluate(Evaluator evaluator) {
                                for (int i = 0; i < conditionCalcs.length; i++) {
                                    if (conditionCalcs[i].evaluateBoolean(evaluator)) {
                                        return exprCalcs[i].evaluate(evaluator);
                                    }
                                }
                                return defaultCalc.evaluate(evaluator);
                            }

                            public Calc[] getCalcs() {
                                return calcs;
                            }
                        };
                    }

                };
            }

            public boolean requiresExpression(int k) {
                return true;
            }
        });

        define(new ResolverBase(
                "_CaseMatch",
                "Case <Expression> When <Expression> Then <Expression> [...] [Else <Expression>] End",
                "Evaluates various expressions, and returns the corresponding expression for the first which matches a particular value.",
                Syntax.Case) {
            public FunDef resolve(
                    Exp[] args, Validator validator, int[] conversionCount) {
                if (args.length < 3) {
                    return null;
                }
                int valueType = args[0].getCategory();
                int returnType = args[2].getCategory();
                int j = 0;
                int clauseCount = (args.length - 1) / 2;
                int mismatchingArgs = 0;
                if (!validator.canConvert(args[j++], valueType, conversionCount)) {
                    mismatchingArgs++;
                }
                for (int i = 0; i < clauseCount; i++) {
                    if (!validator.canConvert(args[j++], valueType, conversionCount)) {
                        mismatchingArgs++;
                    }
                    if (!validator.canConvert(args[j++], returnType, conversionCount)) {
                        mismatchingArgs++;
                    }
                }
                if (j < args.length) {
                    if (!validator.canConvert(args[j++], returnType, conversionCount)) {
                        mismatchingArgs++;
                    }
                }
                Util.assertTrue(j == args.length);
                if (mismatchingArgs != 0) {
                    return null;
                }
                return new FunDefBase(this, returnType, ExpBase.getTypes(args)) {
                    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                        final Exp[] args = call.getArgs();
                        final List calcList = new ArrayList();
                        final Calc valueCalc =
                                compiler.compileScalar(args[0], true);
                        calcList.add(valueCalc);
                        final int matchCount = (args.length - 1) / 2;
                        final Calc[] matchCalcs = new Calc[matchCount];
                        final Calc[] exprCalcs = new Calc[matchCount];
                        for (int i = 0, j = 1; i < exprCalcs.length; i++) {
                            matchCalcs[i] =
                                    compiler.compileScalar(args[j++], true);
                            calcList.add(matchCalcs[i]);
                            exprCalcs[i] =
                                    compiler.compileScalar(args[j++], true);
                            calcList.add(exprCalcs[i]);
                        }
                        final Calc defaultCalc =
                                args.length % 2 == 0 ?
                                compiler.compileScalar(args[args.length - 1], true) :
                                ConstantCalc.constantNull(call.getType());
                        calcList.add(defaultCalc);
                        final Calc[] calcs = (Calc[])
                                calcList.toArray(new Calc[calcList.size()]);

                        return new AbstractCalc(call) {
                            public Object evaluate(Evaluator evaluator) {

                                Object value = valueCalc.evaluate(evaluator);
                                for (int i = 0; i < matchCalcs.length; i++) {
                                    Object match = matchCalcs[i].evaluate(evaluator);
                                    if (match.equals(value)) {
                                        return exprCalcs[i].evaluate(evaluator);
                                    }
                                }
                                return defaultCalc.evaluate(evaluator);
                            }

                            public Calc[] getCalcs() {
                                return calcs;
                            }
                        };
                    }

                };
            }

            public boolean requiresExpression(int k) {
                return true;
            }
        });

        define(new PropertiesFunDef.Resolver());

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

        define(new MultiResolver(
                "FirstQ",
                "FirstQ(<Set>[, <Numeric Expression>])",
                "Returns the 1st quartile value of a numeric expression evaluated over a set.",
                new String[]{"fnx", "fnxn"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new AbstractAggregateFunDef(dummyFunDef) {
                    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                        final ListCalc listCalc =
                                compiler.compileList(call.getArg(0));
                        final DoubleCalc doubleCalc =
                                call.getArgCount() > 1 ?
                                compiler.compileDouble(call.getArg(1)) :
                                new ValueCalc(call);
                        return new AbstractDoubleCalc(call, new Calc[] {listCalc, doubleCalc}) {
                            public double evaluateDouble(Evaluator evaluator) {
                                List members = listCalc.evaluateList(evaluator);
                                return quartile(evaluator.push(), members, doubleCalc, 1);
                            }

                            public boolean dependsOn(Dimension dimension) {
                                return anyDependsButFirst(getCalcs(), dimension);
                            }
                        };
                    }
                };
            }
        });

        define(new MultiResolver(
                "ThirdQ",
                "ThirdQ(<Set>[, <Numeric Expression>])",
                "Returns the 3rd quartile value of a numeric expression evaluated over a set.",
                new String[]{"fnx", "fnxn"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new AbstractAggregateFunDef(dummyFunDef) {
                    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
                        final ListCalc listCalc =
                                compiler.compileList(call.getArg(0));
                        final DoubleCalc doubleCalc =
                                call.getArgCount() > 1 ?
                                compiler.compileDouble(call.getArg(1)) :
                                new ValueCalc(call);
                        return new AbstractDoubleCalc(call, new Calc[] {listCalc, doubleCalc}) {
                            public double evaluateDouble(Evaluator evaluator) {
                                List members = listCalc.evaluateList(evaluator);
                                return quartile(evaluator.push(), members, doubleCalc, 3);
                            }

                            public boolean dependsOn(Dimension dimension) {
                                return anyDependsButFirst(getCalcs(), dimension);
                            }
                        };
                    }
                };
            }
        });
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

    /** Returns (creating if necessary) the singleton. **/
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
     *
     * @param evaluator
     * @param level
     * @return
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

    static class StrToSetFunDef extends FunDefBase {
        public StrToSetFunDef(int[] parameterTypes) {
            super("StrToSet", "StrToSet(<String Expression>)",
                    "Constructs a set from a string expression.",
                    Syntax.Function, Category.Set, parameterTypes);
        }

        public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
            throw new UnsupportedOperationException();
        }

        public Exp createCall(Validator validator, Exp[] args) {
            final int argCount = args.length;
            if (argCount <= 1) {
                throw MondrianResource.instance().MdxFuncArgumentsNum.ex(getName());
            }
            for (int i = 1; i < argCount; i++) {
                final Exp arg = args[i];
                if (arg instanceof DimensionExpr) {
                    // if arg is a dimension, switch to dimension's default
                    // hierarchy
                    DimensionExpr dimensionExpr = (DimensionExpr) arg;
                    Dimension dimension = dimensionExpr.getDimension();
                    args[i] = new HierarchyExpr(dimension.getHierarchy());
                } else if (arg instanceof HierarchyExpr) {
                    // nothing
                } else {
                    throw MondrianResource.instance().MdxFuncNotHier.ex(
                            new Integer(i + 1), getName());
                }
            }
            return super.createCall(validator, args);
        }

        public Type getResultType(Validator validator, Exp[] args) {
            if (args.length == 1) {
                // This is a call to the standard version of StrToSet,
                // which doesn't give us any hints about type.
                return new SetType(null);
            } else {
                // This is a call to Mondrian's extended version of
                // StrToSet, of the form
                //   StrToSet(s, <Hier1>, ... , <HierN>)
                //
                // The result is a set of tuples
                //  (<Hier1>, ... ,  <HierN>)
                final ArrayList list = new ArrayList();
                for (int i = 1; i < args.length; i++) {
                    Exp arg = args[i];
                    final Type type = arg.getType();
                    list.add(type);
                }
                final Type[] types =
                        (Type[]) list.toArray(new Type[list.size()]);
                return new SetType(new TupleType(types));
            }
        }

        public static class Resolver extends ResolverBase {
            Resolver() {
                super(
                        "StrToSet",
                        "StrToSet(<String Expression>)",
                        "Constructs a set from a string expression.",
                        Syntax.Function);
            }

            public FunDef resolve(
                    Exp[] args, Validator validator, int[] conversionCount) {
                if (args.length < 1) {
                    return null;
                }
                Type type = args[0].getType();
                if (!(type instanceof StringType)) {
                    return null;
                }
                for (int i = 1; i < args.length; i++) {
                    Exp exp = args[i];
                    if (!(exp instanceof DimensionExpr)) {
                        return null;
                    }
                }
                int[] argTypes = new int[args.length];
                argTypes[0] = Category.String;
                for (int i = 1; i < argTypes.length; i++) {
                    argTypes[i] = Category.Hierarchy;
                }
                return new StrToSetFunDef(argTypes);
            }
        }
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

    public static class DimensionCurrentMemberFunDef extends FunDefBase {
        public DimensionCurrentMemberFunDef() {
            super("CurrentMember", "<Dimension>.CurrentMember",
                    "Returns the current member along a dimension during an iteration.",
                    "pmd");
        }

        public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
            final DimensionCalc dimensionCalc =
                    compiler.compileDimension(call.getArg(0));
            return new CalcImpl(call, dimensionCalc);
        }

        public static class CalcImpl extends AbstractMemberCalc {
            private final DimensionCalc dimensionCalc;

            public CalcImpl(Exp exp, DimensionCalc dimensionCalc) {
                super(exp, new Calc[] {dimensionCalc});
                this.dimensionCalc = dimensionCalc;
            }

            protected String getName() {
                return "CurrentMember";
            }

            public Member evaluateMember(Evaluator evaluator) {
                Dimension dimension =
                        dimensionCalc.evaluateDimension(evaluator);
                return evaluator.getContext(dimension);
            }

            public boolean dependsOn(Dimension dimension) {
                return dimensionCalc.getType().usesDimension(dimension, true) ;
            }
        }
    }

    public static class HierarchyCurrentMemberFunDef extends FunDefBase {
        public HierarchyCurrentMemberFunDef() {
            super("CurrentMember", "<Hierarchy>.CurrentMember",
                    "Returns the current member along a hierarchy during an iteration.",
                    "pmh");
        }

        public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
            final HierarchyCalc hierarchyCalc =
                    compiler.compileHierarchy(call.getArg(0));
            return new CalcImpl(call, hierarchyCalc);
        }

        public static class CalcImpl extends AbstractMemberCalc {
            private final HierarchyCalc hierarchyCalc;

            public CalcImpl(Exp exp, HierarchyCalc hierarchyCalc) {
                super(exp, new Calc[] {hierarchyCalc});
                this.hierarchyCalc = hierarchyCalc;
            }

            protected String getName() {
                return "CurrentMember";
            }

            public Member evaluateMember(Evaluator evaluator) {
                Hierarchy hierarchy =
                        hierarchyCalc.evaluateHierarchy(evaluator);
                Member member = evaluator.getContext(hierarchy.getDimension());
                // If the dimension has multiple hierarchies, and the current
                // member belongs to a different hierarchy, then this hierarchy
                // reverts to its default member.
                //
                // For example, if the current member of the [Time] dimension
                // is [Time.Weekly].[2003].[Week 4], then the current member
                // of the [Time.Monthly] hierarchy is its default member,
                // [Time.Monthy].[All].
                if (member.getHierarchy() != hierarchy) {
                    member = hierarchy.getDefaultMember();
                }
                return member;
            }

            public boolean dependsOn(Dimension dimension) {
                return hierarchyCalc.getType().usesDimension(dimension, true) ;
            }
        }
    }

    public static class HierarchyDimensionFunDef extends FunDefBase {
        public HierarchyDimensionFunDef() {
            super("Dimension", "<Hierarchy>.Dimension",
                    "Returns the dimension that contains a specified hierarchy.",
                    "pdh");
        }

        public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
            final HierarchyCalc hierarchyCalc =
                    compiler.compileHierarchy(call.getArg(0));
            return new CalcImpl(call, hierarchyCalc);
        }

        public static class CalcImpl extends AbstractDimensionCalc {
            private final HierarchyCalc hierarchyCalc;

            public CalcImpl(Exp exp, HierarchyCalc hierarchyCalc) {
                super(exp, new Calc[] {hierarchyCalc});
                this.hierarchyCalc = hierarchyCalc;
            }

            public Dimension evaluateDimension(Evaluator evaluator) {
                Hierarchy hierarchy =
                        hierarchyCalc.evaluateHierarchy(evaluator);
                return hierarchy.getDimension();
            }
        }
    }

    public static class MemberHierarchyFunDef extends FunDefBase {
        public MemberHierarchyFunDef() {
            super("Hierarchy", "<Member>.Hierarchy",
                    "Returns a member's hierarchy.", "phm");
        }

        public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
            final MemberCalc memberCalc =
                    compiler.compileMember(call.getArg(0));
            return new CalcImpl(call, memberCalc);
        }

        public static class CalcImpl extends AbstractHierarchyCalc {
            private final MemberCalc memberCalc;

            public CalcImpl(Exp exp, MemberCalc memberCalc) {
                super(exp, new Calc[] {memberCalc});
                this.memberCalc = memberCalc;
            }

            public Hierarchy evaluateHierarchy(Evaluator evaluator) {
                Member member = memberCalc.evaluateMember(evaluator);
                return member.getHierarchy();
            }
        }
    }

    public static class MemberLevelFunDef extends FunDefBase {
        public MemberLevelFunDef() {
            super("Level", "<Member>.Level", "Returns a member's level.", "plm");
        }

        public Type getResultType(Validator validator, Exp[] args) {
            final Type argType = args[0].getType();
            return LevelType.forType(argType);
        }

        public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
            final MemberCalc memberCalc =
                    compiler.compileMember(call.getArg(0));
            return new CalcImpl(call, memberCalc);
        }

        public static class CalcImpl extends AbstractLevelCalc {
            private final MemberCalc memberCalc;

            public CalcImpl(Exp exp, MemberCalc memberCalc) {
                super(exp, new Calc[] {memberCalc});
                this.memberCalc = memberCalc;
            }

            public Level evaluateLevel(Evaluator evaluator) {
                Member member = memberCalc.evaluateMember(evaluator);
                return member.getLevel();
            }
        }
    }

    public static class LevelHierarchyFunDef extends FunDefBase {
        public LevelHierarchyFunDef() {
            super("Hierarchy", "<Level>.Hierarchy",
                    "Returns a level's hierarchy.", "phl");
        }

        public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
            final LevelCalc levelCalc =
                    compiler.compileLevel(call.getArg(0));
            return new CalcImpl(call, levelCalc);
        }

        public static class CalcImpl extends AbstractHierarchyCalc {
            private final LevelCalc levelCalc;

            public CalcImpl(Exp exp, LevelCalc levelCalc) {
                super(exp, new Calc[] {levelCalc});
                this.levelCalc = levelCalc;
            }

            public Hierarchy evaluateHierarchy(Evaluator evaluator) {
                Level level = levelCalc.evaluateLevel(evaluator);
                return level.getHierarchy();
            }
        }
    }
}

// End BuiltinFunTable.java
