/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2002-2005 Julian Hyde
// Copyright (C) 2005-2011 Pentaho and others
// All Rights Reserved.
*/
package mondrian.olap.fun;

import mondrian.calc.*;
import mondrian.calc.impl.AbstractListCalc;
import mondrian.calc.impl.UnaryTupleList;
import mondrian.mdx.*;
import mondrian.olap.*;
import mondrian.olap.type.*;
import mondrian.resource.MondrianResource;

import java.util.*;

/**
 * Definition of the <code>Descendants</code> MDX function.
 *
 * @author jhyde
 * @since Mar 23, 2006
 */
class DescendantsFunDef extends FunDefBase {

    static final ReflectiveMultiResolver Resolver =
        new ReflectiveMultiResolver(
            "Descendants",
            "Descendants(<Member>[, <Level>[, <Desc_flag>]])",
            "Returns the set of descendants of a member at a specified level, optionally including or excluding descendants in other levels.",
            new String[]{"fxm", "fxml", "fxmly", "fxmn", "fxmny", "fxmey"},
            DescendantsFunDef.class,
            Flag.getNames());

    static final ReflectiveMultiResolver Resolver2 =
        new ReflectiveMultiResolver(
            "Descendants",
            "Descendants(<Set>[, <Level>[, <Desc_flag>]])",
            "Returns the set of descendants of a set of members at a specified level, optionally including or excluding descendants in other levels.",
            new String[]{"fxx", "fxxl", "fxxly", "fxxn", "fxxny", "fxxey"},
            DescendantsFunDef.class,
            Flag.getNames());

    public DescendantsFunDef(FunDef dummyFunDef) {
        super(dummyFunDef);
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final Type type0 = call.getArg(0).getType();
        if (type0 instanceof SetType) {
            final SetType setType = (SetType) type0;
            if (setType.getElementType() instanceof TupleType) {
                throw MondrianResource.instance()
                    .DescendantsAppliedToSetOfTuples.ex();
            }

            MemberType memberType = (MemberType) setType.getElementType();
            final Hierarchy hierarchy = memberType.getHierarchy();
            if (hierarchy == null) {
                throw MondrianResource.instance().CannotDeduceTypeOfSet.ex();
            }
            // Convert
            //   Descendants(<set>, <args>)
            // into
            //   Generate(<set>, Descendants(<dimension>.CurrentMember, <args>))
            Exp[] descendantsArgs = call.getArgs().clone();
            descendantsArgs[0] =
                new UnresolvedFunCall(
                    "CurrentMember",
                    Syntax.Property,
                    new Exp[] {
                        new HierarchyExpr(hierarchy)
                    });
            final ResolvedFunCall generateCall =
                (ResolvedFunCall) compiler.getValidator().validate(
                    new UnresolvedFunCall(
                        "Generate",
                        new Exp[] {
                            call.getArg(0),
                            new UnresolvedFunCall(
                                "Descendants",
                                descendantsArgs)
                        }),
                    false);
            return generateCall.accept(compiler);
        }

        final MemberCalc memberCalc = compiler.compileMember(call.getArg(0));
        Flag flag = Flag.SELF;
        if (call.getArgCount() == 1) {
            flag = Flag.SELF_BEFORE_AFTER;
        }
        final boolean depthSpecified =
            call.getArgCount() >= 2
            && call.getArg(1).getType() instanceof NumericType;
        final boolean depthEmpty =
            call.getArgCount() >= 2
            && call.getArg(1).getType() instanceof EmptyType;
        if (call.getArgCount() >= 3) {
            flag = FunUtil.getLiteralArg(call, 2, Flag.SELF, Flag.class);
        }

        if (call.getArgCount() >= 2 && depthEmpty) {
            if (flag != Flag.LEAVES) {
                throw Util.newError(
                    "depth must be specified unless DESC_FLAG is LEAVES");
            }
        }
        if ((depthSpecified || depthEmpty) && flag.leaves) {
            final IntegerCalc depthCalc =
                depthSpecified
                ? compiler.compileInteger(call.getArg(1))
                : null;
            return new AbstractListCalc(
                call, new Calc[] {memberCalc, depthCalc})
            {
                public TupleList evaluateList(Evaluator evaluator) {
                    final Member member = memberCalc.evaluateMember(evaluator);
                    List<Member> result = new ArrayList<Member>();
                    int depth = -1;
                    if (depthCalc != null) {
                        depth = depthCalc.evaluateInteger(evaluator);
                        if (depth < 0) {
                            depth = -1; // no limit
                        }
                    }
                    final SchemaReader schemaReader =
                        evaluator.getSchemaReader();
                    descendantsLeavesByDepth(
                        member, result, schemaReader, depth);
                    hierarchizeMemberList(result, false);
                    return new UnaryTupleList(result);
                }
            };
        } else if (depthSpecified) {
            final IntegerCalc depthCalc =
                compiler.compileInteger(call.getArg(1));
            final Flag flag1 = flag;
            return new AbstractListCalc(
                call, new Calc[] {memberCalc, depthCalc})
            {
                public TupleList evaluateList(Evaluator evaluator) {
                    final Member member = memberCalc.evaluateMember(evaluator);
                    List<Member> result = new ArrayList<Member>();
                    final int depth = depthCalc.evaluateInteger(evaluator);
                    final SchemaReader schemaReader =
                        evaluator.getSchemaReader();
                    descendantsByDepth(
                        member, result, schemaReader,
                        depth, flag1.before, flag1.self, flag1.after,
                        evaluator);
                    hierarchizeMemberList(result, false);
                    return new UnaryTupleList(result);
                }
            };
        } else {
            final LevelCalc levelCalc =
                call.getArgCount() > 1
                ? compiler.compileLevel(call.getArg(1))
                : null;
            final Flag flag2 = flag;
            return new AbstractListCalc(
                call, new Calc[] {memberCalc, levelCalc})
            {
                public TupleList evaluateList(Evaluator evaluator) {
                    final Evaluator context =
                            evaluator.isNonEmpty() ? evaluator : null;
                    final Member member = memberCalc.evaluateMember(evaluator);
                    List<Member> result = new ArrayList<Member>();
                    final SchemaReader schemaReader =
                        evaluator.getSchemaReader();
                    final Level level =
                        levelCalc != null
                        ? levelCalc.evaluateLevel(evaluator)
                        : member.getLevel();
                    descendantsByLevel(
                        schemaReader, member, level, result,
                        flag2.before, flag2.self,
                        flag2.after, flag2.leaves, context);
                    hierarchizeMemberList(result, false);
                    return new UnaryTupleList(result);
                }
            };
        }
    }

    private static void descendantsByDepth(
        Member member,
        List<Member> result,
        final SchemaReader schemaReader,
        final int depthLimitFinal,
        final boolean before,
        final boolean self,
        final boolean after,
        Evaluator context)
    {
        if (!context.isNonEmpty()) {
            context = null;
        }
        List<Member> children = new ArrayList<Member>();
        children.add(member);
        for (int depth = 0;; ++depth) {
            if (depth == depthLimitFinal) {
                if (self) {
                    result.addAll(children);
                }
                if (!after) {
                    break; // no more results after this level
                }
            } else if (depth < depthLimitFinal) {
                if (before) {
                    result.addAll(children);
                }
            } else {
                if (after) {
                    result.addAll(children);
                } else {
                    break; // no more results after this level
                }
            }

            children = schemaReader.getMemberChildren(children, context);
            if (children.size() == 0) {
                break;
            }
        }
    }

    /**
     * Populates 'result' with the descendants at the leaf level at depth
     * 'depthLimit' or less. If 'depthLimit' is -1, does not apply a depth
     * constraint.
     */
    private static void descendantsLeavesByDepth(
        final Member member,
        final List<Member> result,
        final SchemaReader schemaReader,
        final int depthLimit)
    {
        if (!schemaReader.isDrillable(member)) {
            if (depthLimit >= 0) {
                result.add(member);
            }
            return;
        }
        List<Member> children = new ArrayList<Member>();
        children.add(member);
        for (int depth = 0; depthLimit == -1 || depth <= depthLimit; ++depth) {
            children = schemaReader.getMemberChildren(children);
            if (children.size() == 0) {
                throw Util.newInternal("drillable member must have children");
            }
            List<Member> nextChildren = new ArrayList<Member>();
            for (Member child : children) {
                // TODO: Implement this more efficiently. The current
                // implementation of isDrillable for a parent-child hierarchy
                // simply retrieves the children sees whether there are any,
                // so we end up fetching each member's children twice.
                if (schemaReader.isDrillable(child)) {
                    nextChildren.add(child);
                } else {
                    result.add(child);
                }
            }
            if (nextChildren.isEmpty()) {
                return;
            }
            children = nextChildren;
        }
    }

    /**
     * Finds all descendants of a member which are before/at/after a level,
     * and/or are leaves (have no descendants) and adds them to a result list.
     *
     * @param schemaReader Member reader
     * @param ancestor Member to find descendants of
     * @param level Level relative to which to filter, must not be null
     * @param result Result list
     * @param before Whether to output members above <code>level</code>
     * @param self Whether to output members at <code>level</code>
     * @param after Whether to output members below <code>level</code>
     * @param leaves Whether to output members which are leaves
     * @param context Evaluation context; determines criteria by which the
     *    result set should be filtered
     */
    static void descendantsByLevel(
        SchemaReader schemaReader,
        Member ancestor,
        Level level,
        List<Member> result,
        boolean before,
        boolean self,
        boolean after,
        boolean leaves,
        Evaluator context)
    {
        // We find the descendants of a member by making breadth-first passes
        // down the hierarchy. Initially the list just contains the ancestor.
        // Then we find its children. We add those children to the result if
        // they fulfill the before/self/after conditions relative to the level.
        //
        // We add a child to the "fertileMembers" list if some of its children
        // might be in the result. Again, this depends upon the
        // before/self/after conditions.
        //
        // Note that for some member readers -- notably the
        // RestrictedMemberReader, when it is reading a ragged hierarchy -- the
        // children of a member do not always belong to the same level. For
        // example, the children of USA include WA (a state) and Washington
        // (a city). This is why we repeat the before/self/after logic for
        // each member.
        final int levelDepth = level.getDepth();
        List<Member> members = Collections.singletonList(ancestor);
        // Each pass, "fertileMembers" has the same contents as "members",
        // except that we omit members whose children we are not interested
        // in. We allocate it once, and clear it each pass, to save a little
        // memory allocation.
        if (leaves) {
            assert !before && !self && !after;
            do {
                List<Member> nextMembers = new ArrayList<Member>();
                for (Member member : members) {
                    final int currentDepth = member.getLevel().getDepth();
                    List<Member> childMembers =
                        schemaReader.getMemberChildren(member, context);
                    if (childMembers.size() == 0) {
                        // this member is a leaf -- add it
                        if (currentDepth == levelDepth) {
                            result.add(member);
                        }
                    } else {
                        // this member is not a leaf -- add its children
                        // to the list to be considered next iteration
                        if (currentDepth <= levelDepth) {
                            nextMembers.addAll(childMembers);
                        }
                    }
                }
                members = nextMembers;
            } while (members.size() > 0);
        } else {
            List<Member> fertileMembers = new ArrayList<Member>();
            do {
                fertileMembers.clear();
                for (Member member : members) {
                    final int currentDepth = member.getLevel().getDepth();
                    if (currentDepth == levelDepth) {
                        if (self) {
                            result.add(member);
                        }
                        if (after) {
                            // we are interested in member's children
                            fertileMembers.add(member);
                        }
                    } else if (currentDepth < levelDepth) {
                        if (before) {
                            result.add(member);
                        }
                        fertileMembers.add(member);
                    } else {
                        if (after) {
                            result.add(member);
                            fertileMembers.add(member);
                        }
                    }
                }
                members =
                    schemaReader.getMemberChildren(fertileMembers, context);
            } while (members.size() > 0);
        }
    }

    /**
     * Enumeration of the flags allowed to the <code>DESCENDANTS</code>
     * function.
     */
    enum Flag {
        SELF(true, false, false, false),
        AFTER(false, true, false, false),
        BEFORE(false, false, true, false),
        BEFORE_AND_AFTER(false, true, true, false),
        SELF_AND_AFTER(true, true, false, false),
        SELF_AND_BEFORE(true, false, true, false),
        SELF_BEFORE_AFTER(true, true, true, false),
        LEAVES(false, false, false, true);

        private final boolean self;
        private final boolean after;
        private final boolean before;
        private final boolean leaves;

        Flag(boolean self, boolean after, boolean before, boolean leaves) {
            this.self = self;
            this.after = after;
            this.before = before;
            this.leaves = leaves;
        }

        public static String[] getNames() {
            List<String> names = new ArrayList<String>();
            for (Flag flags : Flag.class.getEnumConstants()) {
                names.add(flags.name());
            }
            return names.toArray(new String[names.size()]);
        }
    }
}

// End DescendantsFunDef.java
