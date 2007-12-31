/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2004-2002 Kana Software, Inc.
// Copyright (C) 2004-2007 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.fun;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import mondrian.olap.*;
import mondrian.olap.type.NumericType;
import mondrian.olap.type.NullType;
import mondrian.calc.*;
import mondrian.calc.impl.AbstractListCalc;
import mondrian.mdx.ResolvedFunCall;

/**
 * Definition of the <code>Descendants</code> MDX function.
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 23, 2006
 */
class DescendantsFunDef extends FunDefBase {

    static final ReflectiveMultiResolver Resolver = new ReflectiveMultiResolver(
            "Descendants",
            "Descendants(<Member>[, <Level>[, <Desc_flag>]])",
            "Returns the set of descendants of a member at a specified level, optionally including or excluding descendants in other levels.",
            new String[]{"fxm", "fxml", "fxmly", "fxmn", "fxmny", "fxmey"},
            DescendantsFunDef.class,
            Flag.getNames());

    public DescendantsFunDef(FunDef dummyFunDef) {
        super(dummyFunDef);

    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final MemberCalc memberCalc = compiler.compileMember(call.getArg(0));
        Flag flag = Flag.SELF;
        if (call.getArgCount() == 1) {
            flag = Flag.SELF_BEFORE_AFTER;
        }
        final boolean depthSpecified = call.getArgCount() >= 2 &&
            call.getArg(1).getType() instanceof NumericType;
        final boolean depthEmpty = call.getArgCount() >= 2 &&
            call.getArg(1).getType() instanceof NullType;
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
            final IntegerCalc depthCalc = depthSpecified ?
                compiler.compileInteger(call.getArg(1)) :
                null;
            return new AbstractListCalc(call, new Calc[] {memberCalc, depthCalc}) {
                public List evaluateList(Evaluator evaluator) {
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
                    hierarchize(result, false);
                    return result;
                }
            };
        } else if (depthSpecified) {
            final IntegerCalc depthCalc = call.getArgCount() > 1 ?
                    compiler.compileInteger(call.getArg(1)) :
                    null;
            final Flag flag1 = flag;
            return new AbstractListCalc(call, new Calc[] {memberCalc, depthCalc}) {
                public List evaluateList(Evaluator evaluator) {
                    final Member member = memberCalc.evaluateMember(evaluator);
                    List result = new ArrayList();
                    final int depth = depthCalc.evaluateInteger(evaluator);
                    final SchemaReader schemaReader = evaluator.getSchemaReader();
                    descendantsByDepth(
                            member, result, schemaReader,
                            depth, flag1.before, flag1.self, flag1.after, evaluator);
                    hierarchize(result, false);
                    return result;
                }
            };
        } else {
            final LevelCalc levelCalc = call.getArgCount() > 1 ?
                    compiler.compileLevel(call.getArg(1)) :
                    null;
            final Flag flag2 = flag;
            return new AbstractListCalc(call, new Calc[] {memberCalc, levelCalc}) {
                public List evaluateList(Evaluator evaluator) {
                    final Evaluator context =
                            evaluator.isNonEmpty() ? evaluator : null;
                    final Member member = memberCalc.evaluateMember(evaluator);
                    List<Member> result = new ArrayList<Member>();
                    final SchemaReader schemaReader = evaluator.getSchemaReader();
                    final Level level = levelCalc != null ?
                            levelCalc.evaluateLevel(evaluator) :
                            member.getLevel();
                    descendantsByLevel(
                            schemaReader, member, level, result,
                        flag2.before, flag2.self,
                        flag2.after, flag2.leaves, context);
                    hierarchize(result, false);
                    return result;
                }
            };
        }
    }

    private static void descendantsByDepth(
            Member member,
            List result,
            final SchemaReader schemaReader,
            final int depthLimitFinal,
            final boolean before,
            final boolean self,
            final boolean after,
            final Evaluator context) {
        Member[] children = {member};
        for (int depth = 0;; ++depth) {
            if (depth == depthLimitFinal) {
                if (self) {
                    addAll(result, children);
                }
                if (!after) {
                    break; // no more results after this level
                }
            } else if (depth < depthLimitFinal) {
                if (before) {
                    addAll(result, children);
                }
            } else {
                if (after) {
                    addAll(result, children);
                } else {
                    break; // no more results after this level
                }
            }

            children = schemaReader.getMemberChildren(children, context);
            if (children.length == 0) {
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
            final int depthLimit) {
        if (!schemaReader.isDrillable(member)) {
            if (depthLimit >= 0) {
                result.add(member);
            }
            return;
        }
        Member[] children = {member};
        for (int depth = 0; depthLimit == -1 || depth <= depthLimit; ++depth) {
            children = schemaReader.getMemberChildren(children);
            if (children.length == 0) {
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
            children = nextChildren.toArray(new Member[nextChildren.size()]);
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
            Evaluator context) {
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
        Member[] members = {ancestor};
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
                    Member[] childMembers =
                        schemaReader.getMemberChildren(member, context);
                    if (childMembers.length == 0) {
                        // this member is a leaf -- add it
                        if (currentDepth == levelDepth) {
                            result.add(member);
                        }
                        continue;
                    } else {
                        // this member is not a leaf -- add its children
                        // to the list to be considered next iteration
                        if (currentDepth <= levelDepth) {
                            nextMembers.addAll(Arrays.asList(childMembers));
                        }
                    }
                }
                members = nextMembers.toArray(new Member[nextMembers.size()]);
            }
            while (members.length > 0);
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
                members = new Member[fertileMembers.size()];
                members = fertileMembers.toArray(members);
                members = schemaReader.getMemberChildren(members, context);
            }
            while (members.length > 0);
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
