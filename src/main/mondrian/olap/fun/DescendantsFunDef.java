/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2004-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.fun;

import mondrian.olap.*;
import mondrian.olap.type.NumericType;

import java.util.List;
import java.util.ArrayList;

/**
 * Definition of the <code>DESCENDANTS</code> MDX function.
 */
class DescendantsFunDef extends FunDefBase {
    private final boolean self;
    private final boolean before;
    private final boolean after;
    private final boolean depthSpecified;
    private final boolean leaves;

    public DescendantsFunDef(
            FunDef dummyFunDef,
            int flag,
            boolean depthSpecified) {
        super(dummyFunDef);

        this.self = FunUtil.checkFlag(flag, Flags.SELF, true);
        this.after = FunUtil.checkFlag(flag, Flags.AFTER, true);
        this.before = FunUtil.checkFlag(flag, Flags.BEFORE, true);
        this.leaves = FunUtil.checkFlag(flag, Flags.LEAVES, true);
        this.depthSpecified = depthSpecified;
    }

    public Object evaluate(Evaluator evaluator, Exp[] args) {
        Member member = getMemberArg(evaluator, args, 0, true);
        List result = new ArrayList();
        final SchemaReader schemaReader = evaluator.getSchemaReader();
        if (depthSpecified) {
            int depthLimit = getIntArg(evaluator, args, 1);
            if (leaves) {
                if (depthLimit < 0) {
                    depthLimit = Integer.MAX_VALUE;
                }
                descendantsLeavesByDepth(
                        member, result, schemaReader, depthLimit);
            } else {
                descendantsByDepth(
                        member, result, schemaReader,
                        depthLimit, before, self, after);
            }
        } else {
            final Level level = args.length > 1
                ?  getLevelArg(evaluator, args, 1, true)
                : member.getLevel();
            schemaReader.getMemberDescendants(
                    member, result, level, before, self, after, leaves);
        }

        hierarchize(result, false);

        return result;
    }

    private static void descendantsByDepth(
            Member member,
            List result,
            final SchemaReader schemaReader,
            final int depthLimitFinal,
            final boolean before,
            final boolean self,
            final boolean after) {
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

            children = schemaReader.getMemberChildren(children);
            if (children.length == 0) {
                break;
            }
        }
    }

    private static void descendantsLeavesByDepth(
            Member member,
            List result,
            final SchemaReader schemaReader,
            final int depthLimit) {
        if (!schemaReader.isDrillable(member)) {
            if (depthLimit >= 0) {
                result.add(member);
            }
            return;
        }
        Member[] children = {member};
        for (int depth = 0; depth <= depthLimit; ++depth) {
            children = schemaReader.getMemberChildren(children);
            if (children.length == 0) {
                throw Util.newInternal("drillable member must have children");
            }
            List nextChildren = new ArrayList();
            for (int i = 0; i < children.length; i++) {
                Member child = children[i];
                if (schemaReader.isDrillable(child)) {
                    nextChildren.add(child);
                } else {
                    result.add(child);
                }
            }
            if (nextChildren.isEmpty()) {
                break;
            }
            children = (Member[])
                    nextChildren.toArray(new Member[nextChildren.size()]);
        }
    }

    /**
     * Enumeration of the flags allowed to the <code>DESCENDANTS</code>
     * function.
     */
    static class Flags extends EnumeratedValues {
        static final Flags instance = new Flags();
        private Flags() {
            super(
                new String[] {
                    "SELF", "AFTER", "BEFORE", "BEFORE_AND_AFTER",
                    "SELF_AND_AFTER", "SELF_AND_BEFORE","SELF_BEFORE_AFTER",
                    "LEAVES"},
                new int[] {
                    SELF, AFTER, BEFORE, BEFORE_AND_AFTER,
                    SELF_AND_AFTER, SELF_AND_BEFORE, SELF_BEFORE_AFTER,
                    LEAVES});
        }
        public static final int SELF = 1;
        public static final int AFTER = 2;
        public static final int BEFORE = 4;
        public static final int BEFORE_AND_AFTER = BEFORE | AFTER;
        public static final int SELF_AND_AFTER = SELF | AFTER;
        public static final int SELF_AND_BEFORE = SELF | BEFORE;
        public static final int SELF_BEFORE_AFTER = SELF | BEFORE | AFTER;
        public static final int LEAVES = 8;
    }

    /**
     * Resolves calls to the <code>DESCENDANTS</code> function.
     */
    static class Resolver extends MultiResolver {
        public Resolver() {
            super("Descendants",
                "Descendants(<Member>[, <Level>[, <Desc_flag>]])",
                "Returns the set of descendants of a member at a specified level, optionally including or excluding descendants in other levels.",
                new String[]{"fxm", "fxml", "fxmly", "fxmn", "fxmny"});
        }

        protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
            int flag = Flags.SELF;
            if (args.length == 1) {
                flag = Flags.SELF_BEFORE_AFTER;
            }
            final boolean depthSpecified;
            if (args.length >= 2) {
                depthSpecified = args[1].getTypeX() instanceof NumericType;
            } else {
                depthSpecified = false;
            }
            if (args.length >= 3) {
                flag = FunUtil.getLiteralArg(args, 2, Flags.SELF,
                    Flags.instance, dummyFunDef);
            }

            return new DescendantsFunDef(
                    dummyFunDef,
                    flag,
                    depthSpecified);
        }

        public String[] getReservedWords() {
            return Flags.instance.getNames();
        }
    }
}

// End DescendantsFunDef.java
