/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2004-2004 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.fun;

import mondrian.olap.*;

import java.util.List;
import java.util.ArrayList;

/**
 * Definition of the <code>DESCENDANTS</code> MDX function.
 */
class DescendantsFunDef extends FunDefBase
{
    private final int flagFinal;
    private final boolean depthSpecifiedFinal;
    private final int depthLimitFinal;

    public DescendantsFunDef(FunDef dummyFunDef, int flagFinal,
        boolean depthSpecifiedFinal, int depthLimitFinal) {
        super(dummyFunDef);
        this.flagFinal = flagFinal;
        this.depthSpecifiedFinal = depthSpecifiedFinal;
        this.depthLimitFinal = depthLimitFinal;
    }

    public Object evaluate(Evaluator evaluator, Exp[] args) {
        Member member = getMemberArg(evaluator, args, 0, true);
        final boolean self = FunUtil.checkFlag(flagFinal,
                Flags.SELF, true);
        final boolean after = FunUtil.checkFlag(flagFinal,
                Flags.AFTER, true);
        final boolean before = FunUtil.checkFlag(flagFinal,
                Flags.BEFORE, true);
//                        final boolean leaves = FunUtil.checkFlag(flagFinal,
//                                Flags.LEAVES, true);
        List result = new ArrayList();
        final SchemaReader schemaReader =
                evaluator.getSchemaReader();
        if (depthSpecifiedFinal) {
            descendantsByDepth(member, result, schemaReader,
                    depthLimitFinal, before, self, after);
        } else {
            final Level level = args.length > 1 ?
                getLevelArg(evaluator, args, 1, true) :
                member.getLevel();
            schemaReader.getMemberDescendants(member, result,
                    level, before, self, after);
        }

        hierarchize(result, false);

        return result;
    }

    private static void descendantsByDepth(Member member, List result,
            final SchemaReader schemaReader, final int depthLimitFinal,
            final boolean before, final boolean self, final boolean after) {
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
    static class Resolver extends MultiResolver
    {
        public Resolver()
        {
            super("Descendants",
                "Descendants(<Member>[, <Level>[, <Desc_flag>]])",
                "Returns the set of descendants of a member at a specified level, optionally including or excluding descendants in other levels.",
                new String[]{"fxm", "fxml", "fxmly", "fxmn", "fxmny"});
        }

        protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
            int depthLimit = -1; // unlimited
            boolean depthSpecified = false;
            int flag = Flags.SELF;
            if (args.length == 1) {
                depthLimit = -1;
                flag = Flags.SELF_BEFORE_AFTER;
            }
            if (args.length >= 2) {
                if (args[1] instanceof Literal) {
                    Literal literal = (Literal) args[1];
                    if (literal.getValue() instanceof Number) {
                        Number number = (Number) literal.getValue();
                        depthLimit = number.intValue();
                        depthSpecified = true;
                    }
                }
            }
            if (args.length >= 3) {
                flag = FunUtil.getLiteralArg(args, 2, Flags.SELF,
                    Flags.instance, dummyFunDef);
            }

            if (FunUtil.checkFlag(flag, Flags.LEAVES, true)) {
                // LEAVES isn't supported
                throw MondrianResource.instance().newLeavesNotSupported();
            }
            final int depthLimitFinal = depthLimit < 0 ?
                Integer.MAX_VALUE : depthLimit;
            final int flagFinal = flag;
            final boolean depthSpecifiedFinal = depthSpecified;
            return new DescendantsFunDef(dummyFunDef, flagFinal,
                depthSpecifiedFinal, depthLimitFinal);
        }
    }
}
