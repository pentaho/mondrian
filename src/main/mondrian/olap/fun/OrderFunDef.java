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

import java.util.List;

/**
 * Definition of the <code>ORDER</code> MDX function.
 */
class OrderFunDef extends FunDefBase
{
    private final boolean desc;
    private final boolean brk;

    public OrderFunDef(FunDef dummyFunDef, boolean desc, boolean brk)
    {
        super(dummyFunDef);
        this.desc = desc;
        this.brk = brk;
    }

    public Object evaluate(Evaluator evaluator, Exp[] args) {
        List members = (List) getArg(evaluator, args, 0);
        ExpBase exp = (ExpBase) getArgNoEval(args, 1);
        sort(evaluator, members, exp, desc, brk);
        return members;
    }

    /**
     * Resolves calls to the <code>ORDER</code> MDX function.
     */
    static class OrderResolver extends MultiResolver
    {
        public OrderResolver()
        {
            super("Order",
                "Order(<Set>, <Value Expression>[, ASC | DESC | BASC | BDESC])",
                "Arranges members of a set, optionally preserving or breaking the hierarchy.",
                new String[]{"fxxvy", "fxxv"});
        }

        public String[] getReservedWords() {
            return OrderFunDef.Flags.instance.getNames();
        }

        protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
            int order = getLiteralArg(args, 2, Flags.ASC, Flags.instance, dummyFunDef);
            final boolean desc = Flags.isDescending(order);
            final boolean brk = Flags.isBreak(order);
            return new OrderFunDef(dummyFunDef, desc, brk);
        }
    }

    /**
     * Enumeration of the flags allowed to the <code>ORDER</code> MDX function.
     */
    static class Flags extends EnumeratedValues {
        static final Flags instance = new Flags();
        private Flags() {
            super(new String[] {"ASC","DESC","BASC","BDESC"});
        }
        public static final int ASC = 0;
        public static final int DESC = 1;
        public static final int BASC = 2;
        public static final int BDESC = 3;
        public static final boolean isDescending(int value) {
            return (value & DESC) == DESC;
        }
        public static final boolean isBreak(int value) {
            return (value & BASC) == BASC;
        }
    }
}
