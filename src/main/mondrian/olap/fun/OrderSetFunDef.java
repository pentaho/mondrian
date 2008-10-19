/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2004-2002 Kana Software, Inc.
// Copyright (C) 2004-2008 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.fun;

import mondrian.calc.*;
import mondrian.calc.impl.*;
import mondrian.olap.*;
import mondrian.olap.type.*;
import mondrian.mdx.*;
import mondrian.olap.fun.FunUtil.Flag;

import java.util.*;

/**
 * Definition of the <code>OrderSet</code> MDX function.
 *
 * @author kvu
 * @version $Id$
 * @since Oct 18, 2008
 */
class OrderSetFunDef extends FunDefBase {

    static final ResolverImpl Resolver = new ResolverImpl();

    public OrderSetFunDef(ResolverBase resolverBase, int type, int[] types) {
        super(resolverBase, type, types);
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        List<SortKeySpec> keySpecList = new ArrayList<SortKeySpec>();
        buildKeySpecList(keySpecList, call, compiler);

        final int keySpecCount = keySpecList.size();
        Calc[] calcList = new Calc[keySpecCount + 1]; // +1 for the listCalc

        final ListCalc listCalc = compiler.compileList(call.getArg(0), true);
        calcList[0] = listCalc;
        for (int i = 0; i < keySpecCount; i++) {
            final Calc expCalc = keySpecList.get(i).getKey();
            calcList[i + 1] = expCalc;
        }
        return new CalcImpl(call, calcList, keySpecList);
    }

    private void buildKeySpecList(
        List<SortKeySpec> keySpecList, ResolvedFunCall call, ExpCompiler compiler)
    {
        final int argCount = call.getArgs().length;
        int j = 1; // args[0] is the input set
        Calc key;
        boolean isMemValExp;
        Flag dir;
        Exp arg;
        while (j < argCount) {
            arg = call.getArg(j);
            if ((arg.getType() instanceof MemberType) &&
                (!arg.getType().getDimension().isMeasures()))
            {
                key = compiler.compileMember(arg);
                isMemValExp = true;
            } else {
                key = compiler.compileScalar(arg, true);
                isMemValExp = false;
            }
            j++;
            if ((j >= argCount) ||
                (call.getArg(j).getCategory() !=  Category.Symbol)) {
                dir = Flag.ASC;
            } else {
                dir = getLiteralArg(call, j, Flag.ASC, Flag.class);
                j++;
            }
            keySpecList.add(new SortKeySpec(key, isMemValExp, dir));
        }
    }

    private static class CalcImpl extends AbstractListCalc {
        private final ListCalc listCalc;
        private final Calc[] sortKeyCalcList;
        private final List<SortKeySpec> keySpecList;
        private final int originalKeySpecCount;

        public CalcImpl(
            ResolvedFunCall call,
            Calc[] calcList,
            List<SortKeySpec> keySpecList)
        {
            super(call, calcList, false);
            this.listCalc = (ListCalc) calcList[0];
            assert listCalc.getResultStyle() == ResultStyle.MUTABLE_LIST;
            this.sortKeyCalcList = calcList;
            this.keySpecList = keySpecList;
            this.originalKeySpecCount = keySpecList.size();
        }

        public List evaluateList(Evaluator evaluator) {
            List list = listCalc.evaluateList(evaluator);
            // go by size of keySpecList before purging
            purgeKeySpecList(keySpecList, list);
            if (!keySpecList.isEmpty()) {
                sortMembers(evaluator.push(), list, keySpecList);
            }
            return list;
        }

        public Calc[] getCalcs() {
            return sortKeyCalcList;
        }

        public boolean dependsOn(Dimension dimension) {
            return anyDependsButFirst(getCalcs(), dimension);
        }

        private void purgeKeySpecList(
            List<SortKeySpec> keySpecList, List list)
        {
            if (list == null || list.isEmpty()) {
                return;
            }
            //if (keySpecList.size() == 1) {
            //    return;
            //}
            Object head = list.get(0);
            List<Dimension> listDimensions = new ArrayList<Dimension>();
            if (head instanceof Object []) {
                for (Object dim : (Object []) head) {
                    listDimensions.add(((Member) dim).getDimension());
                }
            } else {
                listDimensions.add(((Member) head).getDimension());
            }
            // do not sort (remove sort key spec from the list) if
            // 1. <member_value_expression> evaluates to a member from a
            // level/dimension which is not used in the first argument
            // 2. <member_value_expression> evaluates to the same member for
            // all cells; for example, a report showing all quarters of
            // year 1998 will not be sorted if the sort key is on the constant
            // member [1998].[Q1]
            Iterator iter = keySpecList.listIterator();
            while (iter.hasNext()) {
                SortKeySpec key = (SortKeySpec) iter.next();
                Calc expCalc = key.getKey();
                if (key.isMemberValueExp()) {
                    if (expCalc instanceof ConstantCalc ||
                        !listDimensions.contains(expCalc.getType().getDimension()))
                    {
                        iter.remove();
                    }
                }
            }
        }
    }

    private static class ResolverImpl extends ResolverBase {
        private final String[] reservedWords;
        static int[] argTypes;

        private ResolverImpl() {
            super(
                    "OrderSet",
                    "OrderSet(<Set> {, <Key Specification>}...)",
                    "Arranges members of a set, optionally preserving or breaking the hierarchy.",
                    Syntax.Function);
            this.reservedWords = Flag.getNames();
        }

        public FunDef resolve(
                Exp[] args, Validator validator, int[] conversionCount)
        {
            argTypes = new int[args.length];

            if (args.length < 2) {
                return null;
            }
            // first arg must be a set
            if (!validator.canConvert(
                args[0], Category.Set, conversionCount))
            {
                return null;
            }
            argTypes[0] = Category.Set;
            // after fist args, should be: value [, symbol]
            int i = 1;
            while (i < args.length) {
                if (!validator.canConvert(
                    args[i], Category.Value, conversionCount))
                {
                    return null;
                } else {
                    argTypes[i] = Category.Value;
                    i++;
                }
                // if symbol is not specified, skip to the next
                if ((i == args.length)) {
                    //done, will default last arg to ASC
                } else {
                    if (!validator.canConvert(
                        args[i], Category.Symbol, conversionCount))
                    {
                        // continue, will default sort flag for prev arg to ASC
                    } else {
                        argTypes[i] = Category.Symbol;
                        i++;
                    }
                }
            }
            return new OrderSetFunDef(this, Category.Set, argTypes);
        }

        public String[] getReservedWords() {
            if (reservedWords != null) {
                return reservedWords;
            }
            return super.getReservedWords();
        }
    }
}

// End OrderSetFunDef.java
