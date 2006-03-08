/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2004-2002 Kana Software, Inc.
// Copyright (C) 2004-2005 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.fun;

import mondrian.olap.*;
import mondrian.calc.*;
import mondrian.calc.impl.AbstractListCalc;
import mondrian.mdx.ResolvedFunCall;

import java.util.*;

/**
 * Definition of the <code>INTERSECT</code> MDX function.
 */
class IntersectFunDef extends FunDefBase
{
    private final boolean all;

    public IntersectFunDef(FunDef dummyFunDef, boolean all)
    {
        super(dummyFunDef);
        this.all = all;
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final ListCalc listCalc1 = compiler.compileList(call.getArg(0));
        final ListCalc listCalc2 = compiler.compileList(call.getArg(1));
        // todo: optimize for member lists vs. tuple lists
        return new AbstractListCalc(call, new Calc[] {listCalc1, listCalc2}) {
            public List evaluateList(Evaluator evaluator) {
                List left = listCalc1.evaluateList(evaluator);
                if (left == null || left.isEmpty()) {
                    return Collections.EMPTY_LIST;
                }
                Collection right = listCalc2.evaluateList(evaluator);
                if (right == null || right.isEmpty()) {
                    return Collections.EMPTY_LIST;
                }
                right = buildSearchableCollection(right);
                List result = new ArrayList();

                for (Iterator i = left.iterator(); i.hasNext();) {
                    Object leftObject = i.next();
                    Object resultObject = leftObject;

                    if (leftObject instanceof Object[]) {
                        leftObject = new FunUtil.ArrayHolder((Object[])leftObject);
                    }

                    if (right.contains(leftObject)) {
                        if (all || !result.contains(leftObject)) {
                            result.add(resultObject);
                        }
                    }
                }
                return result;
            }
        };
    }

    private static Collection buildSearchableCollection(Collection right) {
        Iterator iter = right.iterator();
        Set result = new HashSet(right.size(), 1);
        while (iter.hasNext()) {
            Object element = iter.next();

            if (element instanceof Object[]) {
                element = new FunUtil.ArrayHolder((Object[])element);
            }

            result.add(element);
        }

        return result;
    }
}

// End IntersectFunDef.java
