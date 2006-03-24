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
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 23, 2006
 */
class IntersectFunDef extends FunDefBase
{
    private static final String[] ReservedWords = new String[] {"ALL"};

    static final Resolver resolver = new ReflectiveMultiResolver(
            "Intersect",
            "Intersect(<Set1>, <Set2>[, ALL])",
            "Returns the intersection of two input sets, optionally retaining duplicates.",
            new String[] {"fxxxy", "fxxx"},
            IntersectFunDef.class,
            ReservedWords);
    
    public IntersectFunDef(FunDef dummyFunDef)
    {
        super(dummyFunDef);
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final ListCalc listCalc1 = compiler.compileList(call.getArg(0));
        final ListCalc listCalc2 = compiler.compileList(call.getArg(1));
        final String literalArg = getLiteralArg(call, 2, "", ReservedWords);
        final boolean all = literalArg.equalsIgnoreCase("ALL");

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
