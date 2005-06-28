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

import mondrian.olap.FunDef;
import mondrian.olap.Evaluator;
import mondrian.olap.Exp;

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

    public Object evaluate(Evaluator evaluator, Exp[] args) {
        List left = (List) getArg(evaluator, args, 0);
        if (left == null) {
            left = Collections.EMPTY_LIST;
        }
        Collection right = (List) getArg(evaluator, args, 1);
        if (right == null) {
            right = Collections.EMPTY_LIST;
        } else {
            right = buildSearchableCollection(right);
        }
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
