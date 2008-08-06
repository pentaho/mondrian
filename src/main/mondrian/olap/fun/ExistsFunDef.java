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

import mondrian.olap.*;
import mondrian.calc.*;
import mondrian.calc.impl.AbstractListCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.type.*;

import java.util.*;

/**
 * Definition of the <code>EXISTS</code> MDX function.
 *
 * @author kvu
 * @version $Id$
 * @since Mar 23, 2008
 */
class ExistsFunDef extends FunDefBase
{
    static final Resolver resolver = new ReflectiveMultiResolver(
            "Exists",
            "Exists(<Set1>, <Set2>])",
            "Returns the the set of tuples of the first set that exist with one or more tuples of the second set.",
            new String[] {"fxxx"},
            ExistsFunDef.class);

    public ExistsFunDef(FunDef dummyFunDef)
    {
        super(dummyFunDef);
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final ListCalc listCalc1 = compiler.compileList(call.getArg(0));
        final ListCalc listCalc2 = compiler.compileList(call.getArg(1));

        return new AbstractListCalc(call, new Calc[] {listCalc1, listCalc2}) {
            public List evaluateList(Evaluator evaluator) {
                List left = listCalc1.evaluateList(evaluator);
                if (left == null || left.isEmpty()) {
                    return Collections.EMPTY_LIST;
                }
                List right = listCalc2.evaluateList(evaluator);
                if (right == null || right.isEmpty()) {
                    return Collections.EMPTY_LIST;
                }
                List result = new ArrayList();

                Object leftHead = left.get(0);
                Object rightHead = right.get(0);
                List<Dimension> leftDims = getDimensions(leftHead);
                List<Dimension> rightDims = getDimensions(rightHead);

                // map dimensions of right object to those in left object
                // return empty list if not all of right dims can be mapped
                int rightSize = rightDims.size();
                int [] idxmap = new int[rightSize];
                for (int i = 0; i < rightSize; i++) {
                    Dimension d = rightDims.get(i);
                    if (leftDims.contains(d)) {
                        idxmap[i] = leftDims.indexOf(d);
                    } else {
                        return Collections.EMPTY_LIST;
                    }
                }

                for (Object  leftObject : left) {
                    if (leftObject instanceof Object[]) { // leftObject is a tuple
                        boolean exist = true;
                        for (Object rightObject : right) {
                            for (int i = 0; i < rightSize; i++) {
                                Object [] leftObjs = (Object []) leftObject;
                                Member leftMem = (Member) leftObjs[idxmap[i]];
                                Member rightMem;
                                if (! (rightObject instanceof Object [])){
                                    rightMem = (Member) rightObject;
                                } else {
                                    Object [] rightObjs =
                                        (Object []) rightObject;
                                    rightMem = (Member) (rightObjs[i]);
                                }
                                if (! isOnSameHierarchyChain(
                                        leftMem, rightMem)) {
                                    exist = false;
                                    break;
                                }
                            }
                            if (exist) {
                                result.add(leftObject);
                                break;
                            }
                        }
                    } else { // leftObject is a member
                        for (Object rightObject : right) {
                            if (isOnSameHierarchyChain(
                                    (Member) leftObject,
                                    (Member) rightObject)) {
                                result.add(leftObject);
                                break;
                            }
                        }
                    }
                }
                return result;
            }
        };
    }

    private static boolean isOnSameHierarchyChain(Member mA, Member mB)
    {
        return (FunUtil.isAncestorOf(mA, mB, false))||
            (FunUtil.isAncestorOf(mB, mA, false));
    }

    private static List<Dimension> getDimensions(Object obj)
    {
        List<Dimension> dimensions = new ArrayList<Dimension>();

        if (obj instanceof Object []) {
            for (Object dim : (Object []) obj) {
                dimensions.add(((Member) dim).getDimension());
            }
        } else {
            dimensions.add(((Member) obj).getDimension());
        }
        return dimensions;
    }
}

// End ExistsFunDef.java
