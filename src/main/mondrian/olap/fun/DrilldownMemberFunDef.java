/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2004-2004 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.fun;

import mondrian.olap.*;

import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;

/**
 * Definition of the "DrilldownMember" MDX function.
 *
 * @author Grzegorz Lojek
 * @since 6 December, 2004
 * @version $Id$
 */
class DrilldownMemberFunDef extends FunDefBase {
    private final boolean recursive;
    static final String[] reservedNames = new String[] {"RECURSIVE"};

    DrilldownMemberFunDef(FunDef funDef, boolean recursive) {
        super(funDef);
        this.recursive = recursive;
    }

	/**
	 * Drills down an element.
     *
     * Algorithm: If object is present in a_hsSet1 then adds to result children
     * of the object. If flag a_bRecursive is set then this method is called
     * recursively for the children.
     *
     * @param element Element of a set, can be either {@link Member} or
     *   {@link Member}[]
     *
	 *
	 */
	protected void drillDownObj(Evaluator evaluator,
        Object element,
        HashSet memberSet,
        List resultList)
    {
		if (null == element) {
            return;
        }

		Member m = null;
		int k = -1;
		if (element instanceof Member) {
			if (!memberSet.contains(element)) {
                return;
            }
			m = (Member) element;
		} else  {
			Util.assertTrue(element instanceof Member[]);
			Member[] members = (Member[]) element;
			for (int j = 0; j < members.length; j++) {
				Member member = members[j];
				if (memberSet.contains(member)) {
					k = j;
					m = member;
					break;
				}
			}
			if (k == -1) {
                return;
            }
		}

		Member[] children = evaluator.getSchemaReader().getMemberChildren(m);
		for (int j = 0; j < children.length; j++) {
			Object objNew;
			if (k < 0) {
				objNew = children[j];
			}  else {
				Member[] members = (Member[]) ((Member[]) element).clone();
				members[k] = children[j];
				objNew = members;
			}

			resultList.add(objNew);
			if (recursive) {
				drillDownObj(evaluator, objNew, memberSet, resultList);
			}
		}
	}

	public Object evaluate(Evaluator evaluator, Exp[] args)  {
		// List of members=Set of members, List of member arrays=set of tuples
		List v0 = (List) getArg(evaluator, args, 0);
		List v1 = (List) getArg(evaluator, args, 1);

		if (null == v0 ||
            v0.isEmpty() ||
            null == v1 ||
            v1.isEmpty()) {
            return v0;
        }

		HashSet set1 = new HashSet();
		set1.addAll(v1);

		List result = new ArrayList();
		int i = 0, n = v0.size();
		while (i < n) {
			Object o = v0.get(i++);
			result.add(o);
			drillDownObj(evaluator, o, set1, result);
		}
		return result;
	}

    /**
     * Resolves calls to the "DrilldownMember" function.
     */
    static class Resolver extends MultiResolver {
        public Resolver() {
            super("DrilldownMember",
                "DrilldownMember(<Set1>, <Set2>[, RECURSIVE])",
                "Drills down the members in a set that are present in a second specified set.",
                new String[]{"fxxx", "fxxxy"});
        }

        protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
            boolean recursive = getLiteralArg(args, 2, "", reservedNames,
                dummyFunDef).equals("RECURSIVE");
            return new DrilldownMemberFunDef(dummyFunDef, recursive);
        }
    }
}

// End DrilldownMemberFunDef.java
