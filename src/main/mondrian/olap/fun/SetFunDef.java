/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2002-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 3 March, 2002
*/
package mondrian.olap.fun;

import mondrian.olap.*;
import mondrian.olap.type.Type;
import mondrian.olap.type.SetType;
import mondrian.olap.type.MemberType;
import mondrian.olap.type.TypeUtil;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

/**
 * <code>SetFunDef</code> implements the 'set' function (whose syntax is the
 * brace operator, <code>{ ... }</code>).
 *
 * @author jhyde
 * @since 3 March, 2002
 * @version $Id$
 **/
class SetFunDef extends FunDefBase {
    SetFunDef(Resolver resolver, int[] argTypes) {
        super(resolver, Category.Set, argTypes);
    }

    public void unparse(Exp[] args, PrintWriter pw) {
        ExpBase.unparseList(pw, args, "{", ", ", "}");
    }

    public Type getResultType(Validator validator, Exp[] args) {
        // All of the members in {<Member1>[,<MemberI>]...} must have the same
        // Hierarchy.  But if there are no members, we can't derive a
        // hierarchy.
        Type type0 = null;
        if (args.length == 0) {
            // No members to go on, so we can't guess the hierarchy.
            type0 = new MemberType(null, null, null);
        } else {
            for (int i = 0; i < args.length; i++) {
                Exp arg = args[i];
                Type type = arg.getTypeX();
                type = TypeUtil.stripSetType(type);
                if (i == 0) {
                    type0 = type;
                } else {
                    if (!TypeUtil.isUnionCompatible(type0, type)) {
                        throw MondrianResource.instance()
                                .newArgsMustHaveSameHierarchy(getName());
                    }
                }
            }
        }
        return new SetType(type0);
    }

    public Object evaluate(Evaluator evaluator, Exp[] args) {
        List list = null;
        for (int i = 0; i < args.length; i++) {
            ExpBase arg = (ExpBase) args[i];
            Object o;
            if (arg instanceof Member) {
                o = arg;
            } else {
                Member[] members = arg.isConstantTuple();
                if (members != null) {
                    o = members;
                } else {
                    o = arg.evaluate(evaluator);
                }
            }
            if (o instanceof List) {
                List list2 = (List) o;
                if (list == null) {
                    list = makeMutable(list2);
                } else {
                    for (int j = 0, count = list2.size(); j < count; j++) {
                        Object o2 = list2.get(j);
                        if (o2 instanceof Member && ((Member) o2).isNull()) {
                            continue;
                        }
                        list.add(o2);
                    }
                }
            } else {
                if (o instanceof Member && ((Member) o).isNull()) {
                    continue;
                }
                if (list == null) {
                    list = new ArrayList();
                }
                list.add(o);
            }
        }
        return list;
    }

}

// End SetFunDef.java
