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
import mondrian.olap.Category;
import mondrian.olap.Exp;
import mondrian.olap.ExpBase;
import mondrian.olap.Syntax;

import java.io.PrintWriter;

/**
 * <code>RangeFunDef</code> implements the ':' operator.
 *
 * @author jhyde
 * @since 3 March, 2002
 * @version $Id$
 **/
class RangeFunDef extends FunDefBase {
    RangeFunDef() {
        super(
            ":",
            "{<Member> : <Member>}",
            "Infix colon operator returns the set of members between a given pair of members.",
            Syntax.Infix,
            Category.Set,
            new int[] {Category.Member, Category.Member});
    }
    public void unparse(Exp[] args, PrintWriter pw) {
        ExpBase.unparseList(pw, args, "{", " : ", "}");
    }
    public int getReturnCategory() {
        return Category.Set;
    }
    public int[] getParameterCategories() {
        return new int[] {Category.Member, Category.Member};
    }
}

// End RangeFunDef.java
