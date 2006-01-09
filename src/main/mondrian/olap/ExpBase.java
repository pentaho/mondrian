/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 1999-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 20 January, 1999
*/

package mondrian.olap;
import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;

import java.io.PrintWriter;

/**
 * Skeleton implementation of {@link Exp} interface.
 */
public abstract class ExpBase
    extends QueryPart
    implements Exp {


    protected static Exp[] cloneArray(Exp[] a) {
        Exp[] a2 = new Exp[a.length];
        for (int i = 0; i < a.length; i++) {
            a2[i] = (Exp) a[i].clone();
        }
        return a2;
    }

    protected ExpBase() {
    }

    public abstract Object clone();

    public static void unparseList(PrintWriter pw, Exp[] exps, String start,
            String mid, String end) {
        pw.print(start);
        for (int i = 0; i < exps.length; i++) {
            if (i > 0) {
                pw.print(mid);
            }
            exps[i].unparse(pw);
        }
        pw.print(end);
    }

    public static int[] getTypes(Exp[] exps) {
        int[] types = new int[exps.length];
        for (int i = 0; i < exps.length; i++) {
            types[i] = exps[i].getCategory();
        }
        return types;
    }

    public Calc accept(ExpCompiler compiler) {
        throw new UnsupportedOperationException(this.toString());
    }
}

// End ExpBase.java
