/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 1999-2005 Julian Hyde
// Copyright (C) 2005-2009 Pentaho and others
// All Rights Reserved.
*/

package mondrian.olap;

import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;

import java.io.PrintWriter;

/**
 * Skeleton implementation of {@link Exp} interface.
 *
 * @author jhyde, 20 January, 1999
 */
public abstract class ExpBase
    extends QueryPart
    implements Exp
{

    protected static Exp[] cloneArray(Exp[] a) {
        Exp[] a2 = new Exp[a.length];
        for (int i = 0; i < a.length; i++) {
            a2[i] = a[i].clone();
        }
        return a2;
    }

    protected ExpBase() {
    }

    public abstract Exp clone();

    public static void unparseList(
        PrintWriter pw,
        Exp[] exps,
        String start,
        String mid,
        String end)
    {
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
