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
import mondrian.olap.type.*;

import java.io.PrintWriter;

/**
 * Skeleton implementation of {@link Exp} interface.
 **/
public abstract class ExpBase
    extends QueryPart
    implements Exp {


    static Exp[] cloneArray(Exp[] a) {
        Exp[] a2 = new Exp[a.length];
        for (int i = 0; i < a.length; i++) {
            a2[i] = (Exp) a[i].clone();
        }
        return a2;
    }

    protected ExpBase() {
    }

    public abstract Object clone();

    public final boolean isSet() {
        if (false) {
            int cat = getCategory();
            return (cat == Category.Set) || (cat == Category.Tuple);
        } else {
            Type typeX = getTypeX();
            return typeX instanceof SetType || typeX instanceof TupleType;
        }
    }

    public final boolean isMember() {
        return getCategory() == Category.Member;
    }

    public final boolean isElement() {
        int category = getCategory();
        return isMember() ||
            (category == Category.Hierarchy) ||
            (category == Category.Level) ||
            (category == Category.Dimension);
    }

    public final boolean isEmptySet()
    {
        if (this instanceof FunCall) {
            FunCall f = (FunCall) this;
            return (f.getSyntax() == Syntax.Braces) &&
                   (f.getArgCount() == 0);
        } else {
            return false;
        }
    }

    /**
     * Returns an array of {@link Member}s if this is a member or a tuple,
     * null otherwise.
     **/
    public final Member[] isConstantTuple()
    {
        if (this instanceof Member) {
            return new Member[] {(Member) this};
        }
        if (!(this instanceof FunCall)) {
            return null;
        }
        FunCall f = (FunCall) this;
        if (!f.isCallToTuple()) {
            return null;
        }
        // Make sure all of the Exp are Members
        int len = f.getArgCount();
        for (int i = 0; i < len; i++) {
            if (!(f.getArg(i) instanceof Member)) {
                return null;
            }
        }
        Member[] members = new Member[len];
        // non-type checking copy
        System.arraycopy(f.getArgs(), 0, members, 0, len);
        return members;
    }

    public int addAtPosition(Exp e, int iPosition) {
        // Since this method has not been overridden for this type of
        // expression, we presume that the expression has a dimensionality of
        // 1.  We therefore return 1 to indicate that we could not add the
        // expression, and that this expression has a dimensionality of 1.
        return 1;
    }

    public Object evaluate(Evaluator evaluator) {
        throw new Error("unsupported");
    }

    public Object evaluateScalar(Evaluator evaluator) {
        Object o = evaluate(evaluator);
        if (o instanceof Member) {
            evaluator = evaluator.push((Member) o);
            return evaluator.evaluateCurrent();
        } else if (o instanceof Member[]) {
            evaluator = evaluator.push((Member[]) o);
            return evaluator.evaluateCurrent();
        } else {
            return o;
        }
    }

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

    /**
     * A simple and incomplete default implementation for
     * {@link Exp#dependsOn(Dimension)}.
     * It assumes that a dimension, that is used somewhere in the expression
     * makes the whole expression independent of that dimension.
     */
    public boolean dependsOn(Dimension dimension) {
        final Type type = getTypeX();
        return !type.usesDimension(dimension);
    }

    /**
     * @deprecated Use {@link #getCategory()}
     **/
    public int getType() {
        return getCategory();
    }
}


// End Exp.java
