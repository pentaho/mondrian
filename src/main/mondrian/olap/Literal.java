/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 1998-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 21 January, 1999
*/

package mondrian.olap;
import mondrian.olap.type.Type;
import mondrian.olap.type.StringType;
import mondrian.olap.type.NumericType;
import mondrian.olap.type.SymbolType;

import java.io.PrintWriter;

/**
 * Constant (just strings and symbols for now).
 **/
public class Literal extends ExpBase {

    public static final Literal emptyString = new Literal("", false);
    public static final Literal zero = new Literal(new Integer(0));
    public static final Literal one = new Literal(new Integer(1));
    public static final Literal negativeOne = new Literal(new Integer(-1));
    public static final Literal doubleZero = new Literal(new Double(0.0));
    public static final Literal doubleOne = new Literal(new Double(1.0));
    public static final Literal doubleNegativeOne = new Literal(new Double(-1.0));

    /**
     * Creates a string literal.
     * @see #createSymbol
     */
    public static Literal createString(String s) {
        return (s.equals(""))
            ? emptyString
            : new Literal(s, false);
    }

    /**
     * Creates a symbol.
     * @see #createString
     */
    public static Literal createSymbol(String s) {
        return new Literal(s, true);
    }

    public static Literal create(Double d) {
        double dv = d.doubleValue();
        if (dv == 0.0) {
            return doubleZero;
        } else if (dv == 1.0) {
            return doubleOne;
        } else if (dv == -1.0) {
            return doubleNegativeOne;
        } else {
            return new Literal(d);
        }
    }

    public static Literal create(Integer i) {
        switch (i.intValue()) {
        case -1:
            return negativeOne;
        case 0:
            return zero;
        case 1:
            return one;
        default:
            return new Literal(i);
        }
    }


    public final int type;
    private final Object o;

    private Literal(String s, boolean isSymbol) {
        this.o = s;
        this.type = isSymbol ? Category.Symbol : Category.String;
    }

    private Literal(Double d) {
        this.o = d;
        this.type = Category.Numeric;
    }
    private Literal(Integer i) {
        this.o = i;
        this.type = Category.Numeric;
    }

    public Object clone() {
        return this;
    }

    public void unparse(PrintWriter pw) {
        switch (type) {
        case Category.Symbol:
        case Category.Numeric:
            pw.print(o);
            break;
        case Category.String:
            pw.print(Util.quoteForMdx((String) o));
            break;
        default:
            throw Util.newInternal("bad literal type " + type);
        }
    }

    public int getCategory() {
        return type;
    }

    public Type getTypeX() {
        switch (type) {
        case Category.Symbol:
            return new SymbolType();
        case Category.Numeric:
            return new NumericType();
        case Category.String:
            return new StringType();
        default:
            throw Category.instance.badValue(type);
        }
    }

    public Exp accept(Validator validator) {
        return this;
    }

    public Object evaluate(Evaluator evaluator) {
        return evaluator.visit(this);
    }

    public Object evaluateScalar(Evaluator evaluator) {
        return o;
    }

    public Object getValue() {
        return o;
    }

    public int getIntValue() {
        if (o instanceof Number) {
            return ((Number) o).intValue();
        } else {
            throw Util.newInternal("cannot convert " + o + " to int");
        }
    }

    public boolean dependsOn(Dimension dimension) {
        return false;
    }
}

// End Literal.java
