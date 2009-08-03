/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 1998-2002 Kana Software, Inc.
// Copyright (C) 2001-2008 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 21 January, 1999
*/

package mondrian.olap;
import mondrian.olap.type.*;
import mondrian.calc.*;
import mondrian.calc.impl.ConstantCalc;
import mondrian.mdx.MdxVisitor;

import java.io.PrintWriter;

/**
 * Represents a constant value, such as a string or number, in a parse tree.
 *
 * <p>Symbols, such as the <code>ASC</code> keyword in
 * <code>Order([Store].Members, [Measures].[Unit Sales], ASC)</code>, are
 * also represented as Literals.
 *
 * @version $Id$
 * @author jhyde
 */
public class Literal extends ExpBase {

    // Data members.

    public final int category;
    private final Object o;


    // Constants for commonly used literals.

    public static final Literal nullValue = new Literal(Category.Null, null);

    public static final Literal emptyString = new Literal("", false);

    public static final Literal zero = new Literal(0);

    public static final Literal one = new Literal(1);

    public static final Literal negativeOne = new Literal(-1);

    public static final Literal doubleZero = new Literal(0.0);

    public static final Literal doubleOne = new Literal(1.0);

    public static final Literal doubleNegativeOne = new Literal(-1.0);

    /**
     * Private constructor.
     *
     * <p>Use the creation methods {@link #createString(String)} etc.
     */
    private Literal(int type, Object o) {
        this.category = type;
        this.o = o;
    }

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

    /**
     * Creates a numeric literal.
     */
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

    /**
     * Creates an integer literal.
     */
    public static Literal create(Integer i) {
        switch (i) {
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

    private Literal(String s, boolean isSymbol) {
        this.o = s;
        this.category = isSymbol ? Category.Symbol : Category.String;
    }

    private Literal(Double d) {
        this.o = d;
        this.category = Category.Numeric;
    }
    private Literal(Integer i) {
        this.o = i;
        this.category = Category.Numeric;
    }

    public Literal clone() {
        return this;
    }

    public void unparse(PrintWriter pw) {
        switch (category) {
        case Category.Symbol:
        case Category.Numeric:
            pw.print(o);
            break;
        case Category.String:
            pw.print(Util.quoteForMdx((String) o));
            break;
        case Category.Null:
            pw.print("NULL");
            break;
        default:
            throw Util.newInternal("bad literal type " + category);
        }
    }

    public int getCategory() {
        return category;
    }

    public Type getType() {
        switch (category) {
        case Category.Symbol:
            return new SymbolType();
        case Category.Numeric:
            return new NumericType();
        case Category.String:
            return new StringType();
        case Category.Null:
            return new NullType();
        default:
            throw Category.instance.badValue(category);
        }
    }

    public Exp accept(Validator validator) {
        return this;
    }

    public Calc accept(ExpCompiler compiler) {
        return new ConstantCalc(getType(), o);
    }

    public Object accept(MdxVisitor visitor) {
        return visitor.visit(this);
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

}

// End Literal.java
