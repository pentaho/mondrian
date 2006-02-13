/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2000-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// leonardk, 10 January, 2000
*/

package mondrian.olap;
import mondrian.olap.type.Type;
import mondrian.resource.MondrianResource;
import mondrian.mdx.MemberExpr;
import mondrian.calc.*;
import mondrian.calc.impl.GenericCalc;

import java.io.PrintWriter;

public class Parameter extends ExpBase {

    private final String name;
    // Category.String, Category.Numeric, or Category.Member
    private final int category;
    private final String description;
    private final Exp defaultExp;
    private int defineCount;
    private final Type type;
    private Object value;

    Parameter(
            String name,
            int category,
            Exp defaultExp,
            String description,
            Type type) {
        this.name = name;
        this.category = category;
        this.defaultExp = defaultExp;
        this.defineCount = 1;
        this.description = description;
        this.type = type;
    }

    static Parameter[] cloneArray(Parameter[] a) {
        if (a == null) {
            return null;
        }
        Parameter[] a2 = new Parameter[a.length];
        for (int i = 0; i < a.length; i++) {
            a2[i] = (Parameter) a[i].clone();
        }
        return a2;
    }

    public int getCategory() {
        return category;
    }

    public Type getType() {
        return type;
    }

    public Exp getDefaultExp() {
        return defaultExp;
    }

    void incrementDefineCount() {
        defineCount++;
    }

    int getDefineCount() {
        return defineCount;
    }

    /**
     * Sets defineCount instance variable to 0.
     */
    void resetDefineCount() {
        defineCount = 0;
    }

    public Exp accept(Validator validator) {
        // There must be some Parameter with this name registered with the
        // Query.  After clone(), there will be many copies of the same
        // parameter, and we rely on this method to bring them down to one.
        // So if this object is not the registered vesion, that's fine, go with
        // the other one.  The registered one will be resolved after everything
        // else in the query has been resolved.
        Parameter p = validator.getQuery().lookupParam(name);
        if (p == null) {
            throw Util.newInternal("parameter '" + name + "' not registered");
        }
        return p;
    }

    public Calc accept(ExpCompiler compiler) {
        final Calc defaultCalc = defaultExp.accept(compiler);
        return new GenericCalc(new DummyExp(type)) {
            public Calc[] getCalcs() {
                return new Calc[] {defaultCalc};
            }

            public Object evaluate(Evaluator evaluator) {
                if (value != null) {
                    return value;
                } else {
                    return defaultCalc.evaluate(evaluator);
                }
            }
        };
    }

    public String getName() {
        return name;
    }

    /**
     * Returns the value of this parameter.
     * @return one of String, Double, Member
     */
    public Object getValue() {
        switch (category) {
        case Category.Numeric:
            // exp can be a unary minus FunCall
            if (defaultExp instanceof FunCall) {
                FunCall f = (FunCall) defaultExp;
                if (f.getFunName().equals("-")) {
                    Literal lit = (Literal)f.getArg(0);
                    Object o = lit.getValue();
                    if (o instanceof Double) {
                        return new Double(-((Double)o).doubleValue());
                    } else if (o instanceof Integer) {
                        // probably impossible
                        return new Integer(-((Integer)o).intValue());
                    } else if (o instanceof Integer) {
                        // probably impossible
                        return o;
                    }
                } else {
                    //unexpected funcall in parameter definition
                    throw Util.newInternal("bad FunCall " + f);
                }
            }
            return ((Literal)defaultExp).getValue();
        case Category.String:
            return ((Literal)defaultExp).getValue();
        default:
            return (Member)defaultExp;
        }
    }


    /**
     * Sets the value of this parameter.
     *
     * @param value Value of the parameter; must be a {@link String},
     *   a {@link Double}, or a {@link Member}
     */
    public void setValue(Object value) {
        if (value instanceof MemberExpr) {
            this.value = ((MemberExpr) value).getMember();
        } else if (value instanceof Literal) {
            this.value = ((Literal) value).getValue();
        } else {
            this.value = value;
        }
    }

    /**
     * Returns "STRING", "NUMERIC" or "MEMBER"
     */
    public String getParameterType() {
        return Category.instance.getName(category).toUpperCase();
    }

    public String getDescription() {
        return description;
    }

    public void validate(Query q) {
        switch (defineCount) {
        case 0:
            throw MondrianResource.instance().MdxParamNeverDef.ex(name);
        case 1:
            break;
        default:
            throw MondrianResource.instance().MdxParamMultipleDef.ex(name,
                new Integer(defineCount));
        }
        if (defaultExp == null) {
            throw MondrianResource.instance().MdxParamValueNotFound.ex(name);
        }
    }

    public Object clone() {
        return new Parameter(name, category, defaultExp, description, type);
    }

    public void unparse(PrintWriter pw) {
        boolean firstPrinting = true;
        if (pw instanceof Query.QueryPrintWriter) {
            firstPrinting = ((Query.QueryPrintWriter) pw).parameters.add(this);
        }
        if (firstPrinting) {
            pw.print("Parameter(" + Util.quoteForMdx(name) + ", ");
            switch (category) {
            case Category.String:
            case Category.Numeric:
                pw.print(getParameterType());
                break;
            case Category.Member:
                String name =
                        type.getLevel() != null ?
                        type.getLevel().getUniqueName() :
                        type.getHierarchy() != null ?
                        type.getHierarchy().getUniqueName() :
                        type.getDimension().getUniqueName();
                pw.print(name);
                break;
            default:
                throw Category.instance.badValue(category);
            }
            pw.print(", ");
            if (value == null) {
                defaultExp.unparse(pw);
            } else if (value instanceof String) {
                String s = (String) value;
                pw.print(Util.quoteForMdx(s));
            } else {
                pw.print(value);
            }
            if (description != null) {
                pw.print(", " + Util.quoteForMdx(description));
            }
            pw.print(")");
        } else {
            pw.print("ParamRef(" + Util.quoteForMdx(name) + ")");
        }
    }

    // For the purposes of type inference and expression substitution, a
    // parameter is atomic; therefore, we ignore the child member, if any.
    public Object[] getChildren() {
        return null;
    }

    /**
     * Returns whether this parameter is equal to another, based upon name,
     * type and value
     */
    public boolean equals(Object other) {
        if (!(other instanceof Parameter)) {
            return false;
        }
        Parameter that = (Parameter) other;
        return that.getName().equals(this.getName()) &&
            (that.getCategory() == this.getCategory()) &&
            that.defaultExp.equals(this.defaultExp);
    }
}

// End Parameter.java

