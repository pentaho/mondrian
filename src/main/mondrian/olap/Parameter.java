/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2000-2003 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// leonardk, 10 January, 2000
*/

package mondrian.olap;
import mondrian.olap.fun.ParameterFunDef;

import java.io.PrintWriter;

public class Parameter extends ExpBase {
	String name;
	int category; // Category.String, Category.Numeric, or Category.Member
	Hierarchy hierarchy;
	Exp exp;
	String description;
	int defineCount;
	/** How many times this parameter has been printed; first time we output
	 * a "Parameter" function, subsequent times, a "ParamRef". **/
	int printCount;

	Parameter(String name, int category, Hierarchy hierarchy, Exp exp,
            String description) {
		this.name = name;
		this.category = category;
		this.hierarchy = hierarchy;
		this.exp = exp;
		this.printCount = 0;
		this.defineCount = 1;
		this.description = description;
	}

	static Parameter[] cloneArray(Parameter[] a) {
		if (a == null)
			return null;
		Parameter[] a2 = new Parameter[a.length];
		for (int i = 0; i < a.length; i++)
			a2[i] = (Parameter) a[i].clone();
		return a2;
	}

	public int getType() {
		return category;
	}

	public Exp getExp() {
		return exp;
	}

    public boolean usesDimension(Dimension dimension)
	{
		Hierarchy mdxHierarchy = getHierarchy();
		return mdxHierarchy != null &&
			mdxHierarchy.getDimension() == dimension;
	}

	public Exp resolve(Resolver resolver)
	{
		// There must be some Parameter with this name registered with the
		// Query.  After clone(), there will be many copies of the same
		// parameter, and we rely on this method to bring them down to one.
		// So if this object is not the registered vesion, that's fine, go with
		// the other one.  The registered one will be resolved after everything
		// else in the query has been resolved.
		Parameter p = resolver.getQuery().lookupParam(name);
        if (p == null) {
            throw Util.newInternal("parameter '" + name + "' not registered");
        }
		if (p != this) {
			return p;			// will resolve it later
		}
		return this;
	}

	public String getName() {
		return name;
	}

	/**
     * @deprecated Call {@link Connection#parseExpression} then
     *   {@link #setValue(Object)}
	 */
	public void setValue(String value, Query query)
	{
		switch (category) {
		case Category.Numeric:
			exp = Literal.create(new Double(value));
			break;
		case Category.String:
			exp = Literal.createString(value);
			break;
		case Category.Member:
			exp = Util.lookup(query, Util.explode(value));
			break;
		default:
			throw Category.instance.badValue(category);
		}
	}

    /**
     * Returns the value of this parameter.
     * @return one of String, Double, Member
     */
    public Object getValue() {
        switch (category) {
        case Category.Numeric:
            // exp can be a unary minus FunCall
            if (exp instanceof FunCall) {
                FunCall f = (FunCall)exp;
                if (f.getFunName().equals("-")) {
                    Literal lit = (Literal)f.args[0];
                    Object o = lit.getValue();
                    if (o instanceof Double) {
                        return new Double(-((Double)o).doubleValue());
                    } else if (o instanceof Integer) {
                        return new Integer(-((Integer)o).intValue()); // probably impossible
                    } else if (o instanceof Integer) {
                        return o; // probably impossible
                    }
                } else {
                    //unexpected funcall in parameter definition
                    throw Util.newInternal("bad FunCall " + f);
                }
            }
            return ((Literal)exp).getValue();
        case Category.String:
            return ((Literal)exp).getValue();
        default:
            return (Member)exp;
        }
    }


    /**
     * Sets the value of this parameter.
     * @param value Value of the parameter; must be a {@link String},
     *   a {@link Double}, or a {@link Member}
     */
    public void setValue(Object value) {
        switch (category) {
        case Category.Numeric:
            exp = Literal.create((Double)value);
            break;
        case Category.String:
            exp = Literal.createString((String)value);
            break;
        default:
            exp = (Member)value;
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
			throw MondrianResource.instance().newMdxParamNeverDef(name);
		case 1:
			break;
		default:
			throw MondrianResource.instance().newMdxParamMultipleDef(name, new Integer(defineCount));
		}
		if (exp == null) {
			throw Util.getRes().newMdxParamValueNotFound(name);
		}
	}

	public Hierarchy getHierarchy() {
		return hierarchy;
	}

	public Object clone() {
		return new Parameter(name, category, hierarchy, exp, description);
	}

	public void unparse(PrintWriter pw)
	{
        if (printCount++ > 0) {
            pw.print("ParamRef(" + Util.quoteForMdx(name) + ")");
        } else {
            pw.print("Parameter(" + Util.quoteForMdx(name) + ", ");
            switch (category) {
            case Category.String:
            case Category.Numeric:
                pw.print(getParameterType());
                break;
            case Category.Member:
                hierarchy.unparse(pw);
                break;
            default:
                throw Category.instance.badValue(category);
            }
            pw.print(", ");
            exp.unparse(pw);
            if (description != null) {
                pw.print(", " + Util.quoteForMdx(description));
            }
            pw.print(")");
        }
	}

	// For the purposes of type inference and expression substitution, a
	// parameter is atomic; therefore, we ignore the child member, if any.
	public Object[] getChildren()
	{
		return null;
	}

	/** Returns whether this parameter is equal to another, based upon name,
	 * type and value */
	public boolean equals(Object other) {
		if (!(other instanceof Parameter)) {
			return false;
		}
		Parameter that = (Parameter) other;
		return that.getName().equals(this.getName()) &&
			that.getType() == this.getType() &&
			that.exp.equals(this.exp);
	}

	public void resetPrintProperty() {
		this.printCount = 0;
	}

	public Object evaluate(Evaluator evaluator) {
		return evaluator.xx(this);
	}
}


// End Parameter.java

