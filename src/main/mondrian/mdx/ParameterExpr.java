/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.mdx;

import mondrian.calc.*;
import mondrian.olap.*;
import mondrian.olap.type.*;

import java.io.PrintWriter;
import java.util.List;

/**
 * MDX expression which is a usage of a {@link mondrian.olap.Parameter}.
 *
 * @author jhyde
 */
public class ParameterExpr extends ExpBase {

    private Parameter parameter;

    /**
     * Creates a ParameterExpr.
     *
     * @param parameter Parameter
     */
    public ParameterExpr(Parameter parameter)
    {
        this.parameter = parameter;
    }

    public Type getType() {
        return parameter.getType();
    }

    public int getCategory() {
        return TypeUtil.typeToCategory(parameter.getType());
    }

    public Exp accept(Validator validator) {
        // There must be some Parameter with this name registered with the
        // Query.  After clone(), there will be many copies of the same
        // parameter, and we rely on this method to bring them down to one.
        // So if this object is not the registered vesion, that's fine, go with
        // the other one.  The registered one will be resolved after everything
        // else in the query has been resolved.
        String parameterName = parameter.getName();
        final SchemaReader schemaReader =
            validator.getQuery().getSchemaReader(false);
        Parameter p = schemaReader.getParameter(parameterName);
        if (p == null) {
            this.parameter =
                validator.createOrLookupParam(
                    true,
                    parameter.getName(),
                    parameter.getType(),
                    parameter.getDefaultExp(),
                    parameter.getDescription());
        } else {
            this.parameter = p;
        }
        return this;
    }

    public Calc accept(ExpCompiler compiler) {
        return ((ParameterCompilable) parameter).compile(compiler);
    }

    public Object accept(MdxVisitor visitor) {
        return visitor.visit(this);
    }

    public ParameterExpr clone() {
        return new ParameterExpr(parameter);
    }

    /**
     * Unparses the definition of this Parameter.
     *
     * <p>The first usage of a parameter in a query becomes a call to the
     * <code>Parameter(paramName, description, defaultValue)</code>
     * function, and subsequent usages become calls to
     * <code>ParamRef(paramName)</code>
     *
     * @param pw PrintWriter
     */
    public void unparse(PrintWriter pw) {
        // Is this the first time we've seen a statement parameter? If so,
        // we will generate a call to the Parameter() function, to define
        // the parameter.
        final boolean def;
        if (pw instanceof QueryPrintWriter
            && parameter.getScope() == Parameter.Scope.Statement)
        {
            def = ((QueryPrintWriter) pw).parameters.add(parameter);
        } else {
            def = false;
        }
        final String name = parameter.getName();
        final Type type = parameter.getType();
        final int category = TypeUtil.typeToCategory(type);
        if (def) {
            pw.print("Parameter(" + Util.quoteForMdx(name) + ", ");
            switch (category) {
            case Category.String:
            case Category.Numeric:
                pw.print(Category.instance.getName(category).toUpperCase());
                break;
            case Category.Member:
                pw.print(uniqueName(type));
                break;
            case Category.Set:
                Type elementType = ((SetType) type).getElementType();
                pw.print(uniqueName(elementType));
                break;
            default:
                throw Category.instance.badValue(category);
            }
            pw.print(", ");
            final Object value = parameter.getValue();
            if (value == null) {
                parameter.getDefaultExp().unparse(pw);
            } else if (value instanceof String) {
                String s = (String) value;
                pw.print(Util.quoteForMdx(s));
            } else if (value instanceof List) {
                List list = (List) value;
                pw.print("{");
                int i = -1;
                for (Object o : list) {
                    ++i;
                    if (i > 0) {
                        pw.print(", ");
                    }
                    pw.print(o);
                }
                pw.print("}");
            } else {
                pw.print(value);
            }
            final String description = parameter.getDescription();
            if (description != null) {
                pw.print(", " + Util.quoteForMdx(description));
            }
            pw.print(")");
        } else {
            pw.print("ParamRef(" + Util.quoteForMdx(name) + ")");
        }
    }

    /**
     * Returns the unique name of the level, hierarchy, or dimension of this
     * type, whichever is most specific.
     *
     * @param type Type
     * @return Most specific description of type
     */
    private String uniqueName(Type type) {
        if (type.getLevel() != null) {
            return type.getLevel().getUniqueName();
        } else if (type.getHierarchy() != null) {
            return type.getHierarchy().getUniqueName();
        } else {
            return type.getDimension().getUniqueName();
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
        if (!(other instanceof ParameterExpr)) {
            return false;
        }
        ParameterExpr that = (ParameterExpr) other;
        return this.parameter == that.parameter;
    }

    public int hashCode() {
        return parameter.hashCode();
    }

    /**
     * Returns whether the parameter can be modified.
     *
     * @return whether parameter can be modified
     */
    public boolean isModifiable() {
        return true;
    }

    /**
     * Returns the parameter used by this expression.
     *
     * @return parameter used by this expression
     */
    public Parameter getParameter() {
        return parameter;
    }
}

// End ParameterExpr.java
