/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2003-2005 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Feb 14, 2003
*/
package mondrian.olap.fun;

import mondrian.olap.*;
import mondrian.olap.type.Type;
import mondrian.olap.type.StringType;
import mondrian.olap.type.NumericType;
import mondrian.olap.type.MemberType;

/**
 * A <code>ParameterFunDef</code> is a pseudo-function describing calls to
 * <code>Parameter</code> and <code>ParamRef</code> functions. It exists only
 * fleetingly, and is then converted into a {@link mondrian.olap.Parameter}.
 * For internal use only.
 *
 * @author jhyde
 * @since Feb 14, 2003
 * @version $Id$
 **/
public class ParameterFunDef extends FunDefBase {
    public final String parameterName;
    private final Hierarchy hierarchy;
    public final Exp exp;
    public final String parameterDescription;

    ParameterFunDef(FunDef funDef,
                    String parameterName,
                    Hierarchy hierarchy,
                    int returnType,
                    Exp exp,
                    String description) {
        super(funDef.getName(),
             funDef.getSignature(),
             funDef.getDescription(),
             funDef.getSyntax(),
             returnType,
             funDef.getParameterTypes());
        assertPrecondition(getName().equals("Parameter") ||
                getName().equals("ParamRef"));
        this.parameterName = parameterName;
        this.hierarchy = hierarchy;
        this.exp = exp;
        this.parameterDescription = description;
    }

    /**
     * Whether this function call defines a parameter ("Parameter"), as opposed
     * to referencing one ("ParamRef").
     */
    public boolean isDefinition() {
        return getName().equals("Parameter");
    }

    public Type getResultType(Validator validator, Exp[] args) {
        switch (returnType) {
        case Category.String:
            return new StringType();
        case Category.Numeric:
            return new NumericType();
        case Category.Member:
            return new MemberType(hierarchy, null, null);
        default:
            throw Category.instance.badValue(returnType);
        }
    }
}

// End ParameterFunDef.java
