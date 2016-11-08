/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.udf;

import mondrian.olap.Evaluator;
import mondrian.olap.Syntax;
import mondrian.olap.type.NumericType;
import mondrian.olap.type.Type;
import mondrian.spi.UserDefinedFunction;

/**
 * VB function <code>Val</code>
 *
 * @author Gang Chen
 */
public class ValUdf implements UserDefinedFunction {

    public Object execute(Evaluator evaluator, Argument[] arguments) {
        Object arg = arguments[0].evaluateScalar(evaluator);

        if (arg instanceof Number) {
            return new Double(((Number) arg).doubleValue());
        } else {
            return new Double(0.0);
        }
    }

    public String getDescription() {
        return "VB function Val";
    }

    public String getName() {
        return "Val";
    }

    public Type[] getParameterTypes() {
        return new Type[] { new NumericType() };
    }

    public String[] getReservedWords() {
        return null;
    }

    public Type getReturnType(Type[] parameterTypes) {
        return new NumericType();
    }

    public Syntax getSyntax() {
        return Syntax.Function;
    }

}

// End ValUdf.java
