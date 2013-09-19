/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.udf;

import mondrian.olap.*;
import mondrian.olap.type.NumericType;
import mondrian.olap.type.Type;
import mondrian.spi.UserDefinedFunction;

/**
 * Definition of the user-defined function "NullValue" which always
 * returns Java "null".
 *
 * @author remberson,jhyde
 */
public class NullValueUdf implements UserDefinedFunction {

    public String getName() {
        return "NullValue";
    }

    public String getDescription() {
        return "Returns the null value";
    }

    public Syntax getSyntax() {
        return Syntax.Function;
    }

    public Type getReturnType(Type[] parameterTypes) {
        return new NumericType();
    }

    public Type[] getParameterTypes() {
        return new Type[0];
    }

    public Object execute(Evaluator evaluator, Argument[] arguments) {
        return Util.nullValue;
    }

    public String[] getReservedWords() {
        // This function does not require any reserved words.
        return null;
    }
}

// End NullValueUdf.java
