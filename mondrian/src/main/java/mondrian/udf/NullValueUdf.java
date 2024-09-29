/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/


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
