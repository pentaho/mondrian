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

import mondrian.olap.Evaluator;
import mondrian.olap.Syntax;
import mondrian.olap.type.StringType;
import mondrian.olap.type.Type;
import mondrian.spi.UserDefinedFunction;
import mondrian.util.Format;

import java.util.Date;
import java.util.Locale;

/**
 * User-defined function <code>CurrentDateString<code>, which returns the
 * current date value as a formatted string, based on a format string passed in
 * as a parameter.  The format string conforms to the format string implemented
 * by {@link Format}.
 *
 * @author Zelaine Fong
 */
public class CurrentDateStringUdf implements UserDefinedFunction {

    public Object execute(Evaluator evaluator, Argument[] arguments) {
        Object arg = arguments[0].evaluateScalar(evaluator);

        final Locale locale = Locale.getDefault();
        final Format format = new Format((String) arg, locale);
        Date currDate = evaluator.getQueryStartTime();
        return format.format(currDate);
    }

    public String getDescription() {
        return "Returns the current date formatted as specified by the format "
            + "parameter.";
    }

    public String getName() {
        return "CurrentDateString";
    }

    public Type[] getParameterTypes() {
        return new Type[] { new StringType() };
    }

    public String[] getReservedWords() {
        return null;
    }

    public Type getReturnType(Type[] parameterTypes) {
        return new StringType();
    }

    public Syntax getSyntax() {
        return Syntax.Function;
    }

}

// End CurrentDateStringUdf.java
