/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2006 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.udf;

import mondrian.olap.Evaluator;
import mondrian.olap.Syntax;
import mondrian.olap.type.StringType;
import mondrian.olap.type.Type;
import mondrian.spi.UserDefinedFunction;
import mondrian.util.*;

import java.util.*;

/**
 * User-defined function <code>CurrentDateString<code>, which returns the
 * current date value as a formatted string, based on a format string passed in
 * as a parameter.  The format string conforms to the format string implemented
 * by {@link Format}.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class CurrentDateStringUdf implements UserDefinedFunction {

    public Object execute(Evaluator evaluator, Argument[] arguments) {
        Object arg = arguments[0].evaluateScalar(evaluator);

        final Locale locale = Locale.getDefault();
        final Format format = new Format((String) arg, locale);
        Date currDate = new Date();
        return format.format(currDate);
    }

    public String getDescription() {
        return "Returns the current date formatted as specified by the format parameter.";
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
