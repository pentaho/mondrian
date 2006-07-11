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

import mondrian.olap.*;
import mondrian.olap.type.*;
import mondrian.spi.UserDefinedFunction;
import mondrian.util.*;

import java.util.*;

/**
 * User-defined function CurrentDateMember.  Arguments to the function are
 * as follows:
 *
 * <code>
 * CurrentDataMember(<Hierarchy>, <FormatString>[, <Find>) returns <Member>
 * </code>
 *
 * The function returns the member from the specified hierarchy that matches
 * the current date, to the granularity specified by the <FormatString>.
 *
 * The format string conforms to the format string implemented by
 * {@link Format}.
 * 
 * @author Zelaine Fong
 */
public class CurrentDateMemberUdf implements UserDefinedFunction {
    
    public Object execute(Evaluator evaluator, Argument[] arguments) {
        
        // determine the current date
        Object formatArg = arguments[1].evaluateScalar(evaluator);
        
        if (!(formatArg instanceof String)) {
            return null;
        }
        final Locale locale = Locale.getDefault();
        final Format format = new Format((String) formatArg, locale);
        Date currDate = new Date();
        String currDateStr = format.format(currDate);
        
        // determine the match type
        int matchType;
        if (arguments.length == 3) {
            String matchStr =
                ((String) arguments[2].evaluateScalar(evaluator)).toString();
            matchType = mapMatchStrToType(matchStr);
        } else {
            matchType = MatchType.EXACT;
        }
        
        String[] uniqueNames = Util.explode(currDateStr);
        return evaluator.getSchemaReader().getMemberByUniqueName(
                uniqueNames, false, matchType);
    }

    public String getDescription() {
        return "Returns the closest member corresponding to the current date";
    }

    public String getName() {
        return "CurrentDateMember";
    }

    public Type[] getParameterTypes() {
        return new Type[] {
            new HierarchyType(null, null),
            new StringType(),
            new SymbolType()
        };
    }

    public String[] getReservedWords() {
        return new String[] {
            "EXACT",
            "BEFORE",
            "AFTER"
        };
    }

    public Type getReturnType(Type[] parameterTypes) {
        return MemberType.Unknown;
    }

    public Syntax getSyntax() {
        return Syntax.Function;
    }

    private int mapMatchStrToType(String matchStr)
    {
        if (matchStr.equals("EXACT")) {
            return MatchType.EXACT;
        } else if (matchStr.equals("BEFORE")) {
            return MatchType.BEFORE;
        } else {
            return MatchType.AFTER;
        }
    }
}

// End CurrentDateMemberUdf.java
