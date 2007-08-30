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
 * User-defined function <code>CurrentDateMember</code>.  Arguments to the
 * function are as follows:
 *
 * <blockquote>
 * <code>
 * CurrentDateMember(&lt;Hierarchy&gt;, &lt;FormatString&gt;[, &lt;Find&gt;)
 * returns &lt;Member&gt;
 * </code>
 * </blockquote>
 *
 * The function returns the member from the specified hierarchy that matches
 * the current date, to the granularity specified by the &lt;FormatString&gt;.
 *
 * The format string conforms to the format string implemented by
 * {@link Format}.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class CurrentDateMemberUdf implements UserDefinedFunction {
    private Object resultDateMember = null;
    
    public Object execute(Evaluator evaluator, Argument[] arguments) {

        if (resultDateMember != null) {
            return resultDateMember;
        }
        
        // determine the current date
        Object formatArg = arguments[1].evaluateScalar(evaluator);

        final Locale locale = Locale.getDefault();
        final Format format = new Format((String) formatArg, locale);
        Date currDate = evaluator.getQueryStartTime();
        String currDateStr = format.format(currDate);

        // determine the match type
        MatchType matchType;
        if (arguments.length == 3) {
            String matchStr = arguments[2].evaluateScalar(evaluator).toString();
            matchType = Enum.valueOf(MatchType.class, matchStr);
        } else {
            matchType = MatchType.EXACT;
        }

        List<Id.Segment> uniqueNames = Util.parseIdentifier(currDateStr);
        resultDateMember =
            evaluator.getSchemaReader().getMemberByUniqueName(
                uniqueNames, false, matchType);
        if (resultDateMember != null) {
            return resultDateMember;
        }

        // if there is no matching member, return the null member for
        // the specified dimension/hierarchy
        Object arg0 = arguments[0].evaluate(evaluator);
        if (arg0 instanceof Hierarchy) {
            resultDateMember = ((Hierarchy) arg0).getNullMember();
        } else {
            resultDateMember = ((Dimension) arg0).getHierarchy().getNullMember();
        }
        return resultDateMember;
    }

    public String getDescription() {
        return "Returns the closest or exact member within the specified dimension corresponding to the current date, in the format specified by the format parameter. "
            + "Format strings are the same as used by the MDX Format function, namely the Visual Basic format strings. See http://www.apostate.com/programming/vb-format.html.";
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

    private MatchType mapMatchStrToType(String matchStr)
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
