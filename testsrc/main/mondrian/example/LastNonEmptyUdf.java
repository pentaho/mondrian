/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2005-2005 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.example;

import mondrian.olap.*;
import mondrian.olap.type.*;
import mondrian.rolap.RolapUtil;
import mondrian.spi.UserDefinedFunction;

import java.util.List;

/**
 * Definition of the user-defined function "LastNonEmpty".
 *
 * @author jhyde
 * @version $Id$
 */
public class LastNonEmptyUdf implements UserDefinedFunction {
    private final String name;

    /**
     * Creates a function definition with a given name.
     */
    public LastNonEmptyUdf(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return "Returns the last member of a set whose value is not empty";
    }

    public Syntax getSyntax() {
        return Syntax.Function;
    }

    public Type getReturnType(Type[] parameterTypes) {
        // Return type is the same as the elements of the first parameter.
        // For example,
        //    LastNonEmpty({[Time].[1997], [Time].[1997].[Q1]},
        //                 [Measures].[Unit Sales])
        // will return a member of the [Time] dimension.
        SetType setType = (SetType) parameterTypes[0];
        MemberType memberType = (MemberType) setType.getElementType();
        return memberType;
    }

    public Type[] getParameterTypes() {
        return new Type[] {
            // The first argument must be a set of members (of any hierarchy).
            new SetType(MemberType.Unknown),
            // The second argument must be a member.
            MemberType.Unknown,
        };
    }

    public Object execute(Evaluator evaluator, Exp[] arguments) {
        final Exp memberListExp = arguments[0];
        final List memberList = (List) memberListExp.evaluate(evaluator);
        final Exp exp = arguments[1];
        for (int i = memberList.size() - 1; i >= 0; --i) {
            Member member = (Member) memberList.get(i);
            // Create an evaluator with the member as its context.
            Evaluator subEvaluator = evaluator.push(member);
            final Object o = exp.evaluateScalar(subEvaluator);
            if (o == Util.nullValue) {
                continue;
            }
            if (o instanceof RuntimeException) {
                RuntimeException runtimeException = (RuntimeException) o;
                if (o == RolapUtil.valueNotReadyException) {
                    // Value is not in the cache yet, so we don't know whether
                    // it will be empty. Carry on...
                    continue;
                }
                return runtimeException;
            }
            return member;
        }
        // Not found. Return the hierarchy's 'null member'.
        final Hierarchy hierarchy = memberListExp.getType().getHierarchy();
        return hierarchy.getNullMember();
    }

    public String[] getReservedWords() {
        // This function does not require any reserved words.
        return null;
    }
}

// End LastNonEmptyUdf.java
