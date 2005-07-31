/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2005-2005 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/

package mondrian.olap.fun;

import mondrian.olap.*;

import java.util.*;

/**
 * Definition of the <code>RANK</code> MDX function.
 *
 * @author Richard Emberson
 * @since 17 January, 2005
 * @version $Id$
 */
public class RankFunDef extends FunkBase {
    public RankFunDef() {
        super();
    }

    // Rank(<<Tuple>>, <<Set>>[, <<Calc Expression>>])
    public Object evaluate(Evaluator evaluator, Exp[] args) {
        // get tuple
        Member[] tuple = getTupleOrMemberArg(evaluator, args, 0);
        if (tuple == null) {
            // Tuple is null.
            return null;
        }
        for (int i = 0; i < tuple.length; i++) {
            // Rank of a null member or partially null tuple returns null.
            Member member = tuple[i];
            if (member.isNull()) {
                return null;
            }
        }

        // get set
        List members = (List) getArg(evaluator, args, 1);

        if (args.length == 3) {
            if (members == null) {
                // If list is empty, the rank is null.
                return Util.nullValue;
            }
            final Exp exp = args[2];
            return eval3(evaluator, tuple, members, exp);
        } else {
            // If the list is empty, MSAS cannot figure out the type of the
            // list, so returns an error "Formula error - dimension count is
            // not valid - in the Rank function". I think it's better to
            // return 0.
            if (members == null) {
                return new Double(0);
            }
            return eval2(tuple, members);
        }
    }

    private Object eval3(
            Evaluator evaluator, Member[] tuple, List members, Exp exp) {
        // Create a new evaluator so we don't corrupt the given one.
        final Evaluator evaluator2 = evaluator.push();
        // Construct an array containing the value of the expression for
        // each member.
        RuntimeException exception = null;
        final Object[] values = new Object[members.size() + 1];
        for (int i = 0; i < members.size(); i++) {
            final Object o = members.get(i);
            if (o instanceof Member) {
                Member member = (Member) o;
                evaluator2.setContext(member);
            } else {
                evaluator2.setContext((Member[]) o);
            }
            values[i] = exp.evaluateScalar(evaluator2);
            if (exception == null && values[i] instanceof RuntimeException) {
                exception = (RuntimeException) values[i];
            }
        }
        // Add the value of the member to be ranked.
        evaluator2.setContext(tuple);
        Object value = exp.evaluateScalar(evaluator2);
        if (exception == null && value instanceof RuntimeException) {
            exception = (RuntimeException) value;
        }
        values[values.length - 1] = value;
        // If there were exceptions, quit now... we'll be back.
        if (exception != null) {
            return exception;
        }
        // Sort the array.
        FunUtil.sortValuesDesc(values);
        // Look for the ranked value in the array.
        int j;
        for (j = 0; j < values.length; ++j) {
            Object o = values[j];
            if (o == value) {
                break;
            }
        }
        assert j < values.length : "Value must be in array somewhere";
        // If the values preceding are equal, increase the rank.
        while (j > 0 && values[j - 1].equals(value)) {
            --j;
        }
        return new Double(j + 1); // 1-based
    }

    private Object eval2(Member[] tuple, List members) {
        int counter = 0;
        Iterator it = members.iterator();
        while (it.hasNext()) {
            ++counter;
            Object o = it.next();
            Member[] m;
            if (o instanceof Member[]) {
                m = (Member[]) o;
            } else if (o instanceof Member) {
                m = new Member[] { (Member)o };
            } else {
                continue;
            }

            boolean matches = equalTuple(tuple, m);
            if (matches) {
                return new Double(counter);
            }
        }

        return new Double(0); // not found
    }

    /**
     * Returns whether two tuples are equal.
     */
    private boolean equalTuple(Member[] tuple, Member[] m) {
        if (tuple.length != m.length) {
            return false;
        }
        for (int i = 0; i < tuple.length; i++) {
            if (! tuple[i].equals(m[i])) {
                return false;
            }
        }
        return true;
    }
}

// End RankFunDef.java
