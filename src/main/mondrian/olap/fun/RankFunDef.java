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

import java.util.List;
import java.util.Iterator;

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
//        debug("TOP");
        // get tuple
        Member[] tuple = getTupleOrMemberArg(evaluator, args, 0);
        for (int i = 0; i < tuple.length; i++) {
            // Rank of a null member or partially null tuple returns null.
            Member member = tuple[i];
            if (member.isNull()) {
                return null;
            }
        }

        //debug("tuple.length=" +tuple.length);
        // get set
        List members = (List) getArg(evaluator, args, 1);
        // TODO: ignore the "calc expression" third arg for now
//        debug("Rank: members.size()=" +members.size());

        if (members == null) {
            return new Double(0);
        }
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
