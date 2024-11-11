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

package mondrian.olap.fun;

import mondrian.mdx.*;
import mondrian.olap.*;

import java.util.*;

/**
 * Visitor class used to locate a resolved function call within an
 * expression
 */
public class ResolvedFunCallFinder
    extends MdxVisitorImpl
{
    private final ResolvedFunCall call;
    public boolean found;
    private final Set<Member> activeMembers = new HashSet<Member>();

    public ResolvedFunCallFinder(ResolvedFunCall call)
    {
        this.call = call;
        found = false;
    }

    public Object visit(ResolvedFunCall funCall)
    {
        if (funCall == call) {
            found = true;
        }
        return null;
    }

    public Object visit(MemberExpr memberExpr) {
        Member member = memberExpr.getMember();
        if (member.isCalculated()) {
            if (activeMembers.add(member)) {
                Exp memberExp = member.getExpression();
                memberExp.accept(this);
                activeMembers.remove(member);
            }
        }
        return null;
    }
}

// End ResolvedFunCallFinder.java