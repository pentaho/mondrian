/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2002-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Dec 24, 2002
*/
package mondrian.jolap;

import mondrian.olap.Exp;
import mondrian.olap.FunCall;
import mondrian.olap.Literal;
import mondrian.olap.Util;

import javax.olap.OLAPException;
import javax.olap.query.dimensionfilters.RankingMemberFilter;
import javax.olap.query.enumerations.RankingType;
import javax.olap.query.enumerations.RankingTypeEnum;

/**
 * Implementation of {@link RankingMemberFilter}.
 *
 * @author jhyde
 * @since Dec 24, 2002
 * @version $Id$
 **/
class MondrianRankingMemberFilter extends MondrianDataBasedMemberFilter
        implements RankingMemberFilter {
    private RankingType type;
    private int top;
    private boolean topPercent;
    private int bottom;
    private boolean bottomPercent;

    public MondrianRankingMemberFilter(MondrianDimensionStepManager manager) {
        super(manager);
    }

    Exp convert(Exp exp) throws OLAPException {
        Exp newExp = _convert(exp);
        return combine(exp, newExp);
    }

    Exp _convert(Exp exp) throws OLAPException {
        if (type == RankingTypeEnum.BOTTOM) {
            if (bottomPercent) {
                return new FunCall("BottomPercent", new Exp[] {exp, Literal.create(new Integer(bottom))});
            } else {
                return new FunCall("Bottom", new Exp[] {exp, Literal.create(new Integer(bottom))});
            }
        } else if (type == RankingTypeEnum.TOP) {
            if (topPercent) {
                return new FunCall("TopPercent", new Exp[] {exp, Literal.create(new Integer(top))});
            } else {
                return new FunCall("Top", new Exp[] {exp, Literal.create(new Integer(top))});
            }
        } else if (type == RankingTypeEnum.TOP_BOTTOM) {
            throw new UnsupportedOperationException();
            // todo: Implement new functions TopCountBottomPercent etc.
        } else {
            throw Util.newInternal("Unknown ranking type " + type);
        }
    }

    public int getTop() throws OLAPException {
        return top;
    }

    public void setTop(int input) throws OLAPException {
        this.top = input;
    }

    public boolean isTopPercent() throws OLAPException {
        return topPercent;
    }

    public void setTopPercent(boolean input) throws OLAPException {
        this.topPercent = input;
    }

    public int getBottom() throws OLAPException {
        return bottom;
    }

    public void setBottom(int input) throws OLAPException {
        this.bottom = input;
    }

    public boolean isBottomPercent() throws OLAPException {
        return bottomPercent;
    }

    public void setBottomPercent(boolean input) throws OLAPException {
        this.bottomPercent = input;
    }

    public RankingType getType() throws OLAPException {
        return type;
    }

    public void setType(RankingType input) throws OLAPException {
        this.type = input;
    }
}

// End MondrianRankingMemberFilter.java
