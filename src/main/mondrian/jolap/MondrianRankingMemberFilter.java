/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Dec 24, 2002
*/
package mondrian.jolap;

import javax.olap.OLAPException;
import javax.olap.query.dimensionfilters.RankingMemberFilter;
import javax.olap.query.enumerations.RankingType;

/**
 * A <code>MondrianRankingMemberFilter</code> is ...
 *
 * @author jhyde
 * @since Dec 24, 2002
 * @version $Id$
 **/
class MondrianRankingMemberFilter extends MondrianDataBasedMemberFilter
		implements RankingMemberFilter {
	private RankingType type;
	private Integer top;
	private Boolean topPercent;
	private Integer bottom;
	private Boolean bottomPercent;

	public MondrianRankingMemberFilter(MondrianDimensionStepManager manager) {
		super(manager);
	}

	public Integer getTop() throws OLAPException {
		return top;
	}

	public void setTop(Integer input) throws OLAPException {
		this.top = input;
	}

	public Boolean getTopPercent() throws OLAPException {
		return topPercent;
	}

	public void setTopPercent(Boolean input) throws OLAPException {
		this.topPercent = input;
	}

	public Integer getBottom() throws OLAPException {
		return bottom;
	}

	public void setBottom(Integer input) throws OLAPException {
		this.bottom = input;
	}

	public Boolean getBottomPercent() throws OLAPException {
		return bottomPercent;
	}

	public void setBottomPercent(Boolean input) throws OLAPException {
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