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

import javax.olap.query.querycoremodel.DimensionStep;
import javax.olap.query.dimensionfilters.Drill;
import javax.olap.query.enumerations.DrillType;
import javax.olap.OLAPException;
import javax.olap.metadata.Hierarchy;
import javax.olap.metadata.Level;
import javax.olap.metadata.Member;

/**
 * A <code>MondrianDrillFilter</code> is ...
 *
 * @author jhyde
 * @since Dec 24, 2002
 * @version $Id$
 **/
class MondrianDrillFilter extends MondrianDimensionFilter implements Drill {
	private DrillType drillType;
	private Hierarchy hierarchy;
	private Level level;
	private Member drillMember;

	public MondrianDrillFilter(MondrianDimensionStepManager manager) {
		super(manager);
	}

	public DrillType getDrillType() throws OLAPException {
		return drillType;
	}

	public void setDrillType(DrillType input) throws OLAPException {
		this.drillType = input;
	}

	public void setHierarchy(Hierarchy input) throws OLAPException {
		this.hierarchy = input;
	}

	public Hierarchy getHierarchy() throws OLAPException {
		return hierarchy;
	}

	public void setLevel(Level input) throws OLAPException {
		this.level = input;
	}

	public Level getLevel() throws OLAPException {
		return level;
	}

	public void setDrillMember(Member input) throws OLAPException {
		this.drillMember = input;
	}

	public Member getDrillMember() throws OLAPException {
		return drillMember;
	}
}

// End MondrianDrillFilter.java