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

import javax.olap.query.querycoremodel.DimensionStepManager;
import javax.olap.query.querycoremodel.DimensionView;
import javax.olap.query.querycoremodel.Segment;
import javax.olap.query.querycoremodel.DimensionStep;
import javax.olap.query.enumerations.DimensionStepType;
import javax.olap.query.enumerations.DimensionStepTypeEnum;
import javax.olap.OLAPException;
import java.util.Collection;
import java.util.List;

/**
 * A <code>MondrianDimensionStepManager</code> is ...
 *
 * @author jhyde
 * @since Dec 24, 2002
 * @version $Id$
 **/
class MondrianDimensionStepManager extends QueryObjectSupport implements DimensionStepManager {
	public MondrianDimensionStepManager() {
		super(false);
	}

	public DimensionView getDimensionView() throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public void setSegment(Collection input) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public Collection getSegment() throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public void addSegment(Segment input) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public void removeSegment(Segment input) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public void setDimensionStep(Collection input) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public List getDimensionStep() throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public void removeDimensionStep(DimensionStep input) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public void moveDimensionStepBefore(DimensionStep before, DimensionStep input) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public void moveDimensionStepAfter(DimensionStep before, DimensionStep input) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public DimensionStep createDimensionStep(DimensionStepType stepType) throws OLAPException {
		if (stepType == DimensionStepTypeEnum.ATTRIBUTEFILTER) {
			return new MondrianAttributeFilter(this);
		} else if (stepType == DimensionStepTypeEnum.LEVELFILTER) {
			return new MondrianLevelFilter(this);
		} else if (stepType == DimensionStepTypeEnum.HIERARCHYFILTER) {
			return new MondrianHierarchyFilter(this);
		} else if (stepType == DimensionStepTypeEnum.DRILLFILTER) {
			return new MondrianDrillFilter(this);
		} else if (stepType == DimensionStepTypeEnum.EXCEPTIONMEMBERFILTER) {
			return new MondrianExceptionMemberFilter(this);
		} else if (stepType == DimensionStepTypeEnum.RANKINGMEMBERFILTER) {
			return new MondrianRankingMemberFilter(this);
		} else {
			throw new UnsupportedOperationException();
		}
	}

	public DimensionStep createDimensionStepBefore(DimensionStepType stepType, DimensionStep member) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public DimensionStep createDimensionStepAfter(DimensionStepType stepType, DimensionStep member) throws OLAPException {
		throw new UnsupportedOperationException();
	}
}

// End MondrianDimensionStepManager.java