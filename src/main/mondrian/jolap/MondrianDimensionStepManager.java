/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2002-2003 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Dec 24, 2002
*/
package mondrian.jolap;

import javax.olap.OLAPException;
import javax.olap.query.enumerations.DimensionStepType;
import javax.olap.query.querycoremodel.DimensionStep;
import javax.olap.query.querycoremodel.DimensionStepManager;
import javax.olap.query.querycoremodel.DimensionView;
import java.util.List;

/**
 * Implementation of {@link DimensionStepManager}.
 *
 * @author jhyde
 * @since Dec 24, 2002
 * @version $Id$
 **/
class MondrianDimensionStepManager extends QueryObjectSupport implements DimensionStepManager {
	OrderedRelationshipList dimensionStep = new OrderedRelationshipList(Meta.dimensionStep);
	MondrianDimensionView dimensionView;

	static abstract class Meta {
		static Relationship dimensionStep = new Relationship(MondrianDimensionStepManager.class, "dimensionStep", MondrianDimensionStep.class);
	}

	public MondrianDimensionStepManager(MondrianDimensionView dimensionView) {
		super(false);
		this.dimensionView = dimensionView;
	}

	public DimensionView getDimensionView() throws OLAPException {
		return dimensionView;
	}

    public List getSegment() throws OLAPException {
		throw new UnsupportedOperationException();
	}

    public List getDimensionStep() throws OLAPException {
        return dimensionStep;
	}

    public DimensionStep createDimensionStep(DimensionStepType stepType) throws OLAPException {
		return (DimensionStep) dimensionStep.addNew(MondrianDimensionStep.create(this, stepType));
	}

	public DimensionStep createDimensionStepBefore(DimensionStepType stepType, DimensionStep member) throws OLAPException {
		return (DimensionStep) dimensionStep.addBefore(member, MondrianDimensionStep.create(this, stepType));
	}

	public DimensionStep createDimensionStepAfter(DimensionStepType stepType, DimensionStep member) throws OLAPException {
		return (DimensionStep) dimensionStep.addAfter(member, MondrianDimensionStep.create(this, stepType));
	}

    public void setDimensionView(DimensionView value) throws OLAPException {
        throw new UnsupportedOperationException();
    }
}

// End MondrianDimensionStepManager.java
