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
		dimensionStep.set(input);
	}

	public List getDimensionStep() throws OLAPException {
		return dimensionStep.get();
	}

	public void removeDimensionStep(DimensionStep input) throws OLAPException {
		dimensionStep.remove(input);
	}

	public void moveDimensionStepBefore(DimensionStep before, DimensionStep input) throws OLAPException {
		dimensionStep.moveBefore(before, input);
	}

	public void moveDimensionStepAfter(DimensionStep before, DimensionStep input) throws OLAPException {
		dimensionStep.moveAfter(before, input);
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
}

// End MondrianDimensionStepManager.java