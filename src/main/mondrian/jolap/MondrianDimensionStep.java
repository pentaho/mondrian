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

import mondrian.olap.Exp;
import mondrian.olap.Util;

import javax.olap.query.querycoremodel.DimensionStep;
import javax.olap.query.querycoremodel.DimensionStepManager;
import javax.olap.query.querycoremodel.CompoundDimensionStep;
import javax.olap.query.enumerations.DimensionStepType;
import javax.olap.query.enumerations.DimensionStepTypeEnum;
import javax.olap.OLAPException;

/**
 * A <code>MondrianDimensionStep</code> is ...
 *
 * @author jhyde
 * @since Dec 24, 2002
 * @version $Id$
 **/
abstract class MondrianDimensionStep extends QueryObjectSupport implements DimensionStep {
	private MondrianDimensionStepManager manager;

	MondrianDimensionStep(MondrianDimensionStepManager manager) {
		super(true);
		this.manager = manager;
	}

	/** Converts this step into a Mondrian expression, taking <code>exp</code>
	 * as its input. **/
	abstract Exp convert(Exp exp) throws OLAPException;

	/** Factory method. **/
	static DimensionStep create(
			MondrianDimensionStepManager stepManager, DimensionStepType stepType) {
		if (stepType == DimensionStepTypeEnum.ATTRIBUTEFILTER) {
			return new MondrianAttributeFilter(stepManager);
		} else if (stepType == DimensionStepTypeEnum.ATTRIBUTESORT) {
			throw new UnsupportedOperationException();
		} else if (stepType == DimensionStepTypeEnum.DATABASEDMEMBERFILTER) {
			throw new UnsupportedOperationException();
		} else if (stepType == DimensionStepTypeEnum.DATABASEDSORT) {
			throw new UnsupportedOperationException();
		} else if (stepType == DimensionStepTypeEnum.DRILLFILTER) {
			return new MondrianDrillFilter(stepManager);
		} else if (stepType == DimensionStepTypeEnum.EXCEPTIONMEMBERFILTER) {
			return new MondrianExceptionMemberFilter(stepManager);
		} else if (stepType == DimensionStepTypeEnum.HIERARCHYFILTER) {
			return new MondrianHierarchyFilter(stepManager);
		} else if (stepType == DimensionStepTypeEnum.HIERARCHICALSORT) {
			return new MondrianHierarchyFilter(stepManager);
		} else if (stepType == DimensionStepTypeEnum.LEVELFILTER) {
			return new MondrianLevelFilter(stepManager);
		} else if (stepType == DimensionStepTypeEnum.MEMBERLISTFILTER) {
			throw new UnsupportedOperationException();
		} else if (stepType == DimensionStepTypeEnum.RANKINGMEMBERFILTER) {
			return new MondrianRankingMemberFilter(stepManager);
		} else if (stepType == DimensionStepTypeEnum.SINGLEMEMBERFILTER) {
			throw new UnsupportedOperationException();
		} else {
			throw Util.newInternal("Unknown DimensionStepType " + stepType);
		}
	}

	public DimensionStepManager getDimensionStepManager() throws OLAPException {
		return manager;
	}

	public CompoundDimensionStep getCompoundDimensionStep() throws OLAPException {
		throw new UnsupportedOperationException();
	}
}

// End MondrianDimensionStep.java