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

import mondrian.olap.Exp;
import mondrian.olap.Util;

import javax.olap.OLAPException;
import javax.olap.query.enumerations.DimensionStepType;
import javax.olap.query.enumerations.DimensionStepTypeEnum;
import javax.olap.query.querycoremodel.CompoundDimensionStep;
import javax.olap.query.querycoremodel.DimensionStep;
import javax.olap.query.querycoremodel.DimensionStepManager;

/**
 * Implementation of {@link DimensionStep}.
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
		if (stepType == DimensionStepTypeEnum.ATTRIBUTE_FILTER) {
			return new MondrianAttributeFilter(stepManager);
		} else if (stepType == DimensionStepTypeEnum.ATTRIBUTE_SORT) {
			throw new UnsupportedOperationException();
		} else if (stepType == DimensionStepTypeEnum.COMPOUND_DIMENSION_STEP) {
			throw new UnsupportedOperationException();
		} else if (stepType == DimensionStepTypeEnum.DATA_BASED_SORT) {
			throw new UnsupportedOperationException();
		} else if (stepType == DimensionStepTypeEnum.DRILL_FILTER) {
			return new MondrianDrillFilter(stepManager);
		} else if (stepType == DimensionStepTypeEnum.EXCEPTION_MEMBER_FILTER) {
			return new MondrianExceptionMemberFilter(stepManager);
		} else if (stepType == DimensionStepTypeEnum.HIERARCHY_MEMBER_FILTER) {
			return new MondrianHierarchyFilter(stepManager);
		} else if (stepType == DimensionStepTypeEnum.HIERARCHICAL_SORT) {
			return new MondrianHierarchyFilter(stepManager);
		} else if (stepType == DimensionStepTypeEnum.LEVEL_FILTER) {
			return new MondrianLevelFilter(stepManager);
		} else if (stepType == DimensionStepTypeEnum.MEMBER_LIST_FILTER) {
			throw new UnsupportedOperationException();
		} else if (stepType == DimensionStepTypeEnum.RANKING_MEMBER_FILTER) {
			return new MondrianRankingMemberFilter(stepManager);
		} else if (stepType == DimensionStepTypeEnum.SINGLE_MEMBER_FILTER) {
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
