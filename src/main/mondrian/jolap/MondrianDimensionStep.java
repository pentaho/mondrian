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
import javax.olap.query.querycoremodel.DimensionStepManager;
import javax.olap.query.querycoremodel.CompoundDimensionStep;
import javax.olap.OLAPException;

/**
 * A <code>MondrianDimensionStep</code> is ...
 *
 * @author jhyde
 * @since Dec 24, 2002
 * @version $Id$
 **/
class MondrianDimensionStep extends QueryObjectSupport implements DimensionStep {
	private MondrianDimensionStepManager manager;

	MondrianDimensionStep(MondrianDimensionStepManager manager) {
		super(true);
		this.manager = manager;
	}

	public DimensionStepManager getDimensionStepManager() throws OLAPException {
		return manager;
	}

	public CompoundDimensionStep getCompoundDimensionStep() throws OLAPException {
		throw new UnsupportedOperationException();
	}
}

// End MondrianDimensionStep.java