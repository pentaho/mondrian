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

import javax.olap.query.querycoremodel.DimensionFilter;
import javax.olap.query.enumerations.SetActionType;
import javax.olap.query.enumerations.DimensionInsertOffsetType;
import javax.olap.query.dimensionfilters.DimensionInsertOffset;
import javax.olap.OLAPException;

/**
 * A <code>MondrianDimensionFilter</code> is ...
 *
 * @author jhyde
 * @since Dec 24, 2002
 * @version $Id$
 **/
class MondrianDimensionFilter extends MondrianDimensionStep
		implements DimensionFilter {
	private SetActionType setAction;

	MondrianDimensionFilter(MondrianDimensionStepManager manager) {
		super(manager);
	}

	public SetActionType getSetAction() throws OLAPException {
		return setAction;
	}

	public void setSetAction(SetActionType input) throws OLAPException {
		this.setAction = input;
	}

	public void setDimensionInsertOffset(DimensionInsertOffset input) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public DimensionInsertOffset getDimensionInsertOffset() throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public DimensionInsertOffset createDimensionInsertOffset(DimensionInsertOffsetType type) throws OLAPException {
		throw new UnsupportedOperationException();
	}
}

// End MondrianDimensionFilter.java